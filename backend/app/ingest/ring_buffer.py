"""A fixed-capacity ring buffer used to decouple ingestion from processing.

Why this exists
----------------
The first version of this pipeline pushed every incoming tick onto an
``asyncio.Queue`` and had the consumer ``await queue.get()`` one message at a
time, doing a bit of aggregation work per message. That works fine at low
volume. Once the synthetic load test (see ``benchmark/load_test.py``) was
pushed to ~10k msg/sec, the consumer coroutine could not drain the queue as
fast as the producer filled it: each ``get``/``put`` pair costs an event-loop
wakeup, and each `Tick` sat in a linked-list node inside the queue. The queue
grew without bound and memory + latency climbed until the process fell over.

Two changes fixed it, and this class implements both:

1. **Bounded capacity with an explicit overflow policy.** A queue that can
   grow forever just hides backpressure until you run out of memory. This
   buffer has a fixed-size, pre-allocated slot array. When it's full the
   producer does not block the event loop -- it overwrites the oldest
   unconsumed slot and increments a `dropped` counter. This is the right
   choice for a live market-data feed: a slightly stale VWAP over the last
   few dropped ticks is fine, but stalling the WebSocket read loop (and
   risking the exchange killing the connection for being slow) is worse.
   ``push_blocking`` is also provided for callers that would rather apply
   true backpressure than lose data.
2. **Batch draining.** ``drain(max_items)`` hands the consumer a list of
   everything currently available in one call instead of forcing a
   coroutine suspension per item. The processing pipeline then updates
   candles/VWAP/moving averages over the whole batch before yielding back
   to the event loop, which amortizes the per-item overhead that was
   killing the naive version.

This is intentionally single-producer/single-consumer per symbol stream
(one ring buffer per symbol). That matches how the ingestion layer is wired
up -- one asyncio task reads the exchange WebSocket and pushes, one task
drains -- so index bookkeeping doesn't need a lock; a `Lock` is still used
around the small critical section for the rare case of multiple producers
(e.g. a REST backfill task also injecting ticks) sharing one buffer.
"""

from __future__ import annotations

import asyncio
from typing import Generic, TypeVar

T = TypeVar("T")


class RingBuffer(Generic[T]):
    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._slots: list[T | None] = [None] * capacity
        self._head = 0  # next index to write
        self._size = 0  # number of unconsumed items
        self._dropped = 0
        self._pushed_total = 0
        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Condition(self._lock)

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def depth(self) -> int:
        return self._size

    @property
    def dropped(self) -> int:
        return self._dropped

    @property
    def pushed_total(self) -> int:
        return self._pushed_total

    def push_nowait(self, item: T) -> bool:
        """Non-blocking push. Returns False (and drops the oldest item) if full."""
        overwritten = False
        if self._size == self._capacity:
            # Buffer is full: drop the oldest entry to make room. The oldest
            # entry lives at (head - size) mod capacity, which after this
            # write becomes (head - size + 1) mod capacity, i.e. we simply
            # advance the logical "tail" by writing over `head` and keeping
            # size pinned at capacity.
            self._dropped += 1
            overwritten = True
        else:
            self._size += 1
        self._slots[self._head] = item
        self._head = (self._head + 1) % self._capacity
        self._pushed_total += 1
        return not overwritten

    async def push(self, item: T) -> bool:
        """Async push that also wakes any waiting drainer."""
        async with self._lock:
            ok = self.push_nowait(item)
            self._not_empty.notify_all()
            return ok

    async def push_blocking(self, item: T, timeout: float | None = None) -> None:
        """True backpressure variant: waits for space instead of dropping."""
        async with self._not_empty:
            while self._size == self._capacity:
                await asyncio.wait_for(self._not_empty.wait(), timeout=timeout)
            self.push_nowait(item)
            self._not_empty.notify_all()

    def drain_nowait(self, max_items: int | None = None) -> list[T]:
        """Synchronously drain up to `max_items` (or everything) without waiting."""
        n = self._size if max_items is None else min(max_items, self._size)
        if n == 0:
            return []
        tail = (self._head - self._size) % self._capacity
        out: list[T] = []
        for i in range(n):
            idx = (tail + i) % self._capacity
            out.append(self._slots[idx])  # type: ignore[arg-type]
            self._slots[idx] = None
        self._size -= n
        return out

    async def drain(self, max_items: int | None = None, wait_timeout: float | None = None) -> list[T]:
        """Drain what's available; if empty, wait up to `wait_timeout` for at least one item."""
        async with self._lock:
            if self._size == 0 and wait_timeout is not None:
                try:
                    await asyncio.wait_for(self._not_empty.wait(), timeout=wait_timeout)
                except asyncio.TimeoutError:
                    pass
            items = self.drain_nowait(max_items)
            if items:
                # Wake any producer blocked in push_blocking() waiting for space.
                self._not_empty.notify_all()
            return items

    def stats(self) -> dict:
        return {
            "capacity": self._capacity,
            "depth": self._size,
            "dropped": self._dropped,
            "pushed_total": self._pushed_total,
        }
