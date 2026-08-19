"""Out-of-order tick handling.

Exchange WebSocket feeds are not guaranteed to deliver messages in strict
timestamp order -- TCP preserves ordering per-connection, but Binance's
combined-stream multiplexer, client-side retries, and our own reconnect
logic can all interleave or re-deliver trades slightly out of sequence.
Feeding an aggregator ticks out of order corrupts OHLC candles (a "low"
that arrives after the candle already closed) and skews VWAP.

`Sequencer` buys a small amount of latency (`watermark_ms`, default 250ms)
to buy correctness: it holds recently-seen ticks in a min-heap keyed by
exchange event time, and only releases ticks once no arrival within the
watermark window could place something ahead of them anymore. This is the
same "allowed lateness" idea used by streaming systems like Flink, scaled
down to fit an in-process asyncio pipeline.

Gap detection is separate and cheap: Binance trade IDs are monotonically
increasing per symbol, so a jump greater than 1 means we missed one or more
trades (dropped by the exchange, a reconnect gap, or our own ring buffer
overwriting the oldest entry under load). We can't recover the missing
trade, but we count it so it's visible in the stats panel rather than
silently corrupting the aggregates.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field

from app.models import Tick


@dataclass(order=True)
class _HeapEntry:
    event_time_ms: int
    seq: int
    tick: Tick = field(compare=False)


class Sequencer:
    def __init__(self, watermark_ms: int = 250) -> None:
        self._watermark_ms = watermark_ms
        self._heap: list[_HeapEntry] = []
        self._seq_counter = 0
        self._max_event_time_seen = 0
        self._last_trade_id: dict[str, int] = {}
        self.out_of_order_count = 0
        self.gap_count = 0

    def offer(self, tick: Tick) -> None:
        """Add a tick to the reorder buffer."""
        if tick.event_time_ms < self._max_event_time_seen:
            self.out_of_order_count += 1
        else:
            self._max_event_time_seen = tick.event_time_ms

        last_id = self._last_trade_id.get(tick.symbol)
        if last_id is not None and tick.trade_id > last_id + 1:
            self.gap_count += tick.trade_id - last_id - 1
        if last_id is None or tick.trade_id > last_id:
            self._last_trade_id[tick.symbol] = tick.trade_id

        self._seq_counter += 1
        heapq.heappush(self._heap, _HeapEntry(tick.event_time_ms, self._seq_counter, tick))

    def poll_ready(self) -> list[Tick]:
        """Pop all ticks old enough that nothing still in-flight could precede them."""
        threshold = self._max_event_time_seen - self._watermark_ms
        ready: list[Tick] = []
        while self._heap and self._heap[0].event_time_ms <= threshold:
            ready.append(heapq.heappop(self._heap).tick)
        return ready

    def flush_all(self) -> list[Tick]:
        """Drain everything regardless of watermark (used on shutdown)."""
        ready = [entry.tick for entry in sorted(self._heap)]
        self._heap.clear()
        return ready

    @property
    def pending(self) -> int:
        return len(self._heap)
