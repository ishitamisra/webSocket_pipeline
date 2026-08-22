"""Wires ingestion, backpressure, reordering, and aggregation together.

Per symbol, the flow is:

    CoinbaseTradeClient (recv loop)
            |  tick (sync callback, no await -> never blocks the socket read)
            v
      RingBuffer.push_nowait          <- bounded, drops oldest under overload
            |
            v
   consumer task: drain a batch  <- batching is the second half of the fix;
            |                        one wakeup processes up to batch_max_size
            v                        ticks instead of one wakeup per tick
      Sequencer.offer / poll_ready  <- reorders by event time within a
            |                          watermark before admitting downstream
            v
   CandleAggregator x N timeframes
   RollingVWAP
   MovingAverageTracker
            |
            v
   events list -> storage sink + websocket broadcaster

Everything downstream of the ring buffer runs inside the consumer task, so
even a slow persistence write or a slow broadcast fan-out only delays that
symbol's own consumer -- it can never block the WebSocket read loop, which
is what keeps the pipeline able to absorb bursts instead of falling behind
and getting disconnected by the exchange for being a slow reader.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Awaitable, Callable, Protocol

from app.config import settings
from app.ingest.ring_buffer import RingBuffer
from app.ingest.sequencer import Sequencer
from app.models import SymbolStats, Tick
from app.processing.candles import CandleAggregator
from app.processing.moving_average import MovingAverageTracker
from app.processing.vwap import RollingVWAP

logger = logging.getLogger("pipeline.core")

EventSink = Callable[[list[dict]], Awaitable[None]]


class PersistenceSink(Protocol):
    def record_tick(self, tick: Tick) -> None: ...
    def record_candle(self, candle_dict: dict) -> None: ...


class Pipeline:
    def __init__(
        self,
        symbols: list[str],
        on_events: EventSink | None = None,
        persistence: PersistenceSink | None = None,
    ) -> None:
        self.symbols = [s.upper() for s in symbols]
        self._on_events = on_events
        self._persistence = persistence

        self.ring_buffers: dict[str, RingBuffer[Tick]] = {
            s: RingBuffer(settings.ring_buffer_capacity) for s in self.symbols
        }
        self.sequencers: dict[str, Sequencer] = {
            s: Sequencer(settings.reorder_watermark_ms) for s in self.symbols
        }
        self.candle_aggs: dict[int, CandleAggregator] = {
            tf: CandleAggregator(tf, settings.candle_history_len)
            for tf in settings.candle_timeframes_sec
        }
        self.vwap = RollingVWAP(settings.vwap_window_sec)
        self.ma_tracker = MovingAverageTracker(settings.moving_average_periods)

        self.stats: dict[str, SymbolStats] = {s: SymbolStats(symbol=s) for s in self.symbols}
        self._msg_counts_window: dict[str, int] = {s: 0 for s in self.symbols}

        self._tasks: list[asyncio.Task] = []
        self._stop = asyncio.Event()

    # -- ingestion entrypoint (sync, called from the WebSocket recv callback) --
    def ingest(self, tick: Tick) -> None:
        buf = self.ring_buffers.get(tick.symbol)
        if buf is None:
            return
        buf.push_nowait(tick)

    def ingest_many(self, ticks: list[Tick]) -> None:
        for t in ticks:
            self.ingest(t)

    async def start(self) -> None:
        for symbol in self.symbols:
            self._tasks.append(asyncio.create_task(self._consume_symbol(symbol), name=f"consumer-{symbol}"))
        self._tasks.append(asyncio.create_task(self._rate_loop(), name="rate-loop"))

    async def stop(self) -> None:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _consume_symbol(self, symbol: str) -> None:
        buf = self.ring_buffers[symbol]
        seq = self.sequencers[symbol]
        wait_s = settings.batch_max_wait_ms / 1000
        while not self._stop.is_set():
            batch = await buf.drain(max_items=settings.batch_max_size, wait_timeout=wait_s)
            if not batch:
                continue
            for tick in batch:
                seq.offer(tick)
            ready = seq.poll_ready()
            if not ready:
                continue
            events = self._process_batch(symbol, ready)
            if events and self._on_events is not None:
                await self._on_events(events)

    def _process_batch(self, symbol: str, ticks: list[Tick]) -> list[dict]:
        events: list[dict] = []
        stat = self.stats[symbol]
        for tick in ticks:
            self._msg_counts_window[symbol] += 1
            stat.msgs_total += 1
            stat.last_price = tick.price

            if self._persistence is not None:
                self._persistence.record_tick(tick)

            for tf, agg in self.candle_aggs.items():
                closed = agg.apply(tick)
                if closed is not None:
                    cdict = closed.to_dict()
                    events.append({"type": "candle_closed", "data": cdict})
                    if self._persistence is not None:
                        self._persistence.record_candle(cdict)
                patched = agg.take_patch()
                if patched is not None:
                    pdict = patched.to_dict()
                    events.append({"type": "candle_patched", "data": pdict})
                    if self._persistence is not None:
                        # Same primary key as the original write (symbol,
                        # timeframe_sec, bucket_start_ms) -> SQLite's
                        # INSERT OR REPLACE overwrites the stale row instead
                        # of erroring or duplicating it.
                        self._persistence.record_candle(pdict)
                current = agg.current(symbol)
                if current is not None:
                    events.append({"type": "candle_update", "data": current.to_dict()})

            vwap_val = self.vwap.update(tick)
            stat.vwap = vwap_val
            ma_vals = self.ma_tracker.update(symbol, tick.price)
            stat.moving_averages = ma_vals

            events.append(
                {
                    "type": "tick",
                    "data": {
                        "symbol": symbol,
                        "price": tick.price,
                        "quantity": tick.quantity,
                        "event_time_ms": tick.event_time_ms,
                        "side": "sell" if tick.is_buyer_maker else "buy",
                        "vwap": vwap_val,
                        "moving_averages": ma_vals,
                    },
                }
            )

        buf = self.ring_buffers[symbol]
        stat.dropped_ring_buffer = buf.dropped
        stat.out_of_order_count = self.sequencers[symbol].out_of_order_count
        stat.gap_count = self.sequencers[symbol].gap_count
        stat.buffer_depth = buf.depth
        stat.buffer_capacity = buf.capacity
        return events

    async def _rate_loop(self) -> None:
        last = time.monotonic()
        while not self._stop.is_set():
            await asyncio.sleep(1.0)
            now = time.monotonic()
            elapsed = now - last
            last = now
            for symbol in self.symbols:
                count = self._msg_counts_window[symbol]
                self._msg_counts_window[symbol] = 0
                self.stats[symbol].msgs_per_sec = count / elapsed if elapsed > 0 else 0.0

    def snapshot_stats(self) -> list[dict]:
        return [s.to_dict() for s in self.stats.values()]
