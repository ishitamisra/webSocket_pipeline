"""OHLC candle aggregation.

Maintains, per (symbol, timeframe), the currently-open candle plus a
bounded history of closed candles. Candles are bucketed by floor-dividing
the tick's exchange event time by the timeframe -- not wall-clock receive
time -- so a burst of late-arriving ticks (already sorted by `Sequencer`)
still lands in the bucket the exchange says it belongs to.

A tick that arrives for a bucket *older* than the current open bucket can
still happen even after the sequencer's watermark (e.g. a tick delayed
longer than the watermark, or arriving during startup catch-up). Rather
than silently drop it or corrupt the now-closed candle, we patch the
already-closed candle in history if one exists for that bucket, and count
it. This keeps the "what actually traded in that second" answer honest at
the cost of a candle occasionally being revised slightly after it closed
-- which is disclosed to consumers via the `closed` flag never lying about
*whether* revision could still happen after emit.
"""

from __future__ import annotations

from collections import deque

from app.models import Candle, Tick


class CandleAggregator:
    def __init__(self, timeframe_sec: int, history_len: int = 500) -> None:
        self.timeframe_sec = timeframe_sec
        self._bucket_ms = timeframe_sec * 1000
        self._history_len = history_len
        self._open: dict[str, Candle] = {}
        self._history: dict[str, deque[Candle]] = {}
        self.late_patches = 0
        self._last_patch: Candle | None = None

    def _bucket_start(self, event_time_ms: int) -> int:
        return (event_time_ms // self._bucket_ms) * self._bucket_ms

    def history(self, symbol: str) -> list[Candle]:
        return list(self._history.get(symbol, ()))

    def current(self, symbol: str) -> Candle | None:
        return self._open.get(symbol)

    def take_patch(self) -> Candle | None:
        """Consume (and clear) the candle revised by the most recent apply()
        call, if that call patched an already-closed candle rather than
        opening/merging/closing one. Callers that persist closed candles
        need this too -- without it, a late patch corrects the in-memory
        candle but the previously-written copy (SQLite, a cache, ...) is
        left silently stale forever."""
        patch = self._last_patch
        self._last_patch = None
        return patch

    def apply(self, tick: Tick) -> Candle | None:
        """Apply one tick, returning the candle that just closed, if any.

        A tick can instead patch an already-closed candle (see the last
        branch below) -- that case returns None here too, but the revised
        candle is available via take_patch() right after this call.
        """
        self._last_patch = None
        bucket_start = self._bucket_start(tick.event_time_ms)
        open_candle = self._open.get(tick.symbol)
        closed_candle: Candle | None = None

        if open_candle is None:
            self._open[tick.symbol] = self._new_candle(tick, bucket_start)
            return None

        if bucket_start == open_candle.bucket_start_ms:
            self._merge(open_candle, tick)
            return None

        if bucket_start > open_candle.bucket_start_ms:
            open_candle.closed = True
            self._push_history(tick.symbol, open_candle)
            closed_candle = open_candle
            self._open[tick.symbol] = self._new_candle(tick, bucket_start)
            return closed_candle

        # Late tick for a bucket that already closed: patch history if present.
        self.late_patches += 1
        hist = self._history.get(tick.symbol)
        if hist:
            for candle in hist:
                if candle.bucket_start_ms == bucket_start:
                    self._merge(candle, tick)
                    self._last_patch = candle
                    break
        return None

    def _new_candle(self, tick: Tick, bucket_start: int) -> Candle:
        return Candle(
            symbol=tick.symbol,
            timeframe_sec=self.timeframe_sec,
            bucket_start_ms=bucket_start,
            open=tick.price,
            high=tick.price,
            low=tick.price,
            close=tick.price,
            volume=tick.quantity,
            trade_count=1,
        )

    @staticmethod
    def _merge(candle: Candle, tick: Tick) -> None:
        candle.high = max(candle.high, tick.price)
        candle.low = min(candle.low, tick.price)
        candle.close = tick.price
        candle.volume += tick.quantity
        candle.trade_count += 1

    def _push_history(self, symbol: str, candle: Candle) -> None:
        if symbol not in self._history:
            self._history[symbol] = deque(maxlen=self._history_len)
        self._history[symbol].append(candle)
