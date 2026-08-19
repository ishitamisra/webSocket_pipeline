"""Read-side facade over the pipeline's time-series data.

Two tiers, picked automatically by `get_candles`:

- **Hot path**: the last `candle_history_len` candles per (symbol,
  timeframe) live in memory inside each `CandleAggregator` -- no I/O, safe
  to hit on every WebSocket broadcast or dashboard refresh.
- **Cold path**: anything older has already been flushed to SQLite by
  `SQLiteWriter` and is queried on demand. This only runs for requests
  that ask for more history than the in-memory ring retains, so it never
  sits on the hot ingestion path.
"""

from __future__ import annotations

import aiosqlite

from app.config import settings
from app.processing.pipeline import Pipeline


class TimeSeriesStore:
    def __init__(self, pipeline: Pipeline, sqlite_path: str = settings.sqlite_path) -> None:
        self._pipeline = pipeline
        self._sqlite_path = sqlite_path

    def get_candles(self, symbol: str, timeframe_sec: int, limit: int = 200) -> list[dict]:
        symbol = symbol.upper()
        agg = self._pipeline.candle_aggs.get(timeframe_sec)
        if agg is None:
            return []
        history = agg.history(symbol)
        current = agg.current(symbol)
        candles = list(history)
        if current is not None:
            candles.append(current)
        return [c.to_dict() for c in candles[-limit:]]

    async def get_candles_from_db(self, symbol: str, timeframe_sec: int, limit: int = 500) -> list[dict]:
        async with aiosqlite.connect(self._sqlite_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT symbol, timeframe_sec, bucket_start_ms, open, high, low, close, volume, trade_count "
                "FROM candles WHERE symbol = ? AND timeframe_sec = ? "
                "ORDER BY bucket_start_ms DESC LIMIT ?",
                (symbol.upper(), timeframe_sec, limit),
            )
            rows = await cursor.fetchall()
        return [dict(row) for row in reversed(rows)]

    def get_vwap(self, symbol: str) -> float:
        stat = self._pipeline.stats.get(symbol.upper())
        return stat.vwap if stat else 0.0

    def get_moving_averages(self, symbol: str) -> dict:
        stat = self._pipeline.stats.get(symbol.upper())
        return stat.moving_averages if stat else {}

    def get_stats(self) -> list[dict]:
        return self._pipeline.snapshot_stats()
