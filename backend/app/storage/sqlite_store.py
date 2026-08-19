"""Durable time-series persistence, batched for the same reason the ring
buffer is: writing one row per trade with its own `INSERT` + commit is
exactly the kind of per-message cost that stops keeping up once the feed
gets busy (fsync-per-row throughput on typical disks is a four-figure
number of writes/sec at best, and 10k msg/sec blows past that).

`SQLiteWriter.record_tick` / `record_candle` are synchronous, allocation-light
appends to an in-memory buffer -- safe to call directly from the pipeline's
hot path. A background task periodically flushes whatever has accumulated
using `executemany`, batching potentially thousands of rows into one
transaction. WAL mode is enabled so readers (the REST API, if it ever reads
straight from SQLite) aren't blocked by the writer.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

import aiosqlite

from app.config import settings
from app.models import Tick

logger = logging.getLogger("pipeline.storage")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks (
    symbol TEXT NOT NULL,
    price REAL NOT NULL,
    quantity REAL NOT NULL,
    trade_id INTEGER NOT NULL,
    event_time_ms INTEGER NOT NULL,
    recv_time_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ticks_symbol_time ON ticks(symbol, event_time_ms);

CREATE TABLE IF NOT EXISTS candles (
    symbol TEXT NOT NULL,
    timeframe_sec INTEGER NOT NULL,
    bucket_start_ms INTEGER NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    trade_count INTEGER NOT NULL,
    PRIMARY KEY (symbol, timeframe_sec, bucket_start_ms)
);
"""


class SQLiteWriter:
    def __init__(self, path: str = settings.sqlite_path) -> None:
        self._path = path
        self._tick_buffer: list[tuple] = []
        self._candle_buffer: list[tuple] = []
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self.rows_written = 0
        self.flush_count = 0

    async def start(self) -> None:
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        async with aiosqlite.connect(self._path) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.executescript(_SCHEMA)
            await db.commit()
        self._task = asyncio.create_task(self._flush_loop(), name="sqlite-flush-loop")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
        await self._flush()

    def record_tick(self, tick: Tick) -> None:
        self._tick_buffer.append(
            (tick.symbol, tick.price, tick.quantity, tick.trade_id, tick.event_time_ms, tick.recv_time_ms)
        )

    def record_candle(self, candle_dict: dict) -> None:
        self._candle_buffer.append(
            (
                candle_dict["symbol"],
                candle_dict["timeframe_sec"],
                candle_dict["bucket_start_ms"],
                candle_dict["open"],
                candle_dict["high"],
                candle_dict["low"],
                candle_dict["close"],
                candle_dict["volume"],
                candle_dict["trade_count"],
            )
        )

    async def _flush_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(settings.sqlite_flush_interval_sec)
            await self._flush()

    async def _flush(self) -> None:
        if not self._tick_buffer and not self._candle_buffer:
            return
        ticks, candles = self._tick_buffer, self._candle_buffer
        self._tick_buffer, self._candle_buffer = [], []

        start = time.monotonic()
        async with aiosqlite.connect(self._path) as db:
            if ticks:
                await db.executemany(
                    "INSERT INTO ticks (symbol, price, quantity, trade_id, event_time_ms, recv_time_ms) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    ticks,
                )
            if candles:
                await db.executemany(
                    "INSERT OR REPLACE INTO candles "
                    "(symbol, timeframe_sec, bucket_start_ms, open, high, low, close, volume, trade_count) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    candles,
                )
            await db.commit()
        self.rows_written += len(ticks) + len(candles)
        self.flush_count += 1
        elapsed_ms = (time.monotonic() - start) * 1000
        logger.debug(
            "flushed %d ticks + %d candles in %.1fms", len(ticks), len(candles), elapsed_ms
        )
