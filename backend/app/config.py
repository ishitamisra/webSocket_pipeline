"""Central configuration for the pipeline.

All tunables live here so the scale characteristics (buffer size, batch
window, watermark delay) can be adjusted without hunting through the
codebase.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env_list(name: str, default: list[str]) -> list[str]:
    raw = os.environ.get(name)
    if not raw:
        return default
    return [s.strip().lower() for s in raw.split(",") if s.strip()]


@dataclass(frozen=True)
class Settings:
    # Exchange feed
    symbols: list[str] = field(default_factory=lambda: _env_list(
        "PIPELINE_SYMBOLS", ["btcusdt", "ethusdt", "solusdt"]
    ))
    # Binance.com's WebSocket rejects connections from US IPs with HTTP 451
    # (it isn't licensed to serve US residents). Binance.US exposes the same
    # combined-stream API and trade message schema, so it's the default here
    # to work out of the box for US-based runs. If you're outside the US and
    # want Binance.com's deeper liquidity/more symbols, override with
    # BINANCE_WS_URL=wss://stream.binance.com:9443/stream
    binance_ws_url: str = os.environ.get(
        "BINANCE_WS_URL", "wss://stream.binance.us:9443/stream"
    )

    # Ring buffer (single-producer/single-consumer per symbol stream)
    ring_buffer_capacity: int = int(os.environ.get("RING_BUFFER_CAPACITY", 16384))

    # Consumer batching: drain up to `batch_max_size` items, or whatever has
    # accumulated after `batch_max_wait_ms`, whichever comes first.
    batch_max_size: int = int(os.environ.get("BATCH_MAX_SIZE", 256))
    batch_max_wait_ms: int = int(os.environ.get("BATCH_MAX_WAIT_MS", 50))

    # Out-of-order handling: hold ticks in a small reorder window before
    # they're admitted to the aggregation pipeline in timestamp order.
    reorder_watermark_ms: int = int(os.environ.get("REORDER_WATERMARK_MS", 250))

    # Candle timeframes to maintain, in seconds.
    candle_timeframes_sec: tuple[int, ...] = (1, 5, 60)

    # Rolling window sizes
    vwap_window_sec: int = int(os.environ.get("VWAP_WINDOW_SEC", 60))
    moving_average_periods: tuple[int, ...] = (20, 50)

    # In-memory history retained per (symbol, timeframe) for REST/UI queries
    candle_history_len: int = int(os.environ.get("CANDLE_HISTORY_LEN", 500))

    # Persistence
    sqlite_path: str = os.environ.get("SQLITE_PATH", "data/pipeline.db")
    sqlite_batch_size: int = int(os.environ.get("SQLITE_BATCH_SIZE", 500))
    sqlite_flush_interval_sec: float = float(os.environ.get("SQLITE_FLUSH_INTERVAL_SEC", 1.0))

    # Reconnect behaviour
    ws_reconnect_initial_delay_sec: float = 1.0
    ws_reconnect_max_delay_sec: float = 30.0


settings = Settings()
