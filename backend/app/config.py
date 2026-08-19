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
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


@dataclass(frozen=True)
class Settings:
    # Exchange feed. Product IDs are Coinbase Exchange's format (BASE-QUOTE).
    #
    # This started out pointed at Binance; that didn't pan out in practice --
    # Binance.com rejects US IPs outright (HTTP 451), and Binance.US, while
    # reachable, produced close to zero real-time trade flow on its
    # combined-stream endpoint when tested (most market makers left after the
    # 2023 SEC action against Binance.US). Coinbase Exchange's public
    # `matches` channel needs no auth and reliably produces multiple trades
    # per second on these pairs.
    symbols: list[str] = field(default_factory=lambda: _env_list(
        "PIPELINE_SYMBOLS", ["BTC-USD", "ETH-USD", "SOL-USD"]
    ))
    coinbase_ws_url: str = os.environ.get(
        "COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com"
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
