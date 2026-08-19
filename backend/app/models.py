"""Shared data types for the pipeline.

`Tick` uses `__slots__` (not a stdlib dataclass) because it is the hottest
object in the system -- one gets allocated per trade message, so at 10k
msg/sec we allocate 10k of these a second. Slots avoid the per-instance
`__dict__` and shave meaningful overhead under sustained load.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


class Tick:
    __slots__ = (
        "symbol",
        "price",
        "quantity",
        "trade_id",
        "event_time_ms",
        "recv_time_ms",
        "is_buyer_maker",
    )

    def __init__(
        self,
        symbol: str,
        price: float,
        quantity: float,
        trade_id: int,
        event_time_ms: int,
        is_buyer_maker: bool = False,
        recv_time_ms: int | None = None,
    ) -> None:
        self.symbol = symbol
        self.price = price
        self.quantity = quantity
        self.trade_id = trade_id
        self.event_time_ms = event_time_ms
        self.is_buyer_maker = is_buyer_maker
        self.recv_time_ms = recv_time_ms if recv_time_ms is not None else int(time.time() * 1000)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"Tick(symbol={self.symbol!r}, price={self.price}, qty={self.quantity}, "
            f"trade_id={self.trade_id}, event_time_ms={self.event_time_ms})"
        )


@dataclass
class Candle:
    symbol: str
    timeframe_sec: int
    bucket_start_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    trade_count: int = 0
    closed: bool = False

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "timeframe_sec": self.timeframe_sec,
            "bucket_start_ms": self.bucket_start_ms,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "trade_count": self.trade_count,
            "closed": self.closed,
        }


@dataclass
class SymbolStats:
    symbol: str
    last_price: float = 0.0
    vwap: float = 0.0
    moving_averages: dict = field(default_factory=dict)
    msgs_total: int = 0
    msgs_per_sec: float = 0.0
    dropped_ring_buffer: int = 0
    out_of_order_count: int = 0
    gap_count: int = 0
    buffer_depth: int = 0
    buffer_capacity: int = 0

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "last_price": self.last_price,
            "vwap": self.vwap,
            "moving_averages": self.moving_averages,
            "msgs_total": self.msgs_total,
            "msgs_per_sec": round(self.msgs_per_sec, 2),
            "dropped_ring_buffer": self.dropped_ring_buffer,
            "out_of_order_count": self.out_of_order_count,
            "gap_count": self.gap_count,
            "buffer_depth": self.buffer_depth,
            "buffer_capacity": self.buffer_capacity,
        }
