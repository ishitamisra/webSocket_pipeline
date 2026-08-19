"""Simple and exponential moving averages of trade price, per symbol.

SMA keeps a fixed-length deque plus a running sum so each update is O(1)
(push one price, and if the window overflowed, subtract the evicted one)
rather than re-summing the window every tick.

EMA needs no history at all -- it's a single multiply-add per update,
which is part of why it's cheap to maintain many periods in parallel even
under heavy tick volume.
"""

from __future__ import annotations

from collections import deque


class _SMA:
    def __init__(self, period: int) -> None:
        self.period = period
        self._values: deque[float] = deque(maxlen=period)
        self._sum = 0.0

    def update(self, price: float) -> float | None:
        if len(self._values) == self.period:
            self._sum -= self._values[0]
        self._values.append(price)
        self._sum += price
        if len(self._values) < self.period:
            return None
        return self._sum / self.period


class _EMA:
    def __init__(self, period: int) -> None:
        self.period = period
        self._multiplier = 2 / (period + 1)
        self._value: float | None = None

    def update(self, price: float) -> float:
        if self._value is None:
            self._value = price
        else:
            self._value = (price - self._value) * self._multiplier + self._value
        return self._value


class MovingAverageTracker:
    """Maintains SMA + EMA for a configurable set of periods, per symbol."""

    def __init__(self, periods: tuple[int, ...]) -> None:
        self._periods = periods
        self._sma: dict[str, dict[int, _SMA]] = {}
        self._ema: dict[str, dict[int, _EMA]] = {}

    def update(self, symbol: str, price: float) -> dict[str, float | None]:
        sma_map = self._sma.setdefault(symbol, {p: _SMA(p) for p in self._periods})
        ema_map = self._ema.setdefault(symbol, {p: _EMA(p) for p in self._periods})
        out: dict[str, float | None] = {}
        for p in self._periods:
            out[f"sma_{p}"] = sma_map[p].update(price)
            out[f"ema_{p}"] = ema_map[p].update(price)
        return out
