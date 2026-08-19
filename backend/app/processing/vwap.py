"""Rolling volume-weighted average price over a sliding time window.

VWAP = sum(price_i * qty_i) / sum(qty_i) over the window. Recomputing that
from scratch on every tick is O(window size) per tick, which is exactly
the kind of per-message cost that falls behind at high throughput. Instead
this keeps a deque of the ticks currently inside the window plus running
sums, so both admitting a new tick and evicting expired ones are O(1)
amortized: each tick is pushed once and popped once over its lifetime.
"""

from __future__ import annotations

from collections import deque

from app.models import Tick


class RollingVWAP:
    def __init__(self, window_sec: int) -> None:
        self._window_ms = window_sec * 1000
        self._ticks: dict[str, deque[Tick]] = {}
        self._sum_pq: dict[str, float] = {}
        self._sum_q: dict[str, float] = {}

    def update(self, tick: Tick) -> float:
        """Admit a tick and return the current VWAP for its symbol."""
        symbol = tick.symbol
        dq = self._ticks.setdefault(symbol, deque())
        self._sum_pq[symbol] = self._sum_pq.get(symbol, 0.0) + tick.price * tick.quantity
        self._sum_q[symbol] = self._sum_q.get(symbol, 0.0) + tick.quantity
        dq.append(tick)
        self._evict_expired(symbol, tick.event_time_ms)
        return self.value(symbol)

    def _evict_expired(self, symbol: str, now_ms: int) -> None:
        dq = self._ticks[symbol]
        threshold = now_ms - self._window_ms
        while dq and dq[0].event_time_ms < threshold:
            old = dq.popleft()
            self._sum_pq[symbol] -= old.price * old.quantity
            self._sum_q[symbol] -= old.quantity
        # Guard against float drift on long-running streams.
        if not dq:
            self._sum_pq[symbol] = 0.0
            self._sum_q[symbol] = 0.0

    def value(self, symbol: str) -> float:
        q = self._sum_q.get(symbol, 0.0)
        if q <= 0:
            return 0.0
        return self._sum_pq[symbol] / q
