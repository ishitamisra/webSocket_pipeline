from app.models import Tick
from app.processing.vwap import RollingVWAP


def make_tick(price, qty, event_time_ms, symbol="BTCUSDT"):
    return Tick(symbol=symbol, price=price, quantity=qty, trade_id=1, event_time_ms=event_time_ms)


def test_vwap_matches_manual_calculation():
    vwap = RollingVWAP(window_sec=60)
    vwap.update(make_tick(100.0, 2.0, 0))
    vwap.update(make_tick(110.0, 1.0, 1000))
    # (100*2 + 110*1) / (2 + 1) = 103.333...
    assert abs(vwap.value("BTCUSDT") - 103.33333333) < 1e-6


def test_ticks_outside_window_are_evicted():
    vwap = RollingVWAP(window_sec=10)
    vwap.update(make_tick(100.0, 1.0, 0))
    vwap.update(make_tick(200.0, 1.0, 20_000))  # 20s later, outside the 10s window
    assert vwap.value("BTCUSDT") == 200.0


def test_symbols_are_independent():
    vwap = RollingVWAP(window_sec=60)
    vwap.update(make_tick(100.0, 1.0, 0, symbol="BTCUSDT"))
    vwap.update(make_tick(3000.0, 1.0, 0, symbol="ETHUSDT"))
    assert vwap.value("BTCUSDT") == 100.0
    assert vwap.value("ETHUSDT") == 3000.0


def test_empty_symbol_returns_zero():
    vwap = RollingVWAP(window_sec=60)
    assert vwap.value("BTCUSDT") == 0.0
