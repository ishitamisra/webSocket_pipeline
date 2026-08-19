from app.models import Tick
from app.processing.candles import CandleAggregator


def make_tick(price, qty, event_time_ms, trade_id=1, symbol="BTCUSDT"):
    return Tick(symbol=symbol, price=price, quantity=qty, trade_id=trade_id, event_time_ms=event_time_ms)


def test_first_tick_opens_a_candle_with_no_close():
    agg = CandleAggregator(timeframe_sec=1)
    closed = agg.apply(make_tick(100.0, 1.0, 1_000))
    assert closed is None
    current = agg.current("BTCUSDT")
    assert current.open == current.high == current.low == current.close == 100.0
    assert current.volume == 1.0


def test_ticks_within_same_bucket_merge_ohlc():
    agg = CandleAggregator(timeframe_sec=1)
    agg.apply(make_tick(100.0, 1.0, 1_000))
    agg.apply(make_tick(105.0, 2.0, 1_200))
    agg.apply(make_tick(95.0, 1.0, 1_800))
    c = agg.current("BTCUSDT")
    assert c.open == 100.0
    assert c.high == 105.0
    assert c.low == 95.0
    assert c.close == 95.0
    assert c.volume == 4.0
    assert c.trade_count == 3


def test_tick_in_new_bucket_closes_previous_candle():
    agg = CandleAggregator(timeframe_sec=1)
    agg.apply(make_tick(100.0, 1.0, 1_000))
    closed = agg.apply(make_tick(110.0, 1.0, 2_500))
    assert closed is not None
    assert closed.close == 100.0
    assert closed.closed is True
    new_current = agg.current("BTCUSDT")
    assert new_current.bucket_start_ms == 2_000
    assert new_current.open == 110.0


def test_late_tick_patches_already_closed_candle_history():
    agg = CandleAggregator(timeframe_sec=1, history_len=10)
    agg.apply(make_tick(100.0, 1.0, 1_000))
    agg.apply(make_tick(110.0, 1.0, 2_500))  # closes bucket 1000
    # Late-arriving tick for the already-closed first bucket
    result = agg.apply(make_tick(120.0, 5.0, 1_900))
    assert result is None
    history = agg.history("BTCUSDT")
    assert len(history) == 1
    patched = history[0]
    assert patched.high == 120.0
    assert patched.volume == 6.0
    assert agg.late_patches == 1


def test_history_length_is_bounded():
    agg = CandleAggregator(timeframe_sec=1, history_len=3)
    for i in range(10):
        agg.apply(make_tick(float(i), 1.0, i * 1000))
    assert len(agg.history("BTCUSDT")) <= 3
