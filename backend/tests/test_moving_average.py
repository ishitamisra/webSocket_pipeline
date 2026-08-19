from app.processing.moving_average import MovingAverageTracker


def test_sma_is_none_until_period_is_filled():
    tracker = MovingAverageTracker(periods=(3,))
    r1 = tracker.update("BTCUSDT", 10.0)
    r2 = tracker.update("BTCUSDT", 20.0)
    assert r1["sma_3"] is None
    assert r2["sma_3"] is None
    r3 = tracker.update("BTCUSDT", 30.0)
    assert r3["sma_3"] == 20.0


def test_sma_slides_window():
    tracker = MovingAverageTracker(periods=(2,))
    tracker.update("BTCUSDT", 10.0)
    tracker.update("BTCUSDT", 20.0)
    r = tracker.update("BTCUSDT", 30.0)
    assert r["sma_2"] == 25.0  # (20 + 30) / 2, 10 fell out of the window


def test_ema_seeds_with_first_price_then_reacts():
    tracker = MovingAverageTracker(periods=(2,))
    r1 = tracker.update("BTCUSDT", 10.0)
    assert r1["ema_2"] == 10.0
    r2 = tracker.update("BTCUSDT", 20.0)
    # multiplier = 2/(2+1) = 0.6667 -> (20-10)*0.6667+10 = 16.667
    assert abs(r2["ema_2"] - 16.6667) < 1e-3


def test_symbols_track_independently():
    tracker = MovingAverageTracker(periods=(2,))
    tracker.update("BTCUSDT", 100.0)
    tracker.update("ETHUSDT", 5.0)
    r_btc = tracker.update("BTCUSDT", 200.0)
    r_eth = tracker.update("ETHUSDT", 7.0)
    assert r_btc["sma_2"] == 150.0
    assert r_eth["sma_2"] == 6.0
