from app.ingest.sequencer import Sequencer
from app.models import Tick


def make_tick(event_time_ms, trade_id, symbol="BTCUSDT", price=100.0):
    return Tick(symbol=symbol, price=price, quantity=1.0, trade_id=trade_id, event_time_ms=event_time_ms)


def test_in_order_ticks_pass_straight_through():
    seq = Sequencer(watermark_ms=100)
    for t in range(5):
        seq.offer(make_tick(event_time_ms=t * 1000, trade_id=t))
    # nothing is "ready" until we've seen something past the watermark
    ready = seq.poll_ready()
    assert [tick.trade_id for tick in ready] == [0, 1, 2, 3]


def test_out_of_order_tick_is_resequenced():
    seq = Sequencer(watermark_ms=500)
    seq.offer(make_tick(event_time_ms=1000, trade_id=1))
    seq.offer(make_tick(event_time_ms=3000, trade_id=3))
    # a late tick with an earlier timestamp arrives after tick 3
    seq.offer(make_tick(event_time_ms=2000, trade_id=2))
    assert seq.out_of_order_count == 1

    # watermark is anchored to the max event time seen (3000); everything
    # at or before 3000 - 500 = 2500 is safe to release, in timestamp
    # order despite tick 2 arriving after tick 3
    ready = seq.poll_ready()
    assert [t.trade_id for t in ready] == [1, 2]

    seq.offer(make_tick(event_time_ms=3600, trade_id=4))
    ready = seq.poll_ready()
    # now threshold is 3600 - 500 = 3100, so tick 3 is finally safe to emit
    assert [t.trade_id for t in ready] == [3]


def test_gap_detection_counts_missing_trade_ids():
    seq = Sequencer(watermark_ms=100)
    seq.offer(make_tick(event_time_ms=0, trade_id=100))
    seq.offer(make_tick(event_time_ms=1000, trade_id=101))
    seq.offer(make_tick(event_time_ms=2000, trade_id=105))  # trades 102-104 missing
    assert seq.gap_count == 3


def test_flush_all_drains_regardless_of_watermark():
    seq = Sequencer(watermark_ms=10_000)
    seq.offer(make_tick(event_time_ms=0, trade_id=1))
    seq.offer(make_tick(event_time_ms=100, trade_id=2))
    assert seq.poll_ready() == []
    flushed = seq.flush_all()
    assert [t.trade_id for t in flushed] == [1, 2]
    assert seq.pending == 0
