import asyncio

import pytest

from app.ingest.ring_buffer import RingBuffer


def test_push_and_drain_preserves_order():
    buf: RingBuffer[int] = RingBuffer(capacity=8)
    for i in range(5):
        assert buf.push_nowait(i) is True
    assert buf.depth == 5
    assert buf.drain_nowait() == [0, 1, 2, 3, 4]
    assert buf.depth == 0


def test_overflow_drops_oldest_and_counts():
    buf: RingBuffer[int] = RingBuffer(capacity=4)
    for i in range(6):
        buf.push_nowait(i)
    # capacity is 4, so the two oldest (0, 1) were overwritten
    assert buf.dropped == 2
    assert buf.depth == 4
    assert buf.drain_nowait() == [2, 3, 4, 5]


def test_partial_drain_leaves_remainder():
    buf: RingBuffer[int] = RingBuffer(capacity=10)
    for i in range(6):
        buf.push_nowait(i)
    first = buf.drain_nowait(max_items=4)
    assert first == [0, 1, 2, 3]
    assert buf.depth == 2
    rest = buf.drain_nowait()
    assert rest == [4, 5]


def test_wraparound_after_many_cycles():
    buf: RingBuffer[int] = RingBuffer(capacity=3)
    seen = []
    for i in range(20):
        buf.push_nowait(i)
        if i % 2 == 1:
            seen.extend(buf.drain_nowait())
    seen.extend(buf.drain_nowait())
    assert seen == list(range(20))


@pytest.mark.asyncio
async def test_async_drain_waits_for_item():
    buf: RingBuffer[int] = RingBuffer(capacity=4)

    async def producer():
        await asyncio.sleep(0.02)
        await buf.push(42)

    asyncio.create_task(producer())
    result = await buf.drain(wait_timeout=1.0)
    assert result == [42]


@pytest.mark.asyncio
async def test_push_blocking_waits_for_space():
    buf: RingBuffer[int] = RingBuffer(capacity=2)
    buf.push_nowait(1)
    buf.push_nowait(2)

    async def consumer():
        await asyncio.sleep(0.02)
        await buf.drain(max_items=1)

    asyncio.create_task(consumer())
    await asyncio.wait_for(buf.push_blocking(3), timeout=1.0)
    assert buf.dropped == 0
    assert buf.depth == 2
