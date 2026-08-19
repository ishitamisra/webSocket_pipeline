"""Synthetic load test: naive per-message pipeline vs. ring-buffer + batching.

This is the benchmark behind the scale claim in the README: "at 10k
messages/sec the naive version fell behind, so batching + a ring buffer
fixed it." It doesn't depend on the live Binance feed (which rarely
sustains 10k msg/sec for a handful of symbols) -- it generates ticks
in-process at a controlled rate so the comparison is reproducible.

Two consumer implementations are benchmarked against the *same* pre-generated
tick stream and the *same* aggregation logic (CandleAggregator, RollingVWAP,
MovingAverageTracker from app.processing):

  naive    - an unbounded asyncio.Queue; the consumer awaits one tick at a
             time and, to mirror what a first pass at "persist what we see"
             usually looks like, issues one SQLite INSERT + COMMIT per tick.
  batched  - a bounded RingBuffer; the consumer drains up to `batch_max_size`
             ticks (or whatever accumulated within `batch_max_wait_ms`) per
             wakeup, processes the whole batch, and persists it with one
             `executemany` + COMMIT per batch.

Run it:

    python -m benchmark.load_test --n 8000 --rate 10000

Expect the naive run to badly miss the target rate (SQLite's per-commit
fsync caps it well under 10k/sec on typical disks) while the batched run
keeps up with headroom to spare. Exact numbers depend on the machine you
run this on -- that's the point: run it on yours and put your own numbers
in the README.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import statistics
import sys
import time
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aiosqlite

from app.ingest.ring_buffer import RingBuffer
from app.models import Tick
from app.processing.candles import CandleAggregator
from app.processing.moving_average import MovingAverageTracker
from app.processing.vwap import RollingVWAP

SYMBOL = "BTCUSDT"


def make_synthetic_ticks(n: int, symbol: str = SYMBOL, start_price: float = 65_000.0) -> list[Tick]:
    rng = random.Random(42)
    price = start_price
    t0 = int(time.time() * 1000)
    ticks = []
    for i in range(n):
        price = max(1.0, price + rng.uniform(-3.0, 3.0))
        qty = round(rng.uniform(0.0001, 0.5), 6)
        ticks.append(
            Tick(
                symbol=symbol,
                price=round(price, 2),
                quantity=qty,
                trade_id=i + 1,
                event_time_ms=t0 + i,
            )
        )
    return ticks


def new_aggregators():
    candle_aggs = {tf: CandleAggregator(tf) for tf in (1, 5, 60)}
    vwap = RollingVWAP(window_sec=60)
    ma = MovingAverageTracker(periods=(20, 50))
    return candle_aggs, vwap, ma


def apply_tick(candle_aggs, vwap, ma, tick: Tick) -> None:
    for agg in candle_aggs.values():
        agg.apply(tick)
    vwap.update(tick)
    ma.update(tick.symbol, tick.price)


@dataclass
class RunResult:
    name: str
    n: int
    target_rate: float
    elapsed_sec: float
    throughput_per_sec: float
    peak_backlog: int
    dropped: int
    latency_ms: dict = field(default_factory=dict)

    def print_report(self) -> None:
        print(f"\n--- {self.name} ---")
        print(f"  ticks processed        : {self.n}")
        print(f"  target ingest rate     : {self.target_rate:.0f} msg/sec")
        print(f"  wall time to fully drain: {self.elapsed_sec:.2f} s")
        print(f"  sustained throughput    : {self.throughput_per_sec:.1f} msg/sec")
        print(f"  peak queue/buffer depth : {self.peak_backlog}")
        print(f"  dropped (buffer overflow): {self.dropped}")
        if self.latency_ms:
            lat = self.latency_ms
            print(
                f"  ingest->processed latency: p50={lat['p50']:.1f}ms  "
                f"p99={lat['p99']:.1f}ms  max={lat['max']:.1f}ms"
            )


async def _pace_producer(ticks: list[Tick], rate_per_sec: float, put_fn) -> None:
    interval = 1.0 / rate_per_sec
    next_t = time.perf_counter()
    for tick in ticks:
        await put_fn(tick)
        next_t += interval
        delay = next_t - time.perf_counter()
        if delay > 0:
            await asyncio.sleep(delay)


async def run_naive(ticks: list[Tick], rate: float, db_path: str) -> RunResult:
    """Unbounded queue, one tick processed and persisted at a time."""
    if os.path.exists(db_path):
        os.remove(db_path)
    db = await aiosqlite.connect(db_path)
    await db.execute(
        "CREATE TABLE ticks (symbol TEXT, price REAL, qty REAL, trade_id INTEGER, event_time_ms INTEGER)"
    )
    await db.commit()

    queue: asyncio.Queue[tuple[Tick, float]] = asyncio.Queue()
    candle_aggs, vwap, ma = new_aggregators()
    latencies: list[float] = []
    depths: list[int] = []
    processed = 0
    n = len(ticks)

    async def put_fn(tick: Tick) -> None:
        await queue.put((tick, time.perf_counter()))

    async def consumer() -> None:
        nonlocal processed
        while processed < n:
            tick, enqueued_at = await queue.get()
            depths.append(queue.qsize())
            apply_tick(candle_aggs, vwap, ma, tick)
            await db.execute(
                "INSERT INTO ticks VALUES (?, ?, ?, ?, ?)",
                (tick.symbol, tick.price, tick.quantity, tick.trade_id, tick.event_time_ms),
            )
            await db.commit()  # naive: fsync on every single tick
            latencies.append((time.perf_counter() - enqueued_at) * 1000)
            processed += 1

    start = time.perf_counter()
    await asyncio.gather(_pace_producer(ticks, rate, put_fn), consumer())
    elapsed = time.perf_counter() - start
    await db.close()

    latencies.sort()
    return RunResult(
        name="naive (per-tick queue.get + per-tick commit)",
        n=n,
        target_rate=rate,
        elapsed_sec=elapsed,
        throughput_per_sec=n / elapsed,
        peak_backlog=max(depths) if depths else 0,
        dropped=0,
        latency_ms={
            "p50": latencies[int(len(latencies) * 0.50)],
            "p99": latencies[int(len(latencies) * 0.99) - 1],
            "max": latencies[-1],
        },
    )


async def run_batched(
    ticks: list[Tick],
    rate: float,
    db_path: str,
    ring_capacity: int = 16384,
    batch_max_size: int = 256,
    batch_max_wait_ms: int = 50,
) -> RunResult:
    """Bounded ring buffer, drained in batches, persisted with executemany."""
    if os.path.exists(db_path):
        os.remove(db_path)
    db = await aiosqlite.connect(db_path)
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute(
        "CREATE TABLE ticks (symbol TEXT, price REAL, qty REAL, trade_id INTEGER, event_time_ms INTEGER)"
    )
    await db.commit()

    buf: RingBuffer[tuple[Tick, float]] = RingBuffer(ring_capacity)
    candle_aggs, vwap, ma = new_aggregators()
    latencies: list[float] = []
    depths: list[int] = []
    processed = 0
    n = len(ticks)
    wait_s = batch_max_wait_ms / 1000

    async def put_fn(tick: Tick) -> None:
        await buf.push((tick, time.perf_counter()))

    async def consumer() -> None:
        nonlocal processed
        while processed < n:
            depths.append(buf.depth)
            batch = await buf.drain(max_items=batch_max_size, wait_timeout=wait_s)
            if not batch:
                continue
            rows = []
            now = time.perf_counter()
            for tick, enqueued_at in batch:
                apply_tick(candle_aggs, vwap, ma, tick)
                rows.append((tick.symbol, tick.price, tick.quantity, tick.trade_id, tick.event_time_ms))
                latencies.append((now - enqueued_at) * 1000)
            await db.executemany("INSERT INTO ticks VALUES (?, ?, ?, ?, ?)", rows)
            await db.commit()  # one fsync per batch, not per tick
            processed += len(batch)

    start = time.perf_counter()
    await asyncio.gather(_pace_producer(ticks, rate, put_fn), consumer())
    elapsed = time.perf_counter() - start
    await db.close()

    latencies.sort()
    return RunResult(
        name="batched (ring buffer + batch drain + batched commit)",
        n=n,
        target_rate=rate,
        elapsed_sec=elapsed,
        throughput_per_sec=n / elapsed,
        peak_backlog=max(depths) if depths else 0,
        dropped=buf.dropped,
        latency_ms={
            "p50": latencies[int(len(latencies) * 0.50)],
            "p99": latencies[int(len(latencies) * 0.99) - 1],
            "max": latencies[-1],
        },
    )


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=8000, help="number of synthetic ticks")
    parser.add_argument("--rate", type=float, default=10_000, help="target ingest rate, msg/sec")
    parser.add_argument("--mode", choices=["naive", "batched", "both"], default="both")
    parser.add_argument("--db-dir", default="/tmp")
    args = parser.parse_args()

    ticks = make_synthetic_ticks(args.n)
    print(f"generated {args.n} synthetic ticks, target rate {args.rate:.0f} msg/sec")

    results = []
    if args.mode in ("naive", "both"):
        results.append(await run_naive(ticks, args.rate, os.path.join(args.db_dir, "bench_naive.db")))
    if args.mode in ("batched", "both"):
        results.append(await run_batched(ticks, args.rate, os.path.join(args.db_dir, "bench_batched.db")))

    for r in results:
        r.print_report()

    if len(results) == 2:
        naive, batched = results
        speedup = batched.throughput_per_sec / naive.throughput_per_sec
        print(f"\n=> batched pipeline sustained {speedup:.1f}x the throughput of the naive one")
        print(
            f"=> naive fell behind the {args.rate:.0f} msg/sec target "
            f"(processed at {naive.throughput_per_sec:.0f}/sec); "
            f"batched kept up (processed at {batched.throughput_per_sec:.0f}/sec)"
        )


if __name__ == "__main__":
    asyncio.run(main())
