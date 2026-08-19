# Crypto Tick Pipeline

A live WebSocket ingestion pipeline for crypto trade data: it connects to
Binance's public trade stream, computes rolling VWAP, moving averages, and
multi-timeframe OHLC candles in real time, persists a time-series history,
and serves it all — REST + a live WebSocket feed — to a dashboard.

It's built around the failure mode you actually hit doing this for real:
**a naive per-message pipeline falls behind under load.** The fix that
matters here isn't a bigger machine, it's two structural changes —
a bounded ring buffer with an explicit overflow policy, and batching the
consumer side instead of processing one message per event-loop wakeup.
See [Scale](#scale-the-naive-version-fell-behind) for the numbers.

## What it does

- **Ingests** Binance's combined trade stream (`btcusdt@trade`,
  `ethusdt@trade`, `solusdt@trade` by default) over a single multiplexed
  WebSocket connection, with automatic reconnect + exponential backoff.
- **Buffers** incoming ticks in a fixed-capacity ring buffer per symbol —
  bounded memory, and a producer that never blocks the socket read loop.
- **Reorders** ticks within a small watermark window before they're
  admitted to aggregation, so a tick that arrives slightly out of sequence
  doesn't corrupt a candle that already closed. Detects gaps (dropped
  trades) via Binance's monotonic trade IDs.
- **Aggregates**, per symbol: rolling VWAP (sliding time window), SMA/EMA
  moving averages, and OHLC candles at 1s / 5s / 1m timeframes.
- **Persists** ticks and closed candles to SQLite in batches (WAL mode),
  so historical queries don't depend on in-memory retention.
- **Serves** it all: a REST API for candle history / VWAP / moving
  averages / pipeline health, and a WebSocket that pushes live tick and
  candle updates to connected dashboard clients.
- **Renders** a dashboard: live candlestick chart with VWAP/SMA overlays,
  a trade tape, and a pipeline health panel (buffer depth, dropped count,
  out-of-order count, gap count, exchange link status).

## Architecture

```
 Binance combined WS  --sync callback-->  RingBuffer[symbol]  --batch drain-->  consumer task[symbol]
 (one socket, all           (bounded, drop-oldest                                     |
  symbols multiplexed)       under overload)                                          v
                                                                              Sequencer (watermark reorder,
                                                                               gap detection via trade_id)
                                                                                        |
                                                                                        v
                                                                     CandleAggregator x {1s,5s,60s}
                                                                     RollingVWAP (sliding window)
                                                                     MovingAverageTracker (SMA/EMA)
                                                                                        |
                                                                     +------------------+------------------+
                                                                     v                                     v
                                                          SQLiteWriter (batched,                BroadcastManager
                                                           executemany + WAL)                (per-client bounded
                                                                                              queue, drop-oldest)
                                                                                                        |
                                                                                                        v
                                                                                          Dashboard (WS live feed
                                                                                           + REST history)
```

One ring buffer and one consumer task per symbol, so a burst on one
symbol can't starve the others. Everything downstream of the ring buffer
— reordering, aggregation, persistence, broadcast — runs inside that
symbol's consumer task, so a slow write or a slow browser tab can only
ever delay that symbol's own pipeline, never the WebSocket read loop that
talks to the exchange.

## Handling out-of-order and dropped messages

- **Out-of-order**: `Sequencer` (`backend/app/ingest/sequencer.py`) holds
  ticks in a min-heap keyed by exchange event time and only releases them
  once nothing still in-flight (within a configurable watermark, default
  250ms) could arrive ahead of them. This is the same "allowed lateness"
  idea streaming systems like Flink use, scaled down for an in-process
  asyncio pipeline. `CandleAggregator` also patches an already-closed
  candle if a tick lands for it after the watermark — rare, but disclosed
  via a `late_patches` counter rather than silently dropped.
- **Dropped/missing**: Binance trade IDs are monotonically increasing per
  symbol. A jump greater than 1 means a trade was missed (exchange-side
  drop, reconnect gap, or the ring buffer overwriting the oldest entry
  under sustained overload). The pipeline can't recover a missing trade,
  but it counts every gap so it's visible in the health panel instead of
  silently skewing VWAP/candles.
- **Backpressure**: the ring buffer has a fixed capacity and an explicit
  overflow policy (drop oldest, count it) rather than growing without
  bound. The dashboard's per-client broadcast queue uses the same
  philosophy at the fan-out edge — a slow browser tab drops its own
  oldest queued messages instead of slowing down the pipeline or other
  clients.

## Scale: the naive version fell behind

The first pass at this pipeline used an `asyncio.Queue`, one tick
processed per `await queue.get()`, persisted with one `INSERT` + `COMMIT`
per tick. That's fine at the trade rate you actually see from 2-3 symbols
on Binance. It is not fine at the rate a busier feed (more symbols, a
volatile session, or just testing your own capacity) can produce.

`backend/benchmark/load_test.py` reproduces this with a synthetic 10k
msg/sec tick generator, running the *same* aggregation logic
(`CandleAggregator`, `RollingVWAP`, `MovingAverageTracker`) through both a
naive consumer and the ring-buffer + batching consumer, against real
SQLite writes. Measured on this machine (`python -m benchmark.load_test
--n 8000 --rate 10000`):

```
--- naive (per-tick queue.get + per-tick commit) ---
  wall time to fully drain: 6.55 s
  sustained throughput    : 1221.4 msg/sec
  peak queue depth        : 7078            (unbounded — kept growing)
  ingest->processed latency: p50=2947.8ms  p99=5690.0ms  max=5749.7ms

--- batched (ring buffer + batch drain + batched commit) ---
  wall time to fully drain: 0.80 s
  sustained throughput    : 9984.3 msg/sec
  peak buffer depth       : 233             (bounded, capacity 16384)
  ingest->processed latency: p50=0.4ms  p99=15.4ms  max=23.4ms

=> batched pipeline sustained 8.2x the throughput of the naive one
```

The naive version couldn't clear more than ~1.2k msg/sec — SQLite's
per-commit fsync is the wall, not the CPU — so its queue backlog grew
without bound and per-tick latency climbed past 5 seconds. Two changes
fixed it:

1. **Batch the writes.** One `executemany` + `COMMIT` per ~250 ticks
   instead of per tick removes the fsync from the hot path almost
   entirely (measured separately: ~0.003ms/row batched vs. ~0.8ms/row
   per-commit on this machine — SQLite fsync latency is disk- and
   OS-dependent, but the multiple holds broadly).
2. **Bound the buffer, and drain it in batches.** A ring buffer with a
   fixed capacity and a batch-drain consumer amortizes the per-item
   event-loop wakeup cost, and gives the system an explicit, visible
   overflow policy instead of an `asyncio.Queue` that grows until memory
   runs out.

Run it yourself — numbers depend on your disk and CPU:

```
./scripts/benchmark.sh --n 8000 --rate 10000
```

## Running it

```
./scripts/run.sh
```

This creates a virtualenv, installs dependencies, and starts the server
at `http://localhost:8000` — the dashboard is served at `/`, the REST API
under `/api/*`, and the live feed at `ws://localhost:8000/ws/stream`.

Configuration is environment-variable driven (see
`backend/app/config.py`) — e.g. `PIPELINE_SYMBOLS=btcusdt,ethusdt`,
`RING_BUFFER_CAPACITY`, `BATCH_MAX_SIZE`, `REORDER_WATERMARK_MS`.

By default it connects to **Binance.US**, not Binance.com — Binance.com's
WebSocket rejects connections from US IPs with `HTTP 451` (it isn't
licensed to serve US residents), so Binance.US is the default that works
out of the box for US-based runs. Same API and trade message schema, just
a different host and somewhat thinner liquidity/symbol coverage. If
you're outside the US and want Binance.com instead:

```
BINANCE_WS_URL=wss://stream.binance.com:9443/stream ./scripts/run.sh
```

### Tests

```
cd backend
pip install -r requirements-dev.txt
pytest
```

### API

| Endpoint | Description |
|---|---|
| `GET /api/symbols` | tracked symbols + supported candle timeframes |
| `GET /api/candles/{symbol}?timeframe=60&limit=200` | OHLC candle history (in-memory or `source=db` for SQLite-backed history) |
| `GET /api/vwap/{symbol}` | current rolling VWAP |
| `GET /api/moving-average/{symbol}` | current SMA/EMA values |
| `GET /api/stats` | per-symbol pipeline health: throughput, buffer depth, dropped/out-of-order/gap counts |
| `WS /ws/stream` | live tick, candle, and stats events |

## Project layout

```
backend/app/
  ingest/         binance_client.py, ring_buffer.py, sequencer.py
  processing/     candles.py, vwap.py, moving_average.py, pipeline.py
  storage/        sqlite_store.py, timeseries_store.py
  api/            routes.py, ws_broadcast.py
  main.py         wires it all together, serves the frontend
backend/tests/     unit tests for the ring buffer, candles, VWAP, sequencer, moving averages
backend/benchmark/ naive-vs-batched throughput benchmark
frontend/          dashboard (vanilla JS + TradingView lightweight-charts)
```
