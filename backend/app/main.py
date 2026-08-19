"""Application entrypoint: wires the ingest -> pipeline -> API stack together
and serves the dashboard frontend."""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api.routes import build_router
from app.api.ws_broadcast import BroadcastManager
from app.config import settings
from app.ingest.coinbase_client import CoinbaseTradeClient
from app.processing.pipeline import Pipeline
from app.storage.sqlite_store import SQLiteWriter
from app.storage.timeseries_store import TimeSeriesStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("pipeline.main")

FRONTEND_DIR = Path(__file__).resolve().parents[2] / "frontend"

broadcast = BroadcastManager()
sqlite_writer = SQLiteWriter()
started_at = time.monotonic()


async def _on_events(events: list[dict]) -> None:
    broadcast.publish_many_nowait(events)


pipeline = Pipeline(settings.symbols, on_events=_on_events, persistence=sqlite_writer)
exchange_client = CoinbaseTradeClient(settings.symbols, on_tick=pipeline.ingest)
store = TimeSeriesStore(pipeline, settings.sqlite_path)

_background_tasks: list[asyncio.Task] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("starting pipeline for symbols=%s", settings.symbols)
    await sqlite_writer.start()
    await pipeline.start()
    _background_tasks.append(asyncio.create_task(exchange_client.run(), name="exchange-client"))
    _background_tasks.append(asyncio.create_task(_stats_heartbeat(), name="stats-heartbeat"))
    try:
        yield
    finally:
        logger.info("shutting down pipeline")
        exchange_client.stop()
        for t in _background_tasks:
            t.cancel()
        await asyncio.gather(*_background_tasks, return_exceptions=True)
        await pipeline.stop()
        await sqlite_writer.stop()


async def _stats_heartbeat() -> None:
    """Periodically push a stats snapshot so the dashboard updates even
    during lulls in trade activity."""
    while True:
        await asyncio.sleep(2.0)
        broadcast.publish_nowait({"type": "stats", "data": pipeline.snapshot_stats()})


app = FastAPI(title="Crypto WebSocket Pipeline", lifespan=lifespan)
app.include_router(build_router(pipeline, store, broadcast, exchange_client, started_at))

if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
