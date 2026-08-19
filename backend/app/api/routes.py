"""REST + WebSocket routes for serving the aggregated pipeline data."""

from __future__ import annotations

import time

from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect

from app.api.ws_broadcast import BroadcastManager
from app.config import settings
from app.ingest.coinbase_client import CoinbaseTradeClient
from app.processing.pipeline import Pipeline
from app.storage.timeseries_store import TimeSeriesStore


def build_router(
    pipeline: Pipeline,
    store: TimeSeriesStore,
    broadcast: BroadcastManager,
    exchange_client: CoinbaseTradeClient,
    started_at: float,
) -> APIRouter:
    router = APIRouter()

    @router.get("/api/symbols")
    async def get_symbols() -> dict:
        return {"symbols": pipeline.symbols, "timeframes_sec": list(pipeline.candle_aggs.keys())}

    @router.get("/api/candles/{symbol}")
    async def get_candles(
        symbol: str,
        timeframe: int = Query(60, description="Candle timeframe in seconds"),
        limit: int = Query(200, ge=1, le=2000),
        source: str = Query("memory", pattern="^(memory|db)$"),
    ) -> dict:
        symbol = symbol.upper()
        if symbol not in pipeline.symbols:
            raise HTTPException(status_code=404, detail=f"unknown symbol {symbol}")
        if timeframe not in pipeline.candle_aggs:
            raise HTTPException(
                status_code=400,
                detail=f"unsupported timeframe {timeframe}, choose from {list(pipeline.candle_aggs)}",
            )
        if source == "db":
            candles = await store.get_candles_from_db(symbol, timeframe, limit)
        else:
            candles = store.get_candles(symbol, timeframe, limit)
        return {"symbol": symbol, "timeframe_sec": timeframe, "candles": candles}

    @router.get("/api/vwap/{symbol}")
    async def get_vwap(symbol: str) -> dict:
        symbol = symbol.upper()
        if symbol not in pipeline.symbols:
            raise HTTPException(status_code=404, detail=f"unknown symbol {symbol}")
        return {
            "symbol": symbol,
            "vwap": store.get_vwap(symbol),
            "window_sec": settings.vwap_window_sec,
        }

    @router.get("/api/moving-average/{symbol}")
    async def get_moving_average(symbol: str) -> dict:
        symbol = symbol.upper()
        if symbol not in pipeline.symbols:
            raise HTTPException(status_code=404, detail=f"unknown symbol {symbol}")
        return {"symbol": symbol, "moving_averages": store.get_moving_averages(symbol)}

    @router.get("/api/stats")
    async def get_stats() -> dict:
        return {
            "symbols": store.get_stats(),
            "connected_clients": broadcast.client_count,
            "exchange_connected": exchange_client.connected,
            "exchange_reconnects": exchange_client.reconnect_count,
            "uptime_sec": round(time.monotonic() - started_at, 1),
        }

    @router.get("/api/health")
    async def health() -> dict:
        return {"status": "ok", "exchange_connected": exchange_client.connected}

    @router.websocket("/ws/stream")
    async def ws_stream(ws: WebSocket) -> None:
        await broadcast.connect(ws)
        try:
            while True:
                # We don't expect inbound messages, but we need to await
                # something so we notice the client disconnecting.
                await ws.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            await broadcast.disconnect(ws)

    return router
