"""Async WebSocket client for Binance's combined trade stream.

Connects to a single multiplexed stream (``/stream?streams=btcusdt@trade/...``)
rather than one connection per symbol -- fewer sockets, fewer reconnect
state machines, and it's how Binance intends multi-symbol consumers to
connect. On disconnect it reconnects with exponential backoff (capped) so a
transient network blip doesn't require a process restart.

The read loop here is deliberately *thin*: parse the JSON, build a `Tick`,
hand it to the dispatcher, go back to `recv()`. Anything heavier (candle
math, VWAP, persistence) happens downstream after the ring buffer, so the
socket read loop is never the thing falling behind.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Callable

import websockets
from websockets.exceptions import ConnectionClosed

from app.config import settings
from app.models import Tick

logger = logging.getLogger("pipeline.binance")

TickHandler = Callable[[Tick], None]


def build_stream_url(symbols: list[str]) -> str:
    streams = "/".join(f"{s.lower()}@trade" for s in symbols)
    return f"{settings.binance_ws_url}?streams={streams}"


def parse_trade_message(raw: dict) -> Tick | None:
    """Parse one combined-stream envelope into a Tick, or None if not a trade."""
    payload = raw.get("data", raw)
    if payload.get("e") != "trade":
        return None
    return Tick(
        symbol=payload["s"].upper(),
        price=float(payload["p"]),
        quantity=float(payload["q"]),
        trade_id=int(payload["t"]),
        event_time_ms=int(payload["E"]),
        is_buyer_maker=bool(payload.get("m", False)),
    )


class BinanceTradeClient:
    def __init__(self, symbols: list[str], on_tick: TickHandler) -> None:
        self._symbols = symbols
        self._on_tick = on_tick
        self._stop = asyncio.Event()
        self._connected = asyncio.Event()
        self.messages_received = 0
        self.reconnect_count = 0

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        url = build_stream_url(self._symbols)
        delay = settings.ws_reconnect_initial_delay_sec
        while not self._stop.is_set():
            try:
                logger.info("connecting to %s", url)
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    self._connected.set()
                    delay = settings.ws_reconnect_initial_delay_sec
                    logger.info("connected, streaming %s", ", ".join(self._symbols))
                    async for raw_message in ws:
                        if self._stop.is_set():
                            break
                        self._handle_raw(raw_message)
            except (ConnectionClosed, OSError) as exc:
                logger.warning("connection lost (%s), reconnecting in %.1fs", exc, delay)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - keep the ingest loop alive
                logger.exception("unexpected error in binance client loop")
            finally:
                self._connected.clear()

            if self._stop.is_set():
                break
            self.reconnect_count += 1
            await asyncio.sleep(delay)
            delay = min(delay * 2, settings.ws_reconnect_max_delay_sec)

    def _handle_raw(self, raw_message: str | bytes) -> None:
        try:
            data = json.loads(raw_message)
            tick = parse_trade_message(data)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            logger.debug("dropping unparseable message: %r", raw_message[:200])
            return
        if tick is None:
            return
        self.messages_received += 1
        self._on_tick(tick)
