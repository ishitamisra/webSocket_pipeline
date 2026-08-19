"""Async WebSocket client for Coinbase Exchange's public trade feed.

This replaced an earlier Binance-based client during development: Binance.com
rejects connections from US IPs (`HTTP 451`), and Binance.US -- while
reachable -- turned out to have close to zero real-time trade flow on its
combined-stream endpoint in practice (a side effect of the liquidity most
market makers pulled after the 2023 SEC action against Binance.US). Coinbase
Exchange's public `matches` channel needs no auth for market data and
produces multiple trades per second on BTC-USD/ETH-USD/SOL-USD, which is
what a demo pipeline like this actually needs.

Subscription here uses Coinbase's message-based protocol (connect to the
base endpoint, then send a `{"type": "subscribe", ...}` frame) rather than a
URL-embedded stream list -- that's simply how Coinbase's API works, unlike
Binance's URL-based combined streams.

Same shape as the ring-buffer-feeding design described in `pipeline.py`: the
read loop here stays thin (parse JSON, build a `Tick`, hand it to the
dispatcher, go back to `recv()`) so a slow downstream consumer can never
starve the socket read loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Callable

import websockets
from websockets.exceptions import ConnectionClosed

from app.config import settings
from app.models import Tick

logger = logging.getLogger("pipeline.coinbase")

TickHandler = Callable[[Tick], None]

_MATCH_TYPES = {"match", "last_match"}


def parse_match_message(payload: dict) -> Tick | None:
    """Parse one `matches` channel message into a Tick, or None if not a trade."""
    if payload.get("type") not in _MATCH_TYPES:
        return None
    event_time_ms = int(datetime.fromisoformat(payload["time"]).timestamp() * 1000)
    # Coinbase's `side` is the resting (maker) order's side, so "buy" means
    # the taker crossed in as a seller -- i.e. this was a sell-initiated
    # trade, the same real-world event Binance's is_buyer_maker=True flags.
    return Tick(
        symbol=payload["product_id"].upper(),
        price=float(payload["price"]),
        quantity=float(payload["size"]),
        trade_id=int(payload["trade_id"]),
        event_time_ms=event_time_ms,
        is_buyer_maker=(payload.get("side") == "buy"),
    )


class CoinbaseTradeClient:
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
        url = settings.coinbase_ws_url
        subscribe_msg = json.dumps(
            {"type": "subscribe", "product_ids": self._symbols, "channels": ["matches"]}
        )
        delay = settings.ws_reconnect_initial_delay_sec
        while not self._stop.is_set():
            try:
                logger.info("connecting to %s", url)
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(subscribe_msg)
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
                logger.exception("unexpected error in coinbase client loop")
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
            if data.get("type") == "error":
                logger.warning("coinbase feed error: %s", data)
                return
            tick = parse_match_message(data)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            logger.debug("dropping unparseable message: %r", raw_message[:200])
            return
        if tick is None:
            return
        self.messages_received += 1
        self._on_tick(tick)
