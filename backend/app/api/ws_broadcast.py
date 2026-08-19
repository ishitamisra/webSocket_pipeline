"""Fan-out of pipeline events to connected dashboard clients.

Each browser tab gets its own small bounded queue and sender task. A
slow/backgrounded tab (laptop lid closed, dev tools open and choking on
JSON parsing, whatever) must not be able to slow down the pipeline or the
other connected clients -- so `publish` is non-blocking: if a client's
queue is full, we drop the oldest queued message for *that client only*
and keep going. This is the same backpressure philosophy as the ingest
ring buffer, applied at the fan-out edge instead of the intake edge.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import WebSocket

logger = logging.getLogger("pipeline.broadcast")

CLIENT_QUEUE_MAXSIZE = 1000


class _Client:
    def __init__(self, ws: WebSocket) -> None:
        self.ws = ws
        self.queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=CLIENT_QUEUE_MAXSIZE)
        self.dropped = 0


class BroadcastManager:
    def __init__(self) -> None:
        self._clients: dict[WebSocket, _Client] = {}
        self._tasks: dict[WebSocket, asyncio.Task] = {}

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        client = _Client(ws)
        self._clients[ws] = client
        self._tasks[ws] = asyncio.create_task(self._sender_loop(client))

    async def disconnect(self, ws: WebSocket) -> None:
        client = self._clients.pop(ws, None)
        task = self._tasks.pop(ws, None)
        if task is not None:
            task.cancel()
        if client is not None:
            logger.info("client disconnected (dropped %d messages while connected)", client.dropped)

    def publish_nowait(self, event: dict) -> None:
        for client in self._clients.values():
            try:
                client.queue.put_nowait(event)
            except asyncio.QueueFull:
                try:
                    client.queue.get_nowait()  # drop oldest
                except asyncio.QueueEmpty:
                    pass
                client.dropped += 1
                try:
                    client.queue.put_nowait(event)
                except asyncio.QueueFull:
                    pass

    def publish_many_nowait(self, events: list[dict]) -> None:
        for event in events:
            self.publish_nowait(event)

    @property
    def client_count(self) -> int:
        return len(self._clients)

    async def _sender_loop(self, client: _Client) -> None:
        try:
            while True:
                event = await client.queue.get()
                await client.ws.send_json(event)
        except asyncio.CancelledError:
            pass
        except Exception:  # noqa: BLE001 - client disconnects surface here
            pass
