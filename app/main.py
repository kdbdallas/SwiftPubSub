from __future__ import annotations

import asyncio
import json
import re
from collections import defaultdict
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
WEB_DIRECTORY = Path(__file__).parent / "web"


@dataclass(frozen=True)
class Channel:
    client_id: str
    session_id: Optional[str] = None

    def as_dict(self) -> dict[str, Optional[str]]:
        return {"clientId": self.client_id, "sessionId": self.session_id}


class PublishRequest(BaseModel):
    message: str = Field(min_length=1, max_length=16_384)


class ConnectionManager:
    """Keeps WebSocket connections partitioned by their exact channel."""

    def __init__(self) -> None:
        self._connections: dict[Channel, set[WebSocket]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def connect(self, channel: Channel, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections[channel].add(websocket)

    async def disconnect(self, channel: Channel, websocket: WebSocket) -> None:
        async with self._lock:
            connections = self._connections.get(channel)
            if connections is None:
                return
            connections.discard(websocket)
            if not connections:
                self._connections.pop(channel, None)

    async def publish(self, channel: Channel, message: str) -> dict[str, object]:
        event: dict[str, object] = {
            "type": "message",
            "id": str(uuid4()),
            "scope": channel.as_dict(),
            "message": message,
            # Whole-second RFC 3339 is accepted by JSONDecoder.iso8601 in Swift.
            "sentAt": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
        }
        async with self._lock:
            recipients = tuple(self._connections.get(channel, ()))

        encoded = json.dumps(event)
        failed: list[WebSocket] = []
        for websocket in recipients:
            try:
                await websocket.send_text(encoded)
            except Exception:
                failed.append(websocket)
        for websocket in failed:
            await self.disconnect(channel, websocket)
        return event


app = FastAPI(title="SwiftPubSub", version="1.0.0")
manager = ConnectionManager()


def channel_for(client_id: str, session_id: Optional[str] = None) -> Channel:
    for label, value in (("clientId", client_id), ("sessionId", session_id)):
        if value is not None and not IDENTIFIER_PATTERN.fullmatch(value):
            raise HTTPException(
                status_code=422,
                detail=f"{label} must be 1-128 letters, digits, underscores, or hyphens",
            )
    return Channel(client_id=client_id, session_id=session_id)


def websocket_channel_or_close(client_id: str, session_id: Optional[str]) -> Optional[Channel]:
    try:
        return channel_for(client_id, session_id)
    except HTTPException:
        return None


@app.get("/", include_in_schema=False)
async def publisher_page() -> FileResponse:
    return FileResponse(WEB_DIRECTORY / "index.html")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


async def publish(channel: Channel, body: PublishRequest) -> dict[str, object]:
    return await manager.publish(channel, body.message)


@app.post("/api/clients/{client_id}/messages")
async def publish_client_message(client_id: str, body: PublishRequest) -> dict[str, object]:
    return await publish(channel_for(client_id), body)


@app.post("/api/clients/{client_id}/sessions/{session_id}/messages")
async def publish_session_message(
    client_id: str, session_id: str, body: PublishRequest
) -> dict[str, object]:
    return await publish(channel_for(client_id, session_id), body)


async def websocket_handler(
    websocket: WebSocket, client_id: str, session_id: Optional[str] = None
) -> None:
    channel = websocket_channel_or_close(client_id, session_id)
    if channel is None:
        await websocket.close(code=1008, reason="Invalid client or session identifier")
        return

    await manager.connect(channel, websocket)
    try:
        await websocket.send_json({"type": "connected", "scope": channel.as_dict()})
        while True:
            payload = json.loads(await websocket.receive_text())
            if payload.get("type") != "publish":
                await websocket.send_json({"type": "error", "message": "Use type=publish"})
                continue
            try:
                body = PublishRequest.model_validate(payload)
            except Exception as error:
                await websocket.send_json({"type": "error", "message": str(error)})
                continue
            await manager.publish(channel, body.message)
    except (WebSocketDisconnect, json.JSONDecodeError):
        pass
    finally:
        with suppress(Exception):
            await manager.disconnect(channel, websocket)


@app.websocket("/ws/clients/{client_id}")
async def client_websocket(websocket: WebSocket, client_id: str) -> None:
    await websocket_handler(websocket, client_id)


@app.websocket("/ws/clients/{client_id}/sessions/{session_id}")
async def session_websocket(websocket: WebSocket, client_id: str, session_id: str) -> None:
    await websocket_handler(websocket, client_id, session_id)
