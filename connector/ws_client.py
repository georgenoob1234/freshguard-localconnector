from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import uuid
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import websockets

from connector.config import ConnectorConfig
from connector.logging import get_logger
from connector.outbox import OutboxDB
from connector.ws_commands import WSCommandHandler

LOGGER = get_logger(__name__)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_ws_url(online_url: str, ws_path: str) -> str:
    parsed = urlsplit(online_url)
    if parsed.scheme == "https":
        ws_scheme = "wss"
    elif parsed.scheme == "http":
        ws_scheme = "ws"
    else:
        raise ValueError("online_url must use http or https")

    normalized_path = ws_path if ws_path.startswith("/") else f"/{ws_path}"
    return urlunsplit((ws_scheme, parsed.netloc, normalized_path, "", ""))


class WebSocketWorker:
    def __init__(
        self,
        config: ConnectorConfig,
        outbox_db: OutboxDB,
        identity: dict[str, str],
        *,
        started_at: str,
        get_forwarder_stats: Any = None,
    ) -> None:
        self.config = config
        self.outbox_db = outbox_db
        self.identity = identity
        self.command_handler = WSCommandHandler(
            config,
            outbox_db,
            identity,
            started_at=started_at,
            get_forwarder_stats=get_forwarder_stats,
        )
        self._cancel_event = asyncio.Event()
        self._active_websocket: Any = None

    async def start(self) -> None:
        LOGGER.info("Starting websocket worker loop")
        self._cancel_event.clear()
        reconnect_seconds = max(1, self.config.ws_reconnect_base_seconds)

        while not self._cancel_event.is_set():
            try:
                await self._run_session()
                reconnect_seconds = max(1, self.config.ws_reconnect_base_seconds)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self._cancel_event.is_set():
                    LOGGER.warning("WebSocket session ended: %s", exc)

            if self._cancel_event.is_set():
                break

            wait_seconds = min(reconnect_seconds, self.config.ws_reconnect_max_seconds)
            try:
                await asyncio.wait_for(self._cancel_event.wait(), timeout=wait_seconds)
            except asyncio.TimeoutError:
                pass
            reconnect_seconds = min(
                wait_seconds * 2, self.config.ws_reconnect_max_seconds
            )

    def stop(self) -> None:
        LOGGER.info("Stopping websocket worker loop")
        self._cancel_event.set()

    def _connect(self, ws_url: str, headers: dict[str, str]) -> Any:
        connect_kwargs: dict[str, Any] = {"ping_interval": None}
        signature = inspect.signature(websockets.connect)
        if "additional_headers" in signature.parameters:
            connect_kwargs["additional_headers"] = headers
        elif "extra_headers" in signature.parameters:
            connect_kwargs["extra_headers"] = headers
        else:
            LOGGER.warning(
                "websockets.connect supports neither additional_headers nor extra_headers; "
                "continuing without auth headers."
            )
        return websockets.connect(ws_url, **connect_kwargs)

    async def _run_session(self) -> None:
        ws_url = build_ws_url(self.identity["online_url"], self.config.online_ws_path)
        auth_headers = {"Authorization": f"Bearer {self.identity['device_token']}"}

        async with self._connect(ws_url, auth_headers) as websocket:
            self._active_websocket = websocket
            LOGGER.info("Connected websocket to %s", ws_url)

            await self._send_envelope(
                websocket,
                message_type="hello",
                payload={
                    "device_id": self.identity["device_id"],
                    "connector_version": self.config.connector_version,
                    "hostname": self.config.hostname,
                    "os": self.config.os_name,
                },
            )
            heartbeat_task = asyncio.create_task(self._heartbeat_loop(websocket))
            try:
                while not self._cancel_event.is_set():
                    try:
                        raw_message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    await self._handle_incoming_message(websocket, raw_message)
            finally:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task
                self._active_websocket = None

    async def _heartbeat_loop(self, websocket: Any) -> None:
        heartbeat_seconds = max(1, self.config.ws_heartbeat_seconds)
        while not self._cancel_event.is_set():
            try:
                await asyncio.wait_for(
                    self._cancel_event.wait(),
                    timeout=heartbeat_seconds,
                )
            except asyncio.TimeoutError:
                await self._send_envelope(
                    websocket,
                    message_type="heartbeat",
                    payload={},
                )

    async def _handle_incoming_message(self, websocket: Any, raw_message: Any) -> None:
        if isinstance(raw_message, bytes):
            raw_text = raw_message.decode("utf-8", errors="replace")
        else:
            raw_text = str(raw_message)

        try:
            incoming = json.loads(raw_text)
        except json.JSONDecodeError:
            await self._send_envelope(
                websocket,
                message_type="error",
                payload={
                    "code": "invalid_json",
                    "message": "Incoming websocket message is not valid JSON.",
                },
            )
            return

        if not isinstance(incoming, dict):
            return
        if incoming.get("type") != "request":
            return

        payload = incoming.get("payload")
        request_payload = payload if isinstance(payload, dict) else {}

        ack_payload, request_context = self.command_handler.build_ack_payload(
            request_payload
        )
        await self._send_envelope(
            websocket,
            message_type="ack",
            payload=ack_payload,
        )

        response_payload = await self.command_handler.build_response_payload(
            request_context
        )
        await self._send_envelope(
            websocket,
            message_type="response",
            payload=response_payload,
        )

    async def _send_envelope(
        self,
        websocket: Any,
        *,
        message_type: str,
        payload: dict[str, Any],
    ) -> None:
        envelope = {
            "type": message_type,
            "ts": utc_now_iso(),
            "message_id": str(uuid.uuid4()),
            "device_id": self.identity["device_id"],
            "payload": payload,
        }
        await websocket.send(json.dumps(envelope))
