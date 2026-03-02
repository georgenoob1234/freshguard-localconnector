from __future__ import annotations

import json
import time
from pathlib import Path

import httpx
import pytest

from connector.config import ConnectorConfig
from connector.outbox import OutboxDB
from connector.ws_commands import WSCommandHandler


async def _build_handler(
    tmp_path: Path,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> tuple[WSCommandHandler, OutboxDB]:
    db_path = tmp_path / "ws-commands.db"
    outbox_db = OutboxDB(db_path)
    await outbox_db.init_db()

    config = ConnectorConfig(
        outbox_db_path=db_path,
        camera_service_url="http://camera.local",
        online_url="https://online.test",
    )
    identity = {
        "device_id": "device-1",
        "device_token": "device-token",
        "registered_at": "2026-03-01T00:00:00Z",
        "online_url": "https://online.test",
    }
    handler = WSCommandHandler(
        config,
        outbox_db,
        identity,
        started_at="2026-03-01T00:00:00Z",
        get_forwarder_stats=lambda: {
            "last_forward_error": "example-forward-error",
            "last_forward_success_at": "2026-03-01T00:01:00Z",
        },
        http_transport=transport,
    )
    return handler, outbox_db


@pytest.mark.asyncio
async def test_allowlist_unknown_request_type_rejected(tmp_path: Path) -> None:
    handler, outbox_db = await _build_handler(tmp_path)
    try:
        ack_payload, response_payload = await handler.handle_request_payload(
            {
                "request_id": "req-unknown",
                "request_type": "not.allowlisted",
                "params": {},
            }
        )

        assert ack_payload["accepted"] is False
        assert ack_payload["reason"] == "unknown_request_type"
        assert response_payload["status"] == "rejected"
        assert response_payload["error"]["code"] == "rejected_not_allowed"
    finally:
        await outbox_db.close()


@pytest.mark.asyncio
async def test_missing_request_id_is_not_accepted(tmp_path: Path) -> None:
    handler, outbox_db = await _build_handler(tmp_path)
    try:
        ack_payload, response_payload = await handler.handle_request_payload(
            {
                "request_type": "ping",
                "params": {},
            }
        )

        assert ack_payload["accepted"] is False
        assert ack_payload["reason"] == "missing_request_id"
        assert response_payload["status"] == "error"
        assert response_payload["error"]["code"] == "invalid_request"
    finally:
        await outbox_db.close()


@pytest.mark.asyncio
async def test_ping_request_returns_pong(tmp_path: Path) -> None:
    handler, outbox_db = await _build_handler(tmp_path)
    try:
        ack_payload, response_payload = await handler.handle_request_payload(
            {
                "request_id": "req-ping",
                "request_type": "ping",
                "params": {},
            }
        )

        assert ack_payload["accepted"] is True
        assert response_payload["status"] == "ok"
        assert response_payload["data"] == {"pong": True}
    finally:
        await outbox_db.close()


@pytest.mark.asyncio
async def test_connector_stats_returns_integer_counts(tmp_path: Path) -> None:
    handler, outbox_db = await _build_handler(tmp_path)
    try:
        now = int(time.time())

        await outbox_db.insert(
            "img-queued",
            {"session_id": "s1", "image_id": "img-queued", "fruits": []},
            now,
        )
        await outbox_db.insert(
            "img-sending",
            {"session_id": "s1", "image_id": "img-sending", "fruits": []},
            now,
        )
        await outbox_db.mark_sending("img-sending")
        await outbox_db.insert(
            "img-sent",
            {"session_id": "s1", "image_id": "img-sent", "fruits": []},
            now,
        )
        await outbox_db.mark_sent("img-sent")
        await outbox_db.insert(
            "img-dead",
            {"session_id": "s1", "image_id": "img-dead", "fruits": []},
            now,
        )
        await outbox_db.mark_failed(
            image_id="img-dead",
            attempts=20,
            next_retry_ts=now + 60,
            error="dead-letter",
            max_attempts=20,
        )

        _, response_payload = await handler.handle_request_payload(
            {
                "request_id": "req-stats",
                "request_type": "connector.stats",
                "params": {},
            }
        )

        assert response_payload["status"] == "ok"
        data = response_payload["data"]
        for key in ("queued", "sending", "sent", "dead"):
            assert isinstance(data[key], int)
        assert data["queued"] == 1
        assert data["sending"] == 1
        assert data["sent"] == 1
        assert data["dead"] == 1
    finally:
        await outbox_db.close()


@pytest.mark.asyncio
async def test_camera_capture_uploads_blob_and_returns_blob_id(tmp_path: Path) -> None:
    image_bytes = b"\xff\xd8\xff\xdbmockjpeg"

    def handle_request(request: httpx.Request) -> httpx.Response:
        request_url = str(request.url)
        if request.method == "POST" and request_url == "http://camera.local/capture":
            payload = json.loads(request.content.decode("utf-8"))
            assert payload["resolution"] == "640x480"
            return httpx.Response(
                200,
                json={
                    "image_id": "img-cam-1",
                    "image_url_or_path": "/images/img-cam-1.jpg",
                },
            )

        if (
            request.method == "GET"
            and request_url == "http://camera.local/images/img-cam-1.jpg"
        ):
            return httpx.Response(
                200,
                content=image_bytes,
                headers={"content-type": "image/jpeg"},
            )

        if request.method == "POST" and request_url == "https://online.test/connector/v1/blobs":
            assert request.headers["authorization"] == "Bearer device-token"
            multipart_payload = request.read()
            assert image_bytes in multipart_payload
            return httpx.Response(200, json={"blob_id": "blob-123"})

        raise AssertionError(f"Unexpected request {request.method} {request_url}")

    transport = httpx.MockTransport(handle_request)
    handler, outbox_db = await _build_handler(tmp_path, transport=transport)
    try:
        ack_payload, response_payload = await handler.handle_request_payload(
            {
                "request_id": "req-camera",
                "request_type": "camera.capture",
                "params": {"resolution": "640x480"},
            }
        )

        assert ack_payload["accepted"] is True
        assert response_payload["status"] == "ok"
        data = response_payload["data"]
        assert data["image_id"] == "img-cam-1"
        assert data["blob_id"] == "blob-123"
        assert data["content_type"] == "image/jpeg"
        assert data["size_bytes"] == len(image_bytes)
        assert all(not isinstance(value, (bytes, bytearray)) for value in data.values())
    finally:
        await outbox_db.close()
