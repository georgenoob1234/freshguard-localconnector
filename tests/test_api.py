from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from connector.config import ConnectorConfig
from connector.main import create_app


@pytest.fixture(autouse=True)
def mock_worker_start():
    with (
        patch("connector.main.ForwarderWorker.start") as mock_forwarder_start,
        patch("connector.main.WebSocketWorker.start") as mock_ws_start,
    ):
        yield mock_forwarder_start, mock_ws_start


@pytest.fixture
def mock_identity(tmp_path: Path):
    identity_path = tmp_path / "identity.json"
    data = {
        "device_id": "test-device",
        "device_token": "test-token-secret",
        "registered_at": "2026-03-01T23:40:00Z",
        "online_url": "http://test-online",
    }
    identity_path.write_text(json.dumps(data))
    return identity_path, data


@pytest.fixture
def test_config(tmp_path: Path, mock_identity):
    identity_path, _ = mock_identity
    db_path = tmp_path / "test.db"
    return ConnectorConfig(identity_path=identity_path, outbox_db_path=db_path)


@pytest.fixture
def test_app(test_config):
    app = create_app(test_config)
    with TestClient(app) as client:
        yield client, app


def test_health_endpoint(test_app):
    client, _ = test_app
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["registered"] is True
    assert data["online_url"] == "http://test-online"
    assert "device_token" not in data


@pytest.mark.asyncio
async def test_update_endpoint_success(test_config):
    app = create_app(test_config)

    async with app.router.lifespan_context(app):
        with TestClient(app) as client:
            payload = {
                "session_id": "s1",
                "image_id": "s1-0001",
                "timestamp": "2026-03-01T23:40:00Z",
                "weight_grams": 120.5,
                "fruits": [],
                "extra_field": "allowed",
            }

            response = client.post("/update", json=payload)
            assert response.status_code == 202
            assert response.json() == {"status": "accepted", "image_id": "s1-0001"}

            db = app.state.outbox_db
            rows = await db.get_queued(current_ts=2_000_000_000)
            assert len(rows) == 1
            assert rows[0]["image_id"] == "s1-0001"


@pytest.mark.asyncio
async def test_update_endpoint_duplicate(test_config):
    app = create_app(test_config)

    async with app.router.lifespan_context(app):
        with TestClient(app) as client:
            payload = {
                "session_id": "s1",
                "image_id": "s1-0002",
                "timestamp": "2026-03-01T23:40:00Z",
                "weight_grams": 120.5,
                "fruits": [],
            }

            response1 = client.post("/update", json=payload)
            assert response1.status_code == 202
            assert response1.json() == {"status": "accepted", "image_id": "s1-0002"}

            response2 = client.post("/update", json=payload)
            assert response2.status_code == 202
            assert response2.json() == {"status": "duplicate", "image_id": "s1-0002"}
