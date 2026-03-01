import pytest
from httpx import AsyncClient, MockTransport, Response
import time
import json
from pathlib import Path

from connector.config import ConnectorConfig
from connector.outbox import OutboxDB
from connector.forwarder import ForwarderWorker

@pytest.mark.asyncio
async def test_forwarder_worker(tmp_path: Path):
    db_path = tmp_path / "test.db"
    outbox_db = OutboxDB(db_path)
    await outbox_db.init_db()
    
    config = ConnectorConfig(
        outbox_db_path=db_path,
        online_update_path="/custom-update"
    )
    identity = {
        "device_id": "test-device",
        "device_token": "secret-token-123",
        "online_url": "https://online.test"
    }
    
    # Insert dummy data
    payload = {
        "session_id": "s1",
        "image_id": "test-image-1",
        "timestamp": "2026-03-01T23:40:00Z",
        "weight_grams": 120.5,
        "fruits": []
    }
    await outbox_db.insert("test-image-1", payload, int(time.time()))
    
    worker = ForwarderWorker(config, outbox_db, identity)
    
    # Mock HTTP client
    requests_received = []
    def handle_request(request):
        requests_received.append(request)
        return Response(202)
        
    transport = MockTransport(handle_request)
    client = AsyncClient(transport=transport)
    
    await worker._process_queued(client)
    
    assert len(requests_received) == 1
    req = requests_received[0]
    
    # Check headers
    assert req.headers["authorization"] == "Bearer secret-token-123"
    assert req.headers["idempotency-key"] == "test-image-1"
    
    # Check envelope
    body = json.loads(req.content)
    assert body["envelope_version"] == "v1"
    assert body["image_id"] == "test-image-1"
    assert body["scan_result"] == payload
    
    # Verify row marked sent
    rows = await outbox_db.get_queued(current_ts=2_000_000_000)
    assert len(rows) == 0
    await client.aclose()
