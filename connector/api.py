import time
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from connector.config import ConnectorConfig
from connector.models import ScanResult
from connector.outbox import OutboxDB

api_router = APIRouter()

def get_config(request: Request) -> ConnectorConfig:
    return request.app.state.config

def get_identity(request: Request) -> dict[str, str]:
    return request.app.state.identity

def get_outbox_db(request: Request) -> OutboxDB:
    return request.app.state.outbox_db

@api_router.get("/health")
async def health_check(identity: dict[str, str] = Depends(get_identity)):
    return {
        "status": "ok",
        "registered": True,
        "online_url": identity["online_url"]
    }

@api_router.post("/update")
async def update_scan(
    result: ScanResult, 
    request: Request,
    outbox_db: OutboxDB = Depends(get_outbox_db)
):
    image_id = result.image_id
    created_at_ts = int(time.time())
    
    # Store raw original JSON for forwarding
    # We load it from request.json() to capture extra fields perfectly.
    # Note: Using result.model_dump() is also fine given extra="allow", 
    # but request.json() is literally what the client sent.
    raw_payload = await request.json()
    
    inserted = await outbox_db.insert(image_id, raw_payload, created_at_ts)
    
    if inserted:
        return JSONResponse(
            status_code=202,
            content={"status": "accepted", "image_id": image_id}
        )
    else:
        return JSONResponse(
            status_code=202,
            content={"status": "duplicate", "image_id": image_id}
        )
