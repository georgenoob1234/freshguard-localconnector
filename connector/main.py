import asyncio
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI

from connector.api import api_router
from connector.config import ConnectorConfig, resolve_config
from connector.forwarder import ForwarderWorker
from connector.logging import get_logger
from connector.outbox import OutboxDB
from connector.register import register_if_needed
from connector.storage import read_identity
from connector.ws_client import WebSocketWorker

LOGGER = get_logger(__name__)

def perform_startup_registration(config: ConnectorConfig) -> dict[str, str]:
    identity = read_identity(config.identity_path)
    if identity is not None:
        return identity

    LOGGER.info("Identity missing or invalid. Attempting auto-registration.")
    
    # Check env vars
    if not config.online_url or not config.enroll_token:
        LOGGER.error("missing ONLINE_URL/ENROLL_TOKEN for auto-registration.")
        sys.exit(1)
        
    if not config.online_url.startswith(("http://", "https://")):
        LOGGER.error("ONLINE_URL invalid.")
        sys.exit(1)
        
    try:
        identity = register_if_needed(config)
        return identity
    except Exception as e:
        # Exit rules
        error_msg = str(e)
        if "TOKEN_" in error_msg:
            LOGGER.error(f"Registration failed due to token error: {error_msg}")
        else:
            LOGGER.error(f"Registration failed: {error_msg}")
        sys.exit(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    config: ConnectorConfig = app.state.config
    identity: dict[str, str] = app.state.identity
    
    # Init DB
    outbox_db = OutboxDB(config.outbox_db_path)
    await outbox_db.init_db()
    app.state.outbox_db = outbox_db
    
    # Start worker
    worker = ForwarderWorker(config, outbox_db, identity)
    worker_task = asyncio.create_task(worker.start())
    app.state.worker = worker

    ws_worker = WebSocketWorker(
        config,
        outbox_db,
        identity,
        started_at=app.state.started_at,
        get_forwarder_stats=worker.get_runtime_stats,
    )
    ws_task = asyncio.create_task(ws_worker.start())
    app.state.ws_worker = ws_worker

    try:
        yield
    finally:
        # Shutdown
        ws_worker.stop()
        worker.stop()
        try:
            await ws_task
            await worker_task
        finally:
            await outbox_db.close()


def create_app(config: ConnectorConfig | None = None) -> FastAPI:
    if config is None:
        config = resolve_config()
        
    identity = perform_startup_registration(config)
    started_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    
    app = FastAPI(lifespan=lifespan)
    app.state.config = config
    app.state.identity = identity
    app.state.started_at = started_at
    
    app.include_router(api_router)
    
    return app
