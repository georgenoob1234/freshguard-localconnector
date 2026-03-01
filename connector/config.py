from __future__ import annotations

import platform
import socket
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


DEFAULT_IDENTITY_PATH = Path("./data/connector_identity.json")
DEFAULT_LABEL = "local-connector"
DEFAULT_VERSION = "0.1.0"


class ConnectorConfig(BaseSettings):
    online_url: Optional[str] = Field(default=None, validation_alias="ONLINE_URL")
    enroll_token: Optional[str] = Field(default=None, validation_alias="ENROLL_TOKEN")
    identity_path: Path = Field(default=DEFAULT_IDENTITY_PATH, validation_alias="CONNECTOR_IDENTITY_PATH")
    
    label: str = Field(default=DEFAULT_LABEL, validation_alias="CONNECTOR_LABEL")
    os_name: str = Field(default_factory=platform.platform, validation_alias="CONNECTOR_OS")
    hostname: str = Field(default_factory=socket.gethostname, validation_alias="CONNECTOR_HOSTNAME")
    connector_version: str = Field(default=DEFAULT_VERSION, validation_alias="CONNECTOR_VERSION")

    service_host: str = Field(default="0.0.0.0", validation_alias="SERVICE_HOST")
    service_port: int = Field(default=8600, validation_alias="SERVICE_PORT")
    outbox_db_path: Path = Field(default=Path("./data/localconnector.db"), validation_alias="OUTBOX_DB_PATH")
    online_update_path: str = Field(default="/update", validation_alias="ONLINE_UPDATE_PATH")
    online_ws_path: str = Field(default="/connector/v1/ws", validation_alias="ONLINE_WS_PATH")
    oms_blob_upload_path: str = Field(default="/connector/v1/blobs", validation_alias="OMS_BLOB_UPLOAD_PATH")
    forward_poll_interval_ms: int = Field(default=500, validation_alias="FORWARD_POLL_INTERVAL_MS")
    forward_timeout_seconds: int = Field(default=8, validation_alias="FORWARD_TIMEOUT_SECONDS")
    outbox_max_attempts: int = Field(default=20, validation_alias="OUTBOX_MAX_ATTEMPTS")
    backoff_base_seconds: int = Field(default=1, validation_alias="BACKOFF_BASE_SECONDS")
    backoff_max_seconds: int = Field(default=60, validation_alias="BACKOFF_MAX_SECONDS")
    ws_heartbeat_seconds: int = Field(default=15, validation_alias="WS_HEARTBEAT_SECONDS")
    ws_reconnect_base_seconds: int = Field(default=1, validation_alias="WS_RECONNECT_BASE_SECONDS")
    ws_reconnect_max_seconds: int = Field(default=30, validation_alias="WS_RECONNECT_MAX_SECONDS")
    command_timeout_seconds: int = Field(default=20, validation_alias="COMMAND_TIMEOUT_SECONDS")
    camera_service_url: str = Field(default="http://localhost:8200", validation_alias="CAMERA_SERVICE_URL")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True
    )


def resolve_config(
    *,
    online_url_override: str | None = None,
    enroll_token_override: str | None = None,
    identity_path_override: str | None = None,
) -> ConnectorConfig:
    config = ConnectorConfig()
    
    if online_url_override:
        config.online_url = online_url_override
    if enroll_token_override:
        config.enroll_token = enroll_token_override
    if identity_path_override:
        config.identity_path = Path(identity_path_override)
        
    if config.online_url:
        config.online_url = config.online_url.rstrip("/")
    if not config.online_ws_path.startswith("/"):
        config.online_ws_path = f"/{config.online_ws_path}"
    if not config.oms_blob_upload_path.startswith("/"):
        config.oms_blob_upload_path = f"/{config.oms_blob_upload_path}"
    config.camera_service_url = config.camera_service_url.rstrip("/")
        
    return config
