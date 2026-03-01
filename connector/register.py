from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from connector.config import ConnectorConfig
from connector.http import OnlineMainServerClient, RegistrationAPIError, RegistrationNetworkError
from connector.logging import get_logger
from connector.storage import (
    IdentityValidationError,
    backup_corrupted_identity,
    read_identity,
    validate_identity,
    write_identity_atomic,
)


LOGGER = get_logger(__name__)


TOKEN_ERROR_MESSAGES = {
    "TOKEN_INVALID": "Enrollment token is invalid. Please request a new token.",
    "TOKEN_EXPIRED": "Enrollment token is expired. Please request a new token.",
    "TOKEN_USED_UP": "Enrollment token is already used up. Please request a new token.",
}


class RegistrationError(RuntimeError):
    """Raised when registration cannot complete."""


def _require_enroll_token(config: ConnectorConfig, override: str | None = None) -> str:
    token = (override or config.enroll_token or "").strip()
    if not token:
        raise RegistrationError(
            "Missing enrollment token. Set ENROLL_TOKEN or pass --enroll-token."
        )
    return token


def _build_device_info(config: ConnectorConfig) -> dict[str, str]:
    return {
        "label": config.label,
        "hostname": config.hostname,
        "os": config.os_name,
        "connector_version": config.connector_version,
    }


def _build_identity(online_url: str, response_json: dict[str, Any]) -> dict[str, str]:
    device_id = response_json.get("device_id")
    device_token = response_json.get("device_token")
    if not isinstance(device_id, str) or not device_id.strip():
        raise RegistrationError("OnlineMainServer response missing a valid device_id.")
    if not isinstance(device_token, str) or not device_token.strip():
        raise RegistrationError("OnlineMainServer response missing a valid device_token.")

    identity = {
        "device_id": device_id,
        "device_token": device_token,
        "registered_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "online_url": online_url,
    }
    return validate_identity(identity)


def register_now(
    config: ConnectorConfig,
    *,
    enroll_token_override: str | None = None,
    client: OnlineMainServerClient | None = None,
) -> dict[str, str]:
    enroll_token = _require_enroll_token(config, override=enroll_token_override)
    registration_client = client or OnlineMainServerClient(config.online_url)
    try:
        response_json = registration_client.register_connector(
            enroll_token,
            _build_device_info(config),
        )
    except RegistrationNetworkError as exc:
        raise RegistrationError(
            "Could not reach OnlineMainServer after retries. Check network and ONLINE_URL."
        ) from exc
    except RegistrationAPIError as exc:
        if exc.error_code in TOKEN_ERROR_MESSAGES:
            raise RegistrationError(TOKEN_ERROR_MESSAGES[exc.error_code]) from exc
        raise RegistrationError(
            f"Registration failed: HTTP {exc.status_code}. Please try again."
        ) from exc

    identity = _build_identity(config.online_url, response_json)
    write_identity_atomic(config.identity_path, identity)
    LOGGER.info("Connector registered successfully with device_id=%s", identity["device_id"])
    return identity


def register_if_needed(
    config: ConnectorConfig,
    *,
    force: bool = False,
    enroll_token_override: str | None = None,
    client: OnlineMainServerClient | None = None,
) -> dict[str, str]:
    identity_path = Path(config.identity_path)

    if force and identity_path.exists():
        identity_path.unlink()
        LOGGER.info("Removed existing identity file due to --force.")

    if not force:
        try:
            existing = read_identity(identity_path)
        except IdentityValidationError:
            if identity_path.exists():
                bad_path = backup_corrupted_identity(identity_path)
                LOGGER.warning("Corrupted identity moved to %s", bad_path)
            existing = None
        if existing is not None:
            LOGGER.info("Identity already present, registration skipped.")
            return existing

    return register_now(
        config,
        enroll_token_override=enroll_token_override,
        client=client,
    )
