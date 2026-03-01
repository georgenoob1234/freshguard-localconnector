from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from connector.storage import IdentityValidationError, read_identity, write_identity_atomic


def _sample_identity() -> dict[str, str]:
    return {
        "device_id": "dev-123",
        "device_token": "tok-abc",
        "registered_at": "2026-01-01T00:00:00Z",
        "online_url": "https://example.com",
    }


def test_identity_write_read_atomic(tmp_path: Path) -> None:
    identity_path = tmp_path / "data" / "connector_identity.json"
    write_identity_atomic(identity_path, _sample_identity())

    loaded = read_identity(identity_path)
    assert loaded == _sample_identity()
    assert identity_path.parent.exists()

    leftover_tmp = list(identity_path.parent.glob("*.tmp"))
    assert leftover_tmp == []

    if os.name != "nt":
        mode = stat.S_IMODE(identity_path.stat().st_mode)
        assert mode == 0o600


def test_identity_validation_rejects_bad_schema(tmp_path: Path) -> None:
    identity_path = tmp_path / "connector_identity.json"
    identity_path.write_text('{"device_id":"only-one-key"}', encoding="utf-8")

    with pytest.raises(IdentityValidationError):
        read_identity(identity_path)
