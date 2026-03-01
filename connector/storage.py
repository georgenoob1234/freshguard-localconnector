from __future__ import annotations

import contextlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Mapping


IDENTITY_KEYS = ("device_id", "device_token", "registered_at", "online_url")


class IdentityValidationError(ValueError):
    """Raised when the identity file content is invalid."""


def validate_identity(data: Mapping[str, Any]) -> dict[str, str]:
    if not isinstance(data, dict):
        raise IdentityValidationError("Identity must be a JSON object.")

    keys = set(data.keys())
    expected = set(IDENTITY_KEYS)
    if keys != expected:
        raise IdentityValidationError(
            f"Identity keys mismatch. Expected exactly: {sorted(expected)}"
        )

    validated: dict[str, str] = {}
    for key in IDENTITY_KEYS:
        value = data.get(key)
        if not isinstance(value, str) or not value.strip():
            raise IdentityValidationError(f"Identity field '{key}' must be non-empty.")
        validated[key] = value
    return validated


def read_identity(path: Path) -> dict[str, str] | None:
    if not path.exists():
        return None

    try:
        raw = path.read_text(encoding="utf-8")
        parsed = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise IdentityValidationError("Identity file is unreadable or invalid JSON.") from exc

    return validate_identity(parsed)


def _set_secure_permissions(path: Path) -> None:
    if os.name == "nt":
        return
    with contextlib.suppress(OSError):
        path.chmod(0o600)


def write_identity_atomic(path: Path, data: Mapping[str, Any]) -> None:
    validated = validate_identity(data)
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    tmp_path = Path(tmp_name)

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(validated, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        _set_secure_permissions(tmp_path)
        os.replace(tmp_path, path)
        _set_secure_permissions(path)
    except Exception:
        with contextlib.suppress(FileNotFoundError):
            tmp_path.unlink()
        raise


def backup_corrupted_identity(path: Path) -> Path:
    candidate = path.with_name(f"{path.name}.bad")
    index = 1
    while candidate.exists():
        candidate = path.with_name(f"{path.name}.{index}.bad")
        index += 1
    path.rename(candidate)
    return candidate
