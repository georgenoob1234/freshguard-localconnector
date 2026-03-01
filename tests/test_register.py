from __future__ import annotations

from pathlib import Path

import pytest

from connector.config import ConnectorConfig
from connector.http import OnlineMainServerClient
from connector.register import RegistrationError, register_if_needed, register_now
from connector.storage import read_identity


class StubClient:
    def __init__(self, response: dict[str, str | None]) -> None:
        self.response = response
        self.calls = 0

    def register_connector(self, enroll_token: str, device_info: dict[str, str]) -> dict[str, str | None]:
        self.calls += 1
        return self.response


class MockResponse:
    def __init__(self, status_code: int, payload: dict[str, str]) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict[str, str]:
        return self._payload


class MockSession:
    def __init__(self, responses: list[MockResponse]) -> None:
        self.responses = responses
        self.calls = 0

    def post(self, url: str, json: dict[str, object], timeout: tuple[float, float]) -> MockResponse:
        self.calls += 1
        return self.responses.pop(0)


def _config(tmp_path: Path, enroll_token: str | None = "enroll-123") -> ConnectorConfig:
    return ConnectorConfig(
        online_url="https://example.com",
        identity_path=tmp_path / "connector_identity.json",
        enroll_token=enroll_token,
        label="connector-A",
        os_name="linux",
        hostname="host-A",
        connector_version="1.0.0",
    )


def test_corrupted_identity_is_renamed_then_registered(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    cfg.identity_path.write_text("{this-is-not-json", encoding="utf-8")
    client = StubClient(
        {"device_id": "dev-9", "device_token": "tok-9", "ws_url": None}
    )

    identity = register_if_needed(cfg, client=client)

    assert client.calls == 1
    assert identity["device_id"] == "dev-9"
    assert read_identity(cfg.identity_path) is not None
    bad_files = list(tmp_path.glob("connector_identity.json*.bad"))
    assert len(bad_files) == 1


@pytest.mark.parametrize(
    ("error_code", "expected_text"),
    [
        ("TOKEN_INVALID", "invalid"),
        ("TOKEN_EXPIRED", "expired"),
        ("TOKEN_USED_UP", "used up"),
    ],
)
def test_register_call_maps_token_errors_from_http_response(
    tmp_path: Path, error_code: str, expected_text: str
) -> None:
    session = MockSession([MockResponse(401, {"error_code": error_code})])
    client = OnlineMainServerClient(
        "https://example.com",
        session=session,  # type: ignore[arg-type]
        sleep_fn=lambda _: None,
    )

    with pytest.raises(RegistrationError, match=expected_text):
        register_now(_config(tmp_path), enroll_token_override="token-from-cli", client=client)

    assert session.calls == 1
