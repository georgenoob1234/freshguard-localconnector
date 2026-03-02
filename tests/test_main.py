import pytest

from connector.config import ConnectorConfig, resolve_config
from connector.main import perform_startup_registration


def test_missing_identity_missing_env_vars(tmp_path):
    identity_path = tmp_path / "missing.json"
    config = ConnectorConfig(identity_path=identity_path)
    
    # Ensure ONLINE_URL and ENROLL_TOKEN are missing
    config.online_url = None
    config.enroll_token = None
    
    with pytest.raises(SystemExit) as exc_info:
        perform_startup_registration(config)
    
    assert exc_info.value.code == 1


def test_resolve_config_normalizes_online_paths(monkeypatch):
    monkeypatch.setenv("ONLINE_UPDATE_PATH", "update")
    monkeypatch.setenv("ONLINE_WS_PATH", "connector-ws")
    monkeypatch.setenv("OMS_BLOB_UPLOAD_PATH", "connector-blobs")

    config = resolve_config(online_url_override="https://online.test/")

    assert config.online_url == "https://online.test"
    assert config.online_update_path == "/update"
    assert config.online_ws_path == "/connector-ws"
    assert config.oms_blob_upload_path == "/connector-blobs"

def test_missing_identity_invalid_url(tmp_path):
    identity_path = tmp_path / "missing.json"
    config = ConnectorConfig(
        identity_path=identity_path,
        online_url="ftp://invalid",
        enroll_token="token123"
    )
    
    with pytest.raises(SystemExit) as exc_info:
        perform_startup_registration(config)
    
    assert exc_info.value.code == 1
