import os
from unittest.mock import patch
import pytest

from connector.main import perform_startup_registration
from connector.config import ConnectorConfig

def test_missing_identity_missing_env_vars(tmp_path):
    identity_path = tmp_path / "missing.json"
    config = ConnectorConfig(identity_path=identity_path)
    
    # Ensure ONLINE_URL and ENROLL_TOKEN are missing
    config.online_url = None
    config.enroll_token = None
    
    with pytest.raises(SystemExit) as exc_info:
        perform_startup_registration(config)
    
    assert exc_info.value.code == 1

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
