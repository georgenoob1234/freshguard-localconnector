from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import requests

from connector.logging import get_logger


LOGGER = get_logger(__name__)


TOKEN_ERROR_CODES = {"TOKEN_INVALID", "TOKEN_EXPIRED", "TOKEN_USED_UP"}


@dataclass
class RegistrationAPIError(Exception):
    message: str
    status_code: int
    error_code: str | None = None

    def __str__(self) -> str:
        return self.message


class RegistrationNetworkError(Exception):
    """Raised when registration fails after retrying network errors."""


class OnlineMainServerClient:
    def __init__(
        self,
        online_url: str,
        *,
        timeout: tuple[float, float] = (5.0, 10.0),
        max_attempts: int = 8,
        initial_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 30.0,
        sleep_fn: Any = time.sleep,
        session: requests.Session | None = None,
    ) -> None:
        self.online_url = online_url.rstrip("/")
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.initial_backoff_seconds = initial_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.sleep_fn = sleep_fn
        self.session = session or requests.Session()

    def register_connector(self, enroll_token: str, device_info: dict[str, str]) -> dict[str, Any]:
        url = f"{self.online_url}/connector/v1/register"
        payload = {
            "enroll_token": enroll_token,
            "device_info": device_info,
        }

        last_exception: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                response = self.session.post(url, json=payload, timeout=self.timeout)
                return self._handle_response(response)
            except requests.RequestException as exc:
                last_exception = exc
                if attempt >= self.max_attempts:
                    break
                delay = min(
                    self.initial_backoff_seconds * (2 ** (attempt - 1)),
                    self.max_backoff_seconds,
                )
                LOGGER.warning(
                    "Registration network failure (attempt %d/%d). Retrying in %.1fs.",
                    attempt,
                    self.max_attempts,
                    delay,
                )
                self.sleep_fn(delay)

        raise RegistrationNetworkError(
            f"Failed to reach OnlineMainServer after {self.max_attempts} attempts."
        ) from last_exception

    @staticmethod
    def _safe_json(response: requests.Response) -> dict[str, Any] | None:
        try:
            parsed = response.json()
        except ValueError:
            return None
        if isinstance(parsed, dict):
            return parsed
        return None

    def _handle_response(self, response: requests.Response) -> dict[str, Any]:
        data = self._safe_json(response)
        if response.status_code == 200:
            if data is None:
                raise RegistrationAPIError(
                    message="OnlineMainServer returned invalid JSON.",
                    status_code=response.status_code,
                    error_code=None,
                )
            return data

        error_code = data.get("error_code") if data else None
        if error_code in TOKEN_ERROR_CODES:
            message = f"Registration rejected with {error_code}."
        else:
            message = f"Registration failed with HTTP {response.status_code}."

        raise RegistrationAPIError(
            message=message,
            status_code=response.status_code,
            error_code=error_code,
        )
