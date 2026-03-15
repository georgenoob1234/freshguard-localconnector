from __future__ import annotations

import asyncio
import hashlib
import mimetypes
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import quote

import httpx

from connector.config import ConnectorConfig
from connector.logging import get_logger
from connector.outbox import OutboxDB

LOGGER = get_logger(__name__)

ALLOWED_REQUEST_TYPES = frozenset(
    {
        "ping",
        "device.info",
        "connector.stats",
        "camera.capture",
        "request_image",
        "tare",
    }
)
REQUEST_CACHE_TTL_SECONDS = 300
REQUEST_CACHE_MAX_ENTRIES = 256


@dataclass(frozen=True)
class RequestContext:
    request_id: str
    request_type: str
    params: dict[str, Any]
    accepted: bool
    reason: str | None


class CommandExecutionError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class CommandRejectedError(RuntimeError):
    """Raised when WeightServer rejects a valid request for business/device-state reasons."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _string_or_empty(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    return ""


def _build_image_fetch_url(camera_service_url: str, image_url_or_path: str) -> str:
    if image_url_or_path.startswith(("http://", "https://")):
        return image_url_or_path
    if image_url_or_path.startswith("/"):
        return f"{camera_service_url}{image_url_or_path}"
    return f"{camera_service_url}/{image_url_or_path}"


def _build_request_image_fetch_urls(
    camera_service_url: str, image_id: str
) -> tuple[str, ...]:
    normalized_image_id = quote(image_id, safe="")
    return (
        f"{camera_service_url}/api/images/{normalized_image_id}.jpg",
        f"{camera_service_url}/api/images/{normalized_image_id}.png",
    )


def _response_json_dict(response: httpx.Response) -> dict[str, Any] | None:
    try:
        payload = response.json()
    except ValueError:
        return None
    if isinstance(payload, dict):
        return payload
    return None


def _resolve_content_type(content_type_header: str | None, image_url_or_path: str) -> str:
    if content_type_header:
        normalized = content_type_header.split(";", 1)[0].strip()
        if normalized:
            return normalized
    guessed, _ = mimetypes.guess_type(image_url_or_path)
    if guessed:
        return guessed
    return "application/octet-stream"


def _resolve_upload_filename(
    image_id: str, image_url_or_path: str, content_type: str
) -> str:
    suffix = Path(image_url_or_path).suffix
    if not suffix:
        suffix = mimetypes.guess_extension(content_type) or ""
    return f"{image_id}{suffix}"


class WSCommandHandler:
    def __init__(
        self,
        config: ConnectorConfig,
        outbox_db: OutboxDB,
        identity: dict[str, str],
        *,
        started_at: str,
        get_forwarder_stats: Callable[[], dict[str, str | None]] | None = None,
        http_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.config = config
        self.outbox_db = outbox_db
        self.identity = identity
        self.started_at = started_at
        self.get_forwarder_stats = get_forwarder_stats or (lambda: {})
        self.http_transport = http_transport
        self._command_lock = asyncio.Lock()

    def build_ack_payload(self, payload: Mapping[str, Any]) -> tuple[dict[str, Any], RequestContext]:
        request_id = _string_or_empty(payload.get("request_id"))
        request_type = _string_or_empty(payload.get("request_type"))
        params = payload.get("params")
        params_dict = params if isinstance(params, dict) else {}
        accepted = request_type in ALLOWED_REQUEST_TYPES
        reason: str | None = None
        if not accepted:
            reason = "unknown_request_type"
        elif not request_id:
            accepted = False
            reason = "missing_request_id"
        elif request_type == "tare":
            mode = _string_or_empty(params_dict.get("mode")) if params_dict else ""
            if mode not in ("set", "reset"):
                accepted = False
                reason = "invalid_tare_params"
            elif accepted:
                LOGGER.info(
                    "Tare request received",
                    extra={
                        "request_id": request_id,
                        "mode": mode,
                        "device_id": self.identity.get("device_id", ""),
                    },
                )
        elif request_type == "request_image":
            image_id = _string_or_empty(params_dict.get("image_id")) if params_dict else ""
            if not image_id:
                accepted = False
                reason = "invalid_request_image_params"
            elif accepted:
                LOGGER.info(
                    "request_image request received",
                    extra={
                        "request_id": request_id,
                        "image_id": image_id,
                        "device_id": self.identity.get("device_id", ""),
                    },
                )

        ack_payload: dict[str, Any] = {
            "request_id": request_id,
            "request_type": request_type,
            "accepted": accepted,
        }
        if reason is not None:
            ack_payload["reason"] = reason

        context = RequestContext(
            request_id=request_id,
            request_type=request_type,
            params=params_dict,
            accepted=accepted,
            reason=reason,
        )
        return ack_payload, context

    async def build_response_payload(self, context: RequestContext) -> dict[str, Any]:
        if not context.request_id:
            return self._error_response(
                context,
                code="invalid_request",
                message="request_id is required.",
            )

        current_ts = int(time.time())
        cached = await self.outbox_db.get_cached_ws_response(
            context.request_id, current_ts
        )
        if cached is not None:
            return cached

        if not context.accepted:
            response = self._rejected_response(context, reason=context.reason)
            await self._cache_response(context.request_id, response, current_ts=current_ts)
            return response

        async with self._command_lock:
            current_ts = int(time.time())
            cached = await self.outbox_db.get_cached_ws_response(
                context.request_id, current_ts
            )
            if cached is not None:
                return cached

            response = await self._execute_with_timeout(context)
            await self._cache_response(context.request_id, response, current_ts=current_ts)
            return response

    async def handle_request_payload(
        self, payload: Mapping[str, Any]
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        ack_payload, context = self.build_ack_payload(payload)
        response_payload = await self.build_response_payload(context)
        error_payload = response_payload.get("error")
        error_code = ""
        if isinstance(error_payload, dict):
            error_code = _string_or_empty(error_payload.get("code"))
        LOGGER.info(
            "WS request processed",
            extra={
                "request_id": context.request_id,
                "request_type": context.request_type,
                "accepted": context.accepted,
                "response_status": _string_or_empty(response_payload.get("status")),
                "error_code": error_code,
                "device_id": self.identity.get("device_id", ""),
            },
        )
        return ack_payload, response_payload

    async def _cache_response(
        self, request_id: str, response_payload: dict[str, Any], *, current_ts: int
    ) -> None:
        await self.outbox_db.put_cached_ws_response(
            request_id=request_id,
            response_payload=response_payload,
            created_at_ts=current_ts,
            expires_at_ts=current_ts + REQUEST_CACHE_TTL_SECONDS,
            max_entries=REQUEST_CACHE_MAX_ENTRIES,
        )

    async def _execute_with_timeout(self, context: RequestContext) -> dict[str, Any]:
        try:
            data = await asyncio.wait_for(
                self._execute_request(context.request_type, context.params),
                timeout=self.config.command_timeout_seconds,
            )
            return {
                "request_id": context.request_id,
                "request_type": context.request_type,
                "status": "ok",
                "data": data,
                "error": None,
            }
        except asyncio.TimeoutError:
            return self._error_response(
                context,
                code="command_timeout",
                message=(
                    f"Command timed out after {self.config.command_timeout_seconds} seconds."
                ),
            )
        except CommandExecutionError as exc:
            return self._error_response(context, code=exc.code, message=exc.message)
        except CommandRejectedError as exc:
            return self._rejected_response(
                context, reason=None, code=exc.code, message=exc.message
            )
        except Exception as exc:  # pragma: no cover - defensive fallback
            return self._error_response(
                context,
                code="internal_error",
                message=str(exc),
            )

    async def _execute_request(self, request_type: str, params: dict[str, Any]) -> dict[str, Any]:
        if request_type == "ping":
            return {"pong": True}
        if request_type == "device.info":
            return {
                "device_id": self.identity["device_id"],
                "connector_version": self.config.connector_version,
                "hostname": self.config.hostname,
                "os": self.config.os_name,
                "started_at": self.started_at,
            }
        if request_type == "connector.stats":
            return await self._connector_stats()
        if request_type == "camera.capture":
            return await self._camera_capture(params)
        if request_type == "request_image":
            return await self._request_image(params)
        if request_type == "tare":
            return await self._tare(params)
        raise CommandExecutionError(
            "rejected_not_allowed",
            "Request type is not allowlisted.",
        )

    def _http_client_kwargs(self) -> dict[str, Any]:
        client_kwargs: dict[str, Any] = {}
        if self.http_transport is not None:
            client_kwargs["transport"] = self.http_transport
        client_kwargs["timeout"] = max(
            1.0,
            float(self.config.command_timeout_seconds) / 2.0,
        )
        return client_kwargs

    async def _upload_image_to_oms(
        self,
        *,
        client: httpx.AsyncClient,
        image_id: str,
        image_url_or_path: str,
        image_bytes: bytes,
        content_type_header: str | None,
        request_type: str,
    ) -> dict[str, Any]:
        content_type = _resolve_content_type(content_type_header, image_url_or_path)
        sha256 = hashlib.sha256(image_bytes).hexdigest()
        size_bytes = len(image_bytes)
        upload_url = f"{self.identity['online_url']}{self.config.oms_blob_upload_path}"
        upload_filename = _resolve_upload_filename(
            image_id, image_url_or_path, content_type
        )
        LOGGER.info(
            "OMS blob upload started",
            extra={
                "request_type": request_type,
                "image_id": image_id,
                "content_type": content_type,
                "size_bytes": size_bytes,
                "device_id": self.identity.get("device_id", ""),
            },
        )
        upload_response = await client.post(
            upload_url,
            headers={"Authorization": f"Bearer {self.identity['device_token']}"},
            data={
                "image_id": image_id,
                "content_type": content_type,
                "sha256": sha256,
            },
            files={
                "file": (upload_filename, image_bytes, content_type),
            },
        )
        if upload_response.status_code >= 400:
            LOGGER.warning(
                "OMS blob upload failed",
                extra={
                    "request_type": request_type,
                    "image_id": image_id,
                    "http_status": upload_response.status_code,
                    "device_id": self.identity.get("device_id", ""),
                },
            )
            raise CommandExecutionError(
                "blob_upload_failed",
                f"OMS blob upload failed with HTTP {upload_response.status_code}.",
            )
        upload_data = _response_json_dict(upload_response)
        blob_id = _string_or_empty(upload_data.get("blob_id") if upload_data else None)
        if not blob_id:
            LOGGER.warning(
                "OMS blob upload response missing blob_id",
                extra={
                    "request_type": request_type,
                    "image_id": image_id,
                    "device_id": self.identity.get("device_id", ""),
                },
            )
            raise CommandExecutionError(
                "blob_upload_failed",
                "OMS blob upload response missing blob_id.",
            )

        LOGGER.info(
            "OMS blob upload succeeded",
            extra={
                "request_type": request_type,
                "image_id": image_id,
                "blob_id": blob_id,
                "device_id": self.identity.get("device_id", ""),
            },
        )
        return {
            "image_id": image_id,
            "blob_id": blob_id,
            "sha256": sha256,
            "size_bytes": size_bytes,
            "content_type": content_type,
        }

    async def _connector_stats(self) -> dict[str, Any]:
        counts = await self.outbox_db.get_status_counts()
        runtime_stats = self.get_forwarder_stats()
        last_error = runtime_stats.get("last_forward_error")
        if not isinstance(last_error, str) or not last_error.strip():
            last_error = await self.outbox_db.get_last_error()
        if isinstance(last_error, str):
            last_error = last_error[:240]
        else:
            last_error = None

        last_success = runtime_stats.get("last_forward_success_at")
        if not isinstance(last_success, str) or not last_success.strip():
            last_success = None

        return {
            "queued": int(counts.get("queued", 0)),
            "sending": int(counts.get("sending", 0)),
            "sent": int(counts.get("sent", 0)),
            "dead": int(counts.get("dead", 0)),
            "last_forward_error": last_error,
            "last_forward_success_at": last_success,
        }

    async def _camera_capture(self, params: dict[str, Any]) -> dict[str, Any]:
        capture_payload = {
            key: params[key]
            for key in ("resolution", "format", "quality")
            if key in params and params[key] is not None
        }

        async with httpx.AsyncClient(**self._http_client_kwargs()) as client:
            capture_kwargs: dict[str, Any] = {}
            if capture_payload:
                capture_kwargs["json"] = capture_payload
            capture_url = f"{self.config.camera_service_url}/capture"
            capture_response = await client.post(capture_url, **capture_kwargs)
            if capture_response.status_code >= 400:
                raise CommandExecutionError(
                    "camera_unavailable",
                    f"Camera service capture failed with HTTP {capture_response.status_code}.",
                )
            capture_data = _response_json_dict(capture_response)
            if capture_data is None:
                raise CommandExecutionError(
                    "camera_unavailable",
                    "Camera service returned invalid capture JSON.",
                )

            image_id = _string_or_empty(capture_data.get("image_id"))
            image_url_or_path = _string_or_empty(capture_data.get("image_url_or_path"))
            if not image_url_or_path:
                image_url_or_path = _string_or_empty(capture_data.get("image_url"))
            if not image_url_or_path:
                image_url_or_path = _string_or_empty(capture_data.get("image_path"))
            if not image_id or not image_url_or_path:
                raise CommandExecutionError(
                    "camera_unavailable",
                    "Camera service response missing image_id or image URL/path.",
                )

            fetch_url = _build_image_fetch_url(
                self.config.camera_service_url, image_url_or_path
            )
            image_response = await client.get(fetch_url)
            if image_response.status_code >= 400:
                raise CommandExecutionError(
                    "camera_fetch_failed",
                    f"Failed to fetch camera image with HTTP {image_response.status_code}.",
                )
            image_bytes = image_response.content
            if not image_bytes:
                raise CommandExecutionError(
                    "camera_fetch_failed",
                    "Camera image fetch returned empty content.",
                )

            return await self._upload_image_to_oms(
                client=client,
                image_id=image_id,
                image_url_or_path=image_url_or_path,
                image_bytes=image_bytes,
                content_type_header=image_response.headers.get("content-type"),
                request_type="camera.capture",
            )

    async def _request_image(self, params: dict[str, Any]) -> dict[str, Any]:
        image_id = _string_or_empty(params.get("image_id"))
        if not image_id:
            raise CommandExecutionError(
                "invalid_request_image_params",
                "Invalid or missing image_id.",
            )

        fetch_urls = _build_request_image_fetch_urls(
            self.config.camera_service_url, image_id
        )
        LOGGER.info(
            "request_image fetch started",
            extra={
                "request_type": "request_image",
                "image_id": image_id,
                "device_id": self.identity.get("device_id", ""),
            },
        )
        last_status_code: int | None = None

        try:
            async with httpx.AsyncClient(**self._http_client_kwargs()) as client:
                for fetch_url in fetch_urls:
                    image_response = await client.get(fetch_url)
                    last_status_code = image_response.status_code
                    if image_response.status_code == 404:
                        continue
                    if image_response.status_code >= 400:
                        LOGGER.warning(
                            "request_image fetch failed",
                            extra={
                                "request_type": "request_image",
                                "image_id": image_id,
                                "http_status": image_response.status_code,
                                "device_id": self.identity.get("device_id", ""),
                            },
                        )
                        raise CommandExecutionError(
                            "camera_fetch_failed",
                            (
                                "Failed to fetch camera image with "
                                f"HTTP {image_response.status_code}."
                            ),
                        )

                    image_bytes = image_response.content
                    if not image_bytes:
                        raise CommandExecutionError(
                            "camera_fetch_failed",
                            "Camera image fetch returned empty content.",
                        )
                    LOGGER.info(
                        "request_image fetch succeeded",
                        extra={
                            "request_type": "request_image",
                            "image_id": image_id,
                            "device_id": self.identity.get("device_id", ""),
                        },
                    )
                    return await self._upload_image_to_oms(
                        client=client,
                        image_id=image_id,
                        image_url_or_path=fetch_url,
                        image_bytes=image_bytes,
                        content_type_header=image_response.headers.get("content-type"),
                        request_type="request_image",
                    )
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout) as exc:
            LOGGER.warning(
                "request_image camera service unavailable",
                extra={
                    "request_type": "request_image",
                    "image_id": image_id,
                    "device_id": self.identity.get("device_id", ""),
                    "error": str(exc),
                },
            )
            raise CommandExecutionError(
                "camera_unavailable",
                f"Camera service unreachable or timed out: {exc!s}",
            ) from exc

        if last_status_code == 404:
            LOGGER.warning(
                "request_image not found",
                extra={
                    "request_type": "request_image",
                    "image_id": image_id,
                    "device_id": self.identity.get("device_id", ""),
                },
            )
            raise CommandExecutionError(
                "camera_fetch_failed",
                "Camera image was not found for provided image_id.",
            )
        raise CommandExecutionError(
            "camera_fetch_failed",
            "Failed to fetch camera image.",
        )

    async def _tare(self, params: dict[str, Any]) -> dict[str, Any]:
        mode = _string_or_empty(params.get("mode"))
        if mode not in ("set", "reset"):
            raise CommandExecutionError(
                "invalid_tare_params",
                "Invalid or missing tare mode. Must be 'set' or 'reset'.",
            )

        device_id = self.identity.get("device_id", "")
        LOGGER.info(
            "Tare execution started",
            extra={"mode": mode, "device_id": device_id},
        )

        client_kwargs: dict[str, Any] = {}
        if self.http_transport is not None:
            client_kwargs["transport"] = self.http_transport
        client_kwargs["timeout"] = max(
            1.0,
            float(self.config.command_timeout_seconds) / 2.0,
        )

        path = "/tare" if mode == "set" else "/tare/reset"
        url = f"{self.config.weight_server_url}{path}"

        try:
            async with httpx.AsyncClient(**client_kwargs) as client:
                response = await client.post(url)
                if response.status_code >= 500:
                    LOGGER.warning(
                        "Tare execution failed",
                        extra={
                            "mode": mode,
                            "device_id": device_id,
                            "code": "weight_server_error",
                            "http_status": response.status_code,
                        },
                    )
                    raise CommandExecutionError(
                        "weight_server_error",
                        f"WeightServer returned HTTP {response.status_code}.",
                    )
                if response.status_code >= 400:
                    data = _response_json_dict(response)
                    code = "tare_rejected"
                    message = "WeightServer rejected the tare request."
                    if isinstance(data, dict):
                        code = _string_or_empty(data.get("code")) or code
                        msg_val = data.get("message") or data.get("error")
                        if isinstance(msg_val, str) and msg_val.strip():
                            message = msg_val.strip()
                    LOGGER.warning(
                        "Tare rejected",
                        extra={
                            "mode": mode,
                            "device_id": device_id,
                            "code": code,
                            "http_status": response.status_code,
                        },
                    )
                    raise CommandRejectedError(code=code, message=message)
                LOGGER.info(
                    "Tare execution finished",
                    extra={"mode": mode, "device_id": device_id},
                )
                return {"accepted": True, "mode": mode}
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout) as exc:
            LOGGER.warning(
                "Tare execution failed",
                extra={
                    "mode": mode,
                    "device_id": device_id,
                    "code": "weight_server_unavailable",
                    "error": str(exc),
                },
            )
            raise CommandExecutionError(
                "weight_server_unavailable",
                f"WeightServer unreachable or timed out: {exc!s}",
            ) from exc

    def _rejected_response(
        self,
        context: RequestContext,
        *,
        reason: str | None = None,
        code: str | None = None,
        message: str | None = None,
    ) -> dict[str, Any]:
        if code is not None and message is not None:
            err = {"code": code, "message": message}
        elif reason == "invalid_tare_params":
            err = {
                "code": "invalid_tare_params",
                "message": "Invalid or missing tare mode. Must be 'set' or 'reset'.",
            }
        elif reason == "invalid_request_image_params":
            err = {
                "code": "invalid_request_image_params",
                "message": "Invalid or missing image_id.",
            }
        else:
            err = {
                "code": "rejected_not_allowed",
                "message": "Request type is not allowlisted.",
            }
        return {
            "request_id": context.request_id,
            "request_type": context.request_type,
            "status": "rejected",
            "error": err,
        }

    def _error_response(
        self,
        context: RequestContext,
        *,
        code: str,
        message: str,
    ) -> dict[str, Any]:
        return {
            "request_id": context.request_id,
            "request_type": context.request_type,
            "status": "error",
            "error": {"code": code, "message": message},
        }
