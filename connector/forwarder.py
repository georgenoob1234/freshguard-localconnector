import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from connector.config import ConnectorConfig
from connector.logging import get_logger
from connector.models import Envelope
from connector.outbox import OutboxDB

LOGGER = get_logger(__name__)

class ForwarderWorker:
    def __init__(self, config: ConnectorConfig, outbox_db: OutboxDB, identity: dict[str, str]):
        self.config = config
        self.outbox_db = outbox_db
        self.identity = identity
        self._cancel_event = asyncio.Event()
        self.last_forward_error: str | None = None
        self.last_forward_success_at: str | None = None

    async def start(self):
        LOGGER.info("Starting forwarder worker loop")
        self._cancel_event.clear()
        
        async with httpx.AsyncClient() as client:
            while not self._cancel_event.is_set():
                try:
                    await self._process_queued(client)
                except Exception as e:
                    LOGGER.error(f"Error in forwarder loop: {e}", exc_info=True)
                
                # Sleep based on poll interval, allowing cancellation
                try:
                    await asyncio.wait_for(
                        self._cancel_event.wait(), 
                        timeout=self.config.forward_poll_interval_ms / 1000.0
                    )
                except asyncio.TimeoutError:
                    pass

    def stop(self):
        LOGGER.info("Stopping forwarder worker loop")
        self._cancel_event.set()

    def get_runtime_stats(self) -> dict[str, str | None]:
        return {
            "last_forward_error": self.last_forward_error,
            "last_forward_success_at": self.last_forward_success_at,
        }

    async def _process_queued(self, client: httpx.AsyncClient):
        current_ts = int(time.time())
        rows = await self.outbox_db.get_queued(current_ts)
        
        for row in rows:
            if self._cancel_event.is_set():
                break

            image_id = row["image_id"]
            attempts = row["attempts"]

            await self.outbox_db.mark_sending(image_id)
            
            try:
                payload = json.loads(row["payload_json"])
                envelope = Envelope(
                    sent_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    image_id=image_id,
                    scan_result=payload,
                )

                url = self.identity["online_url"] + self.config.online_update_path
                headers = {
                    "Authorization": f"Bearer {self.identity['device_token']}",
                    "Idempotency-Key": image_id,
                    "Content-Type": "application/json",
                }

                response = await client.post(
                    url,
                    json=envelope.model_dump(),
                    headers=headers,
                    timeout=self.config.forward_timeout_seconds,
                )

                if response.status_code in (200, 201, 202, 409):
                    await self.outbox_db.mark_sent(image_id)
                    self.last_forward_success_at = (
                        datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                    )
                    LOGGER.info("Successfully forwarded image_id=%s", image_id)
                else:
                    raise httpx.HTTPStatusError(
                        f"HTTP {response.status_code}",
                        request=response.request,
                        response=response,
                    )

            except Exception as e:
                new_attempts = attempts + 1
                delay = min(
                    self.config.backoff_base_seconds * (2 ** (new_attempts - 1)),
                    self.config.backoff_max_seconds,
                )
                next_retry_ts = int(time.time()) + delay
                error_msg = str(e)
                self.last_forward_error = error_msg
                await self.outbox_db.mark_failed(
                    image_id=image_id,
                    attempts=new_attempts,
                    next_retry_ts=next_retry_ts,
                    error=error_msg,
                    max_attempts=self.config.outbox_max_attempts,
                )
                LOGGER.warning(
                    "Failed to forward image_id=%s, attempt=%s. Error: %s",
                    image_id,
                    new_attempts,
                    error_msg,
                )
