from __future__ import annotations

from datetime import datetime
from typing import Any, List

from pydantic import BaseModel, ConfigDict


class ScanResult(BaseModel):
    session_id: str
    image_id: str
    timestamp: datetime
    weight_grams: float
    fruits: List[Any]

    model_config = ConfigDict(extra="allow")


class Envelope(BaseModel):
    envelope_version: str = "v1"
    sent_at: str
    image_id: str
    scan_result: dict[str, Any]
