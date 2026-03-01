from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from connector.outbox import OutboxDB


def _create_legacy_outbox(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE outbox (
                scan_id TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_retry_ts INTEGER,
                created_at_ts INTEGER NOT NULL,
                last_error TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO outbox (
                scan_id,
                payload_json,
                status,
                attempts,
                next_retry_ts,
                created_at_ts,
                last_error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-1",
                '{"session_id":"s1","image_id":"legacy-1","fruits":[]}',
                "queued",
                3,
                int(time.time()) - 60,
                int(time.time()) - 120,
                "legacy error",
            ),
        )
        conn.commit()


@pytest.mark.asyncio
async def test_outbox_migrates_scan_id_schema_to_image_id(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"
    _create_legacy_outbox(db_path)

    outbox = OutboxDB(db_path)
    await outbox.init_db()

    with sqlite3.connect(db_path) as conn:
        columns = [row[1] for row in conn.execute("PRAGMA table_info(outbox)").fetchall()]
    assert "image_id" in columns
    assert "scan_id" not in columns

    queued_rows = await outbox.get_queued(current_ts=int(time.time()) + 3600)
    assert len(queued_rows) == 1
    assert queued_rows[0]["image_id"] == "legacy-1"
    assert queued_rows[0]["attempts"] == 3
    assert queued_rows[0]["last_error"] == "legacy error"
