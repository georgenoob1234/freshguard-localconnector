from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import aiosqlite

from connector.logging import get_logger

LOGGER = get_logger(__name__)


CREATE_OUTBOX_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS outbox (
    image_id TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_ts INTEGER,
    created_at_ts INTEGER NOT NULL,
    last_error TEXT
)
"""

CREATE_WS_REQUEST_CACHE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ws_request_cache (
    request_id TEXT PRIMARY KEY,
    response_json TEXT NOT NULL,
    created_at_ts INTEGER NOT NULL,
    expires_at_ts INTEGER NOT NULL
)
"""


class OutboxDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    async def init_db(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            if not await self._outbox_exists(db):
                await db.execute(CREATE_OUTBOX_TABLE_SQL)
            else:
                columns = await self._outbox_columns(db)
                if "scan_id" in columns:
                    await self._migrate_scan_id_to_image_id(db)
                elif "image_id" not in columns:
                    raise RuntimeError(
                        "Unsupported outbox schema: expected image_id or scan_id column."
                    )
            await db.execute(CREATE_WS_REQUEST_CACHE_TABLE_SQL)
            await db.commit()

    async def _outbox_exists(self, db: aiosqlite.Connection) -> bool:
        async with db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='outbox'"
        ) as cursor:
            return await cursor.fetchone() is not None

    async def _outbox_columns(self, db: aiosqlite.Connection) -> set[str]:
        async with db.execute("PRAGMA table_info(outbox)") as cursor:
            rows = await cursor.fetchall()
        return {str(row[1]) for row in rows}

    async def _migrate_scan_id_to_image_id(self, db: aiosqlite.Connection) -> None:
        LOGGER.info("Migrating outbox schema: scan_id -> image_id")
        await db.execute("ALTER TABLE outbox RENAME TO outbox_legacy_scan_id")
        await db.execute(CREATE_OUTBOX_TABLE_SQL)
        await db.execute(
            """
            INSERT INTO outbox (
                image_id,
                payload_json,
                status,
                attempts,
                next_retry_ts,
                created_at_ts,
                last_error
            )
            SELECT
                scan_id,
                payload_json,
                status,
                attempts,
                next_retry_ts,
                created_at_ts,
                last_error
            FROM outbox_legacy_scan_id
            """
        )
        await db.execute("DROP TABLE outbox_legacy_scan_id")

    async def insert(
        self, image_id: str, payload: dict[str, Any], created_at_ts: int
    ) -> bool:
        """Returns True if inserted, False if duplicate"""
        try:
            payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    """
                    INSERT INTO outbox (image_id, payload_json, status, created_at_ts)
                    VALUES (?, ?, ?, ?)
                    """,
                    (image_id, payload_json, "queued", created_at_ts),
                )
                await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

    async def get_queued(self, current_ts: int) -> list[dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT * FROM outbox 
                WHERE status = 'queued' 
                  AND (next_retry_ts IS NULL OR next_retry_ts <= ?)
                ORDER BY created_at_ts ASC
                """,
                (current_ts,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def mark_sending(self, image_id: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE outbox SET status = 'sending' WHERE image_id = ?",
                (image_id,),
            )
            await db.commit()

    async def mark_sent(self, image_id: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE outbox SET status = 'sent' WHERE image_id = ?",
                (image_id,),
            )
            await db.commit()

    async def mark_failed(
        self,
        image_id: str,
        attempts: int,
        next_retry_ts: int,
        error: str,
        max_attempts: int,
    ) -> None:
        new_status = "queued"
        if attempts >= max_attempts:
            new_status = "dead"

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE outbox 
                SET status = ?, attempts = ?, next_retry_ts = ?, last_error = ? 
                WHERE image_id = ?
                """,
                (new_status, attempts, next_retry_ts, error, image_id),
            )
            await db.commit()

    async def get_status_counts(self) -> dict[str, int]:
        counts = {"queued": 0, "sending": 0, "sent": 0, "dead": 0}
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT status, COUNT(*) FROM outbox GROUP BY status"
            ) as cursor:
                rows = await cursor.fetchall()
        for status, count in rows:
            if status in counts:
                counts[str(status)] = int(count)
        return counts

    async def get_last_error(self) -> str | None:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT last_error
                FROM outbox
                WHERE last_error IS NOT NULL AND last_error != ''
                ORDER BY created_at_ts DESC
                LIMIT 1
                """
            ) as cursor:
                row = await cursor.fetchone()
        if row is None:
            return None
        return str(row[0])

    async def get_cached_ws_response(
        self, request_id: str, current_ts: int
    ) -> dict[str, Any] | None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "DELETE FROM ws_request_cache WHERE expires_at_ts <= ?",
                (current_ts,),
            )
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT response_json
                FROM ws_request_cache
                WHERE request_id = ?
                """,
                (request_id,),
            ) as cursor:
                row = await cursor.fetchone()
            await db.commit()
        if row is None:
            return None
        try:
            parsed = json.loads(row["response_json"])
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, dict):
            return parsed
        return None

    async def put_cached_ws_response(
        self,
        request_id: str,
        response_payload: dict[str, Any],
        created_at_ts: int,
        expires_at_ts: int,
        *,
        max_entries: int = 256,
    ) -> None:
        response_json = json.dumps(response_payload, separators=(",", ":"), sort_keys=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO ws_request_cache (
                    request_id,
                    response_json,
                    created_at_ts,
                    expires_at_ts
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(request_id)
                DO UPDATE SET
                    response_json = excluded.response_json,
                    created_at_ts = excluded.created_at_ts,
                    expires_at_ts = excluded.expires_at_ts
                """,
                (request_id, response_json, created_at_ts, expires_at_ts),
            )
            await self._prune_ws_cache(db, max_entries=max_entries)
            await db.commit()

    async def _prune_ws_cache(
        self, db: aiosqlite.Connection, *, max_entries: int
    ) -> None:
        if max_entries <= 0:
            return
        async with db.execute("SELECT COUNT(*) FROM ws_request_cache") as cursor:
            row = await cursor.fetchone()
        total = int(row[0]) if row else 0
        overflow = total - max_entries
        if overflow <= 0:
            return
        async with db.execute(
            """
            SELECT request_id
            FROM ws_request_cache
            ORDER BY created_at_ts ASC
            LIMIT ?
            """,
            (overflow,),
        ) as cursor:
            old_rows = await cursor.fetchall()
        if not old_rows:
            return
        await db.executemany(
            "DELETE FROM ws_request_cache WHERE request_id = ?",
            [(str(row[0]),) for row in old_rows],
        )
