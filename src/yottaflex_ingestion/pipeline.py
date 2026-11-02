from __future__ import annotations

import csv
import hashlib
import json
import logging
import sqlite3
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from yottaflex_ingestion.database import connect_database, initialize_database
from yottaflex_ingestion.models import (
    REQUIRED_COLUMNS,
    Event,
    normalize_timestamp,
    serialize_record,
    validate_event,
)


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunResult:
    run_id: str
    source_name: str
    source_file: str
    status: str
    input_rows: int
    extracted_count: int
    applied_count: int
    stale_count: int
    duplicate_count: int
    rejected_count: int
    unextractable_count: int
    skipped_count: int
    batch_count: int
    error_message: str | None
    watermark_arrived_at: str | None
    watermark_event_key: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _fingerprint(source_file: str, row_number: int, raw_record: str) -> str:
    return _sha256(f"{source_file}\0{row_number}\0{raw_record}")


def _store_reject(
    connection: sqlite3.Connection,
    run_id: str,
    source_file: str,
    row_number: int,
    event_id: str | None,
    reason: str,
    raw_record: str,
    fingerprint: str,
) -> None:
    connection.execute(
        """
        INSERT INTO rejected_records (
            fingerprint, source_file, source_row_number, run_id, event_id,
            reason, raw_record, rejected_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(fingerprint) DO NOTHING
        """,
        (fingerprint, source_file, row_number, run_id, event_id, reason, raw_record, _now()),
    )


def _update_run_metrics(connection: sqlite3.Connection, run_id: str, metrics: dict[str, int]) -> None:
    connection.execute(
        """
        UPDATE ingestion_runs SET
            input_rows = ?, extracted_count = ?, applied_count = ?, stale_count = ?,
            duplicate_count = ?, rejected_count = ?, unextractable_count = ?,
            skipped_count = ?, batch_count = ?
        WHERE run_id = ?
        """,
        (
            metrics["input_rows"],
            metrics["extracted_count"],
            metrics["applied_count"],
            metrics["stale_count"],
            metrics["duplicate_count"],
            metrics["rejected_count"],
            metrics["unextractable_count"],
            metrics["skipped_count"],
            metrics["batch_count"],
            run_id,
        ),
    )


def _stage_source(
    connection: sqlite3.Connection,
    source_path: Path,
    run_id: str,
    source_file: str,
    metrics: dict[str, int],
) -> None:
    connection.execute(
        """
        CREATE TEMP TABLE source_stage (
            source_row_number INTEGER PRIMARY KEY,
            arrived_at TEXT,
            cursor_key TEXT NOT NULL,
            event_id TEXT,
            content_hash TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            raw_record TEXT NOT NULL
        )
        """
    )
    connection.execute("CREATE INDEX source_stage_cursor ON source_stage(arrived_at, cursor_key)")
    with source_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames
        if not fieldnames:
            raise ValueError("CSV input must have a header row")
        if len(fieldnames) != len(set(fieldnames)):
            raise ValueError("CSV header contains duplicate column names")
        if tuple(fieldnames) != REQUIRED_COLUMNS:
            raise ValueError(
                f"unexpected CSV schema; expected columns in this order: {', '.join(REQUIRED_COLUMNS)}"
            )
        for row_number, record in enumerate(reader, start=2):
            metrics["input_rows"] += 1
            raw_record = serialize_record(record)
            content_hash = _sha256(raw_record)
            fingerprint = _fingerprint(source_file, row_number, raw_record)
            event_id_value = record.get("event_id")
            event_id = event_id_value.strip() if isinstance(event_id_value, str) and event_id_value.strip() else None
            cursor_key = f"{row_number:012d}::{event_id or ''}"
            try:
                arrived_at = normalize_timestamp(record.get("arrived_at"))
            except (TypeError, ValueError):
                _store_reject(
                    connection,
                    run_id,
                    source_file,
                    row_number,
                    event_id,
                    "arrived_at must be an ISO-8601 timestamp with timezone",
                    raw_record,
                    fingerprint,
                )
                metrics["rejected_count"] += 1
                metrics["unextractable_count"] += 1
                continue
            connection.execute(
                """
                INSERT INTO source_stage (
                    source_row_number, arrived_at, cursor_key, event_id,
                    content_hash, fingerprint, raw_record
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (row_number, arrived_at, cursor_key, event_id, content_hash, fingerprint, raw_record),
            )


def _is_behind_watermark(
    arrived_at: str,
    cursor_key: str,
    watermark_arrived_at: str | None,
    watermark_event_key: str | None,
) -> bool:
    if watermark_arrived_at is None or watermark_event_key is None:
        return False
    return (arrived_at, cursor_key) <= (watermark_arrived_at, watermark_event_key)


def _is_stale(connection: sqlite3.Connection, event: Event) -> bool:
    table = "customers" if event.entity_type == "customer" else "orders"
    key_column = "customer_id" if event.entity_type == "customer" else "order_id"
    current = connection.execute(
        f"SELECT source_updated_at, last_event_id FROM {table} WHERE {key_column} = ?",
        (event.record_id,),
    ).fetchone()
    if current is None:
        return False
    return (event.source_updated_at, event.event_id) <= (current["source_updated_at"], current["last_event_id"])


def _apply_event(connection: sqlite3.Connection, event: Event) -> tuple[str | None, str | None]:
    if _is_stale(connection, event):
        return "stale", None

    if event.entity_type == "customer":
        if event.operation == "upsert":
            conflicting_email = connection.execute(
                "SELECT customer_id FROM customers WHERE email = ? AND is_deleted = 0 AND customer_id <> ?",
                (event.payload["email"], event.record_id),
            ).fetchone()
            if conflicting_email is not None:
                return None, "customer email is already assigned to another active customer"
            connection.execute(
                """
                INSERT INTO customers (
                    customer_id, name, email, is_deleted, source_updated_at, last_event_id
                ) VALUES (?, ?, ?, 0, ?, ?)
                ON CONFLICT(customer_id) DO UPDATE SET
                    name = excluded.name,
                    email = excluded.email,
                    is_deleted = 0,
                    source_updated_at = excluded.source_updated_at,
                    last_event_id = excluded.last_event_id
                """,
                (
                    event.record_id,
                    event.payload["name"],
                    event.payload["email"],
                    event.source_updated_at,
                    event.event_id,
                ),
            )
        else:
            connection.execute(
                """
                INSERT INTO customers (
                    customer_id, name, email, is_deleted, source_updated_at, last_event_id
                ) VALUES (?, NULL, NULL, 1, ?, ?)
                ON CONFLICT(customer_id) DO UPDATE SET
                    is_deleted = 1,
                    source_updated_at = excluded.source_updated_at,
                    last_event_id = excluded.last_event_id
                """,
                (event.record_id, event.source_updated_at, event.event_id),
            )
    elif event.operation == "upsert":
        customer = connection.execute(
            "SELECT is_deleted FROM customers WHERE customer_id = ?",
            (event.payload["customer_id"],),
        ).fetchone()
        if customer is None or customer["is_deleted"]:
            return None, "order references a missing or deleted customer"
        connection.execute(
            """
            INSERT INTO orders (
                order_id, customer_id, amount, currency, status, is_deleted,
                source_updated_at, last_event_id
            ) VALUES (?, ?, ?, ?, ?, 0, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                customer_id = excluded.customer_id,
                amount = excluded.amount,
                currency = excluded.currency,
                status = excluded.status,
                is_deleted = 0,
                source_updated_at = excluded.source_updated_at,
                last_event_id = excluded.last_event_id
            """,
            (
                event.record_id,
                event.payload["customer_id"],
                event.payload["amount"],
                event.payload["currency"],
                event.payload["status"],
                event.source_updated_at,
                event.event_id,
            ),
        )
    else:
        connection.execute(
            """
            INSERT INTO orders (
                order_id, customer_id, amount, currency, status, is_deleted,
                source_updated_at, last_event_id
            ) VALUES (?, NULL, NULL, NULL, NULL, 1, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                is_deleted = 1,
                source_updated_at = excluded.source_updated_at,
                last_event_id = excluded.last_event_id
            """,
            (event.record_id, event.source_updated_at, event.event_id),
        )
    return "applied", None


def _record_processed(
    connection: sqlite3.Connection,
    event: Event,
    content_hash: str,
    run_id: str,
    source_file: str,
    row_number: int,
    outcome: str,
) -> None:
    connection.execute(
        """
        INSERT INTO processed_events (
            event_id, content_hash, entity_type, record_id, operation,
            outcome, source_updated_at, first_run_id, source_file, source_row_number
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event.event_id,
            content_hash,
            event.entity_type,
            event.record_id,
            event.operation,
            outcome,
            event.source_updated_at,
            run_id,
            source_file,
            row_number,
        ),
    )


def _process_row(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    run_id: str,
    source_file: str,
) -> str:
    record = json.loads(row["raw_record"])
    event_id = row["event_id"]
    if event_id:
        previously_seen = connection.execute(
            "SELECT content_hash FROM processed_events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if previously_seen is not None:
            if previously_seen["content_hash"] == row["content_hash"]:
                return "duplicate"
            _store_reject(
                connection,
                run_id,
                source_file,
                row["source_row_number"],
                event_id,
                "event_id was reused with different source content",
                row["raw_record"],
                row["fingerprint"],
            )
            return "rejected"

    event, errors = validate_event(record)
    if errors:
        _store_reject(
            connection,
            run_id,
            source_file,
            row["source_row_number"],
            event_id,
            "; ".join(errors),
            row["raw_record"],
            row["fingerprint"],
        )
        return "rejected"
    if event is None:
        raise RuntimeError("event validation returned no event and no errors")

    outcome, reason = _apply_event(connection, event)
    if reason:
        _store_reject(
            connection,
            run_id,
            source_file,
            row["source_row_number"],
            event.event_id,
            reason,
            row["raw_record"],
            row["fingerprint"],
        )
        return "rejected"
    if outcome is None:
        raise RuntimeError("event application returned no outcome and no rejection reason")
    _record_processed(
        connection,
        event,
        row["content_hash"],
        run_id,
        source_file,
        row["source_row_number"],
        outcome,
    )
    return outcome


def _run_result(
    connection: sqlite3.Connection,
    run_id: str,
) -> RunResult:
    row = connection.execute(
        """
        SELECT runs.*, state.watermark_arrived_at, state.watermark_event_key
        FROM ingestion_runs AS runs
        LEFT JOIN ingestion_state AS state ON state.source_name = runs.source_name
        WHERE runs.run_id = ?
        """,
        (run_id,),
    ).fetchone()
    return RunResult(
        run_id=row["run_id"],
        source_name=row["source_name"],
        source_file=row["source_file"],
        status=row["status"],
        input_rows=row["input_rows"],
        extracted_count=row["extracted_count"],
        applied_count=row["applied_count"],
        stale_count=row["stale_count"],
        duplicate_count=row["duplicate_count"],
        rejected_count=row["rejected_count"],
        unextractable_count=row["unextractable_count"],
        skipped_count=row["skipped_count"],
        batch_count=row["batch_count"],
        error_message=row["error_message"],
        watermark_arrived_at=row["watermark_arrived_at"],
        watermark_event_key=row["watermark_event_key"],
    )


def run_ingestion(
    source_path: str | Path,
    database_path: str | Path = "database/ingestion.db",
    source_name: str = "default",
    batch_size: int = 500,
    fail_after_batches: int | None = None,
) -> RunResult:
    if batch_size < 1:
        raise ValueError("batch_size must be at least one")
    if fail_after_batches is not None and fail_after_batches < 1:
        raise ValueError("fail_after_batches must be at least one")
    input_path = Path(source_path)
    source_file = input_path.name
    run_id = str(uuid.uuid4())
    metrics = {
        "input_rows": 0,
        "extracted_count": 0,
        "applied_count": 0,
        "stale_count": 0,
        "duplicate_count": 0,
        "rejected_count": 0,
        "unextractable_count": 0,
        "skipped_count": 0,
        "batch_count": 0,
    }
    durable_metrics = metrics.copy()
    initialize_database(database_path)
    with connect_database(database_path) as connection:
        connection.execute(
            """
            UPDATE ingestion_runs SET status = 'interrupted', completed_at = ?, 
                error_message = 'process ended before the run was finalized'
            WHERE status = 'running'
            """,
            (_now(),),
        )
        connection.execute(
            """
            INSERT INTO ingestion_runs (run_id, source_name, source_file, status, started_at)
            VALUES (?, ?, ?, 'running', ?)
            """,
            (run_id, source_name, source_file, _now()),
        )
        connection.commit()
        try:
            _stage_source(connection, input_path, run_id, source_file, metrics)
            watermark = connection.execute(
                """
                SELECT watermark_arrived_at, watermark_event_key
                FROM ingestion_state WHERE source_name = ?
                """,
                (source_name,),
            ).fetchone()
            watermark_arrived_at = watermark["watermark_arrived_at"] if watermark else None
            watermark_event_key = watermark["watermark_event_key"] if watermark else None
            if watermark_arrived_at is None:
                eligible_query = "SELECT COUNT(*) FROM source_stage"
                eligible_params: tuple[Any, ...] = ()
            else:
                eligible_query = """
                    SELECT COUNT(*) FROM source_stage
                    WHERE arrived_at > ? OR (arrived_at = ? AND cursor_key > ?)
                """
                eligible_params = (watermark_arrived_at, watermark_arrived_at, watermark_event_key)
            eligible_count = connection.execute(eligible_query, eligible_params).fetchone()[0]
            stage_count = connection.execute("SELECT COUNT(*) FROM source_stage").fetchone()[0]
            metrics["skipped_count"] = stage_count - eligible_count
            _update_run_metrics(connection, run_id, metrics)
            connection.commit()
            durable_metrics = metrics.copy()

            if watermark_arrived_at is None:
                rows = connection.execute(
                    "SELECT * FROM source_stage ORDER BY arrived_at, cursor_key, source_row_number"
                )
            else:
                rows = connection.execute(
                    """
                    SELECT * FROM source_stage
                    WHERE arrived_at > ? OR (arrived_at = ? AND cursor_key > ?)
                    ORDER BY arrived_at, cursor_key, source_row_number
                    """,
                    (watermark_arrived_at, watermark_arrived_at, watermark_event_key),
                )

            while True:
                batch = rows.fetchmany(batch_size)
                if not batch:
                    break
                batch_counts = {
                    "extracted_count": len(batch),
                    "applied_count": 0,
                    "stale_count": 0,
                    "duplicate_count": 0,
                    "rejected_count": 0,
                    "batch_count": 1,
                }
                for row in batch:
                    outcome = _process_row(connection, row, run_id, source_file)
                    if outcome == "rejected":
                        batch_counts["rejected_count"] += 1
                    elif outcome == "duplicate":
                        batch_counts["duplicate_count"] += 1
                    elif outcome == "stale":
                        batch_counts["stale_count"] += 1
                    else:
                        batch_counts["applied_count"] += 1
                last_row = batch[-1]
                connection.execute(
                    """
                    INSERT INTO ingestion_state (
                        source_name, watermark_arrived_at, watermark_event_key, updated_at
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(source_name) DO UPDATE SET
                        watermark_arrived_at = excluded.watermark_arrived_at,
                        watermark_event_key = excluded.watermark_event_key,
                        updated_at = excluded.updated_at
                    """,
                    (source_name, last_row["arrived_at"], last_row["cursor_key"], _now()),
                )
                new_metrics = metrics.copy()
                for key, value in batch_counts.items():
                    new_metrics[key] += value
                _update_run_metrics(connection, run_id, new_metrics)
                connection.commit()
                metrics = new_metrics
                durable_metrics = metrics.copy()
                LOGGER.debug(
                    "run_id=%s committed_batch=%s rows=%s watermark=%s",
                    run_id,
                    metrics["batch_count"],
                    len(batch),
                    last_row["arrived_at"],
                )
                if fail_after_batches == metrics["batch_count"]:
                    raise RuntimeError(f"simulated failure after committed batch {metrics['batch_count']}")

            connection.execute(
                "UPDATE ingestion_runs SET status = 'completed', completed_at = ? WHERE run_id = ?",
                (_now(), run_id),
            )
            connection.commit()
        except Exception as error:
            connection.rollback()
            metrics = durable_metrics
            connection.execute(
                """
                UPDATE ingestion_runs SET status = 'failed', completed_at = ?,
                    error_message = ?
                WHERE run_id = ?
                """,
                (_now(), str(error)[:2000], run_id),
            )
            _update_run_metrics(connection, run_id, metrics)
            connection.commit()
            LOGGER.exception("run_id=%s failed", run_id)
        result = _run_result(connection, run_id)
    LOGGER.info(
        "run_id=%s status=%s input=%s applied=%s stale=%s duplicate=%s rejected=%s",
        result.run_id,
        result.status,
        result.input_rows,
        result.applied_count,
        result.stale_count,
        result.duplicate_count,
        result.rejected_count,
    )
    return result
