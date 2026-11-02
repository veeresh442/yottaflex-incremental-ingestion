from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any

from yottaflex_ingestion.database import connect_database


def _fingerprint(connection, table: str, key_column: str, columns: tuple[str, ...]) -> dict[str, Any]:
    digest = hashlib.sha256()
    count = 0
    column_sql = ", ".join(columns)
    for row in connection.execute(f"SELECT {column_sql} FROM {table} ORDER BY {key_column}"):
        digest.update(json.dumps(tuple(row), separators=(",", ":"), allow_nan=False).encode("utf-8"))
        digest.update(b"\n")
        count += 1
    return {"row_count": count, "sha256": digest.hexdigest()}


def _state_snapshot(database_path: str | Path) -> dict[str, Any]:
    with connect_database(database_path) as connection:
        customers = {
            "active": connection.execute("SELECT COUNT(*) FROM customers WHERE is_deleted = 0").fetchone()[0],
            "deleted": connection.execute("SELECT COUNT(*) FROM customers WHERE is_deleted = 1").fetchone()[0],
        }
        orders = {
            "active": connection.execute("SELECT COUNT(*) FROM orders WHERE is_deleted = 0").fetchone()[0],
            "deleted": connection.execute("SELECT COUNT(*) FROM orders WHERE is_deleted = 1").fetchone()[0],
        }
        totals = [
            {"currency": row["currency"], "active_order_amount": round(row["total"], 2)}
            for row in connection.execute(
                "SELECT currency, SUM(amount) AS total FROM orders "
                "WHERE is_deleted = 0 GROUP BY currency ORDER BY currency"
            )
        ]
        customer_fingerprint = _fingerprint(
            connection, "customers", "customer_id",
            ("customer_id", "name", "email", "is_deleted", "source_updated_at", "last_event_id"),
        )
        order_fingerprint = _fingerprint(
            connection, "orders", "order_id",
            ("order_id", "customer_id", "amount", "currency", "status", "is_deleted",
             "source_updated_at", "last_event_id"),
        )
    return {
        "customers": customers,
        "orders": orders,
        "active_order_amount_by_currency": totals,
        "customer_state_fingerprint": customer_fingerprint,
        "order_state_fingerprint": order_fingerprint,
    }


def _source_target_comparison(
    database_path: str | Path,
    source_path: str | Path,
    source_name: str,
    actual: dict[str, Any],
) -> dict[str, Any]:
    from yottaflex_ingestion.pipeline import run_ingestion

    feed = Path(source_path)
    if not feed.is_file():
        return {"status": "failed", "passed": False, "reason": "source CSV does not exist"}
    with connect_database(database_path) as connection:
        source_names = [
            row["source_name"]
            for row in connection.execute(
                "SELECT DISTINCT source_name FROM ingestion_runs ORDER BY source_name"
            )
        ]
    if source_names != [source_name]:
        return {
            "status": "not_comparable",
            "passed": False,
            "reason": "comparison requires one source feed per target database",
            "database_source_names": source_names,
        }

    with tempfile.TemporaryDirectory(prefix="yottaflex-reconciliation-") as temporary:
        reference_db = Path(temporary) / "expected.db"
        replay = run_ingestion(feed, reference_db, source_name=source_name, batch_size=1000)
        if replay.status != "completed":
            return {
                "status": "failed",
                "passed": False,
                "reason": "the source feed could not be replayed to completion",
                "source_replay": {
                    "status": replay.status,
                    "input_rows": replay.input_rows,
                    "error_message": replay.error_message,
                },
            }
        expected = _state_snapshot(reference_db)

    checks = {
        "customer_counts_match": expected["customers"] == actual["customers"],
        "order_counts_match": expected["orders"] == actual["orders"],
        "active_order_totals_match": (
            expected["active_order_amount_by_currency"] == actual["active_order_amount_by_currency"]
        ),
        "customer_rows_match": (
            expected["customer_state_fingerprint"] == actual["customer_state_fingerprint"]
        ),
        "order_rows_match": expected["order_state_fingerprint"] == actual["order_state_fingerprint"],
    }
    passed = all(checks.values())
    return {
        "status": "passed" if passed else "failed",
        "passed": passed,
        "source_file": feed.name,
        "method": "Replay the complete appendable CSV in a temporary database and compare it with the target.",
        "source_replay": {
            "status": replay.status,
            "input_rows": replay.input_rows,
            "applied": replay.applied_count,
            "stale": replay.stale_count,
            "duplicates": replay.duplicate_count,
            "rejected": replay.rejected_count,
        },
        "expected_from_source": expected,
        "actual_target": actual,
        "checks": checks,
    }


def build_reconciliation(
    database_path: str | Path,
    run_id: str,
    source_path: str | Path | None = None,
) -> dict[str, Any]:
    with connect_database(database_path) as connection:
        run = connection.execute(
            "SELECT * FROM ingestion_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if run is None:
            raise ValueError(f"run_id not found: {run_id}")
        foreign_key_violations = [
            dict(row) for row in connection.execute("PRAGMA foreign_key_check")
        ]
        accounted_input_rows = (
            run["skipped_count"] + run["extracted_count"] + run["unextractable_count"]
        )
        unprocessed_input_rows = max(0, run["input_rows"] - accounted_input_rows)
        event_rejects = run["rejected_count"] - run["unextractable_count"]
        accounted_events = (
            run["applied_count"] + run["stale_count"]
            + run["duplicate_count"] + event_rejects
        )
        unprocessed_extracted_events = max(0, run["extracted_count"] - accounted_events)

    actual_state = _state_snapshot(database_path)
    if source_path is None:
        source_target = {
            "status": "not_requested",
            "passed": None,
            "reason": "Pass the full appendable source CSV for a source-to-target comparison.",
        }
    else:
        source_target = _source_target_comparison(
            database_path, source_path, run["source_name"], actual_state
        )

    basic_passed = (
        run["input_rows"] == accounted_input_rows + unprocessed_input_rows
        and run["extracted_count"] == accounted_events + unprocessed_extracted_events
        and (
            run["status"] != "completed"
            or (unprocessed_input_rows == 0 and unprocessed_extracted_events == 0)
        )
        and not foreign_key_violations
        and run["status"] == "completed"
    )
    passed = basic_passed and (source_target["passed"] is None or source_target["passed"])

    return {
        "run": {
            "run_id": run["run_id"],
            "source_name": run["source_name"],
            "source_file": run["source_file"],
            "status": run["status"],
            "started_at": run["started_at"],
            "completed_at": run["completed_at"],
            "error_message": run["error_message"],
        },
        "run_counts": {
            "input_rows": run["input_rows"],
            "skipped_before_watermark": run["skipped_count"],
            "extracted_after_watermark": run["extracted_count"],
            "applied": run["applied_count"],
            "stale": run["stale_count"],
            "duplicates": run["duplicate_count"],
            "rejected": run["rejected_count"],
            "unextractable_without_arrival_timestamp": run["unextractable_count"],
            "unprocessed_after_failure": unprocessed_input_rows,
            "committed_batches": run["batch_count"],
        },
        "target_snapshot": {
            **actual_state,
            "foreign_key_violation_count": len(foreign_key_violations),
            "foreign_key_violations": foreign_key_violations,
        },
        "source_target_reconciliation": source_target,
        "reconciliation": {
            "input_rows_account_for": (
                run["input_rows"] == accounted_input_rows + unprocessed_input_rows
            ),
            "extracted_events_account_for": (
                run["extracted_count"] == accounted_events + unprocessed_extracted_events
            ),
            "unprocessed_extracted_events": unprocessed_extracted_events,
            "target_foreign_keys_valid": not foreign_key_violations,
            "source_target_state_matches": source_target["passed"],
            "passed": passed,
        },
    }


def write_reconciliation_report(
    database_path: str | Path,
    run_id: str,
    output_path: str | Path | None = None,
    source_path: str | Path | None = None,
) -> tuple[dict[str, Any], Path]:
    report = build_reconciliation(database_path, run_id, source_path)
    path = Path(output_path) if output_path else Path("reports") / f"reconciliation_{run_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return report, path
