from __future__ import annotations

import sqlite3

from yottaflex_ingestion.pipeline import run_ingestion
from yottaflex_ingestion.reporting import build_reconciliation


def test_ingestion_rerun_is_idempotent(tmp_path, make_event, write_csv):
    source = write_csv(
        tmp_path / "events.csv",
        [
            make_event(
                "customer-1",
                "customer",
                "upsert",
                "cust-1",
                1,
                {"name": "Ada Lovelace", "email": "ada@example.test"},
            ),
            make_event(
                "order-1",
                "order",
                "upsert",
                "order-1",
                2,
                {"customer_id": "cust-1", "amount": "25.50", "currency": "USD", "status": "paid"},
            ),
        ],
    )
    database = tmp_path / "target.db"

    first = run_ingestion(source, database)
    second = run_ingestion(source, database)

    assert first.status == "completed"
    assert first.applied_count == 2
    assert second.status == "completed"
    assert second.extracted_count == 0
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM customers").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM processed_events").fetchone()[0] == 2
    assert build_reconciliation(database, first.run_id)["reconciliation"]["passed"]


def test_late_arrival_is_found_but_stale_version_does_not_overwrite(
    tmp_path, make_event, write_csv
):
    source = write_csv(
        tmp_path / "events.csv",
        [
            make_event(
                "customer-new",
                "customer",
                "upsert",
                "cust-1",
                1,
                {"name": "Current Name", "email": "current@example.test"},
                source_offset=10,
            ),
            make_event(
                "customer-late",
                "customer",
                "upsert",
                "cust-1",
                2,
                {"name": "Old Name", "email": "old@example.test"},
                source_offset=3,
            ),
            make_event(
                "customer-late-new-key",
                "customer",
                "upsert",
                "cust-2",
                3,
                {"name": "Late but new", "email": "late@example.test"},
                source_offset=0,
            ),
        ],
    )
    database = tmp_path / "target.db"

    result = run_ingestion(source, database)

    assert result.applied_count == 2
    assert result.stale_count == 1
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT name FROM customers WHERE customer_id = 'cust-1'").fetchone()[0] == "Current Name"
        assert connection.execute("SELECT name FROM customers WHERE customer_id = 'cust-2'").fetchone()[0] == "Late but new"


def test_tombstone_keeps_customer_and_order_history(tmp_path, make_event, write_csv):
    source = write_csv(
        tmp_path / "events.csv",
        [
            make_event(
                "customer-create",
                "customer",
                "upsert",
                "cust-1",
                1,
                {"name": "Grace Hopper", "email": "grace@example.test"},
            ),
            make_event(
                "order-create",
                "order",
                "upsert",
                "order-1",
                2,
                {"customer_id": "cust-1", "amount": "5.00", "currency": "USD", "status": "pending"},
            ),
            make_event("customer-delete", "customer", "delete", "cust-1", 3, {}),
        ],
    )
    database = tmp_path / "target.db"

    result = run_ingestion(source, database)

    assert result.applied_count == 3
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT is_deleted FROM customers WHERE customer_id = 'cust-1'").fetchone()[0] == 1
        assert connection.execute("SELECT customer_id FROM orders WHERE order_id = 'order-1'").fetchone()[0] == "cust-1"
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_invalid_data_is_quarantined_with_reason(tmp_path, make_event, write_csv):
    source = write_csv(
        tmp_path / "events.csv",
        [
            make_event(
                "customer-create",
                "customer",
                "upsert",
                "cust-1",
                1,
                {"name": "Lin Chen", "email": "lin@example.test"},
            ),
            make_event(
                "bad-reference",
                "order",
                "upsert",
                "order-1",
                2,
                {"customer_id": "missing", "amount": "10.00", "currency": "USD", "status": "paid"},
            ),
            make_event(
                "bad-payload",
                "customer",
                "upsert",
                "cust-2",
                3,
                {"name": "No Email"},
            ),
            make_event(
                "bad-amount",
                "order",
                "upsert",
                "order-2",
                4,
                {"customer_id": "cust-1", "amount": "-2.00", "currency": "USD", "status": "paid"},
            ),
            make_event(
                "bad-duplicate-email",
                "customer",
                "upsert",
                "cust-3",
                5,
                {"name": "Duplicate Email", "email": "lin@example.test"},
            ),
            make_event(
                "bad-status-type",
                "order",
                "upsert",
                "order-3",
                6,
                {"customer_id": "cust-1", "amount": "2.00", "currency": "USD", "status": []},
            ),
        ],
    )
    database = tmp_path / "target.db"

    result = run_ingestion(source, database)

    assert result.applied_count == 1
    assert result.rejected_count == 5
    with sqlite3.connect(database) as connection:
        rejects = connection.execute("SELECT event_id, reason, source_row_number FROM rejected_records").fetchall()
    assert {row[0] for row in rejects} == {
        "bad-reference",
        "bad-payload",
        "bad-amount",
        "bad-duplicate-email",
        "bad-status-type",
    }
    assert all(row[1] and row[2] > 1 for row in rejects)


def test_partial_failure_resumes_after_last_committed_batch(tmp_path, make_event, write_csv):
    source = write_csv(
        tmp_path / "events.csv",
        [
            make_event(
                f"customer-{index}",
                "customer",
                "upsert",
                f"cust-{index}",
                index,
                {"name": f"Customer {index}", "email": f"c{index}@example.test"},
            )
            for index in range(1, 5)
        ],
    )
    database = tmp_path / "target.db"

    failed = run_ingestion(source, database, batch_size=2, fail_after_batches=1)
    resumed = run_ingestion(source, database, batch_size=2)

    assert failed.status == "failed"
    assert failed.applied_count == 2
    assert resumed.status == "completed"
    assert resumed.applied_count == 2
    assert resumed.skipped_count == 2
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM customers").fetchone()[0] == 4
        assert connection.execute("SELECT COUNT(*) FROM processed_events").fetchone()[0] == 4
    failed_report = build_reconciliation(database, failed.run_id)
    assert failed_report["run_counts"]["unprocessed_after_failure"] == 2
    assert not failed_report["reconciliation"]["passed"]


def test_identical_duplicate_event_is_counted_once(tmp_path, make_event, write_csv):
    customer = make_event(
        "customer-duplicate",
        "customer",
        "upsert",
        "cust-1",
        1,
        {"name": "Duplicate", "email": "dupe@example.test"},
    )
    source = write_csv(tmp_path / "events.csv", [customer, customer])

    result = run_ingestion(source, tmp_path / "target.db")

    assert result.applied_count == 1
    assert result.duplicate_count == 1


def test_schema_change_fails_before_target_writes(tmp_path, make_event, write_csv):
    event = make_event(
        "customer-1",
        "customer",
        "upsert",
        "cust-1",
        1,
        {"name": "Ada", "email": "ada@example.test"},
    )
    source = write_csv(tmp_path / "events.csv", [event], columns=(*event.keys(), "new_column"))
    database = tmp_path / "target.db"

    result = run_ingestion(source, database)

    assert result.status == "failed"
    assert "unexpected CSV schema" in result.error_message
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM customers").fetchone()[0] == 0


def test_empty_file_completes_without_moving_watermark(tmp_path, write_csv):
    source = write_csv(tmp_path / "events.csv", [])

    result = run_ingestion(source, tmp_path / "target.db")

    assert result.status == "completed"
    assert result.input_rows == 0
    assert result.watermark_arrived_at is None


def test_out_of_order_source_rows_load_in_arrival_order(tmp_path, make_event, write_csv):
    customer = make_event(
        "customer-first-by-arrival",
        "customer",
        "upsert",
        "cust-1",
        1,
        {"name": "Out of Order", "email": "order@example.test"},
    )
    order = make_event(
        "order-second-by-arrival",
        "order",
        "upsert",
        "order-1",
        2,
        {"customer_id": "cust-1", "amount": "12.00", "currency": "USD", "status": "paid"},
    )
    source = write_csv(tmp_path / "events.csv", [order, customer])

    result = run_ingestion(source, tmp_path / "target.db")

    assert result.status == "completed"
    assert result.applied_count == 2


def test_missing_arrival_timestamp_is_quarantined_idempotently(tmp_path, make_event, write_csv):
    event = make_event(
        "event-without-arrival",
        "customer",
        "upsert",
        "cust-1",
        1,
        {"name": "No Arrival", "email": "no-arrival@example.test"},
    )
    event["arrived_at"] = ""
    source = write_csv(tmp_path / "events.csv", [event])
    database = tmp_path / "target.db"

    first = run_ingestion(source, database)
    second = run_ingestion(source, database)

    assert first.unextractable_count == 1
    assert first.rejected_count == 1
    assert first.watermark_arrived_at is None
    assert second.unextractable_count == 1
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM rejected_records").fetchone()[0] == 1


def test_reused_event_id_with_changed_content_is_rejected(tmp_path, make_event, write_csv):
    first_event = make_event(
        "reused-id",
        "customer",
        "upsert",
        "cust-1",
        1,
        {"name": "First", "email": "first@example.test"},
    )
    changed_event = make_event(
        "reused-id",
        "customer",
        "upsert",
        "cust-2",
        2,
        {"name": "Changed", "email": "changed@example.test"},
    )
    source = write_csv(tmp_path / "events.csv", [first_event, changed_event])

    result = run_ingestion(source, tmp_path / "target.db")

    assert result.applied_count == 1
    assert result.rejected_count == 1
    with sqlite3.connect(tmp_path / "target.db") as connection:
        assert connection.execute("SELECT COUNT(*) FROM customers").fetchone()[0] == 1
        reason = connection.execute("SELECT reason FROM rejected_records").fetchone()[0]
    assert "reused with different source content" in reason
