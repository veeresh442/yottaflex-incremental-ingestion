from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_file TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'interrupted')),
    started_at TEXT NOT NULL,
    completed_at TEXT,
    input_rows INTEGER NOT NULL DEFAULT 0,
    extracted_count INTEGER NOT NULL DEFAULT 0,
    applied_count INTEGER NOT NULL DEFAULT 0,
    stale_count INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0,
    unextractable_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    batch_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS ingestion_state (
    source_name TEXT PRIMARY KEY,
    watermark_arrived_at TEXT,
    watermark_event_key TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    is_deleted INTEGER NOT NULL DEFAULT 0 CHECK (is_deleted IN (0, 1)),
    source_updated_at TEXT NOT NULL,
    last_event_id TEXT NOT NULL,
    CHECK (is_deleted = 1 OR (name IS NOT NULL AND email IS NOT NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_customers_active_email
    ON customers(email) WHERE is_deleted = 0 AND email IS NOT NULL;

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT REFERENCES customers(customer_id),
    amount REAL,
    currency TEXT,
    status TEXT,
    is_deleted INTEGER NOT NULL DEFAULT 0 CHECK (is_deleted IN (0, 1)),
    source_updated_at TEXT NOT NULL,
    last_event_id TEXT NOT NULL,
    CHECK (is_deleted = 1 OR (
        customer_id IS NOT NULL AND amount > 0 AND currency IS NOT NULL AND status IS NOT NULL
    ))
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    record_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('applied', 'stale')),
    source_updated_at TEXT NOT NULL,
    first_run_id TEXT NOT NULL REFERENCES ingestion_runs(run_id),
    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rejected_records (
    reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint TEXT NOT NULL UNIQUE,
    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    run_id TEXT NOT NULL REFERENCES ingestion_runs(run_id),
    event_id TEXT,
    reason TEXT NOT NULL,
    raw_record TEXT NOT NULL,
    rejected_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rejected_records_run ON rejected_records(run_id);
CREATE INDEX IF NOT EXISTS idx_processed_events_record ON processed_events(entity_type, record_id);
"""


@contextmanager
def connect_database(path: str | Path) -> Iterator[sqlite3.Connection]:
    database_path = Path(path)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    try:
        yield connection
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def initialize_database(path: str | Path) -> None:
    with connect_database(path) as connection:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.executescript(SCHEMA)
