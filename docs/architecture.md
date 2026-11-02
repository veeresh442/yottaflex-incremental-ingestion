# Architecture

## Flow

~~~mermaid
flowchart LR
    CSV[Appendable CSV event feed] --> STAGE[SQLite temporary staging table]
    STAGE --> CURSOR[Arrival-time plus event-ID cursor]
    CURSOR --> VALIDATE[Schema and business validation]
    VALIDATE -->|valid| APPLY[Transactional customer/order upsert]
    VALIDATE -->|invalid| QUARANTINE[Rejected records]
    APPLY --> EVENTS[Processed event ledger]
    APPLY --> STATE[Watermark state]
    QUARANTINE --> STATE
    APPLY --> RUNS[Run history and counts]
    STATE --> REPORT[Reconciliation report]
    RUNS --> REPORT
~~~

## Modules

- `generator.py` creates deterministic, synthetic customer and order events.
- `models.py` normalizes timestamps and validates event schema and domain rules.
- `database.py` creates the SQLite schema and configures foreign keys.
- `pipeline.py` stages, orders, validates, applies, quarantines, and checkpoints events.
- `reporting.py` reports run accounting, target counts, money totals, and FK checks.
- `cli.py` exposes generation, ingestion, reconciliation, and a performance command.

## Cursor and late arrivals

The durable cursor is `(arrived_at, source_row_number, event_id)`, compared lexicographically after timestamps are normalized to UTC. The row number makes same-time duplicate event rows independently checkpointable and lets newly appended rows at the same timestamp advance the cursor. `arrived_at` means when the source made an event available to this feed. `source_updated_at` remains the source's business-change time. A late event therefore receives a new arrival time even when its source update time is old, so it remains eligible for extraction.

For the same customer or order, the target keeps the greatest `(source_updated_at, event_id)` pair. A late event older than the current entity version is recorded as stale and does not overwrite newer state. If a producer cannot provide trustworthy arrival times, use a source change sequence or CDC offset instead; a bounded timestamp lookback alone cannot guarantee discovery of arbitrarily late events.

Events must be immutable by `event_id`. Reusing an ID with different content is quarantined. The source feed is scanned and staged on each run, which allows the extractor to handle out-of-order CSV rows without holding the full file in Python memory. SQLite temporary storage and the target database should have enough local disk space for the staged source.

## Atomicity and recovery

Each batch applies target changes, records accepted event IDs, and advances the watermark in one SQLite transaction. A failed transaction commits none of those changes. Earlier committed batches remain durable; a restart resumes after their watermark. Quarantine writes and their checkpoint are committed together for timestamped rows. A row without a valid arrival timestamp cannot advance the cursor, so it is stored in quarantine idempotently and is visible again on later scans.

Deletes are tombstones (`is_deleted = 1`) so history and foreign keys remain intact. A hard delete is intentionally not used. Run rows left in `running` state by a terminated process are marked `interrupted` when the next run starts.

## Deliberate limits

- The example source is a local CSV event feed, not a live API or CDC connector.
- One writer at a time is the intended operating mode. SQLite serializes writes; production multi-writer workloads should use a server database and distributed coordination.
- Timestamp and event IDs are required to be stable. Events with malformed arrival timestamps are quarantined but cannot be ordered into the cursor.
- Currency is recorded per order and totals are grouped by currency; no FX conversion is attempted.
- Schema changes fail the run before target writes. Versioned schema migrations and compatibility policy belong in a production deployment.


## Source-to-target reconciliation

The run command passes its CSV feed to the reconciliation report. Reporting replays the complete appendable feed into a temporary SQLite database, then compares expected and actual customer/order counts, active/deleted counts, active order totals by currency, and SHA-256 fingerprints of current entity state. The comparison assumes one source feed owns the target tables and the CSV contains that feed's full history. A delta-only file or multiple source feeds sharing target tables cannot be compared as a complete source snapshot. The replay adds one additional ingestion pass. Because the reference uses the same pipeline implementation, the check detects incomplete or divergent target state but does not independently prove the transformation rules are correct.
