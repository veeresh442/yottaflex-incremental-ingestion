# Data Dictionary

All timestamps are ISO-8601 UTC strings ending in `Z`. Entity keys and event IDs are stable strings. Boolean flags are stored as SQLite integers (`0` or `1`).

## Source CSV

| Column | Required | Meaning |
| --- | --- | --- |
| `event_id` | Yes | Immutable unique identifier for one source event; IDs are unique across the target database. |
| `entity_type` | Yes | `customer` or `order`. |
| `operation` | Yes | `upsert` or `delete`. Deletes use an empty JSON object. |
| `record_id` | Yes | Stable key of the customer or order being changed. |
| `source_updated_at` | Yes | Source business-change time; used to prevent stale overwrites. |
| `arrived_at` | Yes | Time this event became available in the feed; drives incremental extraction. |
| `payload` | Yes | JSON object matching the entity-specific schema. |

Customer upserts contain exactly `name` and `email`. Names have 1–200 non-whitespace characters; email values must have a basic email shape and are normalized to lowercase. Active customer emails are unique.

Order upserts contain exactly `customer_id`, `amount`, `currency`, and `status`. Amounts must be positive, at most 1,000,000,000, and have at most two decimal places. Currency is three uppercase letters. Status is `pending`, `paid`, `refunded`, or `cancelled`. The customer must exist and be active when the order event is applied.

## SQLite tables

| Table | Grain and key | Purpose |
| --- | --- | --- |
| `customers` | One current row per `customer_id` | Current customer state, source version, and tombstone flag. |
| `orders` | One current row per `order_id` | Current order state, customer foreign key, source version, and tombstone flag. |
| `processed_events` | One accepted event per `event_id` | Idempotency ledger and applied/stale outcome. |
| `rejected_records` | One rejected raw row fingerprint | Original row, source location, reason, and detecting run. |
| `ingestion_state` | One row per `source_name` | Durable (`watermark_arrived_at`, `watermark_event_key`) cursor; the event key uses source row number then event ID as tie-breakers. |
| `ingestion_runs` | One row per `run_id` | Start/end status, row metrics, committed batches, and failure reason. |

`orders.customer_id` is a nullable foreign key because an order tombstone may arrive before its corresponding order. Active orders always require a customer. Customer tombstones retain the customer key so historical orders remain referentially valid.
