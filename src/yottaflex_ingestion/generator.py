from __future__ import annotations

import csv
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

from yottaflex_ingestion.models import REQUIRED_COLUMNS


def _event(
    event_id: str,
    entity_type: str,
    operation: str,
    record_id: str,
    source_updated_at: datetime,
    arrived_at: datetime,
    payload: dict[str, Any],
) -> dict[str, str]:
    return {
        "event_id": event_id,
        "entity_type": entity_type,
        "operation": operation,
        "record_id": record_id,
        "source_updated_at": source_updated_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "arrived_at": arrived_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "payload": json.dumps(payload, sort_keys=True, separators=(",", ":")),
    }


def _baseline_events(record_count: int, seed: int) -> Iterator[dict[str, str]]:
    randomizer = random.Random(seed)
    origin = datetime(2025, 1, 1, tzinfo=timezone.utc)
    customer_count = min(100, max(1, record_count // 100))
    for index in range(customer_count):
        moment = origin + timedelta(seconds=index)
        yield _event(
            f"customer-create-{index:06d}",
            "customer",
            "upsert",
            f"customer-{index:06d}",
            moment,
            moment,
            {"name": f"Sample Customer {index:06d}", "email": f"customer{index:06d}@example.test"},
        )
    reserve_deleted_customer = customer_count > 1 and record_count > customer_count
    active_customer_count = customer_count - 1 if reserve_deleted_customer else customer_count
    for index in range(customer_count, record_count):
        moment = origin + timedelta(seconds=index)
        if index == record_count - 1 and reserve_deleted_customer:
            yield _event(
                "customer-delete-final",
                "customer",
                "delete",
                f"customer-{customer_count - 1:06d}",
                moment,
                moment,
                {},
            )
        elif index % 97 == 0:
            customer_index = randomizer.randrange(active_customer_count)
            yield _event(
                f"customer-update-{index:08d}",
                "customer",
                "upsert",
                f"customer-{customer_index:06d}",
                moment,
                moment,
                {
                    "name": f"Updated Customer {customer_index:06d} at {index}",
                    "email": f"customer{customer_index:06d}@example.test",
                },
            )
        else:
            customer_index = randomizer.randrange(active_customer_count)
            yield _event(
                f"order-create-{index:08d}",
                "order",
                "upsert",
                f"order-{index:08d}",
                moment,
                moment,
                {
                    "customer_id": f"customer-{customer_index:06d}",
                    "amount": f"{randomizer.randint(100, 500000) / 100:.2f}",
                    "currency": "USD",
                    "status": randomizer.choice(("pending", "paid", "refunded", "cancelled")),
                },
            )


def _edge_events(record_count: int) -> Iterator[dict[str, str]]:
    origin = datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=record_count + 1)
    old_time = datetime(2020, 1, 1, tzinfo=timezone.utc)
    yield _event(
        "sample-event-900001",
        "customer",
        "upsert",
        "customer-900001",
        old_time,
        origin,
        {"name": "Late Arrival Example", "email": "late@example.test"},
    )
    duplicate = _event(
        "sample-event-900002",
        "customer",
        "upsert",
        "customer-900002",
        origin + timedelta(seconds=1),
        origin + timedelta(seconds=1),
        {"name": "Duplicate Example", "email": "duplicate@example.test"},
    )
    yield duplicate
    yield duplicate
    yield _event(
        "sample-event-900003",
        "customer",
        "upsert",
        "customer-900003",
        origin + timedelta(seconds=2),
        origin + timedelta(seconds=2),
        {"name": "Missing Email Example"},
    )
    yield _event(
        "sample-event-900004",
        "order",
        "upsert",
        "order-900004",
        origin + timedelta(seconds=3),
        origin + timedelta(seconds=3),
        {
            "customer_id": "customer-999999",
            "amount": "10.00",
            "currency": "USD",
            "status": "pending",
        },
    )
    yield _event(
        "sample-event-900005",
        "order",
        "upsert",
        "order-900005",
        origin + timedelta(seconds=4),
        origin + timedelta(seconds=4),
        {"customer_id": "customer-000000", "amount": "-1.00", "currency": "USD", "status": "pending"},
    )


def generate_events(
    output_path: str | Path,
    record_count: int = 100,
    seed: int = 42,
    include_edge_cases: bool = True,
) -> int:
    if record_count < 1:
        raise ValueError("record_count must be at least one")
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REQUIRED_COLUMNS)
        writer.writeheader()
        for record in _baseline_events(record_count, seed):
            writer.writerow(record)
            count += 1
        if include_edge_cases:
            for record in _edge_events(record_count):
                writer.writerow(record)
                count += 1
    return count
