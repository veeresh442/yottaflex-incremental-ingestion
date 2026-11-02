from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from yottaflex_ingestion.models import REQUIRED_COLUMNS


@pytest.fixture
def make_event():
    origin = datetime(2025, 1, 1, tzinfo=timezone.utc)

    def build(
        event_id: str,
        entity_type: str,
        operation: str,
        record_id: str,
        offset: int,
        payload: dict[str, Any],
        source_offset: int | None = None,
    ) -> dict[str, str]:
        source_time = origin + timedelta(seconds=source_offset if source_offset is not None else offset)
        arrival_time = origin + timedelta(seconds=offset)
        return {
            "event_id": event_id,
            "entity_type": entity_type,
            "operation": operation,
            "record_id": record_id,
            "source_updated_at": source_time.isoformat().replace("+00:00", "Z"),
            "arrived_at": arrival_time.isoformat().replace("+00:00", "Z"),
            "payload": json.dumps(payload, sort_keys=True, separators=(",", ":")),
        }

    return build


@pytest.fixture
def write_csv():
    def write(path: Path, records: list[dict[str, str]], columns=REQUIRED_COLUMNS) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            writer.writerows(records)
        return path

    return write
