from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any


REQUIRED_COLUMNS = (
    "event_id",
    "entity_type",
    "operation",
    "record_id",
    "source_updated_at",
    "arrived_at",
    "payload",
)
CUSTOMER_FIELDS = {"name", "email"}
ORDER_FIELDS = {"customer_id", "amount", "currency", "status"}
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")
ORDER_STATUSES = {"pending", "paid", "refunded", "cancelled"}


@dataclass(frozen=True)
class Event:
    event_id: str
    entity_type: str
    operation: str
    record_id: str
    source_updated_at: str
    arrived_at: str
    payload: dict[str, Any]


def normalize_timestamp(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("timestamp is required")
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    normalized = parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")
    return normalized.replace("+00:00", "Z")


def serialize_record(record: dict[str, Any]) -> str:
    return json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def validate_event(record: dict[str, Any]) -> tuple[Event | None, tuple[str, ...]]:
    errors: list[str] = []
    if None in record:
        errors.append("row contains values outside the declared CSV columns")
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in record or record[column] is None]
    if missing_columns:
        errors.append(f"missing required fields: {', '.join(missing_columns)}")
    if errors:
        return None, tuple(errors)

    event_id = str(record["event_id"]).strip()
    entity_type = str(record["entity_type"]).strip().lower()
    operation = str(record["operation"]).strip().lower()
    record_id = str(record["record_id"]).strip()

    if not event_id or len(event_id) > 200:
        errors.append("event_id must contain 1 to 200 characters")
    if entity_type not in {"customer", "order"}:
        errors.append("entity_type must be customer or order")
    if operation not in {"upsert", "delete"}:
        errors.append("operation must be upsert or delete")
    if not record_id or len(record_id) > 200:
        errors.append("record_id must contain 1 to 200 characters")

    try:
        source_updated_at = normalize_timestamp(record["source_updated_at"])
    except (TypeError, ValueError):
        source_updated_at = ""
        errors.append("source_updated_at must be an ISO-8601 timestamp with timezone")
    try:
        arrived_at = normalize_timestamp(record["arrived_at"])
    except (TypeError, ValueError):
        arrived_at = ""
        errors.append("arrived_at must be an ISO-8601 timestamp with timezone")

    try:
        payload = json.loads(record["payload"])
        if not isinstance(payload, dict):
            raise ValueError
    except (TypeError, json.JSONDecodeError, ValueError):
        payload = {}
        errors.append("payload must be a JSON object")

    if operation == "delete":
        if payload:
            errors.append("delete events must have an empty payload")
    elif entity_type == "customer":
        if set(payload) != CUSTOMER_FIELDS:
            errors.append("customer payload must contain exactly name and email")
        name = payload.get("name")
        email = payload.get("email")
        if not isinstance(name, str) or not name.strip() or len(name.strip()) > 200:
            errors.append("customer name must contain 1 to 200 characters")
        if not isinstance(email, str) or not EMAIL_PATTERN.fullmatch(email.strip()):
            errors.append("customer email must be a valid email address")
    elif entity_type == "order":
        if set(payload) != ORDER_FIELDS:
            errors.append("order payload must contain exactly customer_id, amount, currency, and status")
        customer_id = payload.get("customer_id")
        if not isinstance(customer_id, str) or not customer_id.strip():
            errors.append("order customer_id is required")
        try:
            amount = Decimal(str(payload.get("amount")))
            if not amount.is_finite() or amount <= 0 or amount > Decimal("1000000000"):
                errors.append("order amount must be greater than zero and at most 1000000000")
            elif amount.as_tuple().exponent < -2:
                errors.append("order amount must have at most two decimal places")
        except (InvalidOperation, ValueError):
            errors.append("order amount must be numeric")
        currency = payload.get("currency")
        if not isinstance(currency, str) or not CURRENCY_PATTERN.fullmatch(currency):
            errors.append("order currency must be a three-letter uppercase code")
        status = payload.get("status")
        if not isinstance(status, str) or status not in ORDER_STATUSES:
            errors.append(f"order status must be one of {', '.join(sorted(ORDER_STATUSES))}")

    if errors:
        return None, tuple(errors)
    if entity_type == "customer" and operation == "upsert":
        payload = {"name": payload["name"].strip(), "email": payload["email"].strip().lower()}
    if entity_type == "order" and operation == "upsert":
        payload = {
            "customer_id": payload["customer_id"].strip(),
            "amount": float(Decimal(str(payload["amount"]))),
            "currency": payload["currency"],
            "status": payload["status"],
        }
    return Event(
        event_id=event_id,
        entity_type=entity_type,
        operation=operation,
        record_id=record_id,
        source_updated_at=source_updated_at,
        arrived_at=arrived_at,
        payload=payload,
    ), ()
