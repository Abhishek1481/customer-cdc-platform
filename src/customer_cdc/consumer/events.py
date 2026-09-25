"""Parsing of Debezium PostgreSQL change events into typed ``ChangeEvent`` objects.

A Debezium envelope looks like::

    {"before": {...} | null, "after": {...} | null,
     "source": {"lsn": 12345, "txId": 771, "ts_ms": 1718000000000, ...},
     "op": "c" | "u" | "d" | "r", "ts_ms": 1718000000123}

When the JSON converter runs with ``schemas.enable=true`` the envelope is
wrapped as ``{"schema": {...}, "payload": {...}}``; both shapes are accepted.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from customer_cdc.consumer.errors import MalformedEventError

TRACKED_COLUMNS: tuple[str, ...] = (
    "first_name",
    "last_name",
    "email",
    "phone",
    "address",
    "city",
    "state",
    "customer_status",
)

# c=create, u=update, d=delete, r=snapshot read
DATA_OPERATIONS = frozenset({"c", "u", "d", "r"})
# t=truncate, m=logical decoding message: valid Debezium ops we deliberately ignore
IGNORED_OPERATIONS = frozenset({"t", "m"})

STAGING_COLUMNS: tuple[str, ...] = (
    "change_key",
    "customer_id",
    "op",
    "is_delete",
    *TRACKED_COLUMNS,
    "source_updated_at",
    "record_hash",
    "source_lsn",
    "source_tx_id",
    "event_ts",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "raw_payload",
)


@dataclass(frozen=True)
class ChangeEvent:
    customer_id: int
    op: str
    attributes: Mapping[str, str | None]
    source_updated_at: datetime | None
    source_lsn: int
    source_tx_id: int | None
    event_ts: datetime
    raw_payload: str
    kafka_topic: str | None = None
    kafka_partition: int | None = None
    kafka_offset: int | None = None

    @property
    def is_delete(self) -> bool:
        return self.op == "d"

    @property
    def change_key(self) -> str:
        """Idempotency key: stable across Kafka redeliveries and connector restarts."""
        return f"{self.customer_id}|{self.source_lsn}|{self.op}"

    @property
    def record_hash(self) -> str:
        return compute_record_hash(self.attributes)

    def to_row(self) -> tuple[Any, ...]:
        """Row in ``STAGING_COLUMNS`` order."""
        return (
            self.change_key,
            self.customer_id,
            self.op,
            self.is_delete,
            *(self.attributes.get(col) for col in TRACKED_COLUMNS),
            self.source_updated_at,
            self.record_hash,
            self.source_lsn,
            self.source_tx_id,
            self.event_ts,
            self.kafka_topic,
            self.kafka_partition,
            self.kafka_offset,
            self.raw_payload,
        )


def compute_record_hash(attributes: Mapping[str, str | None]) -> str:
    """MD5 over the tracked attributes in a fixed order.

    ``updated_at`` is deliberately excluded so that "touch" updates do not
    create new SCD2 versions. NULL is encoded distinctly from empty string.
    """
    parts = [
        "\x00" if attributes.get(c) is None else str(attributes.get(c)) for c in TRACKED_COLUMNS
    ]
    return hashlib.md5("\x1f".join(parts).encode("utf-8"), usedforsecurity=False).hexdigest()


def _to_utc_naive(value: datetime) -> datetime:
    if value.tzinfo is not None:
        value = value.astimezone(UTC)
    return value.replace(tzinfo=None)


def parse_source_timestamp(value: Any) -> datetime | None:
    """Parse a Debezium temporal value.

    ``timestamptz`` arrives as an ISO-8601 string (ZonedTimestamp); a plain
    ``timestamp`` arrives as epoch microseconds (MicroTimestamp).
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise MalformedEventError(f"invalid timestamp value {value!r}")
    if isinstance(value, int | float):
        return datetime.fromtimestamp(value / 1_000_000, tz=UTC).replace(tzinfo=None)
    if isinstance(value, str):
        try:
            return _to_utc_naive(datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError as exc:
            raise MalformedEventError(f"invalid timestamp {value!r}") from exc
    raise MalformedEventError(f"unsupported timestamp type {type(value).__name__}")


def _normalise(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _require_int(container: Mapping[str, Any], key: str, where: str) -> int:
    value = container.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise MalformedEventError(f"{where}.{key} must be an integer, got {value!r}")
    return value


def decode_envelope(value: bytes | str | Mapping[str, Any]) -> dict[str, Any]:
    """Decode raw message bytes to the Debezium envelope dict."""
    if isinstance(value, Mapping):
        envelope: Any = dict(value)
    else:
        try:
            text = value.decode("utf-8") if isinstance(value, bytes) else value
            envelope = json.loads(text)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MalformedEventError(f"message is not valid UTF-8 JSON: {exc}") from exc
    if not isinstance(envelope, dict):
        raise MalformedEventError("message JSON is not an object")
    if "payload" in envelope and "schema" in envelope:
        envelope = envelope["payload"]
        if not isinstance(envelope, dict):
            raise MalformedEventError("payload is not an object")
    return envelope


def parse_change_event(
    value: bytes | str | Mapping[str, Any],
    *,
    topic: str | None = None,
    partition: int | None = None,
    offset: int | None = None,
) -> ChangeEvent | None:
    """Parse one Debezium message value.

    Returns ``None`` for operations that carry no row data we track
    (truncate / logical messages). Raises ``MalformedEventError`` for
    anything that is not a well-formed customer change event.
    """
    envelope = decode_envelope(value)

    op = envelope.get("op")
    if op in IGNORED_OPERATIONS:
        return None
    if op not in DATA_OPERATIONS:
        raise MalformedEventError(f"unknown operation {op!r}")

    image = envelope.get("before") if op == "d" else envelope.get("after")
    if not isinstance(image, dict):
        side = "before" if op == "d" else "after"
        raise MalformedEventError(f"op={op!r} event has no '{side}' row image")

    source = envelope.get("source")
    if not isinstance(source, dict):
        raise MalformedEventError("event has no 'source' block")

    customer_id = _require_int(image, "customer_id", "row")
    lsn = _require_int(source, "lsn", "source")
    ts_ms = _require_int(source, "ts_ms", "source")
    tx_id = source.get("txId")

    return ChangeEvent(
        customer_id=customer_id,
        op=op,
        attributes={col: _normalise(image.get(col)) for col in TRACKED_COLUMNS},
        source_updated_at=parse_source_timestamp(image.get("updated_at")),
        source_lsn=lsn,
        source_tx_id=tx_id if isinstance(tx_id, int) and not isinstance(tx_id, bool) else None,
        event_ts=datetime.fromtimestamp(ts_ms / 1000, tz=UTC).replace(tzinfo=None),
        raw_payload=json.dumps(envelope, separators=(",", ":"), sort_keys=True),
        kafka_topic=topic,
        kafka_partition=partition,
        kafka_offset=offset,
    )
