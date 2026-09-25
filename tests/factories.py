"""Builders for Debezium-format test events (JSON converter, schemas disabled)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

BASE_TS_MS = 1_717_236_000_000  # 2024-06-01T10:00:00Z


def customer_row(customer_id: int, **overrides: Any) -> dict[str, Any]:
    row = {
        "customer_id": customer_id,
        "first_name": "Ada",
        "last_name": "Lovelace",
        "email": f"ada{customer_id}@example.com",
        "phone": "+1-415-555-0100",
        "address": "1 Analytical Way",
        "city": "London",
        "state": "NY",
        "customer_status": "ACTIVE",
        "created_at": "2024-06-01T10:00:00.000000Z",
        "updated_at": "2024-06-01T10:00:00.000000Z",
    }
    row.update(overrides)
    return row


def envelope(
    op: str,
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
    lsn: int,
    ts_ms: int,
    tx_id: int | None = None,
) -> dict[str, Any]:
    return {
        "before": before,
        "after": after,
        "source": {
            "version": "3.x",
            "connector": "postgresql",
            "name": "cdc",
            "ts_ms": ts_ms,
            "snapshot": "true" if op == "r" else "false",
            "db": "customers_db",
            "schema": "public",
            "table": "customers",
            "txId": tx_id if tx_id is not None else lsn // 10,
            "lsn": lsn,
        },
        "op": op,
        "ts_ms": ts_ms + 150,
        "transaction": None,
    }


@dataclass
class EventStream:
    """Stateful helper producing a realistic, ordered change stream."""

    lsn: int = 24_000_000
    ts_ms: int = BASE_TS_MS

    def _tick(self, seconds: int = 60) -> tuple[int, int]:
        self.lsn += 128
        self.ts_ms += seconds * 1000
        return self.lsn, self.ts_ms

    def insert(self, row: dict[str, Any], op: str = "c") -> dict[str, Any]:
        lsn, ts = self._tick()
        return envelope(op, None, row, lsn, ts)

    def update(
        self, before: dict[str, Any], **changes: Any
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        lsn, ts = self._tick()
        after = {**before, **changes}
        return envelope("u", before, after, lsn, ts), after

    def delete(self, before: dict[str, Any]) -> dict[str, Any]:
        lsn, ts = self._tick()
        return envelope("d", before, None, lsn, ts)


def to_bytes(event: dict[str, Any] | None) -> bytes | None:
    return None if event is None else json.dumps(event).encode()


@dataclass
class FakeMessage:
    """Duck-typed ``confluent_kafka.Message``."""

    _value: bytes | None
    _offset: int = 0
    _partition: int = 0
    _topic: str = "cdc.public.customers"
    _key: bytes | None = None
    _error: Any = None

    def value(self) -> bytes | None:
        return self._value

    def key(self) -> bytes | None:
        return self._key

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def error(self) -> Any:
        return self._error


def messages(events: list[dict[str, Any] | bytes | None]) -> list[FakeMessage]:
    out = []
    for i, e in enumerate(events):
        value = e if isinstance(e, bytes) else to_bytes(e)
        out.append(FakeMessage(value, _offset=i))
    return out
