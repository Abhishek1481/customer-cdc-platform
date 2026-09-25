from __future__ import annotations

import json
from datetime import datetime

import pytest

from customer_cdc.consumer.errors import MalformedEventError
from customer_cdc.consumer.events import (
    compute_record_hash,
    parse_change_event,
    parse_source_timestamp,
)
from factories import EventStream, customer_row, envelope


def test_parses_insert_event(stream: EventStream) -> None:
    raw = stream.insert(customer_row(7))
    event = parse_change_event(json.dumps(raw).encode(), topic="t", partition=1, offset=42)

    assert event is not None
    assert event.op == "c"
    assert event.customer_id == 7
    assert event.attributes["email"] == "ada7@example.com"
    assert event.source_lsn == raw["source"]["lsn"]
    assert event.event_ts == datetime(2024, 6, 1, 10, 1)
    assert event.change_key == f"7|{raw['source']['lsn']}|c"
    assert (event.kafka_topic, event.kafka_partition, event.kafka_offset) == ("t", 1, 42)
    assert not event.is_delete


def test_update_uses_after_image(stream: EventStream) -> None:
    raw, _ = stream.update(customer_row(1), city="Paris", customer_status="SUSPENDED")
    event = parse_change_event(raw)
    assert event is not None
    assert event.op == "u"
    assert event.attributes["city"] == "Paris"
    assert event.attributes["customer_status"] == "SUSPENDED"


def test_delete_uses_before_image(stream: EventStream) -> None:
    event = parse_change_event(stream.delete(customer_row(3, city="Austin")))
    assert event is not None
    assert event.is_delete
    assert event.customer_id == 3
    assert event.attributes["city"] == "Austin"


def test_accepts_schema_wrapped_envelope(stream: EventStream) -> None:
    wrapped = {"schema": {"type": "struct"}, "payload": stream.insert(customer_row(2))}
    event = parse_change_event(json.dumps(wrapped))
    assert event is not None
    assert event.customer_id == 2


def test_snapshot_read_is_a_data_operation(stream: EventStream) -> None:
    event = parse_change_event(stream.insert(customer_row(5), op="r"))
    assert event is not None
    assert event.op == "r"


@pytest.mark.parametrize("op", ["t", "m"])
def test_truncate_and_message_ops_are_ignored(op: str) -> None:
    assert parse_change_event(envelope(op, None, None, lsn=1, ts_ms=1)) is None


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (b"\xff\xfe not utf8", "UTF-8 JSON"),
        (b"{not json", "UTF-8 JSON"),
        (b"[1, 2]", "not an object"),
        (json.dumps({"op": "x"}).encode(), "unknown operation"),
        (json.dumps(envelope("c", None, None, 1, 1)).encode(), "no 'after'"),
        (json.dumps(envelope("d", None, None, 1, 1)).encode(), "no 'before'"),
        (
            json.dumps({**envelope("c", None, customer_row(1), 1, 1), "source": None}).encode(),
            "no 'source'",
        ),
        (json.dumps(envelope("c", None, customer_row("abc"), 1, 1)).encode(), "customer_id"),
        (json.dumps(envelope("c", None, customer_row(1), "12", 1, tx_id=1)).encode(), "lsn"),
        (
            json.dumps(envelope("c", None, customer_row(1, updated_at="yesterday"), 1, 1)).encode(),
            "invalid timestamp",
        ),
    ],
)
def test_malformed_events_raise(payload: bytes, reason: str) -> None:
    with pytest.raises(MalformedEventError, match=reason):
        parse_change_event(payload)


def test_timestamp_formats() -> None:
    assert parse_source_timestamp("2024-06-01T10:00:00.5Z") == datetime(
        2024, 6, 1, 10, 0, 0, 500000
    )
    assert parse_source_timestamp("2024-06-01T12:00:00+02:00") == datetime(2024, 6, 1, 10)
    # MicroTimestamp (timestamp without time zone) = epoch microseconds
    assert parse_source_timestamp(1_717_236_000_000_000) == datetime(2024, 6, 1, 10)
    assert parse_source_timestamp(None) is None


def test_record_hash_ignores_updated_at_and_distinguishes_null() -> None:
    a = customer_row(1)
    b = customer_row(1, updated_at="2030-01-01T00:00:00Z")
    assert compute_record_hash(a) == compute_record_hash(b)
    assert compute_record_hash({**a, "phone": None}) != compute_record_hash({**a, "phone": ""})
    assert compute_record_hash(a) != compute_record_hash({**a, "city": "Boston"})


def test_blank_strings_normalised_to_null(stream: EventStream) -> None:
    event = parse_change_event(stream.insert(customer_row(1, phone="   ", city=" Reno ")))
    assert event is not None
    assert event.attributes["phone"] is None
    assert event.attributes["city"] == "Reno"
