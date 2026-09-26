"""Runs the shared snowflake/scd_type_2.sql against an in-process DuckDB warehouse."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from customer_cdc.consumer.events import ChangeEvent, parse_change_event
from customer_cdc.warehouse.duckdb_warehouse import DuckDBWarehouse
from factories import EventStream, customer_row


def parse_all(raw_events: list[dict[str, Any]]) -> list[ChangeEvent]:
    events = [
        parse_change_event(e, topic="t", partition=0, offset=i) for i, e in enumerate(raw_events)
    ]
    return [e for e in events if e is not None]


def history(wh: DuckDBWarehouse, customer_id: int) -> list[dict[str, Any]]:
    cols, rows = wh.query(
        "SELECT customer_status, city, is_deleted, valid_from, valid_to, is_current, source_op "
        f"FROM core.customer_history WHERE customer_id = {customer_id} ORDER BY source_lsn"
    )
    return [dict(zip(cols, r, strict=True)) for r in rows]


def test_insert_creates_single_current_version(
    warehouse: DuckDBWarehouse, stream: EventStream
) -> None:
    warehouse.stage_events(parse_all([stream.insert(customer_row(1))]))
    result = warehouse.apply_scd()

    assert result.versions_inserted == 1
    [row] = history(warehouse, 1)
    assert row["is_current"] is True
    assert row["valid_to"] is None
    assert row["is_deleted"] is False


def test_update_closes_old_version_and_opens_new(
    warehouse: DuckDBWarehouse, stream: EventStream
) -> None:
    row = customer_row(1)
    warehouse.stage_events(parse_all([stream.insert(row)]))
    warehouse.apply_scd()

    update, _ = stream.update(row, city="Denver")
    warehouse.stage_events(parse_all([update]))
    warehouse.apply_scd()

    old, new = history(warehouse, 1)
    assert old["is_current"] is False
    assert old["valid_to"] == new["valid_from"] == datetime(2024, 6, 1, 10, 2)
    assert new["is_current"] is True
    assert new["valid_to"] is None
    assert new["city"] == "Denver"


def test_multiple_changes_in_one_batch_are_chained(
    warehouse: DuckDBWarehouse, stream: EventStream
) -> None:
    row = customer_row(1)
    ins = stream.insert(row)
    u1, row = stream.update(row, city="Denver")
    u2, row = stream.update(row, customer_status="SUSPENDED")
    # Arrival order shuffled: ordering must come from the LSN, not arrival.
    warehouse.stage_events(parse_all([u2, ins, u1]))
    warehouse.apply_scd()

    versions = history(warehouse, 1)
    assert [v["source_op"] for v in versions] == ["c", "u", "u"]
    assert [v["is_current"] for v in versions] == [False, False, True]
    assert versions[0]["valid_to"] == versions[1]["valid_from"]
    assert versions[1]["valid_to"] == versions[2]["valid_from"]
    assert versions[2]["customer_status"] == "SUSPENDED"


def test_delete_creates_current_tombstone_version(
    warehouse: DuckDBWarehouse, stream: EventStream
) -> None:
    row = customer_row(1, city="Austin")
    warehouse.stage_events(parse_all([stream.insert(row), stream.delete(row)]))
    warehouse.apply_scd()

    live, tomb = history(warehouse, 1)
    assert live["is_current"] is False
    assert live["valid_to"] == tomb["valid_from"]
    assert tomb["is_deleted"] is True
    assert tomb["is_current"] is True
    assert tomb["city"] == "Austin"  # last known attributes are preserved

    _, current = warehouse.query("SELECT * FROM analytics.customer_current")
    assert current == []


def test_reinsert_after_delete_reopens_customer(
    warehouse: DuckDBWarehouse, stream: EventStream
) -> None:
    row = customer_row(1)
    warehouse.stage_events(parse_all([stream.insert(row), stream.delete(row)]))
    warehouse.apply_scd()
    warehouse.stage_events(parse_all([stream.insert(customer_row(1, email="new@example.com"))]))
    warehouse.apply_scd()

    versions = history(warehouse, 1)
    assert [v["is_deleted"] for v in versions] == [False, True, False]
    assert [v["is_current"] for v in versions] == [False, False, True]


def test_noop_update_does_not_create_version(
    warehouse: DuckDBWarehouse, stream: EventStream
) -> None:
    row = customer_row(1)
    touch, _ = stream.update(row, updated_at="2024-06-02T00:00:00Z")  # only updated_at changed
    warehouse.stage_events(parse_all([stream.insert(row), touch]))
    result = warehouse.apply_scd()

    assert result.events_processed == 2
    assert result.versions_inserted == 1
    assert len(history(warehouse, 1)) == 1


def test_duplicate_delivery_is_idempotent(warehouse: DuckDBWarehouse, stream: EventStream) -> None:
    row = customer_row(1)
    ins = stream.insert(row)
    upd, _ = stream.update(row, city="Denver")
    events = parse_all([ins, upd])

    assert warehouse.stage_events(events) == 2
    warehouse.apply_scd()
    # Kafka redelivers the same batch (e.g. crash before offset commit).
    assert warehouse.stage_events(events) == 0
    assert warehouse.apply_scd().versions_inserted == 0
    assert len(history(warehouse, 1)) == 2


def test_replayed_events_older_than_current_version_are_ignored(
    warehouse: DuckDBWarehouse, stream: EventStream
) -> None:
    row = customer_row(1)
    ins = stream.insert(row)
    upd, _ = stream.update(row, city="Denver")
    warehouse.stage_events(parse_all([ins, upd]))
    warehouse.apply_scd()

    # Same change re-emitted at a different Kafka offset (connector restart).
    replay = parse_all([ins])[0]
    replay = ChangeEvent(**{**replay.__dict__, "op": "r"})  # new change_key, old LSN
    warehouse.stage_events([replay])
    assert warehouse.apply_scd().versions_inserted == 0
    assert history(warehouse, 1)[-1]["city"] == "Denver"


def test_only_one_current_version_per_customer(
    warehouse: DuckDBWarehouse, stream: EventStream
) -> None:
    rows = {i: customer_row(i) for i in range(1, 6)}
    batch = [stream.insert(r) for r in rows.values()]
    for i in (1, 2, 3):
        upd, rows[i] = stream.update(rows[i], customer_status="INACTIVE")
        batch.append(upd)
    batch.append(stream.delete(rows[4]))
    warehouse.stage_events(parse_all(batch))
    warehouse.apply_scd()

    _, bad = warehouse.query(
        "SELECT customer_id FROM core.customer_history GROUP BY customer_id "
        "HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1"
    )
    assert bad == []
    _, summary = warehouse.query(
        "SELECT version_count, customers FROM ("
        + warehouse.named_queries()["versions_per_customer"]
        + ")"
    )
    assert summary == [(1, 1), (2, 4)]


def test_all_named_analytics_queries_execute(
    warehouse: DuckDBWarehouse, stream: EventStream
) -> None:
    row = customer_row(1)
    ins = stream.insert(row)
    upd, row = stream.update(row, city="Denver")
    warehouse.stage_events(parse_all([ins, upd]))
    warehouse.apply_scd()

    queries = warehouse.named_queries()
    assert {
        "current_customers",
        "customer_history",
        "changed_customers",
        "versions_per_customer",
        "customer_as_of",
        "pipeline_lag",
    } <= set(queries)
    for sql in queries.values():
        warehouse.query(sql)
    _, changed = warehouse.query(queries["changed_customers"])
    assert changed[0][0] == 1
