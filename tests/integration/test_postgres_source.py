"""Source database tests against a real PostgreSQL (set TEST_POSTGRES_DSN).

Creates a throwaway database, applies postgres/init.sql + seed.sql and drives
the workload generator. When the server runs with wal_level=logical, it also
verifies at the WAL level that REPLICA IDENTITY FULL delivers complete
before-images for deletes - the property the SCD2 tombstone relies on.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from typing import Any

import pytest

from customer_cdc.producer.workload import CustomerWorkload
from customer_cdc.utils.config import PROJECT_ROOT

psycopg = pytest.importorskip("psycopg")
DSN = os.environ.get("TEST_POSTGRES_DSN")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not DSN, reason="TEST_POSTGRES_DSN not set"),
]


@pytest.fixture
def source_db() -> Iterator[Any]:
    name = f"cdc_test_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(DSN, autocommit=True) as admin:
        admin.execute(f"CREATE DATABASE {name}")
    conninfo = psycopg.conninfo.make_conninfo(DSN, dbname=name)
    try:
        with psycopg.connect(conninfo, autocommit=True) as conn:
            for script in ("init.sql", "seed.sql"):
                conn.execute((PROJECT_ROOT / "postgres" / script).read_text(encoding="utf-8"))
            yield conn
    finally:
        with psycopg.connect(DSN, autocommit=True) as admin:
            admin.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


def count(conn: Any) -> int:
    return int(conn.execute("SELECT COUNT(*) FROM public.customers").fetchone()[0])


def test_schema_and_seed(source_db: Any) -> None:
    assert count(source_db) == 40
    identity = source_db.execute(
        "SELECT relreplident FROM pg_class WHERE relname = 'customers'"
    ).fetchone()[0]
    assert identity == "f"  # REPLICA IDENTITY FULL
    pubs = source_db.execute(
        "SELECT tablename FROM pg_publication_tables WHERE pubname = 'customers_publication'"
    ).fetchall()
    assert pubs == [("customers",)]


def test_updated_at_trigger(source_db: Any) -> None:
    before = source_db.execute(
        "SELECT updated_at FROM public.customers WHERE customer_id = 1"
    ).fetchone()[0]
    source_db.execute("UPDATE public.customers SET city = 'Reno' WHERE customer_id = 1")
    after = source_db.execute(
        "SELECT updated_at FROM public.customers WHERE customer_id = 1"
    ).fetchone()[0]
    assert after > before


def test_workload_generates_inserts_updates_deletes(source_db: Any) -> None:
    result = CustomerWorkload(source_db, seed=7).run(inserts=5, updates=10, deletes=2)
    assert (result.inserted, result.updated, result.deleted) == (5, 10, 2)
    assert count(source_db) == 40 + 5 - 2


def test_status_check_constraint(source_db: Any) -> None:
    with pytest.raises(psycopg.errors.CheckViolation):
        source_db.execute(
            "UPDATE public.customers SET customer_status = 'VIP' WHERE customer_id = 1"
        )


def _pgoutput_tuple(data: bytes, pos: int) -> tuple[list[str | None], int]:
    """Decode a pgoutput TupleData block starting at ``pos``."""
    ncols = int.from_bytes(data[pos : pos + 2], "big")
    pos += 2
    values: list[str | None] = []
    for _ in range(ncols):
        kind = chr(data[pos])
        pos += 1
        if kind == "t":
            length = int.from_bytes(data[pos : pos + 4], "big")
            values.append(data[pos + 4 : pos + 4 + length].decode())
            pos += 4 + length
        else:  # 'n' null / 'u' unchanged TOAST
            values.append(None)
    return values, pos


def test_wal_contains_full_before_image_for_delete(source_db: Any) -> None:
    """Reads the WAL through pgoutput + customers_publication - exactly what
    Debezium uses - and checks the DELETE carries the whole old row."""
    wal_level = source_db.execute("SHOW wal_level").fetchone()[0]
    if wal_level != "logical":
        pytest.skip("server not started with wal_level=logical")
    slot = f"test_slot_{uuid.uuid4().hex[:8]}"
    source_db.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,))
    try:
        lifecycle_id = CustomerWorkload(source_db, seed=1).lifecycle()
        messages = [
            bytes(row[0])
            for row in source_db.execute(
                "SELECT data FROM pg_logical_slot_get_binary_changes(%s, NULL, NULL, "
                "'proto_version', '1', 'publication_names', 'customers_publication')",
                (slot,),
            )
        ]
    finally:
        source_db.execute("SELECT pg_drop_replication_slot(%s)", (slot,))

    kinds = [chr(m[0]) for m in messages if chr(m[0]) in "IUD"]
    assert kinds == ["I", "U", "U", "D"]

    delete = next(m for m in messages if chr(m[0]) == "D")
    # 'D' | relation oid (4 bytes) | 'O' = full old tuple ('K' would be key only)
    assert chr(delete[5]) == "O"
    old_row, _ = _pgoutput_tuple(delete, 6)
    assert old_row[0] == str(lifecycle_id)
    assert "San Francisco" in old_row
    assert "SUSPENDED" in old_row
