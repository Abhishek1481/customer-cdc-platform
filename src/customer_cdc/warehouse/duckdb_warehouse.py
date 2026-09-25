"""DuckDB implementation of the warehouse: the local development stand-in for Snowflake.

DuckDB allows one writer process per database file, so connections are opened
per operation and closed immediately; this lets the consumer, the SCD job and
ad-hoc queries take turns on the same file. A lock conflict surfaces as
``RetryableWarehouseError`` and is retried by the caller.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from importlib import resources
from pathlib import Path

import duckdb

from customer_cdc.consumer.errors import RetryableWarehouseError
from customer_cdc.warehouse.base import (
    STAGING_COLUMN_LIST,
    Cursor,
    Warehouse,
    staging_select_list,
)


class DuckDBWarehouse(Warehouse):
    name = "duckdb"

    def __init__(self, path: str) -> None:
        self.path = path
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        # A single shared connection is required for in-memory databases,
        # otherwise every operation would see a fresh empty database.
        self._memory_conn = duckdb.connect(":memory:") if path == ":memory:" else None

    @contextmanager
    def cursor(self) -> Iterator[Cursor]:
        try:
            conn = self._memory_conn.cursor() if self._memory_conn else duckdb.connect(self.path)
        except duckdb.IOException as exc:  # file locked by another process
            raise RetryableWarehouseError(f"DuckDB file unavailable: {exc}") from exc
        try:
            conn.execute("SET TimeZone = 'UTC'")  # match the Snowflake session setting
            yield conn  # type: ignore[misc]
        except duckdb.TransactionException as exc:
            raise RetryableWarehouseError(f"DuckDB transaction conflict: {exc}") from exc
        finally:
            conn.close()

    def schema_scripts(self) -> list[str]:
        return [
            resources.files("customer_cdc.warehouse")
            .joinpath("sql/duckdb_schema.sql")
            .read_text(encoding="utf-8")
        ]

    def incoming_table_ddl(self) -> str:
        return (
            "CREATE OR REPLACE TEMPORARY TABLE incoming_events AS "
            f"SELECT {STAGING_COLUMN_LIST} FROM raw.customer_cdc_events WHERE FALSE"
        )

    def insert_from_incoming_sql(self) -> str:
        return (
            f"INSERT INTO raw.customer_cdc_events ({STAGING_COLUMN_LIST}) "
            f"SELECT {staging_select_list()} FROM incoming_events i "
            "WHERE NOT EXISTS (SELECT 1 FROM raw.customer_cdc_events e "
            "WHERE e.change_key = i.change_key) "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY change_key ORDER BY kafka_offset) = 1"
        )

    def close(self) -> None:
        if self._memory_conn is not None:
            self._memory_conn.close()
