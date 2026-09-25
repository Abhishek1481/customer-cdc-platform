"""Warehouse abstraction shared by the DuckDB and Snowflake implementations."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Protocol

from customer_cdc.consumer.events import STAGING_COLUMNS, ChangeEvent
from customer_cdc.utils.config import sql_dir
from customer_cdc.utils.sql import load_named_queries, split_statements

logger = logging.getLogger(__name__)

_STAGING_COLUMN_LIST = ", ".join(STAGING_COLUMNS)


@dataclass(frozen=True)
class ScdResult:
    events_processed: int
    versions_inserted: int


class Cursor(Protocol):
    description: Any

    def execute(self, sql: str, params: Sequence[Any] | None = None, /) -> Any: ...
    def executemany(self, sql: str, seq: Sequence[Sequence[Any]], /) -> Any: ...
    def fetchall(self) -> list[tuple[Any, ...]]: ...
    def fetchone(self) -> tuple[Any, ...] | None: ...


class Warehouse(ABC):
    """Template for a SQL warehouse that holds RAW -> CORE -> ANALYTICS layers."""

    name: str = "warehouse"

    @abstractmethod
    @contextmanager
    def cursor(self) -> Iterator[Cursor]:
        """Yield a cursor; implementations translate transient errors to
        ``RetryableWarehouseError``."""

    @abstractmethod
    def schema_scripts(self) -> list[str]:
        """DDL scripts (as text) that create the warehouse objects."""

    @abstractmethod
    def incoming_table_ddl(self) -> str:
        """DDL for the session-scoped table that receives a staging batch."""

    @abstractmethod
    def insert_from_incoming_sql(self) -> str:
        """SQL moving new (not yet staged) rows from incoming to RAW."""

    def close(self) -> None:  # noqa: B027 - optional hook
        """Release long-lived resources, if any."""

    def run_script(self, cur: Cursor, script: str) -> None:
        for statement in split_statements(script):
            cur.execute(statement)

    def initialize(self) -> None:
        with self.cursor() as cur:
            for script in self.schema_scripts():
                self.run_script(cur, script)
            self.run_script(cur, (sql_dir() / "analytics_views.sql").read_text(encoding="utf-8"))
        logger.info("warehouse initialized", extra={"warehouse": self.name})

    def stage_events(self, events: Sequence[ChangeEvent]) -> int:
        """Insert events into RAW, skipping change_keys that are already staged.

        Returns the number of newly staged rows. Safe to call repeatedly with
        the same events (idempotent), which is what makes Kafka's
        at-least-once delivery harmless.
        """
        if not events:
            return 0
        placeholders = ", ".join("?" for _ in STAGING_COLUMNS)
        with self.cursor() as cur:
            cur.execute(self.incoming_table_ddl())
            cur.executemany(
                f"INSERT INTO incoming_events ({_STAGING_COLUMN_LIST}) VALUES ({placeholders})",
                [e.to_row() for e in events],
            )
            cur.execute("SELECT COUNT(*) FROM raw.customer_cdc_events")
            before = _scalar(cur)
            cur.execute(self.insert_from_incoming_sql())
            cur.execute("SELECT COUNT(*) FROM raw.customer_cdc_events")
            inserted = _scalar(cur) - before
        logger.info(
            "events staged",
            extra={
                "warehouse": self.name,
                "received": len(events),
                "staged": inserted,
                "duplicates_skipped": len(events) - inserted,
            },
        )
        return inserted

    def apply_scd(self) -> ScdResult:
        """Run snowflake/scd_type_2.sql and report what it did."""
        script = (sql_dir() / "scd_type_2.sql").read_text(encoding="utf-8")
        with self.cursor() as cur:
            try:
                self.run_script(cur, script)
            except Exception:
                _rollback_quietly(cur)
                raise
            cur.execute("SELECT COUNT(*) FROM scd_pending")
            processed = _scalar(cur)
            cur.execute("SELECT COUNT(*) FROM scd_new_versions")
            versions = _scalar(cur)
        result = ScdResult(events_processed=processed, versions_inserted=versions)
        logger.info(
            "scd type 2 applied",
            extra={
                "warehouse": self.name,
                "events_processed": processed,
                "versions_inserted": versions,
            },
        )
        return result

    def query(self, sql: str) -> tuple[list[str], list[tuple[Any, ...]]]:
        with self.cursor() as cur:
            cur.execute(sql)
            columns = [d[0].lower() for d in cur.description]
            return columns, cur.fetchall()

    def named_queries(self) -> dict[str, str]:
        return load_named_queries(sql_dir() / "analytics_queries.sql")


def _scalar(cur: Cursor) -> int:
    row = cur.fetchone()
    return int(row[0]) if row else 0


def _rollback_quietly(cur: Cursor) -> None:
    try:
        cur.execute("ROLLBACK")
    except Exception:
        logger.debug("rollback skipped (no open transaction)")


def staging_select_list(raw_payload_expr: str = "raw_payload") -> str:
    return ", ".join(raw_payload_expr if c == "raw_payload" else c for c in STAGING_COLUMNS)


STAGING_COLUMN_LIST = _STAGING_COLUMN_LIST
