"""Snowflake implementation of the warehouse.

Requires the optional ``snowflake`` extra and credentials in the environment
(see .env.example). Key-pair authentication is preferred over passwords.
Credentials are never logged: ``SnowflakeSettings`` hides secrets from repr.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from customer_cdc.consumer.errors import RetryableWarehouseError
from customer_cdc.utils.config import SnowflakeSettings, sql_dir
from customer_cdc.warehouse.base import (
    STAGING_COLUMN_LIST,
    Cursor,
    Warehouse,
    staging_select_list,
)

logger = logging.getLogger(__name__)


def _load_private_key(path: str, passphrase: str | None) -> bytes:
    from pathlib import Path

    from cryptography.hazmat.primitives import serialization

    key = serialization.load_pem_private_key(
        Path(path).read_bytes(),
        password=passphrase.encode() if passphrase else None,
    )
    der: bytes = key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return der


class SnowflakeWarehouse(Warehouse):
    name = "snowflake"

    def __init__(self, settings: SnowflakeSettings, connector: Any | None = None) -> None:
        self.settings = settings
        if connector is None:
            import snowflake.connector

            connector = snowflake.connector
        self._connector: Any = connector
        # Same "?" placeholders as DuckDB, so the shared staging code works unchanged.
        self._connector.paramstyle = "qmark"
        self._conn: Any | None = None

    @classmethod
    def from_env(cls) -> SnowflakeWarehouse:
        return cls(SnowflakeSettings.from_env())

    def _connect_params(self) -> dict[str, Any]:
        s = self.settings
        params: dict[str, Any] = {
            "account": s.account,
            "user": s.user,
            "database": s.database,
            "session_parameters": {"TIMEZONE": "UTC", "QUERY_TAG": "customer-cdc"},
            "client_session_keep_alive": True,
        }
        if s.role:
            params["role"] = s.role
        if s.warehouse:
            params["warehouse"] = s.warehouse
        if s.private_key_path:
            params["private_key"] = _load_private_key(s.private_key_path, s.private_key_passphrase)
        else:
            params["password"] = s.password
        return params

    def _connection(self) -> Any:
        if self._conn is None or self._conn.is_closed():
            logger.info(
                "connecting to snowflake",
                extra={"account": self.settings.account, "database": self.settings.database},
            )
            self._conn = self._connector.connect(**self._connect_params())
        return self._conn

    @contextmanager
    def cursor(self) -> Iterator[Cursor]:
        errors = self._connector.errors
        try:
            cur = self._connection().cursor()
        except (errors.OperationalError, errors.InterfaceError) as exc:
            self._conn = None
            raise RetryableWarehouseError(f"Snowflake connection failed: {exc}") from exc
        try:
            yield cur
        except (errors.OperationalError, errors.InterfaceError) as exc:
            self._conn = None
            raise RetryableWarehouseError(f"Snowflake operation failed: {exc}") from exc
        finally:
            cur.close()

    def schema_scripts(self) -> list[str]:
        return [
            (sql_dir() / "schema.sql").read_text(encoding="utf-8"),
            (sql_dir() / "staging.sql").read_text(encoding="utf-8"),
        ]

    def incoming_table_ddl(self) -> str:
        # RAW_PAYLOAD is bound as text and converted with PARSE_JSON on insert,
        # because VARIANT columns cannot be bound directly.
        return (
            "CREATE OR REPLACE TEMPORARY TABLE incoming_events AS "
            f"SELECT {staging_select_list('TO_VARCHAR(raw_payload) AS raw_payload')} "
            "FROM raw.customer_cdc_events WHERE FALSE"
        )

    def insert_from_incoming_sql(self) -> str:
        return (
            f"INSERT INTO raw.customer_cdc_events ({STAGING_COLUMN_LIST}) "
            f"SELECT {staging_select_list('PARSE_JSON(raw_payload)')} FROM incoming_events i "
            "WHERE NOT EXISTS (SELECT 1 FROM raw.customer_cdc_events e "
            "WHERE e.change_key = i.change_key) "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY change_key ORDER BY kafka_offset) = 1"
        )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
