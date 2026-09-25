"""Snowflake adapter tests with a fake connector module (no credentials needed)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from customer_cdc.consumer.errors import RetryableWarehouseError
from customer_cdc.utils.config import ConfigError, SnowflakeSettings
from customer_cdc.warehouse.snowflake_warehouse import SnowflakeWarehouse


class OperationalError(Exception):
    pass


class InterfaceError(Exception):
    pass


class FakeCursor:
    def __init__(self, log: list[str], fail_on: str | None) -> None:
        self.log, self.fail_on = log, fail_on
        self.description = [("COUNT",)]

    def execute(self, sql: str, params: Any = None) -> None:
        if self.fail_on and self.fail_on in sql:
            raise OperationalError("network timeout")
        self.log.append(sql)

    def executemany(self, sql: str, seq: Any) -> None:
        self.log.append(sql)

    def fetchone(self) -> tuple[int]:
        return (0,)

    def fetchall(self) -> list[tuple[int]]:
        return []

    def close(self) -> None:
        pass


class FakeConnection:
    def __init__(self, log: list[str], fail_on: str | None) -> None:
        self.log, self.fail_on, self.closed = log, fail_on, False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self.log, self.fail_on)

    def is_closed(self) -> bool:
        return self.closed

    def close(self) -> None:
        self.closed = True


def fake_connector(log: list[str], fail_on: str | None = None) -> Any:
    captured: dict[str, Any] = {}

    def connect(**params: Any) -> FakeConnection:
        captured.update(params)
        return FakeConnection(log, fail_on)

    return SimpleNamespace(
        connect=connect,
        paramstyle="pyformat",
        errors=SimpleNamespace(OperationalError=OperationalError, InterfaceError=InterfaceError),
        captured=captured,
    )


SETTINGS = SnowflakeSettings(
    account="xy12345",
    user="svc_cdc",
    password="not-a-real-secret",
    private_key_path=None,
    private_key_passphrase=None,
    role="CDC_LOADER",
    warehouse="CDC_WH",
    database="CUSTOMER_CDC",
)


def test_connect_params_use_utc_and_switch_to_qmark() -> None:
    conn = fake_connector([])
    wh = SnowflakeWarehouse(SETTINGS, connector=conn)
    wh.initialize()

    assert conn.paramstyle == "qmark"
    assert conn.captured["session_parameters"]["TIMEZONE"] == "UTC"
    assert conn.captured["role"] == "CDC_LOADER"


def test_initialize_runs_snowflake_ddl() -> None:
    log: list[str] = []
    SnowflakeWarehouse(SETTINGS, connector=fake_connector(log)).initialize()
    joined = "\n".join(log)
    assert "CREATE TABLE IF NOT EXISTS CORE.CUSTOMER_HISTORY" in joined
    assert "RAW_PAYLOAD        VARIANT" in joined
    assert "CREATE OR REPLACE VIEW analytics.customer_current" in joined


def test_scd_script_is_executed_statement_by_statement() -> None:
    log: list[str] = []
    SnowflakeWarehouse(SETTINGS, connector=fake_connector(log)).apply_scd()
    assert log[0].startswith("CREATE OR REPLACE TEMPORARY TABLE scd_pending")
    assert "BEGIN" in log and "COMMIT" in log
    # DDL must come before BEGIN because Snowflake auto-commits on DDL.
    assert all("CREATE" not in s for s in log[log.index("BEGIN") :])


def test_operational_errors_become_retryable() -> None:
    wh = SnowflakeWarehouse(SETTINGS, connector=fake_connector([], fail_on="scd_pending"))
    with pytest.raises(RetryableWarehouseError):
        wh.apply_scd()


def test_secrets_are_hidden_from_repr() -> None:
    assert "not-a-real-secret" not in repr(SETTINGS)


def test_settings_require_a_credential(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in {
        "SNOWFLAKE_ACCOUNT": "a",
        "SNOWFLAKE_USER": "u",
        "SNOWFLAKE_DATABASE": "d",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)
    monkeypatch.delenv("SNOWFLAKE_PRIVATE_KEY_PATH", raising=False)
    with pytest.raises(ConfigError, match="PRIVATE_KEY_PATH"):
        SnowflakeSettings.from_env()
