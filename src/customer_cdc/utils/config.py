"""Environment-driven configuration. Nothing here has a default credential."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]


class ConfigError(ValueError):
    """Raised when a required setting is missing or invalid."""


def env_str(name: str, default: str | None = None, *, required: bool = False) -> str | None:
    value = os.environ.get(name)
    if value is None or value == "":
        if required:
            raise ConfigError(f"Required environment variable {name} is not set")
        return default
    return value


def env_int(name: str, default: int) -> int:
    raw = env_str(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


def env_float(name: str, default: float) -> float:
    raw = env_str(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from exc


def sql_dir() -> Path:
    """Directory holding the Snowflake/portable SQL files."""
    return Path(env_str("CDC_SQL_DIR") or PROJECT_ROOT / "snowflake")


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    topic: str
    dlq_topic: str
    group_id: str
    batch_size: int
    batch_timeout_seconds: float

    @classmethod
    def from_env(cls) -> KafkaSettings:
        return cls(
            bootstrap_servers=env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092") or "",
            topic=env_str("KAFKA_TOPIC", "cdc.public.customers") or "",
            dlq_topic=env_str("KAFKA_DLQ_TOPIC", "cdc.public.customers.dlq") or "",
            group_id=env_str("KAFKA_GROUP_ID", "customer-cdc-warehouse-loader") or "",
            batch_size=env_int("CONSUMER_BATCH_SIZE", 500),
            batch_timeout_seconds=env_float("CONSUMER_BATCH_TIMEOUT_SECONDS", 5.0),
        )


@dataclass(frozen=True)
class RetrySettings:
    max_attempts: int = 5
    initial_wait_seconds: float = 1.0
    max_wait_seconds: float = 30.0

    @classmethod
    def from_env(cls) -> RetrySettings:
        return cls(
            max_attempts=env_int("RETRY_MAX_ATTEMPTS", 5),
            initial_wait_seconds=env_float("RETRY_INITIAL_WAIT_SECONDS", 1.0),
            max_wait_seconds=env_float("RETRY_MAX_WAIT_SECONDS", 30.0),
        )


@dataclass(frozen=True)
class SnowflakeSettings:
    account: str
    user: str
    password: str | None = field(repr=False)
    private_key_path: str | None
    private_key_passphrase: str | None = field(repr=False)
    role: str | None
    warehouse: str | None
    database: str

    @classmethod
    def from_env(cls) -> SnowflakeSettings:
        settings = cls(
            account=env_str("SNOWFLAKE_ACCOUNT", required=True) or "",
            user=env_str("SNOWFLAKE_USER", required=True) or "",
            password=env_str("SNOWFLAKE_PASSWORD"),
            private_key_path=env_str("SNOWFLAKE_PRIVATE_KEY_PATH"),
            private_key_passphrase=env_str("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
            role=env_str("SNOWFLAKE_ROLE"),
            warehouse=env_str("SNOWFLAKE_WAREHOUSE"),
            database=env_str("SNOWFLAKE_DATABASE", required=True) or "",
        )
        if not settings.password and not settings.private_key_path:
            raise ConfigError("Set SNOWFLAKE_PRIVATE_KEY_PATH (preferred) or SNOWFLAKE_PASSWORD")
        return settings


@dataclass(frozen=True)
class WarehouseSettings:
    target: str
    duckdb_path: str

    @classmethod
    def from_env(cls) -> WarehouseSettings:
        target = (env_str("WAREHOUSE_TARGET", "duckdb") or "duckdb").lower()
        if target not in {"duckdb", "snowflake"}:
            raise ConfigError(f"WAREHOUSE_TARGET must be 'duckdb' or 'snowflake', got {target!r}")
        return cls(
            target=target,
            duckdb_path=env_str("DUCKDB_PATH", str(PROJECT_ROOT / "data" / "warehouse.duckdb"))
            or "",
        )


@dataclass(frozen=True)
class SourceDbSettings:
    host: str
    port: int
    dbname: str
    user: str
    password: str | None = field(repr=False)

    @classmethod
    def from_env(cls) -> SourceDbSettings:
        return cls(
            host=env_str("POSTGRES_HOST", "localhost") or "",
            port=env_int("POSTGRES_PORT", 5432),
            dbname=env_str("POSTGRES_DB", "customers_db") or "",
            user=env_str("POSTGRES_USER", required=True) or "",
            password=env_str("POSTGRES_PASSWORD"),
        )

    def conninfo(self) -> dict[str, str | int | None]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
        }
