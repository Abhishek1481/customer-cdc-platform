"""Warehouse targets: DuckDB (local development) and Snowflake."""

from __future__ import annotations

from customer_cdc.utils.config import WarehouseSettings
from customer_cdc.warehouse.base import ScdResult, Warehouse


def create_warehouse(settings: WarehouseSettings | None = None) -> Warehouse:
    settings = settings or WarehouseSettings.from_env()
    if settings.target == "snowflake":
        from customer_cdc.warehouse.snowflake_warehouse import SnowflakeWarehouse

        return SnowflakeWarehouse.from_env()
    from customer_cdc.warehouse.duckdb_warehouse import DuckDBWarehouse

    return DuckDBWarehouse(settings.duckdb_path)


__all__ = ["ScdResult", "Warehouse", "create_warehouse"]
