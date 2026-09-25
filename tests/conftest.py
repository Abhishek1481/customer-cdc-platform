from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from customer_cdc.warehouse.duckdb_warehouse import DuckDBWarehouse
from factories import EventStream


@pytest.fixture
def warehouse(tmp_path: Path) -> Iterator[DuckDBWarehouse]:
    wh = DuckDBWarehouse(str(tmp_path / "warehouse.duckdb"))
    wh.initialize()
    yield wh
    wh.close()


@pytest.fixture
def stream() -> EventStream:
    return EventStream()
