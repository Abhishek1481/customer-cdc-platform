from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pytest

from customer_cdc.consumer.replay import replay
from customer_cdc.utils.bootstrap import ConnectClient, load_topic_specs
from customer_cdc.utils.config import PROJECT_ROOT, ConfigError, WarehouseSettings
from customer_cdc.utils.logging_setup import JsonFormatter
from customer_cdc.utils.sql import load_named_queries, split_statements
from customer_cdc.warehouse.duckdb_warehouse import DuckDBWarehouse


def test_split_statements_ignores_semicolons_in_strings_and_comments() -> None:
    sql = "-- a; comment\nSELECT 'a;b';\nSELECT 2; -- trailing;\n"
    assert split_statements(sql) == ["SELECT 'a;b'", "SELECT 2"]


def test_load_named_queries(tmp_path: Path) -> None:
    f = tmp_path / "q.sql"
    f.write_text("-- name: one\nSELECT 1;\n\n-- name: two\n-- doc\nSELECT 2;\n")
    assert load_named_queries(f) == {"one": "SELECT 1", "two": "SELECT 2"}


def test_json_log_formatter_includes_extras() -> None:
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "batch processed", None, None)
    record.staged = 5
    payload = json.loads(JsonFormatter().format(record))
    assert payload["msg"] == "batch processed"
    assert payload["staged"] == 5


def test_invalid_warehouse_target(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WAREHOUSE_TARGET", "bigquery")
    with pytest.raises(ConfigError):
        WarehouseSettings.from_env()


def test_topic_specs_are_keyed_and_partitioned() -> None:
    specs = {s["name"]: s for s in load_topic_specs()}
    assert specs["cdc.public.customers"]["partitions"] == 3
    assert "cdc.public.customers.dlq" in specs


def test_connector_config_has_no_literal_credentials() -> None:
    config = json.loads((PROJECT_ROOT / "debezium" / "connector-config.json").read_text())["config"]
    assert config["database.password"].startswith("${env:")
    assert config["plugin.name"] == "pgoutput"
    assert config["table.include.list"] == "public.customers"


class FakeResponse:
    def __init__(self, body: dict[str, Any], status: int = 200) -> None:
        self.body, self.status_code = body, status

    def json(self) -> dict[str, Any]:
        return self.body

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(str(self.status_code))


class FakeSession:
    def __init__(self, statuses: list[dict[str, Any]]) -> None:
        self.statuses = statuses
        self.put_calls: list[tuple[str, Any]] = []

    def put(self, url: str, json: Any, timeout: float) -> FakeResponse:
        self.put_calls.append((url, json))
        return FakeResponse({"name": "c"})

    def get(self, url: str, timeout: float) -> FakeResponse:
        return FakeResponse(self.statuses.pop(0))


def test_connect_client_waits_for_running() -> None:
    session = FakeSession(
        [
            {"connector": {"state": "RUNNING"}, "tasks": []},
            {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]},
        ]
    )
    client = ConnectClient("http://connect:8083/", session=session)  # type: ignore[arg-type]
    client.upsert_connector("c", {"a": "b"})
    status = client.wait_until_running("c", timeout_seconds=5, poll_seconds=0)
    assert session.put_calls == [("http://connect:8083/connectors/c/config", {"a": "b"})]
    assert status["tasks"][0]["state"] == "RUNNING"


def test_connect_client_raises_on_failed_task() -> None:
    session = FakeSession([{"connector": {"state": "RUNNING"}, "tasks": [{"state": "FAILED"}]}])
    client = ConnectClient("http://connect:8083", session=session)  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="failed"):
        client.wait_until_running("c", timeout_seconds=5, poll_seconds=0)


def test_replay_sample_file_end_to_end(warehouse: DuckDBWarehouse) -> None:
    stats, scd, rejected = replay(
        PROJECT_ROOT / "sample_events" / "customer_changes.jsonl", warehouse
    )
    assert rejected == 1  # the sample intentionally contains one malformed line
    assert stats.tombstones >= 1
    assert scd.versions_inserted > 0
    _, bad = warehouse.query(
        "SELECT customer_id FROM core.customer_history GROUP BY customer_id "
        "HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1"
    )
    assert bad == []
