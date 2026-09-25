"""``cdc-bootstrap``: create Kafka topics and register the Debezium connector.

Both steps are idempotent: existing topics are left alone and the connector
is created-or-updated with ``PUT /connectors/<name>/config``.

    cdc-bootstrap topics
    cdc-bootstrap connector
    cdc-bootstrap all
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

import requests

from customer_cdc.utils.config import PROJECT_ROOT, env_float, env_str
from customer_cdc.utils.logging_setup import configure_logging

logger = logging.getLogger("customer_cdc.bootstrap")

TOPICS_FILE = PROJECT_ROOT / "kafka" / "topics.json"
CONNECTOR_FILE = PROJECT_ROOT / "debezium" / "connector-config.json"


def load_topic_specs(path: Path = TOPICS_FILE) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = json.loads(path.read_text(encoding="utf-8"))["topics"]
    return specs


def create_topics(bootstrap_servers: str, specs: list[dict[str, Any]]) -> dict[str, str]:
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    new_topics = [
        NewTopic(
            s["name"],
            num_partitions=s["partitions"],
            replication_factor=s["replication_factor"],
            config=s.get("config", {}),
        )
        for s in specs
    ]
    outcome: dict[str, str] = {}
    for name, future in admin.create_topics(new_topics, request_timeout=30).items():
        try:
            future.result()
            outcome[name] = "created"
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                outcome[name] = "exists"
            else:
                raise
        logger.info("topic ready", extra={"topic": name, "result": outcome[name]})
    return outcome


class ConnectClient:
    """Tiny Kafka Connect REST client."""

    def __init__(
        self, base_url: str, session: requests.Session | None = None, timeout: float = 10.0
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.timeout = timeout

    def upsert_connector(self, name: str, config: dict[str, str]) -> dict[str, Any]:
        resp = self.session.put(
            f"{self.base_url}/connectors/{name}/config", json=config, timeout=self.timeout
        )
        resp.raise_for_status()
        body: dict[str, Any] = resp.json()
        return body

    def status(self, name: str) -> dict[str, Any]:
        resp = self.session.get(f"{self.base_url}/connectors/{name}/status", timeout=self.timeout)
        resp.raise_for_status()
        body: dict[str, Any] = resp.json()
        return body

    def wait_until_running(
        self, name: str, timeout_seconds: float = 60.0, poll_seconds: float = 2.0
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        status: dict[str, Any] = {}
        while time.monotonic() < deadline:
            try:
                status = self.status(name)
            except requests.HTTPError:
                status = {}
            tasks = status.get("tasks", [])
            states = [status.get("connector", {}).get("state")] + [t.get("state") for t in tasks]
            if "FAILED" in states:
                raise RuntimeError(f"connector {name} failed: {json.dumps(status)[:2000]}")
            if tasks and all(s == "RUNNING" for s in states):
                return status
            time.sleep(poll_seconds)
        raise TimeoutError(f"connector {name} not RUNNING after {timeout_seconds}s: {status}")


def register_connector(connect_url: str, path: Path = CONNECTOR_FILE) -> dict[str, Any]:
    spec = json.loads(path.read_text(encoding="utf-8"))
    client = ConnectClient(connect_url)
    client.upsert_connector(spec["name"], spec["config"])
    status = client.wait_until_running(
        spec["name"], timeout_seconds=env_float("CONNECT_WAIT_SECONDS", 90)
    )
    logger.info("connector running", extra={"connector": spec["name"]})
    return status


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("step", choices=["topics", "connector", "all"])
    args = parser.parse_args(argv)
    configure_logging()
    if args.step in ("topics", "all"):
        create_topics(
            env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092") or "", load_topic_specs()
        )
    if args.step in ("connector", "all"):
        register_connector(env_str("KAFKA_CONNECT_URL", "http://localhost:8083") or "")
    return 0


if __name__ == "__main__":
    sys.exit(main())
