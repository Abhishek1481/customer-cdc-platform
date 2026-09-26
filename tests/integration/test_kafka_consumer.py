"""Consumer tests against a real Kafka broker (set TEST_KAFKA_BOOTSTRAP).

Publishes Debezium-format events to a fresh topic, runs the real
confluent-kafka consumer loop into DuckDB, then checks offsets, DLQ routing
and the resulting SCD2 history. Runs in CI with a Kafka service container.
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest

from customer_cdc.consumer.dead_letter import KafkaDeadLetterSink
from customer_cdc.consumer.processor import BatchProcessor
from customer_cdc.consumer.runner import CdcConsumer
from customer_cdc.utils.bootstrap import create_topics
from customer_cdc.utils.config import KafkaSettings, RetrySettings
from customer_cdc.warehouse.duckdb_warehouse import DuckDBWarehouse
from factories import EventStream, customer_row

BOOTSTRAP = os.environ.get("TEST_KAFKA_BOOTSTRAP")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not BOOTSTRAP, reason="TEST_KAFKA_BOOTSTRAP not set"),
]


def test_consume_stage_commit_and_dead_letter(tmp_path: Path) -> None:
    from confluent_kafka import Consumer, Producer, TopicPartition

    suffix = uuid.uuid4().hex[:8]
    topic, dlq, group = f"it.customers.{suffix}", f"it.customers.{suffix}.dlq", f"it-{suffix}"
    create_topics(
        BOOTSTRAP or "",
        [
            {"name": topic, "partitions": 3, "replication_factor": 1},
            {"name": dlq, "partitions": 1, "replication_factor": 1},
        ],
    )

    stream = EventStream()
    # Build events in source order: each helper call advances the LSN.
    row = customer_row(1)
    ins = stream.insert(row)
    upd, row = stream.update(row, city="Denver")
    events = [
        ins,
        upd,
        stream.insert(customer_row(2)),
        stream.delete(row),
    ]
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    for event in events:
        customer_id = (event["after"] or event["before"])["customer_id"]
        key = json.dumps({"customer_id": customer_id})
        producer.produce(topic, key=key.encode(), value=json.dumps(event).encode())
    producer.produce(topic, key=b'{"customer_id": 1}', value=None)  # tombstone
    producer.produce(topic, key=b'{"customer_id": 9}', value=b"{broken")
    assert producer.flush(30) == 0

    settings = KafkaSettings(
        BOOTSTRAP or "", topic, dlq, group, batch_size=100, batch_timeout_seconds=3.0
    )
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    warehouse = DuckDBWarehouse(str(tmp_path / "wh.duckdb"))
    warehouse.initialize()
    fast = RetrySettings(max_attempts=2, initial_wait_seconds=0, max_wait_seconds=0)
    processor = BatchProcessor(
        warehouse, KafkaDeadLetterSink(Producer({"bootstrap.servers": BOOTSTRAP}), dlq), fast
    )
    CdcConsumer(
        consumer, processor, warehouse, settings, scd_interval_seconds=0, retry_settings=fast
    ).run(max_batches=10)

    _, history = warehouse.query(
        "SELECT customer_id, city, is_deleted, is_current FROM core.customer_history "
        "ORDER BY customer_id, source_lsn"
    )
    assert history == [
        (1, "London", False, False),
        (1, "Denver", False, False),
        (1, "Denver", True, True),
        (2, "London", False, True),
    ]

    # Offsets were committed: a new consumer in the same group sees nothing.
    check = Consumer(
        {"bootstrap.servers": BOOTSTRAP, "group.id": group, "enable.auto.commit": False}
    )
    committed = check.committed([TopicPartition(topic, p) for p in range(3)], timeout=10)
    assert sum(max(tp.offset, 0) for tp in committed) == 6
    check.close()

    # The malformed message landed on the DLQ with its error header.
    dlq_consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"{group}-dlq",
            "auto.offset.reset": "earliest",
        }
    )
    dlq_consumer.subscribe([dlq])
    msg = None
    for _ in range(20):
        msg = dlq_consumer.poll(1.0)
        if msg is not None and msg.error() is None:
            break
    dlq_consumer.close()
    assert msg is not None and msg.value() == b"{broken"
    assert dict(msg.headers())["dlq.error"].startswith(b"message is not valid UTF-8 JSON")
