from __future__ import annotations

from typing import Any

import pytest
from confluent_kafka import KafkaError

from customer_cdc.consumer.dead_letter import InMemoryDeadLetterSink, KafkaDeadLetterSink
from customer_cdc.consumer.errors import RetryableWarehouseError
from customer_cdc.consumer.processor import BatchProcessor
from customer_cdc.consumer.runner import CdcConsumer
from customer_cdc.utils.config import KafkaSettings, RetrySettings
from customer_cdc.warehouse.duckdb_warehouse import DuckDBWarehouse
from factories import EventStream, FakeMessage, customer_row, messages

FAST_RETRY = RetrySettings(max_attempts=3, initial_wait_seconds=0, max_wait_seconds=0)
KAFKA = KafkaSettings("unused:9092", "cdc.public.customers", "dlq", "g", 100, 0.1)


class FakeKafkaConsumer:
    def __init__(self, batches: list[list[Any]]) -> None:
        self.batches = batches
        self.commits = 0
        self.closed = False
        self.subscribed: list[str] = []

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = topics

    def consume(self, num_messages: int, timeout: float) -> list[Any]:
        return self.batches.pop(0) if self.batches else []

    def commit(self, asynchronous: bool) -> None:
        assert asynchronous is False
        self.commits += 1

    def close(self) -> None:
        self.closed = True


class FlakyWarehouse(DuckDBWarehouse):
    """Fails stage_events a configurable number of times before succeeding."""

    def __init__(self, path: str, failures: int) -> None:
        super().__init__(path)
        self.failures = failures
        self.calls = 0

    def stage_events(self, events: Any) -> int:
        self.calls += 1
        if self.calls <= self.failures:
            raise RetryableWarehouseError("simulated lock conflict")
        return super().stage_events(events)


class FakeError:
    def __init__(self, code: int, fatal: bool = False) -> None:
        self._code, self._fatal = code, fatal

    def code(self) -> int:
        return self._code

    def fatal(self) -> bool:
        return self._fatal


def test_processor_counts_operations_and_dead_letters(
    warehouse: DuckDBWarehouse, stream: EventStream
) -> None:
    row = customer_row(1)
    ins = stream.insert(row)
    upd, row = stream.update(row, city="Denver")
    batch = messages([ins, upd, stream.delete(row), None, b"garbage"])
    dlq = InMemoryDeadLetterSink()

    stats = BatchProcessor(warehouse, dlq, FAST_RETRY).process(batch)

    assert (stats.received, stats.parsed, stats.tombstones, stats.dead_lettered) == (5, 3, 1, 1)
    assert (stats.inserts, stats.updates, stats.deletes, stats.staged) == (1, 1, 1, 3)
    assert "UTF-8 JSON" in dlq.rejected[0][1]


def test_processor_retries_transient_warehouse_errors(tmp_path: Any, stream: EventStream) -> None:
    wh = FlakyWarehouse(str(tmp_path / "w.duckdb"), failures=2)
    wh.initialize()
    stats = BatchProcessor(wh, InMemoryDeadLetterSink(), FAST_RETRY).process(
        messages([stream.insert(customer_row(1))])
    )
    assert wh.calls == 3
    assert stats.staged == 1


def test_processor_gives_up_after_max_attempts(tmp_path: Any, stream: EventStream) -> None:
    wh = FlakyWarehouse(str(tmp_path / "w.duckdb"), failures=10)
    wh.initialize()
    with pytest.raises(RetryableWarehouseError):
        BatchProcessor(wh, InMemoryDeadLetterSink(), FAST_RETRY).process(
            messages([stream.insert(customer_row(1))])
        )
    assert wh.calls == FAST_RETRY.max_attempts


def test_consumer_commits_after_staging_and_applies_scd(
    warehouse: DuckDBWarehouse, stream: EventStream
) -> None:
    kafka = FakeKafkaConsumer([messages([stream.insert(customer_row(1))]), []])
    app = CdcConsumer(
        kafka,
        BatchProcessor(warehouse, InMemoryDeadLetterSink(), FAST_RETRY),
        warehouse,
        KAFKA,
        scd_interval_seconds=3600,
        retry_settings=FAST_RETRY,
    )
    app.run(max_batches=2)

    assert kafka.subscribed == ["cdc.public.customers"]
    assert kafka.commits == 1  # empty poll does not commit
    assert kafka.closed
    # SCD interval not reached during the loop, but shutdown forces a final run.
    _, rows = warehouse.query("SELECT COUNT(*) FROM core.customer_history")
    assert rows == [(1,)]


def test_consumer_does_not_commit_when_staging_fails(tmp_path: Any, stream: EventStream) -> None:
    wh = FlakyWarehouse(str(tmp_path / "w.duckdb"), failures=10)
    wh.initialize()
    kafka = FakeKafkaConsumer([messages([stream.insert(customer_row(1))])])
    app = CdcConsumer(
        kafka,
        BatchProcessor(wh, InMemoryDeadLetterSink(), FAST_RETRY),
        wh,
        KAFKA,
        retry_settings=FAST_RETRY,
    )
    with pytest.raises(RetryableWarehouseError):
        app.run(max_batches=1)
    assert kafka.commits == 0
    assert kafka.closed


def test_consumer_skips_partition_eof_and_raises_on_fatal(warehouse: DuckDBWarehouse) -> None:
    eof = FakeMessage(None, _error=FakeError(KafkaError._PARTITION_EOF))
    app = CdcConsumer(
        FakeKafkaConsumer([[eof]]),
        BatchProcessor(warehouse, InMemoryDeadLetterSink()),
        warehouse,
        KAFKA,
    )
    assert app.poll_once() == 0

    fatal = FakeMessage(None, _error=FakeError(-1, fatal=True))
    app = CdcConsumer(
        FakeKafkaConsumer([[fatal]]),
        BatchProcessor(warehouse, InMemoryDeadLetterSink()),
        warehouse,
        KAFKA,
    )
    with pytest.raises(RuntimeError, match="fatal"):
        app.poll_once()


def test_stop_ends_run_loop(warehouse: DuckDBWarehouse) -> None:
    kafka = FakeKafkaConsumer([])
    app = CdcConsumer(kafka, BatchProcessor(warehouse, InMemoryDeadLetterSink()), warehouse, KAFKA)
    original = app.poll_once

    def poll_then_stop() -> int:
        app.stop()  # simulates SIGTERM arriving mid-batch
        return original()

    app.poll_once = poll_then_stop  # type: ignore[method-assign]
    app.run()
    assert kafka.closed


class FakeProducer:
    def __init__(self, fail_delivery: bool = False, pending: int = 0) -> None:
        self.produced: list[dict[str, Any]] = []
        self.fail_delivery = fail_delivery
        self.pending = pending
        self._callbacks: list[Any] = []

    def produce(self, topic: str, key: Any, value: Any, headers: Any, on_delivery: Any) -> None:
        self.produced.append({"topic": topic, "value": value, "headers": dict(headers)})
        self._callbacks.append(on_delivery)

    def poll(self, _timeout: float) -> None:
        return None

    def flush(self, _timeout: float) -> int:
        for cb in self._callbacks:
            cb("broker down" if self.fail_delivery else None, None)
        self._callbacks = []
        return self.pending


def test_kafka_dlq_preserves_payload_and_adds_error_headers() -> None:
    producer = FakeProducer()
    sink = KafkaDeadLetterSink(producer, "cdc.dlq")
    sink.publish(FakeMessage(b"bad", _offset=9, _partition=2), "unknown operation 'x'")
    sink.flush()

    [record] = producer.produced
    assert record["topic"] == "cdc.dlq"
    assert record["value"] == b"bad"
    assert record["headers"]["dlq.error"] == b"unknown operation 'x'"
    assert record["headers"]["dlq.source.offset"] == b"9"


@pytest.mark.parametrize("producer", [FakeProducer(fail_delivery=True), FakeProducer(pending=1)])
def test_kafka_dlq_flush_raises_when_delivery_incomplete(producer: FakeProducer) -> None:
    sink = KafkaDeadLetterSink(producer, "cdc.dlq")
    sink.publish(FakeMessage(b"bad"), "reason")
    with pytest.raises(RuntimeError, match="dead-letter delivery incomplete"):
        sink.flush()
