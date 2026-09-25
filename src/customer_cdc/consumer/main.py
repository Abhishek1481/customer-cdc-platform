"""Entry point: ``cdc-consumer`` (consume Debezium events from Kafka into the warehouse)."""

from __future__ import annotations

import argparse
import logging
import sys

from customer_cdc.consumer.dead_letter import KafkaDeadLetterSink
from customer_cdc.consumer.processor import BatchProcessor
from customer_cdc.consumer.runner import CdcConsumer
from customer_cdc.utils.config import KafkaSettings, RetrySettings, env_float
from customer_cdc.utils.logging_setup import configure_logging
from customer_cdc.warehouse import create_warehouse

logger = logging.getLogger("customer_cdc.consumer")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="stop after N poll cycles (useful for demos/tests)",
    )
    args = parser.parse_args(argv)

    configure_logging()
    from confluent_kafka import Consumer, Producer

    kafka = KafkaSettings.from_env()
    retry = RetrySettings.from_env()
    warehouse = create_warehouse()
    warehouse.initialize()

    consumer = Consumer(
        {
            "bootstrap.servers": kafka.bootstrap_servers,
            "group.id": kafka.group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": False,
            "partition.assignment.strategy": "cooperative-sticky",
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": kafka.bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    processor = BatchProcessor(warehouse, KafkaDeadLetterSink(producer, kafka.dlq_topic), retry)
    app = CdcConsumer(
        consumer,
        processor,
        warehouse,
        kafka,
        scd_interval_seconds=env_float("SCD_INTERVAL_SECONDS", 30.0),
        retry_settings=retry,
    )
    app.install_signal_handlers()
    try:
        app.run(max_batches=args.max_batches)
    except Exception:
        logger.exception("consumer failed; offsets for the failed batch were not committed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
