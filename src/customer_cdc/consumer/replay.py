"""``cdc-replay``: feed Debezium events from a JSONL file through the same
parse -> stage -> SCD path the Kafka consumer uses.

This is the local development mode: it exercises everything downstream of
Kafka without Docker. Each line is one message value (``null`` = tombstone).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

from customer_cdc.consumer.dead_letter import InMemoryDeadLetterSink
from customer_cdc.consumer.processor import BatchProcessor, BatchStats
from customer_cdc.utils.logging_setup import configure_logging
from customer_cdc.warehouse import ScdResult, Warehouse, create_warehouse

logger = logging.getLogger("customer_cdc.replay")


@dataclass
class FileMessage:
    """Minimal stand-in for ``confluent_kafka.Message``."""

    _value: bytes | None
    _offset: int
    _topic: str = "replay"

    def value(self) -> bytes | None:
        return self._value

    def key(self) -> bytes | None:
        return None

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset

    def error(self) -> None:
        return None


def read_messages(path: Path) -> list[FileMessage]:
    messages = []
    for offset, line in enumerate(path.read_text(encoding="utf-8").splitlines()):
        stripped = line.strip()
        if not stripped:
            continue
        value = None if stripped == "null" else stripped.encode("utf-8")
        messages.append(FileMessage(value, offset, path.name))
    return messages


def replay(path: Path, warehouse: Warehouse) -> tuple[BatchStats, ScdResult, int]:
    dead_letters = InMemoryDeadLetterSink()
    stats = BatchProcessor(warehouse, dead_letters).process(read_messages(path))
    scd = warehouse.apply_scd()
    return stats, scd, len(dead_letters.rejected)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", type=Path, help="JSONL files of Debezium events")
    args = parser.parse_args(argv)
    configure_logging()
    warehouse = create_warehouse()
    warehouse.initialize()
    try:
        for path in args.files:
            stats, scd, rejected = replay(path, warehouse)
            print(
                json.dumps(
                    {
                        "file": str(path),
                        "batch": vars(stats),
                        "scd": vars(scd),
                        "rejected": rejected,
                    }
                )
            )
    finally:
        warehouse.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
