"""Batch processing logic, independent of the Kafka client so it is unit-testable."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from customer_cdc.consumer.dead_letter import DeadLetterSink
from customer_cdc.consumer.errors import MalformedEventError
from customer_cdc.consumer.events import ChangeEvent, parse_change_event
from customer_cdc.utils.config import RetrySettings
from customer_cdc.utils.retry import call_with_retry
from customer_cdc.warehouse.base import Warehouse

logger = logging.getLogger(__name__)


@dataclass
class BatchStats:
    received: int = 0
    parsed: int = 0
    tombstones: int = 0
    ignored: int = 0
    dead_lettered: int = 0
    staged: int = 0
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    snapshot_reads: int = 0

    def count_op(self, event: ChangeEvent) -> None:
        field = {"c": "inserts", "u": "updates", "d": "deletes", "r": "snapshot_reads"}[event.op]
        setattr(self, field, getattr(self, field) + 1)


class BatchProcessor:
    """Parse -> dead-letter bad messages -> stage good ones (with retry)."""

    def __init__(
        self,
        warehouse: Warehouse,
        dead_letters: DeadLetterSink,
        retry_settings: RetrySettings | None = None,
    ) -> None:
        self.warehouse = warehouse
        self.dead_letters = dead_letters
        self.retry_settings = retry_settings or RetrySettings()

    def parse(self, messages: Sequence[Any], stats: BatchStats) -> list[ChangeEvent]:
        events: list[ChangeEvent] = []
        for msg in messages:
            stats.received += 1
            value = msg.value()
            if value is None:
                # Tombstone emitted after a delete for log compaction; carries no data.
                stats.tombstones += 1
                continue
            try:
                event = parse_change_event(
                    value, topic=msg.topic(), partition=msg.partition(), offset=msg.offset()
                )
            except MalformedEventError as exc:
                stats.dead_lettered += 1
                logger.warning(
                    "malformed event sent to dead-letter topic",
                    extra={
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "reason": str(exc),
                    },
                )
                self.dead_letters.publish(msg, str(exc))
                continue
            if event is None:
                stats.ignored += 1
                continue
            stats.parsed += 1
            stats.count_op(event)
            events.append(event)
        return events

    def process(self, messages: Sequence[Any]) -> BatchStats:
        stats = BatchStats()
        events = self.parse(messages, stats)
        if events:
            stats.staged = call_with_retry(
                lambda: self.warehouse.stage_events(events), self.retry_settings
            )
        self.dead_letters.flush()
        logger.info("batch processed", extra=vars(stats))
        return stats
