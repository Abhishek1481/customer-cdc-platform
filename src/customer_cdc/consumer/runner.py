"""Kafka consume loop with manual offset commits and graceful shutdown.

Delivery semantics: at-least-once. Offsets are committed only after a batch
has been durably staged in the warehouse (or dead-lettered). If the process
dies between staging and committing, the batch is re-consumed and the
warehouse's change_key de-duplication makes the replay a no-op, which gives
effectively-once results in CORE.CUSTOMER_HISTORY.
"""

from __future__ import annotations

import logging
import signal
import time
from collections.abc import Callable
from types import FrameType
from typing import Any

from confluent_kafka import KafkaError

from customer_cdc.consumer.processor import BatchProcessor
from customer_cdc.utils.config import KafkaSettings, RetrySettings
from customer_cdc.utils.retry import call_with_retry
from customer_cdc.warehouse.base import Warehouse

logger = logging.getLogger(__name__)


class CdcConsumer:
    def __init__(
        self,
        consumer: Any,
        processor: BatchProcessor,
        warehouse: Warehouse,
        kafka_settings: KafkaSettings,
        scd_interval_seconds: float = 30.0,
        retry_settings: RetrySettings | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.consumer = consumer
        self.processor = processor
        self.warehouse = warehouse
        self.settings = kafka_settings
        self.scd_interval_seconds = scd_interval_seconds
        self.retry_settings = retry_settings or RetrySettings()
        self.clock = clock
        self._running = False
        self._last_scd = clock()
        # True at start-up so events staged by a previous run that crashed
        # before its SCD step are applied without waiting for new traffic.
        self._pending_scd = True

    def install_signal_handlers(self) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._handle_signal)

    def _handle_signal(self, signum: int, _frame: FrameType | None) -> None:
        logger.info("shutdown requested", extra={"signal": signal.Signals(signum).name})
        self.stop()

    def stop(self) -> None:
        self._running = False

    def _filter_errors(self, messages: list[Any]) -> list[Any]:
        good = []
        for msg in messages:
            err = msg.error()
            if err is None:
                good.append(msg)
            elif err.code() == KafkaError._PARTITION_EOF:
                continue
            elif err.fatal():
                raise RuntimeError(f"fatal Kafka error: {err}")
            else:
                # librdkafka retries transient broker errors internally; surface them.
                logger.warning("kafka error", extra={"error": str(err)})
        return good

    def run_scd_if_due(self, force: bool = False) -> None:
        due = self.clock() - self._last_scd >= self.scd_interval_seconds
        if self._pending_scd and (force or due):
            call_with_retry(self.warehouse.apply_scd, self.retry_settings)
            self._pending_scd = False
            self._last_scd = self.clock()

    def poll_once(self) -> int:
        messages = self.consumer.consume(
            num_messages=self.settings.batch_size,
            timeout=self.settings.batch_timeout_seconds,
        )
        messages = self._filter_errors(messages)
        if messages:
            stats = self.processor.process(messages)
            # Commit only after staging + DLQ flush succeeded.
            self.consumer.commit(asynchronous=False)
            self._pending_scd = self._pending_scd or stats.staged > 0
        self.run_scd_if_due()
        return len(messages)

    def run(self, max_batches: int | None = None) -> None:
        self.consumer.subscribe([self.settings.topic])
        self._running = True
        batches = 0
        logger.info(
            "consumer started",
            extra={
                "topic": self.settings.topic,
                "group_id": self.settings.group_id,
                "warehouse": self.warehouse.name,
            },
        )
        try:
            while self._running:
                self.poll_once()
                batches += 1
                if max_batches is not None and batches >= max_batches:
                    break
            self.run_scd_if_due(force=True)
        finally:
            self.consumer.close()
            self.warehouse.close()
            logger.info("consumer stopped", extra={"batches": batches})
