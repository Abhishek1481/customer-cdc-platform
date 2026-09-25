"""Dead-letter publishing for messages that cannot be parsed."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, Protocol

logger = logging.getLogger(__name__)


class DeadLetterSink(Protocol):
    def publish(self, message: Any, reason: str) -> None: ...
    def flush(self) -> None: ...


class KafkaDeadLetterSink:
    """Re-publishes the original key/value to the DLQ topic with error headers."""

    def __init__(self, producer: Any, topic: str, flush_timeout_seconds: float = 30.0) -> None:
        self._producer = producer
        self._topic = topic
        self._flush_timeout = flush_timeout_seconds
        self._delivery_errors: list[str] = []

    def _on_delivery(self, err: Any, _msg: Any) -> None:
        if err is not None:
            self._delivery_errors.append(str(err))

    def publish(self, message: Any, reason: str) -> None:
        headers = [
            ("dlq.error", reason.encode("utf-8")),
            ("dlq.source.topic", str(message.topic()).encode()),
            ("dlq.source.partition", str(message.partition()).encode()),
            ("dlq.source.offset", str(message.offset()).encode()),
            ("dlq.failed_at", datetime.now(UTC).isoformat().encode()),
        ]
        self._producer.produce(
            self._topic,
            key=message.key(),
            value=message.value(),
            headers=headers,
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self) -> None:
        """Block until DLQ writes are acknowledged.

        Raises if anything is still undelivered: offsets must not be committed
        for a message that neither reached the warehouse nor the DLQ.
        """
        remaining = self._producer.flush(self._flush_timeout)
        errors, self._delivery_errors = self._delivery_errors, []
        if remaining or errors:
            raise RuntimeError(
                f"dead-letter delivery incomplete: {remaining} pending, errors={errors}"
            )


class InMemoryDeadLetterSink:
    """Used by replay mode and tests: keeps rejected messages in memory."""

    def __init__(self) -> None:
        self.rejected: list[tuple[Any, str]] = []

    def publish(self, message: Any, reason: str) -> None:
        self.rejected.append((message, reason))
        logger.warning("event rejected", extra={"reason": reason})

    def flush(self) -> None:
        return None
