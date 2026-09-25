"""Error taxonomy used to decide between dead-lettering, retrying and crashing."""


class MalformedEventError(ValueError):
    """The message cannot be interpreted as a customer change event.

    Non-retryable: retrying the same bytes will fail the same way, so the
    message is routed to the dead-letter topic and the consumer moves on.
    """


class RetryableError(RuntimeError):
    """A transient failure (network blip, lock contention, warehouse busy)."""


class RetryableWarehouseError(RetryableError):
    """Transient warehouse failure; safe to retry the whole operation."""
