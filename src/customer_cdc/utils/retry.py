"""Retry policy for transient failures (exponential backoff with jitter)."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TypeVar

from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from customer_cdc.consumer.errors import RetryableError
from customer_cdc.utils.config import RetrySettings

T = TypeVar("T")
logger = logging.getLogger(__name__)


def _log_retry(state: RetryCallState) -> None:
    exc = state.outcome.exception() if state.outcome else None
    logger.warning(
        "transient failure, retrying",
        extra={
            "attempt": state.attempt_number,
            "wait_seconds": round(state.next_action.sleep, 2) if state.next_action else None,
            "error": str(exc),
        },
    )


def call_with_retry(func: Callable[[], T], settings: RetrySettings | None = None) -> T:
    """Call ``func`` retrying ``RetryableError`` subclasses; re-raise when exhausted."""
    settings = settings or RetrySettings()
    retrying = Retrying(
        stop=stop_after_attempt(settings.max_attempts),
        wait=wait_exponential_jitter(
            initial=settings.initial_wait_seconds, max=settings.max_wait_seconds
        ),
        retry=retry_if_exception_type(RetryableError),
        before_sleep=_log_retry,
        reraise=True,
    )
    return retrying(func)
