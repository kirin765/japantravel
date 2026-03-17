"""Retry helpers for network operations."""

from __future__ import annotations

from typing import Callable, TypeVar, cast

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .exceptions import ExternalServiceError

F = TypeVar("F", bound=Callable)


def with_retry(
    func: F | None = None,
    *,
    max_attempts: int = 3,
    min_wait: float = 1.0,
    max_wait: float = 5.0,
    multiplier: float = 1.0,
) -> F | Callable[[F], F]:
    """Create a retry decorator for transient external service errors.

    Supports both:
    - @with_retry
    - @with_retry(max_attempts=..., min_wait=..., max_wait=..., multiplier=...)
    """

    def decorate(target: F) -> F:
        return cast(
            F,
            retry(
                stop=stop_after_attempt(max_attempts),
                wait=wait_exponential(multiplier=multiplier, min=min_wait, max=max_wait),
                retry=retry_if_exception_type(ExternalServiceError),
                reraise=True,
            )(target),
        )

    if func is not None:
        return decorate(func)

    return decorate
