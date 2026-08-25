"""Delivery of order events from the producer to the Position service."""

from __future__ import annotations

import enum
import logging
import time
from typing import Callable, Protocol

import httpx

from trading.events import OrderEvent

LOGGER = logging.getLogger("transport")


class SendResult(enum.Enum):
    """The outcome of trying to deliver one event."""

    ACCEPTED = "accepted"
    """The receiver applied the event."""

    DUPLICATE = "duplicate"
    """The receiver had already applied this event_id and ignored it."""

    REJECTED = "rejected"
    """The receiver refused the event as invalid. Retrying cannot help."""

    FAILED = "failed"
    """Delivery did not succeed after exhausting retries."""


class EventPublisher(Protocol):
    """What the Order Update Service needs from a delivery mechanism.

    Depending on this narrow interface rather than on httpx keeps the pipeline
    testable with a recording double, and means swapping HTTP for another
    transport would not touch the producer's logic.
    """

    def send(self, event: OrderEvent) -> SendResult:
        """Deliver one event."""
        ...


class HttpEventPublisher:
    """Delivers events to the Position service over HTTP.

    Delivery is at-least-once. A request that fails after the receiver has
    already applied it (a timeout on the response, say) is retried, so the
    receiver can see the same event twice. That is safe because every event
    carries an ``event_id`` and the receiver ignores IDs it has already
    applied, which makes the observable effect exactly-once.

    Connection and delivery errors are surfaced two ways: each attempt is
    logged with its cause, and an event that exhausts its retries is returned
    as :attr:`SendResult.FAILED` so the caller can count it and set a non-zero
    exit status.
    """

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 5.0,
        max_attempts: int = 3,
        backoff_seconds: float = 0.25,
        client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._backoff_seconds = backoff_seconds
        self._sleep = sleep
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=timeout)

    @property
    def events_url(self) -> str:
        """Where events are POSTed."""
        return f"{self._base_url}/events"

    def wait_until_ready(self, timeout: float = 10.0, poll_interval: float = 0.2) -> bool:
        """Poll ``/health`` until the receiver answers or ``timeout`` elapses.

        Waiting up front turns a race during startup into a short, silent
        delay instead of a burst of connection errors on the first events.
        """
        deadline = time.monotonic() + timeout
        attempt = 0
        while True:
            attempt += 1
            try:
                response = self._client.get(f"{self._base_url}/health", timeout=self._timeout)
                if response.status_code == 200:
                    return True
                LOGGER.debug(
                    "Health check returned HTTP %d, retrying", response.status_code
                )
            except httpx.HTTPError as exc:
                LOGGER.debug("Health check attempt %d failed: %s", attempt, exc)

            if time.monotonic() >= deadline:
                return False
            self._sleep(poll_interval)

    def send(self, event: OrderEvent) -> SendResult:
        """POST one event, retrying transient failures.

        Retried: connection errors, timeouts, and 5xx responses, since the
        receiver may simply not be ready yet.

        Not retried: 4xx responses. The receiver understood the request and
        refused it, so an identical retry would be refused identically.
        """
        payload = event.to_payload()
        last_error: str | None = None

        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._client.post(
                    self.events_url, json=payload, timeout=self._timeout
                )
            except httpx.HTTPError as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                LOGGER.warning(
                    "Delivery of %s failed on attempt %d/%d: %s",
                    event.event_id,
                    attempt,
                    self._max_attempts,
                    last_error,
                )
                self._backoff(attempt)
                continue

            if response.status_code == 202:
                return SendResult.ACCEPTED
            if response.status_code == 200:
                return SendResult.DUPLICATE
            if 400 <= response.status_code < 500:
                LOGGER.error(
                    "Receiver rejected %s with HTTP %d: %s",
                    event.event_id,
                    response.status_code,
                    _describe(response),
                )
                return SendResult.REJECTED

            last_error = f"HTTP {response.status_code}: {_describe(response)}"
            LOGGER.warning(
                "Delivery of %s failed on attempt %d/%d: %s",
                event.event_id,
                attempt,
                self._max_attempts,
                last_error,
            )
            self._backoff(attempt)

        LOGGER.error(
            "Giving up on %s after %d attempt(s): %s",
            event.event_id,
            self._max_attempts,
            last_error,
        )
        return SendResult.FAILED

    def _backoff(self, attempt: int) -> None:
        """Wait before the next attempt, growing the delay exponentially."""
        if attempt >= self._max_attempts or self._backoff_seconds <= 0:
            return
        self._sleep(self._backoff_seconds * (2 ** (attempt - 1)))

    def close(self) -> None:
        """Release the HTTP connection pool if this object created it."""
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> HttpEventPublisher:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def _describe(response: httpx.Response, limit: int = 200) -> str:
    """Summarise a response body for a log line, without flooding it."""
    try:
        text = response.text
    except Exception:  # pragma: no cover - body already consumed or undecodable
        return "<unreadable body>"
    text = " ".join(text.split())
    return text[:limit] + "..." if len(text) > limit else text
