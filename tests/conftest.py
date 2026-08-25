"""Shared test fixtures and helpers."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Make the repository root importable so the suite runs from a clone with no
# install step. Keeps `pytest` working straight after `pip install -r`.
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from trading.events import OrderEvent  # noqa: E402
from trading.transport import SendResult  # noqa: E402


class RecordingPublisher:
    """An in-memory :class:`~trading.transport.EventPublisher` for tests.

    Records everything it is asked to send, so a test can assert on both the
    content and the order of delivery without any network involved.
    """

    def __init__(self, result: SendResult = SendResult.ACCEPTED) -> None:
        self.result = result
        self.events: list[OrderEvent] = []

    def send(self, event: OrderEvent) -> SendResult:
        self.events.append(event)
        return self.result

    @property
    def event_ids(self) -> list[str]:
        return [event.event_id for event in self.events]


class FakeClock:
    """A controllable monotonic clock and sleep pair.

    Lets throttling tests assert the exact pacing decisions instead of
    measuring wall-clock time, which keeps them fast and non-flaky.
    """

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, duration: float) -> None:
        self.sleeps.append(duration)
        self.now += duration

    def advance(self, duration: float) -> None:
        """Move time forward without recording a sleep."""
        self.now += duration


@pytest.fixture
def publisher() -> RecordingPublisher:
    return RecordingPublisher()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def write_csv(tmp_path: Path):
    """Return a helper that writes CSV lines to a temporary file."""

    def _write(*lines: str, name: str = "orders.csv") -> Path:
        path = tmp_path / name
        path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="")
        return path

    return _write


def row(
    event_id: str = "evt-1",
    symbol: str = "RELIANCE",
    transaction_type: str = "BUY",
    quantity: object = 10,
) -> dict[str, object]:
    """Build a payload dict, overriding only the field under test."""
    return {
        "event_id": event_id,
        "symbol": symbol,
        "transaction_type": transaction_type,
        "quantity": quantity,
    }
