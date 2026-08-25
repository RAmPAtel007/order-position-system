"""In-memory net position state for the Position Maintaining Service."""

from __future__ import annotations

import threading
from dataclasses import dataclass

from trading.events import OrderEvent


@dataclass(frozen=True)
class StoreStats:
    """A consistent point-in-time summary of the store."""

    applied_events: int
    duplicate_events: int
    symbols: int


class PositionStore:
    """Tracks the net position per symbol and the event IDs already applied.

    Thread-safe. The service runs its request handlers in a worker thread pool
    so that ``GET /position`` stays responsive while events are streaming in,
    which means ingest and reads genuinely run in parallel. A single lock
    guards both the positions and the seen-event set so they can never be
    observed out of step with each other.

    The lock is held only for small dictionary operations, so read latency
    stays low even under a continuous ingest load.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._positions: dict[str, int] = {}
        self._seen_event_ids: set[str] = set()
        self._duplicate_count = 0

    def apply(self, event: OrderEvent) -> bool:
        """Apply ``event`` to the net position for its symbol.

        The first event accepted for an ``event_id`` wins; any later event
        carrying that ID is ignored even if its other fields differ.

        Returns:
            ``True`` if the event was applied, ``False`` if it was a duplicate.
        """
        with self._lock:
            if event.event_id in self._seen_event_ids:
                self._duplicate_count += 1
                return False

            self._seen_event_ids.add(event.event_id)
            # setdefault keeps the symbol present even once it nets to zero,
            # which the GET /position contract requires.
            current = self._positions.setdefault(event.symbol, 0)
            self._positions[event.symbol] = current + event.signed_quantity
            return True

    def snapshot(self) -> dict[str, int]:
        """Return a copy of every symbol seen in an accepted event.

        Symbols whose net position is zero are included. The copy is taken
        under the lock, so a response can never show a half-applied event.
        """
        with self._lock:
            return dict(self._positions)

    def stats(self) -> StoreStats:
        """Return counters for health and log output."""
        with self._lock:
            return StoreStats(
                applied_events=len(self._seen_event_ids),
                duplicate_events=self._duplicate_count,
                symbols=len(self._positions),
            )

    def has_seen(self, event_id: str) -> bool:
        """Whether ``event_id`` has already been applied."""
        with self._lock:
            return event_id in self._seen_event_ids
