"""Outbound rate limiting for the Order Update Service."""

from __future__ import annotations

import time
from collections import deque
from typing import Callable

WINDOW_SECONDS = 1.0


class RateLimiter:
    """Paces calls so no one-second window ever contains more than ``rate``.

    Two mechanisms work together, because either alone has a gap:

    * **Even pacing** keeps a minimum interval of ``1 / rate`` between
      releases. This spreads the load smoothly instead of firing a burst and
      then idling.
    * **A sliding window** records the last ``rate`` release times and blocks
      until the oldest one ages out. Even pacing alone is not quite enough: it
      only bounds the gap between neighbours, so a slow send followed by a
      catch-up could still cluster events. The window guard bounds the count
      directly, making the documented ceiling exact rather than approximate.

    The window is half-open, ``[t, t + 1s)``, which is the usual rate-limiting
    convention: a release exactly one second after an earlier one belongs to
    the next window, not the same one.

    The clock and sleep functions are injectable so tests can assert pacing
    decisions directly rather than measuring wall-clock time, which keeps the
    suite fast and free of timing flakiness.

    Args:
        rate_per_second: Maximum releases in any one-second window. Zero or
            negative disables throttling, which is useful in tests.
    """

    def __init__(
        self,
        rate_per_second: float,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._rate = float(rate_per_second)
        self._interval = 1.0 / self._rate if self._rate > 0 else 0.0
        self._capacity = int(self._rate) if self._rate > 0 else 0
        self._monotonic = monotonic
        self._sleep = sleep
        self._next_release_at: float | None = None
        self._recent_releases: deque[float] = deque()

    @property
    def enabled(self) -> bool:
        """Whether this limiter actually delays anything."""
        return self._interval > 0

    @property
    def interval(self) -> float:
        """Minimum seconds between two consecutive releases."""
        return self._interval

    def acquire(self) -> float:
        """Block until the next release is permitted.

        Returns:
            How long this call slept, in seconds. ``0.0`` when no wait was
            needed, so the caller can report throttling without timing it.
        """
        if not self.enabled:
            return 0.0

        slept = self._wait_for_even_pacing()
        slept += self._wait_for_window_capacity()
        self._recent_releases.append(self._monotonic())
        return slept

    def _wait_for_even_pacing(self) -> float:
        """Hold the minimum interval since the previous release."""
        now = self._monotonic()
        if self._next_release_at is None:
            # The first call is released immediately; pacing starts after it.
            self._next_release_at = now + self._interval
            return 0.0

        wait = self._next_release_at - now
        if wait <= 0:
            # We fell behind (a slow send, say). Resume pacing from now rather
            # than firing a catch-up burst to reclaim the lost time.
            self._next_release_at = now + self._interval
            return 0.0

        self._sleep(wait)
        self._next_release_at += self._interval
        return wait

    def _wait_for_window_capacity(self) -> float:
        """Hold until fewer than ``rate`` releases sit in the last second."""
        slept = 0.0
        while True:
            now = self._monotonic()
            cutoff = now - WINDOW_SECONDS
            while self._recent_releases and self._recent_releases[0] <= cutoff:
                self._recent_releases.popleft()

            if len(self._recent_releases) < self._capacity:
                return slept

            # Wait for the oldest release in the window to age out of it.
            wait = self._recent_releases[0] + WINDOW_SECONDS - now
            if wait <= 0:  # pragma: no cover - clock went backwards
                return slept
            self._sleep(wait)
            slept += wait
            self._next_release_at = self._monotonic() + self._interval
