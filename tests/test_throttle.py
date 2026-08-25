"""Rate limiting behaviour.

Every timing assertion uses the injected fake clock, so the suite never waits
on real time and cannot fail because a machine was momentarily busy.
"""

from __future__ import annotations

import pytest

from trading.throttle import RateLimiter


def limiter(rate: float, clock) -> RateLimiter:
    return RateLimiter(rate, monotonic=clock.monotonic, sleep=clock.sleep)


class TestConfiguration:
    def test_interval_is_the_reciprocal_of_the_rate(self, clock):
        assert limiter(50, clock).interval == pytest.approx(0.02)
        assert limiter(10, clock).interval == pytest.approx(0.1)

    @pytest.mark.parametrize("rate", [0, -1, -50])
    def test_a_non_positive_rate_disables_throttling(self, rate, clock):
        rate_limiter = limiter(rate, clock)
        assert not rate_limiter.enabled
        for _ in range(100):
            assert rate_limiter.acquire() == 0.0
        assert clock.sleeps == []

    def test_a_positive_rate_is_enabled(self, clock):
        assert limiter(50, clock).enabled


class TestPacing:
    def test_the_first_call_is_not_delayed(self, clock):
        assert limiter(50, clock).acquire() == 0.0
        assert clock.sleeps == []

    def test_subsequent_calls_are_spaced_by_the_interval(self, clock):
        rate_limiter = limiter(50, clock)
        for _ in range(5):
            rate_limiter.acquire()
        assert clock.sleeps == pytest.approx([0.02] * 4)

    def test_acquire_reports_how_long_it_waited(self, clock):
        rate_limiter = limiter(50, clock)
        rate_limiter.acquire()
        assert rate_limiter.acquire() == pytest.approx(0.02)

    def test_a_slow_caller_is_not_delayed_further(self, clock):
        # If the caller already took longer than the interval, releasing must
        # be immediate rather than adding more delay on top.
        rate_limiter = limiter(50, clock)
        rate_limiter.acquire()
        clock.advance(0.5)
        assert rate_limiter.acquire() == 0.0

    def test_a_stall_does_not_trigger_a_catch_up_burst(self, clock):
        # After a long pause the limiter must resume at the normal rate, not
        # fire a burst to reclaim the idle time.
        rate_limiter = limiter(50, clock)
        rate_limiter.acquire()
        clock.advance(5.0)
        for _ in range(3):
            rate_limiter.acquire()
        # Three releases after the stall: one free, then normal spacing.
        assert clock.sleeps == pytest.approx([0.02, 0.02])

    def test_a_slower_rate_produces_a_longer_gap(self, clock):
        rate_limiter = limiter(4, clock)
        rate_limiter.acquire()
        rate_limiter.acquire()
        assert clock.sleeps == pytest.approx([0.25])


class TestCeiling:
    """The headline guarantee: never more than `rate` releases in one second."""

    def _release_times(self, rate: int, count: int, clock) -> list[float]:
        rate_limiter = limiter(rate, clock)
        times = []
        for _ in range(count):
            rate_limiter.acquire()
            times.append(clock.now)
        return times

    @pytest.mark.parametrize("rate", [5, 10, 50])
    def test_no_one_second_window_exceeds_the_rate(self, rate, clock):
        times = self._release_times(rate, rate * 6, clock)
        worst = max(
            sum(1 for t in times if start <= t < start + 1.0) for start in times
        )
        assert worst <= rate

    def test_no_calendar_second_exceeds_the_rate(self, clock):
        times = self._release_times(50, 400, clock)
        buckets: dict[int, int] = {}
        for moment in times:
            buckets[int(moment)] = buckets.get(int(moment), 0) + 1
        assert max(buckets.values()) <= 50

    def test_sustained_throughput_matches_the_configured_rate(self, clock):
        times = self._release_times(50, 500, clock)
        span = times[-1] - times[0]
        assert (len(times) - 1) / span == pytest.approx(50, rel=0.05)

    def test_a_burst_after_an_idle_period_is_still_bounded(self, clock):
        # Idling must not bank credit that allows a later over-rate burst.
        rate_limiter = limiter(10, clock)
        rate_limiter.acquire()
        clock.advance(10.0)
        times = []
        for _ in range(30):
            rate_limiter.acquire()
            times.append(clock.now)
        worst = max(sum(1 for t in times if s <= t < s + 1.0) for s in times)
        assert worst <= 10


class TestRealClock:
    def test_the_default_limiter_uses_real_time(self):
        # One assertion against the real clock, with a generous bound so it
        # cannot flake: 5 events at 20/s must take at least 3 intervals.
        import time

        rate_limiter = RateLimiter(20)
        start = time.monotonic()
        for _ in range(5):
            rate_limiter.acquire()
        assert time.monotonic() - start >= 3 * 0.05
