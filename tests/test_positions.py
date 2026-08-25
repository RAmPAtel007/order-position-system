"""Net position arithmetic and idempotency in the store."""

from __future__ import annotations

import threading

import pytest

from trading.events import OrderEvent
from trading.positions import PositionStore


@pytest.fixture
def store() -> PositionStore:
    return PositionStore()


def event(event_id, symbol, transaction_type, quantity) -> OrderEvent:
    return OrderEvent(event_id, symbol, transaction_type, quantity)


class TestPositionArithmetic:
    def test_buy_increases_the_position(self, store):
        store.apply(event("e1", "RELIANCE", "BUY", 90))
        assert store.snapshot() == {"RELIANCE": 90}

    def test_sell_decreases_the_position(self, store):
        store.apply(event("e1", "TCS", "SELL", 75))
        assert store.snapshot() == {"TCS": -75}

    def test_buys_accumulate(self, store):
        store.apply(event("e1", "INFY", "BUY", 30))
        store.apply(event("e2", "INFY", "BUY", 20))
        assert store.snapshot() == {"INFY": 50}

    def test_buys_and_sells_net_off(self, store):
        store.apply(event("e1", "SBIN", "BUY", 100))
        store.apply(event("e2", "SBIN", "SELL", 40))
        assert store.snapshot() == {"SBIN": 60}

    def test_a_sell_beyond_the_holding_goes_negative(self, store):
        store.apply(event("e1", "LT", "BUY", 10))
        store.apply(event("e2", "LT", "SELL", 85))
        assert store.snapshot() == {"LT": -75}

    def test_order_of_application_does_not_change_the_net(self, store):
        other = PositionStore()
        events = [
            event("e1", "ITC", "BUY", 70),
            event("e2", "ITC", "SELL", 25),
            event("e3", "ITC", "BUY", 5),
        ]
        for item in events:
            store.apply(item)
        for item in reversed(events):
            other.apply(item)
        assert store.snapshot() == other.snapshot() == {"ITC": 50}


class TestMultipleSymbols:
    def test_symbols_are_tracked_independently(self, store):
        store.apply(event("e1", "RELIANCE", "BUY", 90))
        store.apply(event("e2", "TCS", "SELL", 75))
        store.apply(event("e3", "HDFCBANK", "BUY", 60))
        assert store.snapshot() == {"RELIANCE": 90, "TCS": -75, "HDFCBANK": 60}

    def test_a_zero_net_symbol_is_still_reported(self, store):
        # Required by the contract: every symbol seen in an accepted event
        # appears, including those whose position nets back to zero.
        store.apply(event("e1", "AXISBANK", "BUY", 40))
        store.apply(event("e2", "AXISBANK", "SELL", 40))
        assert store.snapshot() == {"AXISBANK": 0}

    def test_negative_zero_and_positive_coexist(self, store):
        store.apply(event("e1", "POS", "BUY", 10))
        store.apply(event("e2", "NEG", "SELL", 10))
        store.apply(event("e3", "FLAT", "BUY", 10))
        store.apply(event("e4", "FLAT", "SELL", 10))
        assert store.snapshot() == {"POS": 10, "NEG": -10, "FLAT": 0}

    def test_symbols_differing_only_by_case_are_distinct(self, store):
        store.apply(event("e1", "INFY", "BUY", 10))
        store.apply(event("e2", "infy", "BUY", 5))
        assert store.snapshot() == {"INFY": 10, "infy": 5}


class TestDuplicateHandling:
    def test_a_repeated_event_id_is_ignored(self, store):
        assert store.apply(event("e1", "RELIANCE", "BUY", 90)) is True
        assert store.apply(event("e1", "RELIANCE", "BUY", 90)) is False
        assert store.snapshot() == {"RELIANCE": 90}

    def test_the_first_event_wins_even_if_other_fields_differ(self, store):
        store.apply(event("e1", "RELIANCE", "BUY", 90))
        store.apply(event("e1", "TCS", "SELL", 999))
        # Neither the quantity nor the second symbol may leak in.
        assert store.snapshot() == {"RELIANCE": 90}

    def test_duplicates_are_counted(self, store):
        store.apply(event("e1", "A", "BUY", 1))
        for _ in range(3):
            store.apply(event("e1", "A", "BUY", 1))
        stats = store.stats()
        assert stats.applied_events == 1
        assert stats.duplicate_events == 3

    def test_has_seen_reports_applied_ids(self, store):
        store.apply(event("e1", "A", "BUY", 1))
        assert store.has_seen("e1")
        assert not store.has_seen("e2")

    def test_distinct_ids_for_the_same_symbol_both_apply(self, store):
        store.apply(event("e1", "A", "BUY", 10))
        store.apply(event("e2", "A", "BUY", 10))
        assert store.snapshot() == {"A": 20}


class TestSnapshotIsolation:
    def test_snapshot_is_a_copy(self, store):
        store.apply(event("e1", "A", "BUY", 10))
        snapshot = store.snapshot()
        snapshot["A"] = 999
        snapshot["INJECTED"] = 1
        assert store.snapshot() == {"A": 10}

    def test_empty_store_returns_an_empty_mapping(self, store):
        assert store.snapshot() == {}
        assert store.stats().applied_events == 0


class TestConcurrency:
    def test_concurrent_writers_apply_each_event_exactly_once(self, store):
        # Every thread submits the same events; the total must reflect each
        # event once, not once per thread.
        events = [event(f"e{i}", "SYM", "BUY", 1) for i in range(500)]

        def worker() -> None:
            for item in events:
                store.apply(item)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert store.snapshot() == {"SYM": 500}
        assert store.stats().applied_events == 500
        assert store.stats().duplicate_events == 500 * 7

    def test_reads_during_writes_never_see_a_torn_total(self, store):
        # A reader running alongside a writer must always see a total that is
        # some prefix of the writes, never a partially applied event.
        total = 1000
        stop = threading.Event()
        observed: list[int] = []

        def writer() -> None:
            for i in range(total):
                store.apply(event(f"e{i}", "SYM", "BUY", 7))
            stop.set()

        def reader() -> None:
            while not stop.is_set():
                observed.append(store.snapshot().get("SYM", 0))

        threads = [threading.Thread(target=writer), threading.Thread(target=reader)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert store.snapshot() == {"SYM": 7 * total}
        # Every observation must be a whole number of applied events.
        assert all(value % 7 == 0 for value in observed)
        assert all(0 <= value <= 7 * total for value in observed)
