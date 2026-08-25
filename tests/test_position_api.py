"""The HTTP surface of the Position Maintaining Service."""

from __future__ import annotations

import threading

import pytest
from conftest import row
from fastapi.testclient import TestClient

from trading.events import OrderEvent
from trading.position_service import create_app
from trading.positions import PositionStore


@pytest.fixture
def store() -> PositionStore:
    return PositionStore()


@pytest.fixture
def client(store: PositionStore) -> TestClient:
    with TestClient(create_app(store)) as test_client:
        yield test_client


class TestGetPosition:
    def test_returns_an_empty_object_before_any_event(self, client):
        response = client.get("/position")
        assert response.status_code == 200
        assert response.json() == {}

    def test_matches_the_documented_example(self, client):
        client.post("/events", json=row("evt-0001", "RELIANCE", "BUY", 90))
        client.post("/events", json=row("evt-0002", "TCS", "SELL", 75))
        response = client.get("/position")
        assert response.status_code == 200
        assert response.json() == {"RELIANCE": 90, "TCS": -75}

    def test_includes_symbols_that_net_to_zero(self, client):
        client.post("/events", json=row("e1", "FLAT", "BUY", 25))
        client.post("/events", json=row("e2", "FLAT", "SELL", 25))
        assert client.get("/position").json() == {"FLAT": 0}

    def test_reports_negative_positions(self, client):
        client.post("/events", json=row("e1", "SHORT", "SELL", 55))
        assert client.get("/position").json() == {"SHORT": -55}

    def test_reflects_a_pre_seeded_store(self, store, client):
        store.apply(OrderEvent("seed", "ITC", "BUY", 70))
        assert client.get("/position").json() == {"ITC": 70}

    def test_content_type_is_json(self, client):
        assert client.get("/position").headers["content-type"].startswith(
            "application/json"
        )

    def test_values_are_integers_not_strings(self, client):
        client.post("/events", json=row("e1", "A", "BUY", 5))
        assert client.get("/position").json()["A"] == 5


class TestIngest:
    def test_a_new_event_is_accepted(self, client):
        response = client.post("/events", json=row("e1", "RELIANCE", "BUY", 90))
        assert response.status_code == 202
        assert response.json() == {"status": "accepted", "event_id": "e1"}

    def test_a_duplicate_is_reported_distinctly(self, client):
        client.post("/events", json=row("e1", "RELIANCE", "BUY", 90))
        response = client.post("/events", json=row("e1", "RELIANCE", "BUY", 90))
        assert response.status_code == 200
        assert response.json() == {"status": "duplicate", "event_id": "e1"}
        assert client.get("/position").json() == {"RELIANCE": 90}

    def test_a_duplicate_id_with_different_fields_changes_nothing(self, client):
        client.post("/events", json=row("e1", "RELIANCE", "BUY", 90))
        client.post("/events", json=row("e1", "TCS", "SELL", 999))
        assert client.get("/position").json() == {"RELIANCE": 90}

    @pytest.mark.parametrize(
        ("payload", "field"),
        [
            (row(event_id=""), "event_id"),
            (row(symbol="  "), "symbol"),
            (row(transaction_type="HOLD"), "transaction_type"),
            (row(transaction_type="buy"), "transaction_type"),
            (row(quantity=0), "quantity"),
            (row(quantity=-5), "quantity"),
            (row(quantity="1.5"), "quantity"),
            (row(quantity=""), "quantity"),
            (row(quantity=True), "quantity"),
        ],
    )
    def test_invalid_events_are_rejected_with_a_reason(self, client, payload, field):
        response = client.post("/events", json=payload)
        assert response.status_code == 422
        body = response.json()
        assert body["status"] == "rejected"
        assert body["field"] == field
        assert body["reason"]

    def test_a_rejected_event_does_not_change_positions(self, client):
        client.post("/events", json=row("e1", "A", "BUY", 10))
        client.post("/events", json=row("e2", "A", "BUY", -1))
        assert client.get("/position").json() == {"A": 10}

    @pytest.mark.parametrize("payload", [["a"], "text", 42, None])
    def test_a_non_object_body_is_rejected_without_crashing(self, client, payload):
        response = client.post("/events", json=payload)
        assert response.status_code == 422
        assert response.json()["status"] == "rejected"

    def test_malformed_json_does_not_crash_the_service(self, client):
        response = client.post(
            "/events",
            content=b"{not json",
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code in (400, 422)
        # The service is still serving afterwards.
        assert client.get("/position").status_code == 200

    def test_unknown_extra_fields_are_ignored(self, client):
        payload = {**row("e1", "A", "BUY", 10), "trader": "alice", "note": "ignored"}
        assert client.post("/events", json=payload).status_code == 202
        assert client.get("/position").json() == {"A": 10}


class TestHealth:
    def test_reports_ok_and_counters(self, client):
        client.post("/events", json=row("e1", "A", "BUY", 10))
        client.post("/events", json=row("e1", "A", "BUY", 10))
        body = client.get("/health").json()
        assert body["status"] == "ok"
        assert body["applied_events"] == 1
        assert body["duplicate_events"] == 1
        assert body["symbols"] == 1


class TestConcurrentAccess:
    def test_reads_stay_correct_while_events_stream_in(self, client):
        # The read endpoint must remain available and consistent during ingest.
        total = 300
        errors: list[Exception] = []
        snapshots: list[dict] = []
        done = threading.Event()

        def ingest() -> None:
            try:
                for i in range(total):
                    client.post("/events", json=row(f"e{i}", "SYM", "BUY", 2))
            except Exception as exc:  # pragma: no cover - surfaced via errors
                errors.append(exc)
            finally:
                done.set()

        def read() -> None:
            try:
                while not done.is_set():
                    response = client.get("/position")
                    assert response.status_code == 200
                    snapshots.append(response.json())
            except Exception as exc:  # pragma: no cover - surfaced via errors
                errors.append(exc)

        threads = [threading.Thread(target=ingest), threading.Thread(target=read)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert not errors
        assert snapshots, "the read endpoint returned nothing during ingest"
        assert client.get("/position").json() == {"SYM": 2 * total}
        # Every intermediate read was a whole number of applied events.
        assert all(snap.get("SYM", 0) % 2 == 0 for snap in snapshots)
