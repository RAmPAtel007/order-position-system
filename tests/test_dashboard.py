"""The optional read-only dashboard served at /.

The dashboard is a view over the public API, so these tests check that it is
served correctly and stays self-contained. Its behaviour is driven entirely by
`GET /position` and `GET /health`, which are covered in `test_position_api.py`.
"""

from __future__ import annotations

import re

import pytest
from conftest import row
from fastapi.testclient import TestClient

from trading.position_service import DASHBOARD_FILE, create_app
from trading.positions import PositionStore


@pytest.fixture
def client() -> TestClient:
    with TestClient(create_app(PositionStore())) as test_client:
        yield test_client


class TestServing:
    def test_the_root_path_serves_html(self, client):
        response = client.get("/")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/html")
        assert "<!doctype html>" in response.text.lower()

    def test_it_is_the_dashboard_and_not_a_placeholder(self, client):
        text = client.get("/").text
        assert "Net position by symbol" in text
        assert 'id="chart"' in text

    def test_it_is_excluded_from_the_api_schema(self, client):
        # The dashboard is not part of the service contract; the API is.
        assert "/" not in client.get("/openapi.json").json()["paths"]

    def test_it_falls_back_to_a_pointer_when_the_file_is_absent(
        self, client, monkeypatch
    ):
        # Losing the dashboard must not take the service down: its job is the
        # API, so a missing asset degrades to a pointer rather than a 500.
        monkeypatch.setattr(
            "trading.position_service.DASHBOARD_FILE",
            DASHBOARD_FILE.with_name("does-not-exist.html"),
        )
        response = client.get("/")
        assert response.status_code == 200
        assert "/position" in response.json()["endpoints"]

    def test_serving_it_does_not_disturb_the_api(self, client):
        client.get("/")
        client.post("/events", json=row("e1", "RELIANCE", "BUY", 90))
        assert client.get("/position").json() == {"RELIANCE": 90}


class TestSelfContained:
    """No external assets: the dashboard must work offline and unpackaged."""

    @pytest.fixture
    def markup(self, client) -> str:
        return client.get("/").text

    def test_it_requests_nothing_from_a_remote_host(self, markup):
        remote = re.findall(r'(?:src|href)\s*=\s*["\'](https?:)?//[^"\']+', markup)
        assert remote == []

    def test_it_has_no_build_step_artefacts(self, markup):
        assert "webpack" not in markup
        assert "node_modules" not in markup

    def test_styles_and_script_are_inline(self, markup):
        assert "<style>" in markup
        assert "<script>" in markup

    def test_it_only_calls_this_services_own_endpoints(self, markup):
        fetched = set(re.findall(r'fetch\(\s*["\']([^"\']+)', markup))
        assert fetched == {"/position", "/health"}


class TestAccessibility:
    """Checks for the properties that are verifiable from the markup."""

    @pytest.fixture
    def markup(self, client) -> str:
        return client.get("/").text

    def test_the_document_declares_a_language(self, markup):
        assert 'lang="en"' in markup

    def test_a_table_view_accompanies_the_chart(self, markup):
        # Every value must be reachable without relying on colour.
        assert "<table" in markup
        assert 'id="table-body"' in markup

    def test_status_is_not_conveyed_by_colour_alone(self, markup):
        # The chip pairs a coloured dot with a text label.
        assert 'id="status-text"' in markup
        assert 'role="status"' in markup

    def test_controls_expose_their_pressed_state(self, markup):
        assert 'aria-pressed' in markup

    def test_the_viewport_is_declared_for_small_screens(self, markup):
        assert 'name="viewport"' in markup

    def test_motion_is_gated_on_a_reduced_motion_preference(self, markup):
        assert "prefers-reduced-motion" in markup

    def test_dark_mode_is_defined_for_both_the_os_and_the_toggle(self, markup):
        assert "prefers-color-scheme: dark" in markup
        assert '[data-theme="dark"]' in markup
