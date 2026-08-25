"""Delivery, retry, and error-surfacing behaviour of the HTTP publisher."""

from __future__ import annotations

import httpx
import pytest

from trading.events import OrderEvent
from trading.transport import HttpEventPublisher, SendResult

EVENT = OrderEvent("evt-1", "RELIANCE", "BUY", 90)
BASE_URL = "http://position.test"


def publisher_with(handler, **kwargs) -> HttpEventPublisher:
    """Build a publisher whose HTTP calls are served by ``handler``.

    httpx.MockTransport keeps these tests entirely in-process: no sockets, no
    ports, and no waiting, while still exercising the real httpx code path.
    """
    client = httpx.Client(transport=httpx.MockTransport(handler))
    kwargs.setdefault("backoff_seconds", 0)
    kwargs.setdefault("sleep", lambda _: None)
    return HttpEventPublisher(BASE_URL, client=client, **kwargs)


class TestSuccessfulDelivery:
    def test_202_maps_to_accepted(self):
        pub = publisher_with(lambda r: httpx.Response(202, json={"status": "accepted"}))
        assert pub.send(EVENT) is SendResult.ACCEPTED

    def test_200_maps_to_duplicate(self):
        pub = publisher_with(lambda r: httpx.Response(200, json={"status": "duplicate"}))
        assert pub.send(EVENT) is SendResult.DUPLICATE

    def test_the_event_is_posted_as_json_to_the_events_path(self):
        seen: dict = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["url"] = str(request.url)
            seen["method"] = request.method
            seen["body"] = request.read().decode()
            return httpx.Response(202)

        publisher_with(handler).send(EVENT)
        assert seen["method"] == "POST"
        assert seen["url"] == f"{BASE_URL}/events"
        assert '"event_id":"evt-1"' in seen["body"].replace(" ", "")
        assert '"quantity":90' in seen["body"].replace(" ", "")

    def test_a_trailing_slash_in_the_base_url_is_normalised(self):
        pub = HttpEventPublisher(BASE_URL + "/")
        assert pub.events_url == f"{BASE_URL}/events"
        pub.close()


class TestRejection:
    @pytest.mark.parametrize("status_code", [400, 404, 409, 422])
    def test_4xx_is_rejected_and_not_retried(self, status_code):
        attempts = []

        def handler(request: httpx.Request) -> httpx.Response:
            attempts.append(1)
            return httpx.Response(status_code, json={"status": "rejected"})

        result = publisher_with(handler, max_attempts=3).send(EVENT)
        assert result is SendResult.REJECTED
        # The receiver understood and refused; an identical retry is pointless.
        assert len(attempts) == 1


class TestRetries:
    def test_a_transient_5xx_is_retried_and_can_succeed(self):
        responses = [httpx.Response(503), httpx.Response(503), httpx.Response(202)]
        attempts = []

        def handler(request: httpx.Request) -> httpx.Response:
            attempts.append(1)
            return responses[len(attempts) - 1]

        assert publisher_with(handler, max_attempts=3).send(EVENT) is SendResult.ACCEPTED
        assert len(attempts) == 3

    def test_a_connection_error_is_retried_and_can_succeed(self):
        attempts = []

        def handler(request: httpx.Request) -> httpx.Response:
            attempts.append(1)
            if len(attempts) == 1:
                raise httpx.ConnectError("connection refused")
            return httpx.Response(202)

        assert publisher_with(handler, max_attempts=3).send(EVENT) is SendResult.ACCEPTED
        assert len(attempts) == 2

    def test_a_timeout_is_retried(self):
        attempts = []

        def handler(request: httpx.Request) -> httpx.Response:
            attempts.append(1)
            if len(attempts) < 3:
                raise httpx.ReadTimeout("timed out")
            return httpx.Response(202)

        assert publisher_with(handler, max_attempts=3).send(EVENT) is SendResult.ACCEPTED
        assert len(attempts) == 3

    def test_exhausted_retries_report_failure(self):
        attempts = []

        def handler(request: httpx.Request) -> httpx.Response:
            attempts.append(1)
            raise httpx.ConnectError("connection refused")

        assert publisher_with(handler, max_attempts=3).send(EVENT) is SendResult.FAILED
        assert len(attempts) == 3

    def test_attempts_are_capped_by_configuration(self):
        attempts = []

        def handler(request: httpx.Request) -> httpx.Response:
            attempts.append(1)
            raise httpx.ConnectError("nope")

        publisher_with(handler, max_attempts=1).send(EVENT)
        assert len(attempts) == 1

    def test_backoff_grows_exponentially_between_attempts(self):
        waits: list[float] = []

        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("nope")

        pub = HttpEventPublisher(
            BASE_URL,
            client=httpx.Client(transport=httpx.MockTransport(handler)),
            max_attempts=4,
            backoff_seconds=0.1,
            sleep=waits.append,
        )
        pub.send(EVENT)
        # Three attempts fail with a wait after each; the final failure does
        # not sleep, because there is nothing left to wait for.
        assert waits == pytest.approx([0.1, 0.2, 0.4])

    def test_max_attempts_must_be_at_least_one(self):
        with pytest.raises(ValueError, match="at least 1"):
            HttpEventPublisher(BASE_URL, max_attempts=0)

    def test_a_failure_does_not_raise_into_the_caller(self):
        # The producer must keep going after an undeliverable event.
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("nope")

        pub = publisher_with(handler, max_attempts=1)
        assert pub.send(EVENT) is SendResult.FAILED
        assert pub.send(EVENT) is SendResult.FAILED


class TestReadinessCheck:
    def test_returns_true_once_health_answers(self):
        pub = publisher_with(lambda r: httpx.Response(200, json={"status": "ok"}))
        assert pub.wait_until_ready(timeout=1.0) is True

    def test_polls_until_the_service_comes_up(self):
        attempts = []

        def handler(request: httpx.Request) -> httpx.Response:
            attempts.append(1)
            if len(attempts) < 3:
                raise httpx.ConnectError("not up yet")
            return httpx.Response(200)

        pub = publisher_with(handler)
        assert pub.wait_until_ready(timeout=5.0, poll_interval=0) is True
        assert len(attempts) == 3

    def test_returns_false_when_the_service_never_answers(self):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("down")

        pub = publisher_with(handler)
        assert pub.wait_until_ready(timeout=0.0, poll_interval=0) is False


class TestResourceHandling:
    def test_it_closes_a_client_it_created(self):
        pub = HttpEventPublisher(BASE_URL)
        with pub:
            pass
        assert pub._client.is_closed

    def test_it_leaves_an_injected_client_open(self):
        # The caller owns a client it supplied and may still be using it.
        client = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(202)))
        with HttpEventPublisher(BASE_URL, client=client):
            pass
        assert not client.is_closed
        client.close()
