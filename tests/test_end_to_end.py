"""End-to-end tests covering both services communicating over real HTTP.

Two levels of fidelity:

* :class:`TestAgainstLiveServer` runs the Position service on a real socket in
  a background thread and drives the real producer against it. Fast enough to
  run on every commit.
* :class:`TestAsSeparateProcesses` launches both services as separate OS
  processes, matching how they are actually deployed.

Nothing here sleeps for a fixed duration waiting for something to happen; the
tests poll for a condition with a timeout instead, so a slow machine makes
them slower rather than flaky.
"""

from __future__ import annotations

import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx
import pytest
import uvicorn

from trading.order_update_service import run
from trading.position_service import create_app
from trading.positions import PositionStore
from trading.transport import HttpEventPublisher

REPO_ROOT = Path(__file__).resolve().parent.parent
HEADER = "event_id,symbol,transaction_type,quantity"
STARTUP_TIMEOUT = 30.0


def free_port() -> int:
    """Reserve a port from the OS, then release it for the service to bind."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for_health(base_url: str, timeout: float = STARTUP_TIMEOUT) -> bool:
    """Poll until the service answers, rather than sleeping a fixed amount."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"{base_url}/health", timeout=1.0).status_code == 200:
                return True
        except httpx.HTTPError:
            pass
        time.sleep(0.05)
    return False


@pytest.fixture
def live_server():
    """Run the Position service on a real port in a background thread."""
    port = free_port()
    store = PositionStore()
    config = uvicorn.Config(
        create_app(store), host="127.0.0.1", port=port, log_level="warning"
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{port}"
    if not wait_for_health(base_url):
        server.should_exit = True
        pytest.fail("the Position service did not start in time")

    try:
        yield base_url, store
    finally:
        server.should_exit = True
        thread.join(timeout=10)


class TestAgainstLiveServer:
    def test_a_full_run_produces_the_expected_positions(self, live_server, write_csv):
        base_url, _ = live_server
        path = write_csv(
            HEADER,
            "evt-1,RELIANCE,BUY,90",
            "evt-2,TCS,SELL,75",
            "evt-3,RELIANCE,BUY,10",
            "evt-4,FLAT,BUY,25",
            "evt-5,FLAT,SELL,25",
            "evt-6,SHORT,SELL,55",
        )
        with HttpEventPublisher(base_url) as publisher:
            summary = run(path, publisher, rate_per_second=0)

        assert summary.sent == 6
        assert summary.failed == 0
        assert httpx.get(f"{base_url}/position").json() == {
            "RELIANCE": 100,
            "TCS": -75,
            "FLAT": 0,
            "SHORT": -55,
        }

    def test_invalid_rows_are_skipped_and_the_rest_still_land(
        self, live_server, write_csv
    ):
        base_url, _ = live_server
        path = write_csv(
            HEADER,
            "evt-1,RELIANCE,BUY,90",
            ",BLANKID,BUY,10",
            "evt-2, ,BUY,10",
            "evt-3,TCS,HOLD,10",
            "evt-4,TCS,BUY,0",
            "evt-5,TCS,BUY,-1",
            "evt-6,TCS,BUY,2.5",
            "evt-7,TCS,BUY,",
            "evt-8,TCS,SELL,75",
        )
        with HttpEventPublisher(base_url) as publisher:
            summary = run(path, publisher, rate_per_second=0)

        assert summary.accepted == 2
        assert summary.rejected == 7
        assert httpx.get(f"{base_url}/position").json() == {"RELIANCE": 90, "TCS": -75}

    def test_duplicate_ids_are_applied_once(self, live_server, write_csv):
        base_url, _ = live_server
        path = write_csv(
            HEADER,
            "evt-1,RELIANCE,BUY,90",
            "evt-1,RELIANCE,BUY,90",
            "evt-1,TCS,SELL,999",
        )
        with HttpEventPublisher(base_url) as publisher:
            run(path, publisher, rate_per_second=0)
        assert httpx.get(f"{base_url}/position").json() == {"RELIANCE": 90}

    def test_resending_the_same_file_changes_nothing(self, live_server, write_csv):
        # Idempotency across runs: a retry of the whole feed must not double
        # the positions, because the receiver still holds the event IDs.
        base_url, _ = live_server
        path = write_csv(HEADER, "evt-1,RELIANCE,BUY,90", "evt-2,TCS,SELL,75")
        expected = {"RELIANCE": 90, "TCS": -75}

        for _ in range(2):
            with HttpEventPublisher(base_url) as publisher:
                run(path, publisher, rate_per_second=0)
            assert httpx.get(f"{base_url}/position").json() == expected

    def test_positions_are_readable_while_events_stream_in(
        self, live_server, write_csv
    ):
        # The read endpoint must stay available during processing.
        base_url, _ = live_server
        path = write_csv(HEADER, *[f"evt-{i},SYM,BUY,1" for i in range(120)])
        observations: list[int] = []
        done = threading.Event()

        def poll() -> None:
            while not done.is_set():
                response = httpx.get(f"{base_url}/position", timeout=5.0)
                assert response.status_code == 200
                observations.append(response.json().get("SYM", 0))

        reader = threading.Thread(target=poll)
        reader.start()
        try:
            with HttpEventPublisher(base_url) as publisher:
                run(path, publisher, rate_per_second=200)
        finally:
            done.set()
            reader.join(timeout=10)

        assert observations, "no reads completed while events were streaming"
        assert httpx.get(f"{base_url}/position").json() == {"SYM": 120}
        # Reads observed the position growing, never exceeding the final total.
        assert max(observations) <= 120

    def test_the_throttle_holds_over_a_real_run(self, live_server, write_csv):
        # A loose bound: 60 events at 30/s cannot finish faster than the rate
        # allows. Generous enough that load on the machine cannot break it.
        base_url, _ = live_server
        path = write_csv(HEADER, *[f"evt-{i},SYM,BUY,1" for i in range(60)])
        start = time.monotonic()
        with HttpEventPublisher(base_url) as publisher:
            run(path, publisher, rate_per_second=30)
        elapsed = time.monotonic() - start
        assert elapsed >= 59 / 30 * 0.8


class TestAsSeparateProcesses:
    """Both services launched as independent OS processes, as deployed."""

    def test_the_two_services_work_over_a_real_network_hop(self, write_csv):
        port = free_port()
        base_url = f"http://127.0.0.1:{port}"
        path = write_csv(
            HEADER,
            "evt-1,RELIANCE,BUY,90",
            "evt-2,TCS,SELL,75",
            "evt-3,INFY,BUY,30",
            "evt-3,INFY,BUY,30",
            "evt-4,BAD,HOLD,1",
            "evt-5,FLAT,BUY,10",
            "evt-6,FLAT,SELL,10",
        )

        position_service = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "trading.position_service",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
            ],
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            assert wait_for_health(base_url), "the Position service did not start"

            producer = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "trading.order_update_service",
                    "--csv",
                    str(path),
                    "--target-url",
                    base_url,
                    "--rate",
                    "0",
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                timeout=120,
            )

            assert producer.returncode == 0, producer.stderr
            assert "Input processing complete" in producer.stderr

            assert httpx.get(f"{base_url}/position").json() == {
                "RELIANCE": 90,
                "TCS": -75,
                "INFY": 30,
                "FLAT": 0,
            }
        finally:
            position_service.terminate()
            try:
                position_service.wait(timeout=15)
            except subprocess.TimeoutExpired:  # pragma: no cover
                position_service.kill()

    def test_the_producer_exits_cleanly_when_the_receiver_is_absent(self, write_csv):
        # Nothing is listening, so the producer must report the problem and
        # exit with the startup status rather than hanging or crashing.
        path = write_csv(HEADER, "evt-1,RELIANCE,BUY,90")
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "trading.order_update_service",
                "--csv",
                str(path),
                "--target-url",
                f"http://127.0.0.1:{free_port()}",
                "--startup-timeout",
                "1",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 2
        assert "did not respond" in result.stderr

    def test_a_missing_input_file_is_reported_without_a_traceback(self, tmp_path):
        port = free_port()
        base_url = f"http://127.0.0.1:{port}"
        position_service = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "trading.position_service",
                "--port",
                str(port),
            ],
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            assert wait_for_health(base_url), "the Position service did not start"
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "trading.order_update_service",
                    "--csv",
                    str(tmp_path / "absent.csv"),
                    "--target-url",
                    base_url,
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                timeout=60,
            )
            assert result.returncode == 2
            assert "not found" in result.stderr
            assert "Traceback" not in result.stderr
        finally:
            position_service.terminate()
            try:
                position_service.wait(timeout=15)
            except subprocess.TimeoutExpired:  # pragma: no cover
                position_service.kill()
