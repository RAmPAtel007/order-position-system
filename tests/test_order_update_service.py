"""The producer pipeline: read, validate, dedupe, throttle, send."""

from __future__ import annotations

import logging

import pytest
from conftest import RecordingPublisher

from trading.csv_source import CsvSourceError
from trading.order_update_service import (
    EXIT_STARTUP_ERROR,
    RunSummary,
    build_parser,
    main,
    run,
)
from trading.throttle import RateLimiter
from trading.transport import SendResult

HEADER = "event_id,symbol,transaction_type,quantity"


def run_file(path, publisher=None, **kwargs) -> tuple[RunSummary, RecordingPublisher]:
    """Run the pipeline with throttling off so tests stay fast."""
    publisher = publisher or RecordingPublisher()
    summary = run(path, publisher, limiter=RateLimiter(0), **kwargs)
    return summary, publisher


class TestHappyPath:
    def test_valid_rows_are_sent_in_file_order(self, write_csv):
        path = write_csv(
            HEADER,
            "evt-1,RELIANCE,BUY,90",
            "evt-2,TCS,SELL,75",
            "evt-3,INFY,BUY,30",
        )
        summary, publisher = run_file(path)
        assert publisher.event_ids == ["evt-1", "evt-2", "evt-3"]
        assert summary.rows_read == 3
        assert summary.accepted == 3
        assert summary.sent == 3
        assert summary.rejected == 0

    def test_sent_events_carry_the_parsed_values(self, write_csv):
        path = write_csv(HEADER, "evt-1,RELIANCE,BUY,90")
        _, publisher = run_file(path)
        event = publisher.events[0]
        assert (event.event_id, event.symbol, event.transaction_type, event.quantity) == (
            "evt-1",
            "RELIANCE",
            "BUY",
            90,
        )

    def test_an_empty_file_sends_nothing_and_still_reports(self, write_csv):
        summary, publisher = run_file(write_csv(HEADER))
        assert publisher.events == []
        assert summary.rows_read == 0


class TestContinuesAfterInvalidRows:
    def test_a_later_valid_row_is_still_processed(self, write_csv):
        path = write_csv(
            HEADER,
            "evt-1,RELIANCE,BUY,notanumber",
            "evt-2,TCS,SELL,75",
        )
        summary, publisher = run_file(path)
        assert publisher.event_ids == ["evt-2"]
        assert summary.rejected == 1
        assert summary.accepted == 1

    def test_every_kind_of_invalid_row_is_skipped_without_stopping(self, write_csv):
        path = write_csv(
            HEADER,
            "evt-1,RELIANCE,BUY,90",  # valid
            ",NOID,BUY,10",  # blank event_id
            "evt-b, ,BUY,10",  # blank symbol
            "evt-c,TCS,HOLD,10",  # invalid transaction type
            "evt-d,TCS,buy,10",  # wrong case
            "evt-e,TCS,BUY,0",  # zero quantity
            "evt-f,TCS,BUY,-5",  # negative quantity
            "evt-g,TCS,BUY,1.5",  # non-integer quantity
            "evt-h,TCS,BUY,",  # blank quantity
            "evt-i,TCS,BUY,abc",  # non-numeric quantity
            "evt-j,TCS",  # short row
            "evt-2,INFY,SELL,30",  # valid, after all the noise
        )
        summary, publisher = run_file(path)
        assert publisher.event_ids == ["evt-1", "evt-2"]
        assert summary.rows_read == 12
        assert summary.accepted == 2
        assert summary.rejected == 10

    def test_rejections_are_tallied_by_field(self, write_csv):
        path = write_csv(
            HEADER,
            ",A,BUY,1",
            "evt-b, ,BUY,1",
            "evt-c,A,HOLD,1",
            "evt-d,A,BUY,0",
            "evt-e,A,BUY,x",
        )
        summary, _ = run_file(path)
        assert summary.rejection_reasons == {
            "event_id": 1,
            "symbol": 1,
            "transaction_type": 1,
            "quantity": 2,
        }

    def test_each_skipped_row_is_logged_with_its_line_and_reason(
        self, write_csv, caplog
    ):
        path = write_csv(HEADER, "evt-1,RELIANCE,BUY,0", "evt-2,TCS,SELL,75")
        with caplog.at_level(logging.WARNING, logger="order_update_service"):
            run_file(path)
        messages = [record.getMessage() for record in caplog.records]
        assert any("line 2" in m and "must be positive" in m for m in messages)

    def test_an_unreadable_line_does_not_stop_the_run(self, write_csv):
        import csv

        original = csv.field_size_limit()
        csv.field_size_limit(64)
        try:
            path = write_csv(
                HEADER,
                "evt-1,RELIANCE,BUY,90",
                "evt-2," + "X" * 500 + ",BUY,1",
                "evt-3,INFY,SELL,30",
            )
            summary, publisher = run_file(path)
        finally:
            csv.field_size_limit(original)
        assert publisher.event_ids == ["evt-1", "evt-3"]
        assert summary.rejection_reasons.get("csv") == 1


class TestDuplicateIdsInFile:
    def test_a_repeated_id_is_sent_only_once(self, write_csv):
        path = write_csv(
            HEADER,
            "evt-1,RELIANCE,BUY,90",
            "evt-1,RELIANCE,BUY,90",
            "evt-2,TCS,SELL,75",
        )
        summary, publisher = run_file(path)
        assert publisher.event_ids == ["evt-1", "evt-2"]
        assert summary.duplicates_in_file == 1
        assert summary.accepted == 2

    def test_the_first_occurrence_wins_when_fields_differ(self, write_csv):
        path = write_csv(
            HEADER,
            "evt-1,RELIANCE,BUY,90",
            "evt-1,TCS,SELL,999",
        )
        _, publisher = run_file(path)
        assert len(publisher.events) == 1
        assert publisher.events[0].symbol == "RELIANCE"
        assert publisher.events[0].quantity == 90

    def test_an_invalid_row_does_not_reserve_its_id(self, write_csv):
        # The contract says the first *valid* event for an ID wins, so an
        # earlier invalid row must not block a later valid one.
        path = write_csv(
            HEADER,
            "evt-1,RELIANCE,BUY,0",
            "evt-1,RELIANCE,BUY,90",
        )
        _, publisher = run_file(path)
        assert publisher.event_ids == ["evt-1"]
        assert publisher.events[0].quantity == 90


class TestDeliveryOutcomes:
    def test_a_duplicate_at_the_receiver_counts_as_delivered(self, write_csv):
        path = write_csv(HEADER, "evt-1,RELIANCE,BUY,90")
        publisher = RecordingPublisher(result=SendResult.DUPLICATE)
        summary, _ = run_file(path, publisher)
        assert summary.sent == 1
        assert summary.duplicates_at_receiver == 1
        assert summary.failed == 0

    def test_an_undeliverable_event_is_counted(self, write_csv):
        path = write_csv(HEADER, "evt-1,RELIANCE,BUY,90", "evt-2,TCS,SELL,75")
        publisher = RecordingPublisher(result=SendResult.FAILED)
        summary, _ = run_file(path, publisher)
        assert summary.failed == 2
        assert summary.sent == 0

    def test_a_failure_does_not_stop_later_rows(self, write_csv):
        path = write_csv(HEADER, "evt-1,A,BUY,1", "evt-2,B,BUY,1", "evt-3,C,BUY,1")
        publisher = RecordingPublisher(result=SendResult.FAILED)
        _, sent = run_file(path, publisher)
        assert sent.event_ids == ["evt-1", "evt-2", "evt-3"]

    def test_a_receiver_rejection_is_counted_separately(self, write_csv):
        path = write_csv(HEADER, "evt-1,RELIANCE,BUY,90")
        publisher = RecordingPublisher(result=SendResult.REJECTED)
        summary, _ = run_file(path, publisher)
        assert summary.rejected_by_receiver == 1
        assert summary.sent == 0


class TestThrottling:
    def test_the_limiter_is_consulted_once_per_sent_event(self, write_csv, clock):
        path = write_csv(HEADER, *[f"evt-{i},SYM,BUY,1" for i in range(5)])
        limiter = RateLimiter(50, monotonic=clock.monotonic, sleep=clock.sleep)
        run(path, RecordingPublisher(), limiter=limiter)
        # Four waits for five events: the first is released immediately.
        assert clock.sleeps == pytest.approx([0.02] * 4)

    def test_invalid_rows_do_not_consume_rate_budget(self, write_csv, clock):
        path = write_csv(HEADER, "evt-1,A,BUY,1", "bad,B,HOLD,1", "evt-2,C,BUY,1")
        limiter = RateLimiter(50, monotonic=clock.monotonic, sleep=clock.sleep)
        run(path, RecordingPublisher(), limiter=limiter)
        assert len(clock.sleeps) == 1


class TestCompletionLogging:
    def test_completion_is_logged(self, write_csv, caplog):
        path = write_csv(HEADER, "evt-1,RELIANCE,BUY,90")
        with caplog.at_level(logging.INFO, logger="order_update_service"):
            run_file(path)
        assert any(
            "Input processing complete" in record.getMessage()
            for record in caplog.records
        )

    def test_the_summary_counts_are_logged(self, write_csv, caplog):
        path = write_csv(HEADER, "evt-1,RELIANCE,BUY,90", "bad,B,HOLD,1")
        with caplog.at_level(logging.INFO, logger="order_update_service"):
            run_file(path)
        joined = " ".join(record.getMessage() for record in caplog.records)
        assert "Rows read: 2" in joined
        assert "accepted: 1" in joined
        assert "rejected: 1" in joined


class TestSourceFailures:
    def test_a_missing_file_raises_a_clear_error(self, tmp_path):
        with pytest.raises(CsvSourceError, match="not found"):
            run_file(tmp_path / "absent.csv")

    def test_a_bad_header_raises_before_sending_anything(self, write_csv):
        path = write_csv("wrong,header", "1,2")
        publisher = RecordingPublisher()
        with pytest.raises(CsvSourceError):
            run(path, publisher, limiter=RateLimiter(0))
        assert publisher.events == []


class TestCommandLine:
    def test_defaults_are_present(self):
        args = build_parser().parse_args([])
        assert args.csv == "data/order_updates.csv"
        assert args.target_url == "http://127.0.0.1:8000"
        assert args.rate == 50.0

    def test_flags_override_defaults(self):
        args = build_parser().parse_args(
            ["--csv", "other.csv", "--target-url", "http://host:9000", "--rate", "10"]
        )
        assert args.csv == "other.csv"
        assert args.target_url == "http://host:9000"
        assert args.rate == 10.0

    def test_environment_variables_supply_defaults(self, monkeypatch):
        monkeypatch.setenv("ORDER_CSV_PATH", "/env/path.csv")
        monkeypatch.setenv("ORDER_EVENT_RATE", "7")
        monkeypatch.setenv("POSITION_SERVICE_URL", "http://env-host:1234")
        args = build_parser().parse_args([])
        assert args.csv == "/env/path.csv"
        assert args.rate == 7.0
        assert args.target_url == "http://env-host:1234"

    def test_a_flag_beats_an_environment_variable(self, monkeypatch):
        monkeypatch.setenv("ORDER_EVENT_RATE", "7")
        assert build_parser().parse_args(["--rate", "99"]).rate == 99.0

    def test_an_unreachable_receiver_exits_with_a_startup_error(self, write_csv):
        path = write_csv(HEADER, "evt-1,RELIANCE,BUY,90")
        # Port 1 is reserved and never listening, so this fails immediately.
        exit_code = main(
            [
                "--csv",
                str(path),
                "--target-url",
                "http://127.0.0.1:1",
                "--startup-timeout",
                "0.01",
                "--log-level",
                "CRITICAL",
            ]
        )
        assert exit_code == EXIT_STARTUP_ERROR
