"""Incremental CSV reading."""

from __future__ import annotations

import csv

import pytest

from trading.csv_source import CsvSourceError, iter_rows
from trading.events import parse_event

HEADER = "event_id,symbol,transaction_type,quantity"


class TestReading:
    def test_yields_each_row_with_its_line_number(self, write_csv):
        path = write_csv(HEADER, "evt-1,RELIANCE,BUY,90", "evt-2,TCS,SELL,75")
        rows = list(iter_rows(path))
        assert [r.line_number for r in rows] == [2, 3]
        assert rows[0].values == {
            "event_id": "evt-1",
            "symbol": "RELIANCE",
            "transaction_type": "BUY",
            "quantity": "90",
        }

    def test_a_header_only_file_yields_nothing(self, write_csv):
        assert list(iter_rows(write_csv(HEADER))) == []

    def test_columns_may_appear_in_any_order(self, write_csv):
        path = write_csv("quantity,transaction_type,symbol,event_id", "90,BUY,RELIANCE,evt-1")
        assert list(iter_rows(path))[0].values["symbol"] == "RELIANCE"

    def test_extra_named_columns_do_not_disturb_the_contracted_ones(self, write_csv):
        path = write_csv(HEADER + ",trader", "evt-1,RELIANCE,BUY,90,alice")
        values = list(iter_rows(path))[0].values
        assert values["event_id"] == "evt-1"
        assert values["quantity"] == "90"
        # The extra column is passed through; validation simply ignores it.
        assert parse_event(values).symbol == "RELIANCE"

    def test_values_beyond_the_header_are_dropped(self, write_csv):
        # csv.DictReader collects unheadered overflow under a None key, which
        # would otherwise reach validation as a bogus field.
        path = write_csv(HEADER, "evt-1,RELIANCE,BUY,90,surplus,more")
        values = list(iter_rows(path))[0].values
        assert None not in values
        assert parse_event(values).quantity == 90

    def test_a_short_row_leaves_missing_fields_as_none(self, write_csv):
        path = write_csv(HEADER, "evt-1,RELIANCE")
        values = list(iter_rows(path))[0].values
        assert values["transaction_type"] is None
        assert values["quantity"] is None

    def test_a_utf8_bom_does_not_corrupt_the_first_column(self, tmp_path):
        path = tmp_path / "bom.csv"
        path.write_bytes(b"\xef\xbb\xbf" + f"{HEADER}\nevt-1,RELIANCE,BUY,90\n".encode())
        assert list(iter_rows(path))[0].values["event_id"] == "evt-1"

    def test_quoted_fields_are_handled(self, write_csv):
        path = write_csv(HEADER, '"evt-1","RELIANCE, LTD","BUY","90"')
        assert list(iter_rows(path))[0].values["symbol"] == "RELIANCE, LTD"

    def test_blank_lines_are_skipped_by_the_csv_module(self, write_csv):
        path = write_csv(HEADER, "evt-1,RELIANCE,BUY,90", "", "evt-2,TCS,SELL,75")
        assert len(list(iter_rows(path))) == 2

    def test_accepts_a_path_given_as_a_string(self, write_csv):
        path = write_csv(HEADER, "evt-1,RELIANCE,BUY,90")
        assert len(list(iter_rows(str(path)))) == 1


class TestStreaming:
    def test_rows_are_produced_before_the_file_is_exhausted(self, write_csv):
        # Proves the reader streams: the first row must be available without
        # consuming the rest of the file.
        path = write_csv(HEADER, *[f"evt-{i},SYM,BUY,1" for i in range(5000)])
        iterator = iter_rows(path)
        first = next(iterator)
        assert first.values["event_id"] == "evt-0"
        iterator.close()

    def test_memory_does_not_scale_with_file_size(self, write_csv):
        import tracemalloc

        small = write_csv(HEADER, *[f"evt-{i},SYM,BUY,1" for i in range(100)], name="s.csv")
        large = write_csv(HEADER, *[f"evt-{i},SYM,BUY,1" for i in range(20000)], name="l.csv")

        def peak_for(path) -> int:
            tracemalloc.start()
            for _ in iter_rows(path):
                pass
            peak = tracemalloc.get_traced_memory()[1]
            tracemalloc.stop()
            return peak

        small_peak, large_peak = peak_for(small), peak_for(large)
        # A file 200x larger must not cost anywhere near 200x the memory.
        assert large_peak < small_peak * 10, (small_peak, large_peak)


class TestSourceErrors:
    def test_a_missing_file_is_reported_clearly(self, tmp_path):
        with pytest.raises(CsvSourceError, match="not found"):
            list(iter_rows(tmp_path / "absent.csv"))

    def test_an_empty_file_is_reported_clearly(self, tmp_path):
        path = tmp_path / "empty.csv"
        path.write_text("", encoding="utf-8")
        with pytest.raises(CsvSourceError, match="empty or has no header"):
            list(iter_rows(path))

    def test_a_header_missing_columns_is_reported_with_the_names(self, write_csv):
        path = write_csv("event_id,symbol", "evt-1,RELIANCE")
        with pytest.raises(CsvSourceError) as exc:
            list(iter_rows(path))
        assert "transaction_type" in str(exc.value)
        assert "quantity" in str(exc.value)

    def test_a_directory_instead_of_a_file_is_reported(self, tmp_path):
        with pytest.raises(CsvSourceError):
            list(iter_rows(tmp_path))

    def test_an_undecodable_line_is_surfaced_as_a_row_error(self, write_csv):
        # An oversized field makes the csv module raise mid-iteration. The
        # reader must report it as a row-level error and carry on, so a single
        # unreadable line cannot end the run.
        original_limit = csv.field_size_limit()
        csv.field_size_limit(64)
        try:
            path = write_csv(
                HEADER,
                "evt-1,RELIANCE,BUY,90",
                "evt-2," + "X" * 500 + ",BUY,1",
                "evt-3,INFY,BUY,30",
            )
            rows = list(iter_rows(path))
        finally:
            csv.field_size_limit(original_limit)

        errored = [r for r in rows if r.error]
        assert len(errored) == 1
        assert "malformed CSV line" in errored[0].error
        assert [r.values.get("event_id") for r in rows if not r.error] == [
            "evt-1",
            "evt-3",
        ], "reading must continue past the bad line"
