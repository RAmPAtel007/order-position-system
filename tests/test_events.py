"""Validation rules for the event contract."""

from __future__ import annotations

import pytest
from conftest import row

from trading.events import OrderEvent, ValidationError, parse_event


class TestValidEvents:
    def test_parses_a_buy_from_csv_strings(self):
        event = parse_event(
            {
                "event_id": "evt-0001",
                "symbol": "RELIANCE",
                "transaction_type": "BUY",
                "quantity": "90",
            }
        )
        assert event == OrderEvent("evt-0001", "RELIANCE", "BUY", 90)

    def test_parses_a_sell_from_json_types(self):
        event = parse_event(row("evt-2", "TCS", "SELL", 75))
        assert event == OrderEvent("evt-2", "TCS", "SELL", 75)

    def test_buy_and_sell_produce_opposite_deltas(self):
        assert parse_event(row(transaction_type="BUY", quantity=90)).signed_quantity == 90
        assert (
            parse_event(row(transaction_type="SELL", quantity=90)).signed_quantity == -90
        )

    def test_symbol_case_is_preserved(self):
        # "Preserve its supplied case and value": these are distinct symbols.
        assert parse_event(row(symbol="Reliance")).symbol == "Reliance"
        assert parse_event(row(symbol="reliance")).symbol == "reliance"

    def test_surrounding_whitespace_is_trimmed(self):
        event = parse_event(row("  evt-3  ", "  TCS  ", "  BUY  ", "  42  "))
        assert event == OrderEvent("evt-3", "TCS", "BUY", 42)

    def test_leading_plus_on_quantity_is_accepted(self):
        assert parse_event(row(quantity="+90")).quantity == 90

    def test_events_are_immutable(self):
        # Frozen so a validated event can cross a thread boundary safely.
        event = parse_event(row())
        with pytest.raises(Exception):
            event.quantity = 999  # type: ignore[misc]

    def test_payload_round_trips(self):
        event = parse_event(row("evt-9", "INFY", "SELL", 30))
        assert parse_event(event.to_payload()) == event


class TestInvalidTransactionType:
    @pytest.mark.parametrize("value", ["HOLD", "buy", "Buy", "SELL ORDER", "B", ""])
    def test_rejects_anything_but_exact_buy_or_sell(self, value):
        # The contract says "exactly BUY or SELL", so matching is case-sensitive.
        with pytest.raises(ValidationError) as exc:
            parse_event(row(transaction_type=value))
        assert exc.value.field == "transaction_type"

    def test_rejects_a_missing_transaction_type(self):
        payload = row()
        del payload["transaction_type"]
        with pytest.raises(ValidationError) as exc:
            parse_event(payload)
        assert exc.value.field == "transaction_type"

    def test_rejects_a_non_string_transaction_type(self):
        with pytest.raises(ValidationError) as exc:
            parse_event(row(transaction_type=1))
        assert exc.value.field == "transaction_type"


class TestInvalidQuantity:
    @pytest.mark.parametrize("value", ["0", 0, "-5", -5, "-0"])
    def test_rejects_zero_and_negative(self, value):
        with pytest.raises(ValidationError, match="positive"):
            parse_event(row(quantity=value))

    @pytest.mark.parametrize("value", ["1.5", 1.5, "1e3", "0x10", "abc", "9,0", "  12.0"])
    def test_rejects_non_integers(self, value):
        with pytest.raises(ValidationError) as exc:
            parse_event(row(quantity=value))
        assert exc.value.field == "quantity"

    @pytest.mark.parametrize("value", ["", "   ", "\t"])
    def test_rejects_blank(self, value):
        with pytest.raises(ValidationError, match="blank"):
            parse_event(row(quantity=value))

    def test_rejects_missing_and_none(self):
        payload = row()
        del payload["quantity"]
        with pytest.raises(ValidationError, match="missing"):
            parse_event(payload)
        with pytest.raises(ValidationError, match="missing"):
            parse_event(row(quantity=None))

    @pytest.mark.parametrize("value", [True, False])
    def test_rejects_booleans(self, value):
        # bool subclasses int in Python, so True would silently become 1.
        with pytest.raises(ValidationError, match="bool"):
            parse_event(row(quantity=value))

    def test_rejects_collections(self):
        with pytest.raises(ValidationError) as exc:
            parse_event(row(quantity=[1]))
        assert exc.value.field == "quantity"


class TestInvalidIdentifiers:
    @pytest.mark.parametrize("value", ["", "   ", "\t", "\n"])
    def test_rejects_blank_event_id(self, value):
        with pytest.raises(ValidationError, match="non-empty"):
            parse_event(row(event_id=value))

    @pytest.mark.parametrize("value", ["", "   ", "\t"])
    def test_rejects_blank_symbol(self, value):
        with pytest.raises(ValidationError, match="non-empty"):
            parse_event(row(symbol=value))

    @pytest.mark.parametrize("field_name", ["event_id", "symbol"])
    def test_rejects_missing_identifier(self, field_name):
        payload = row()
        del payload[field_name]
        with pytest.raises(ValidationError) as exc:
            parse_event(payload)
        assert exc.value.field == field_name

    @pytest.mark.parametrize("field_name", ["event_id", "symbol"])
    def test_rejects_none_identifier(self, field_name):
        # csv.DictReader fills absent trailing columns with None.
        with pytest.raises(ValidationError, match="missing"):
            parse_event({**row(), field_name: None})

    def test_rejects_non_string_identifier(self):
        with pytest.raises(ValidationError, match="expected a string"):
            parse_event(row(event_id=123))


class TestErrorReporting:
    def test_error_names_the_offending_field_and_reason(self):
        with pytest.raises(ValidationError) as exc:
            parse_event(row(quantity="-5"))
        assert exc.value.field == "quantity"
        assert "must be positive" in exc.value.reason
        assert str(exc.value).startswith("quantity:")

    def test_validation_error_is_a_value_error(self):
        # Callers can catch the stdlib type if they do not import ours.
        with pytest.raises(ValueError):
            parse_event(row(quantity="nope"))

    def test_reports_the_first_invalid_field_in_contract_order(self):
        # Everything is wrong here; the message should name event_id first so
        # the log points at the most identifying problem.
        with pytest.raises(ValidationError) as exc:
            parse_event(row(event_id="", symbol="", transaction_type="X", quantity="x"))
        assert exc.value.field == "event_id"
