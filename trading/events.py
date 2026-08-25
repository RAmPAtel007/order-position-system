"""The order event contract shared by both services.

Both the Order Update Service and the Position Maintaining Service validate
through :func:`parse_event`. Keeping one implementation means the producer and
the consumer can never disagree about what a valid event is, and it lets the
consumer defend itself against any client, not just our own producer.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Final, Mapping

BUY: Final[str] = "BUY"
SELL: Final[str] = "SELL"
TRANSACTION_TYPES: Final[tuple[str, ...]] = (BUY, SELL)

#: Columns an accepted event must carry, in canonical order.
REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "event_id",
    "symbol",
    "transaction_type",
    "quantity",
)

# Deliberately strict: only an optionally signed run of digits is an integer.
# This rejects "1.5", "1e3", "0x10", "", and "  " while still accepting "+90".
_INTEGER_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[+-]?\d+$")


class ValidationError(ValueError):
    """A raw row or payload could not be turned into an :class:`OrderEvent`.

    Carries the offending field so logs can name it precisely instead of
    reporting a generic parse failure.
    """

    def __init__(self, field: str, reason: str) -> None:
        self.field = field
        self.reason = reason
        super().__init__(f"{field}: {reason}")


@dataclass(frozen=True)
class OrderEvent:
    """A validated order update.

    Immutable so that an event can be handed to another thread without any
    risk of it being mutated after validation.
    """

    event_id: str
    symbol: str
    transaction_type: str
    quantity: int

    @property
    def signed_quantity(self) -> int:
        """The position delta: BUY adds, SELL subtracts."""
        return self.quantity if self.transaction_type == BUY else -self.quantity

    def to_payload(self) -> dict[str, Any]:
        """The JSON body sent over the wire between the two services."""
        return {
            "event_id": self.event_id,
            "symbol": self.symbol,
            "transaction_type": self.transaction_type,
            "quantity": self.quantity,
        }


def parse_event(raw: Mapping[str, Any]) -> OrderEvent:
    """Validate ``raw`` and return an :class:`OrderEvent`.

    Accepts both CSV rows (every value is a string) and decoded JSON payloads
    (``quantity`` may already be an ``int``).

    Raises:
        ValidationError: if any field violates the event contract.
    """
    return OrderEvent(
        event_id=_parse_required_text(raw, "event_id"),
        symbol=_parse_required_text(raw, "symbol"),
        transaction_type=_parse_transaction_type(raw),
        quantity=_parse_quantity(raw),
    )


def _lookup(raw: Mapping[str, Any], field: str) -> Any:
    """Return ``raw[field]``, treating a missing key and ``None`` alike.

    ``csv.DictReader`` fills absent trailing columns with ``None``, so a short
    row and an omitted JSON key produce the same clear message.
    """
    if field not in raw:
        raise ValidationError(field, "field is missing")
    value = raw[field]
    if value is None:
        raise ValidationError(field, "field is missing")
    return value


def _parse_required_text(raw: Mapping[str, Any], field: str) -> str:
    """Validate a non-empty text field, preserving its supplied case.

    Surrounding whitespace is trimmed so that a padded CSV column such as
    ``" RELIANCE "`` is treated as ``"RELIANCE"``; a value that is only
    whitespace is rejected as blank. Case and inner characters are never
    altered, so ``RELIANCE`` and ``reliance`` remain distinct symbols.
    """
    value = _lookup(raw, field)
    if not isinstance(value, str):
        raise ValidationError(field, f"expected a string, got {type(value).__name__}")
    trimmed = value.strip()
    if not trimmed:
        raise ValidationError(field, "must be a non-empty string")
    return trimmed


def _parse_transaction_type(raw: Mapping[str, Any]) -> str:
    """Validate that the transaction type is exactly ``BUY`` or ``SELL``.

    The contract says *exactly*, so the comparison is case-sensitive and
    ``"buy"`` is rejected rather than silently coerced.
    """
    value = _lookup(raw, "transaction_type")
    if not isinstance(value, str):
        raise ValidationError(
            "transaction_type", f"expected a string, got {type(value).__name__}"
        )
    candidate = value.strip()
    if candidate not in TRANSACTION_TYPES:
        raise ValidationError(
            "transaction_type",
            f"must be exactly one of {', '.join(TRANSACTION_TYPES)}; got {value!r}",
        )
    return candidate


def _parse_quantity(raw: Mapping[str, Any]) -> int:
    """Validate that the quantity is a positive integer."""
    value = _lookup(raw, "quantity")

    if isinstance(value, bool):
        # bool is a subclass of int in Python; True must not become quantity 1.
        raise ValidationError("quantity", "expected an integer, got bool")

    if isinstance(value, int):
        quantity = value
    elif isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            raise ValidationError("quantity", "must not be blank")
        if not _INTEGER_PATTERN.match(candidate):
            raise ValidationError(
                "quantity", f"must be an integer; got {value!r}"
            )
        quantity = int(candidate)
    else:
        raise ValidationError(
            "quantity", f"expected an integer, got {type(value).__name__}"
        )

    if quantity <= 0:
        raise ValidationError("quantity", f"must be positive; got {quantity}")
    return quantity
