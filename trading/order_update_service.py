"""Order Update Service: streams the CSV, validates rows, sends valid events.

Run it with::

    python -m trading.order_update_service --csv data/order_updates.csv
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

from trading.csv_source import CsvSourceError, iter_rows
from trading.events import ValidationError, parse_event
from trading.logging_setup import configure_logging
from trading.throttle import RateLimiter
from trading.transport import EventPublisher, HttpEventPublisher, SendResult

LOGGER = logging.getLogger("order_update_service")

DEFAULT_CSV_PATH = "data/order_updates.csv"
DEFAULT_TARGET_URL = "http://127.0.0.1:8000"
DEFAULT_RATE = 50.0

EXIT_OK = 0
EXIT_DELIVERY_FAILURES = 1
EXIT_STARTUP_ERROR = 2


@dataclass
class RunSummary:
    """Counters describing one pass over the input file."""

    rows_read: int = 0
    accepted: int = 0
    rejected: int = 0
    duplicates_in_file: int = 0
    sent: int = 0
    duplicates_at_receiver: int = 0
    rejected_by_receiver: int = 0
    failed: int = 0
    rejection_reasons: dict[str, int] = field(default_factory=dict)

    def record_rejection(self, field_name: str) -> None:
        """Tally a rejection so the summary can show what went wrong most."""
        self.rejected += 1
        self.rejection_reasons[field_name] = (
            self.rejection_reasons.get(field_name, 0) + 1
        )


def run(
    csv_path: str | Path,
    publisher: EventPublisher,
    *,
    rate_per_second: float = DEFAULT_RATE,
    limiter: RateLimiter | None = None,
) -> RunSummary:
    """Stream ``csv_path`` and deliver every valid event through ``publisher``.

    Rows are read, validated, and sent one at a time and in file order, so a
    large file never has to fit in memory. An invalid row is logged with the
    reason and skipped; processing continues with the next row.

    Args:
        csv_path: Input file to stream.
        publisher: Delivery mechanism for valid events.
        rate_per_second: Ceiling on events emitted per second.
        limiter: Supply a pre-built limiter to control pacing in tests.

    Returns:
        A :class:`RunSummary` of what happened.

    Raises:
        CsvSourceError: if the file cannot be streamed at all.
    """
    throttle = limiter if limiter is not None else RateLimiter(rate_per_second)
    summary = RunSummary()
    # The producer drops repeat IDs before sending so it does not spend its
    # rate budget on events the receiver would discard anyway. The receiver
    # dedupes as well, and that copy is the authoritative one.
    seen_event_ids: set[str] = set()

    LOGGER.info("Reading %s", csv_path)
    if throttle.enabled:
        LOGGER.info(
            "Throttling to %.0f event(s)/second (%.0f ms apart)",
            rate_per_second,
            throttle.interval * 1000,
        )
    else:
        LOGGER.info("Throttling disabled; sending as fast as the receiver allows")

    for row in iter_rows(csv_path):
        summary.rows_read += 1

        if row.error is not None:
            LOGGER.warning("Skipping line %d: %s", row.line_number, row.error)
            summary.record_rejection("csv")
            continue

        try:
            event = parse_event(row.values)
        except ValidationError as exc:
            LOGGER.warning(
                "Skipping line %d (event_id=%r): %s",
                row.line_number,
                row.values.get("event_id", ""),
                exc,
            )
            summary.record_rejection(exc.field)
            continue

        if event.event_id in seen_event_ids:
            LOGGER.warning(
                "Skipping line %d: duplicate event_id %s; the first valid event wins",
                row.line_number,
                event.event_id,
            )
            summary.duplicates_in_file += 1
            continue

        seen_event_ids.add(event.event_id)
        summary.accepted += 1
        LOGGER.debug(
            "Accepted %s: %s %s %d",
            event.event_id,
            event.transaction_type,
            event.symbol,
            event.quantity,
        )

        throttle.acquire()
        _record_send(summary, event.event_id, publisher.send(event))

    _log_summary(summary)
    return summary


def _record_send(summary: RunSummary, event_id: str, result: SendResult) -> None:
    """Tally one delivery outcome and log it at a level matching its severity."""
    if result is SendResult.ACCEPTED:
        summary.sent += 1
        LOGGER.info("Sent %s", event_id)
    elif result is SendResult.DUPLICATE:
        summary.sent += 1
        summary.duplicates_at_receiver += 1
        # Normal when a retry lands after the receiver already applied the
        # event; worth an INFO line so the count is explainable, not alarming.
        LOGGER.info("Sent %s; receiver had already applied it", event_id)
    elif result is SendResult.REJECTED:
        summary.rejected_by_receiver += 1
        LOGGER.error("Receiver rejected %s", event_id)
    else:
        summary.failed += 1
        LOGGER.error("Could not deliver %s", event_id)


def _log_summary(summary: RunSummary) -> None:
    """Report the outcome of the run in a form that is easy to scan."""
    LOGGER.info("Input processing complete")
    LOGGER.info(
        "Rows read: %d | accepted: %d | rejected: %d | duplicate ids in file: %d",
        summary.rows_read,
        summary.accepted,
        summary.rejected,
        summary.duplicates_in_file,
    )
    LOGGER.info(
        "Delivered: %d (of which already applied: %d) | "
        "rejected by receiver: %d | undelivered: %d",
        summary.sent,
        summary.duplicates_at_receiver,
        summary.rejected_by_receiver,
        summary.failed,
    )
    if summary.rejection_reasons:
        breakdown = ", ".join(
            f"{field_name}={count}"
            for field_name, count in sorted(summary.rejection_reasons.items())
        )
        LOGGER.info("Rejections by field: %s", breakdown)
    if summary.failed:
        LOGGER.error(
            "%d event(s) could not be delivered; positions are incomplete",
            summary.failed,
        )


def build_parser() -> argparse.ArgumentParser:
    """Command-line interface, with environment variables as the defaults."""
    parser = argparse.ArgumentParser(
        prog="order-update-service",
        description="Stream order updates from a CSV to the Position service.",
    )
    parser.add_argument(
        "--csv",
        default=os.environ.get("ORDER_CSV_PATH", DEFAULT_CSV_PATH),
        help="Path to the input CSV (env: ORDER_CSV_PATH, default: %(default)s).",
    )
    parser.add_argument(
        "--target-url",
        default=os.environ.get("POSITION_SERVICE_URL", DEFAULT_TARGET_URL),
        help=(
            "Base URL of the Position service "
            "(env: POSITION_SERVICE_URL, default: %(default)s)."
        ),
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=float(os.environ.get("ORDER_EVENT_RATE", DEFAULT_RATE)),
        help=(
            "Maximum events per second; 0 disables throttling "
            "(env: ORDER_EVENT_RATE, default: %(default)s)."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=float(os.environ.get("ORDER_REQUEST_TIMEOUT", 5.0)),
        help=(
            "Per-request timeout in seconds "
            "(env: ORDER_REQUEST_TIMEOUT, default: %(default)s)."
        ),
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=int(os.environ.get("ORDER_MAX_ATTEMPTS", 3)),
        help=(
            "Delivery attempts per event before giving up "
            "(env: ORDER_MAX_ATTEMPTS, default: %(default)s)."
        ),
    )
    parser.add_argument(
        "--startup-timeout",
        type=float,
        default=float(os.environ.get("ORDER_STARTUP_TIMEOUT", 10.0)),
        help=(
            "Seconds to wait for the Position service to become reachable; "
            "0 skips the check (env: ORDER_STARTUP_TIMEOUT, default: %(default)s)."
        ),
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        help="Logging level (env: LOG_LEVEL, default: %(default)s).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point for ``python -m trading.order_update_service``."""
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)

    LOGGER.info("Starting Order Update Service")
    LOGGER.info("Target Position service: %s", args.target_url)

    with HttpEventPublisher(
        args.target_url,
        timeout=args.timeout,
        max_attempts=args.max_attempts,
    ) as publisher:
        if args.startup_timeout > 0 and not publisher.wait_until_ready(
            args.startup_timeout
        ):
            LOGGER.error(
                "Position service at %s did not respond within %.1fs. "
                "Start it first, or point --target-url at a running instance.",
                args.target_url,
                args.startup_timeout,
            )
            return EXIT_STARTUP_ERROR

        try:
            summary = run(args.csv, publisher, rate_per_second=args.rate)
        except CsvSourceError as exc:
            LOGGER.error("%s", exc)
            return EXIT_STARTUP_ERROR

    return EXIT_DELIVERY_FAILURES if summary.failed else EXIT_OK


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
