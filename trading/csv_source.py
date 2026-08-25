"""Incremental CSV reading for the Order Update Service."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from trading.events import REQUIRED_FIELDS


class CsvSourceError(Exception):
    """The file cannot be streamed at all (missing, unreadable, bad header).

    This is a startup problem rather than a bad row, so the service reports it
    and exits instead of processing a file it cannot interpret.
    """


@dataclass(frozen=True)
class RawRow:
    """One row as read from disk, before validation."""

    line_number: int
    values: dict[str, str | None]
    error: str | None = None
    """Set when the CSV module itself could not decode the line."""


def iter_rows(path: str | Path) -> Iterator[RawRow]:
    """Yield rows from ``path`` one at a time.

    The file is streamed: ``csv.DictReader`` pulls a single line from the file
    handle per iteration, so memory use stays flat regardless of file size and
    the consumer starts receiving events before the file has been fully read.

    A line the csv module cannot decode is yielded as a :class:`RawRow` with
    ``error`` set, so the caller can log and skip it and keep going rather than
    aborting the run.

    Raises:
        CsvSourceError: if the file is missing, unreadable, or has a header
            that does not cover every required column.
    """
    csv_path = Path(path)
    try:
        # utf-8-sig transparently strips a UTF-8 BOM, which would otherwise
        # corrupt the first header name into "\ufeffevent_id".
        handle = csv_path.open("r", newline="", encoding="utf-8-sig")
    except FileNotFoundError as exc:
        raise CsvSourceError(f"input file not found: {csv_path}") from exc
    except OSError as exc:
        raise CsvSourceError(f"cannot read input file {csv_path}: {exc}") from exc

    with handle:
        reader = csv.DictReader(handle)
        _validate_header(reader.fieldnames, csv_path)

        row_iterator = iter(reader)
        while True:
            try:
                values = next(row_iterator)
            except StopIteration:
                return
            except csv.Error as exc:
                # Malformed line (for example an unterminated quote). Report it
                # and continue with the next line instead of crashing.
                yield RawRow(
                    line_number=reader.line_num,
                    values={},
                    error=f"malformed CSV line: {exc}",
                )
                continue

            # Columns beyond the header land under the restkey (None); drop
            # them so validation sees exactly the contracted fields.
            values.pop(None, None)  # type: ignore[call-overload]
            yield RawRow(line_number=reader.line_num, values=dict(values))


def _validate_header(fieldnames: list[str] | None, csv_path: Path) -> None:
    """Reject a file whose header cannot supply the required columns."""
    if not fieldnames:
        raise CsvSourceError(f"input file is empty or has no header: {csv_path}")

    present = {name.strip() for name in fieldnames if name}
    missing = [field for field in REQUIRED_FIELDS if field not in present]
    if missing:
        raise CsvSourceError(
            f"input file {csv_path} is missing required column(s): "
            f"{', '.join(missing)} (found: {', '.join(fieldnames)})"
        )
