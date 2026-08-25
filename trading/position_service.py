"""Position Maintaining Service: receives order events, serves net positions.

Run it with::

    python -m trading.position_service --host 127.0.0.1 --port 8000
"""

from __future__ import annotations

import argparse
import logging
import os
from typing import Any

import uvicorn
from fastapi import Body, FastAPI, Response

from trading.events import ValidationError, parse_event
from trading.logging_setup import configure_logging
from trading.positions import PositionStore

LOGGER = logging.getLogger("position_service")

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000

# Plain integers rather than fastapi.status constants: the constant names
# have been renamed across Starlette versions, and the numbers have not.
HTTP_OK = 200
HTTP_ACCEPTED = 202
HTTP_UNPROCESSABLE_CONTENT = 422


def create_app(store: PositionStore | None = None) -> FastAPI:
    """Build the FastAPI application.

    Accepting an injected store lets tests drive the API against a pre-seeded
    state without starting a server.
    """
    app = FastAPI(
        title="Position Maintaining Service",
        version="1.0.0",
        summary="Maintains the net position per trading symbol in memory.",
    )
    app.state.store = store if store is not None else PositionStore()

    # These handlers are deliberately synchronous (def, not async def).
    # FastAPI runs sync handlers in a worker thread pool, so ingesting events
    # and serving GET /position genuinely proceed in parallel and the read
    # endpoint stays available while the stream is processed. The lock inside
    # PositionStore is what makes that safe.

    @app.post("/events", status_code=HTTP_ACCEPTED)
    def ingest_event(response: Response, payload: Any = Body(...)) -> dict[str, Any]:
        """Accept a single order event.

        Returns 202 when the event is applied, 200 when it is a duplicate that
        was ignored, and 422 when it violates the event contract. Giving the
        duplicate case its own status lets the sender tell "already counted"
        apart from "counted just now" without guessing.
        """
        if not isinstance(payload, dict):
            response.status_code = HTTP_UNPROCESSABLE_CONTENT
            reason = f"expected a JSON object, got {type(payload).__name__}"
            LOGGER.warning("Rejected event: %s", reason)
            return {"status": "rejected", "reason": reason}

        try:
            # The same validation the producer runs. Re-checking here keeps the
            # service correct against any client, not just our own producer.
            event = parse_event(payload)
        except ValidationError as exc:
            response.status_code = HTTP_UNPROCESSABLE_CONTENT
            LOGGER.warning(
                "Rejected event %r: %s", payload.get("event_id", "<unknown>"), exc
            )
            return {"status": "rejected", "reason": str(exc), "field": exc.field}

        store: PositionStore = app.state.store
        if not store.apply(event):
            response.status_code = HTTP_OK
            LOGGER.info("Ignored duplicate event %s", event.event_id)
            return {"status": "duplicate", "event_id": event.event_id}

        LOGGER.info(
            "Applied event %s: %s %s %d",
            event.event_id,
            event.transaction_type,
            event.symbol,
            event.quantity,
        )
        return {"status": "accepted", "event_id": event.event_id}

    @app.get("/position")
    def get_position() -> dict[str, int]:
        """Return the current net position for every symbol seen.

        Includes symbols whose net position is zero. Negative values are valid
        and mean a net short position.
        """
        return app.state.store.snapshot()

    @app.get("/health")
    def health() -> dict[str, Any]:
        """Liveness plus counters, used by the producer to wait for startup."""
        stats = app.state.store.stats()
        return {
            "status": "ok",
            "applied_events": stats.applied_events,
            "duplicate_events": stats.duplicate_events,
            "symbols": stats.symbols,
        }

    return app


app = create_app()


def build_parser() -> argparse.ArgumentParser:
    """Command-line interface, with environment variables as the defaults."""
    parser = argparse.ArgumentParser(
        prog="position-service",
        description="Maintain net positions per symbol and serve GET /position.",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("POSITION_SERVICE_HOST", DEFAULT_HOST),
        help="Interface to bind (env: POSITION_SERVICE_HOST, default: %(default)s).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("POSITION_SERVICE_PORT", DEFAULT_PORT)),
        help="Port to bind (env: POSITION_SERVICE_PORT, default: %(default)s).",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        help="Logging level (env: LOG_LEVEL, default: %(default)s).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point for python -m trading.position_service."""
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)

    LOGGER.info("Starting Position Maintaining Service on %s:%d", args.host, args.port)
    LOGGER.info("Positions available at http://%s:%d/position", args.host, args.port)
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level=args.log_level.lower(),
        access_log=False,  # Our handlers already log every event outcome.
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
