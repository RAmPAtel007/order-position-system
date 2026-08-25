# Order Updates → Net Positions

Two independently runnable services. The **Order Update Service** streams a CSV
of order updates, validates each row, and forwards valid events at a capped
rate. The **Position Maintaining Service** applies those events to an in-memory
net position per symbol and serves them over HTTP.

```
┌──────────────────────────┐   POST /events    ┌────────────────────────────┐
│  Order Update Service    │   (one JSON       │ Position Maintaining       │
│                          │    event/request) │ Service                    │
│  read CSV row ──┐        │ ────────────────► │                            │
│  validate       │        │                   │  validate again            │
│  drop repeat id │ ≤50/s  │ ◄──────────────── │  drop seen event_id        │
│  send in order ─┘        │   202 accepted    │  apply BUY(+) / SELL(−)    │
└──────────────────────────┘   200 duplicate   │                            │
                               422 rejected    │  GET /position ──► {...}   │
                                               └────────────────────────────┘
```

---

## Quick start

Requires Python 3.10 or newer.

```bash
python -m pip install -r requirements.txt
```

**Terminal 1 — start the Position service:**

```bash
python -m trading.position_service --port 8000
```

**Terminal 2 — stream the CSV into it:**

```bash
python -m trading.order_update_service --csv data/order_updates.csv --target-url http://127.0.0.1:8000
```

**Terminal 3 — read the positions at any time, including mid-run:**

```bash
curl http://127.0.0.1:8000/position
```

The supplied file holds 1000 events; at the default 50/second the run takes
about 20 seconds. Positions are queryable throughout.

No install step is required — the package sits at the repository root, so
`python -m` works straight from a clone. `pip install -e .` additionally
provides `position-service` and `order-update-service` as commands.

---

## Running the tests

```bash
python -m pip install -r requirements-dev.txt
```

```bash
python -m pytest
```

188 tests, roughly 22 seconds. Useful variations:

```bash
python -m pytest -v
```

```bash
python -m pytest tests/test_end_to_end.py -v
```

```bash
python -m pytest --ignore=tests/test_end_to_end.py -q
```

| File | Covers |
| --- | --- |
| `tests/test_events.py` | Every validation rule in the event contract |
| `tests/test_positions.py` | BUY/SELL arithmetic, multi-symbol, negative and zero nets, duplicates, thread safety |
| `tests/test_csv_source.py` | Incremental reading, flat memory use, malformed lines, header errors |
| `tests/test_throttle.py` | Pacing and the rate ceiling, using an injected clock |
| `tests/test_transport.py` | Status mapping, retries, backoff, readiness polling |
| `tests/test_position_api.py` | `GET /position`, ingest outcomes, concurrent reads during ingest |
| `tests/test_order_update_service.py` | The pipeline, continuing past invalid rows, CLI and env config |
| `tests/test_end_to_end.py` | Both services over real HTTP, including as two separate OS processes |

Timing-sensitive tests inject a fake clock and assert the pacing *decisions*
rather than measuring elapsed time, so they are fast and cannot fail because
the machine was momentarily busy. The one wall-clock assertion uses a bound
loose enough that only a genuine regression can break it.

---

## Architecture and the reasoning behind it

### Why HTTP between the services

HTTP was chosen over gRPC, Redis Streams, ZeroMQ, or a custom TCP protocol
because it is the only option that needs **no external infrastructure and no
schema toolchain** while still giving a well-defined request/response contract.
The assessment explicitly allows it, and the properties that actually matter
here come for free:

- **A per-event acknowledgement.** The producer learns whether each event was
  applied, was a duplicate, or was refused. A fire-and-forget transport such as
  plain UDP or Redis pub/sub would leave the producer unable to report what
  actually landed.
- **Status codes already model the outcomes.** 202 / 200 / 422 map cleanly onto
  applied / duplicate / rejected, so no bespoke reply envelope is needed.
- **Ordinary tooling works.** `curl`, a browser, and the auto-generated OpenAPI
  page at `/docs` are enough to inspect a running system.
- **Testability.** `httpx.MockTransport` and FastAPI's `TestClient` exercise the
  real code path in-process, so most tests need no socket at all.

A broker such as Redis Streams would add durability and replay, but durable
delivery and restart recovery are explicitly out of scope, and requiring a
running broker would make the project harder to evaluate for no benefit within
that scope.

The producer depends on a narrow `EventPublisher` protocol (`send(event) ->
SendResult`), not on `httpx`. Swapping in another transport means writing one
class; no pipeline logic would change.

### Event payload

One event per `POST /events` request, `Content-Type: application/json`:

```json
{
  "event_id": "evt-0001",
  "symbol": "RELIANCE",
  "transaction_type": "BUY",
  "quantity": 90
}
```

| Field | Type | Rule |
| --- | --- | --- |
| `event_id` | string | Non-empty. Uniquely identifies the event. |
| `symbol` | string | Non-empty. Case and value preserved. |
| `transaction_type` | string | Exactly `BUY` or `SELL`. |
| `quantity` | integer | Strictly positive. |

Responses:

| Status | Body | Meaning |
| --- | --- | --- |
| `202` | `{"status": "accepted", "event_id": "evt-0001"}` | Applied to the position. |
| `200` | `{"status": "duplicate", "event_id": "evt-0001"}` | This ID was already applied; ignored. |
| `422` | `{"status": "rejected", "reason": "...", "field": "..."}` | Violates the contract. |

Every rejection uses that one shape, including bodies FastAPI itself cannot
decode, so a client needs a single error path.

### One validation implementation, used by both services

`trading/events.py` is the single source of truth, and both services call it.
The producer validates so it never wastes rate budget on rows that would be
refused; the receiver validates because it is reachable by any client, not only
by our producer. Because it is the same function, the two can never drift apart.

Choices worth calling out:

- **`transaction_type` is case-sensitive.** The contract says *exactly* `BUY` or
  `SELL`, so `buy` is rejected rather than silently coerced.
- **`symbol` keeps its case**, so `INFY` and `infy` are different symbols. Only
  surrounding whitespace is trimmed, so a padded column like `" RELIANCE "` is
  read as `RELIANCE`; a value that is *only* whitespace is rejected as blank.
- **`quantity` must be a plain integer.** `1.5`, `1e3`, `0x10`, and `abc` are all
  rejected; `+90` is accepted. Booleans are rejected explicitly, because `bool`
  subclasses `int` in Python and `True` would otherwise become a quantity of 1.
- **Errors name the offending field**, which is what makes the skip logs
  actionable rather than a generic parse failure.

### Idempotency in two places

The receiver's dedupe is authoritative: it holds the set of applied `event_id`s
and ignores repeats, so the first valid event for an ID wins even if a later one
differs in every other field. The producer *also* drops repeat IDs, purely so it
does not spend its rate budget on events that would be discarded on arrival.

This is what makes at-least-once delivery safe. Replaying the entire feed into a
running receiver is a no-op:

```
Rows read: 1000 | accepted: 1000 | rejected: 0 | duplicate ids in file: 0
Delivered: 1000 (of which already applied: 1000) | rejected by receiver: 0 | undelivered: 0
```

…and `GET /position` returns exactly what it did before the replay.

### Concurrency

Request handlers are deliberately **synchronous** (`def`, not `async def`).
FastAPI runs sync handlers in a worker thread pool, so ingesting events and
serving `GET /position` genuinely run in parallel and the read endpoint stays
available while the stream is being processed.

That makes real locking necessary rather than decorative. `PositionStore` guards
the positions map and the seen-ID set with **one** lock, so a reader can never
see them out of step — no response can show a half-applied event. `snapshot()`
copies under the lock and returns the copy, so callers cannot mutate live state.
The lock is held only for small dictionary operations, so reads stay fast under
a continuous ingest load.

The suite covers this directly: concurrent writers must apply each event exactly
once, and a reader polling during a 300-event ingest must only ever observe
whole numbers of applied events.

### Streaming, not loading

`csv.DictReader` pulls one line per iteration from an open handle. Memory stays
flat regardless of file size, and events begin flowing before the file has been
fully read. A test asserts this: a file 200× larger must not cost anywhere near
200× the peak memory.

### The rate limit

Two mechanisms combine, because either alone leaves a gap:

- **Even pacing** holds a minimum `1/rate` interval between sends, spreading load
  smoothly instead of bursting and idling.
- **A sliding window** tracks the last `rate` send times and blocks until the
  oldest ages out, bounding the *count* directly.

Pacing alone only bounds the gap between neighbours, so a slow send followed by
a catch-up could still cluster events. The window makes the ceiling exact. The
window is half-open, `[t, t+1s)` — the usual rate-limiting convention, where a
send exactly one second later belongs to the next window.

A stall does not bank credit: after an idle period the limiter resumes at the
normal rate rather than firing a burst to reclaim lost time. Verified at a
sustained 50.00 events/second over 400 releases, with no window exceeding the
limit under either sliding-window or calendar-second measurement.

---

## How errors surface

**Invalid rows** are logged at `WARNING` with the line number, the event ID, and
the specific reason, then skipped. Processing always continues:

```
INFO     [order_update_service] Sent evt-9001
WARNING  [order_update_service] Skipping line 3 (event_id=''): event_id: must be a non-empty string
WARNING  [order_update_service] Skipping line 5 (event_id='evt-9003'): transaction_type: must be exactly one of BUY, SELL; got 'HOLD'
WARNING  [order_update_service] Skipping line 7 (event_id='evt-9005'): quantity: must be positive; got 0
WARNING  [order_update_service] Skipping line 9 (event_id='evt-9007'): quantity: must be an integer; got '1.5'
WARNING  [order_update_service] Skipping line 12: duplicate event_id evt-9001; the first valid event wins
INFO     [order_update_service] Sent evt-9011
INFO     [order_update_service] Input processing complete
INFO     [order_update_service] Rows read: 15 | accepted: 4 | rejected: 10 | duplicate ids in file: 1
INFO     [order_update_service] Rejections by field: event_id=1, quantity=5, symbol=1, transaction_type=3
```

Reproduce it with the included sample:

```bash
python -m trading.order_update_service --csv data/sample_invalid.csv --target-url http://127.0.0.1:8000 --rate 0
```

**Connection and delivery errors** surface three ways: each failed attempt is
logged with its cause, an event that exhausts its retries is logged at `ERROR`
and counted as undelivered in the summary, and the process exits non-zero.

- Retried: connection errors, timeouts, and 5xx — the receiver may just not be
  ready yet. Backoff doubles between attempts (default 3 attempts, 0.25s base).
- Not retried: 4xx. The receiver understood the request and refused it, so an
  identical retry would be refused identically.

**Startup problems** are reported as messages, not tracebacks. A missing input
file, a header lacking required columns, or an unreachable receiver each exit
with status 2 and a one-line explanation.

| Exit status | Meaning |
| --- | --- |
| `0` | Every valid event was delivered. |
| `1` | The run completed but some events could not be delivered. |
| `2` | Startup failed: unusable input file, or the receiver never answered. |

The producer polls `/health` before reading, turning a startup race into a short
silent wait instead of a burst of connection errors on the first events.

---

## Configuration

Every option has a flag and an environment variable; the flag wins. Nothing is
hard-coded to a machine-specific path.

### Position Maintaining Service

| Flag | Environment variable | Default | Purpose |
| --- | --- | --- | --- |
| `--host` | `POSITION_SERVICE_HOST` | `127.0.0.1` | Interface to bind. Use `0.0.0.0` to accept remote connections. |
| `--port` | `POSITION_SERVICE_PORT` | `8000` | Port to bind. |
| `--log-level` | `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`. |

### Order Update Service

| Flag | Environment variable | Default | Purpose |
| --- | --- | --- | --- |
| `--csv` | `ORDER_CSV_PATH` | `data/order_updates.csv` | Input file. |
| `--target-url` | `POSITION_SERVICE_URL` | `http://127.0.0.1:8000` | Base URL of the receiver. |
| `--rate` | `ORDER_EVENT_RATE` | `50` | Max events/second. `0` disables throttling. |
| `--timeout` | `ORDER_REQUEST_TIMEOUT` | `5.0` | Per-request timeout, seconds. |
| `--max-attempts` | `ORDER_MAX_ATTEMPTS` | `3` | Delivery attempts before giving up. |
| `--startup-timeout` | `ORDER_STARTUP_TIMEOUT` | `10.0` | Wait for the receiver. `0` skips the check. |
| `--log-level` | `LOG_LEVEL` | `INFO` | Logging level. |

Examples:

```bash
python -m trading.position_service --host 0.0.0.0 --port 9000
```

```bash
POSITION_SERVICE_URL=http://10.0.0.5:9000 ORDER_EVENT_RATE=10 python -m trading.order_update_service
```

---

## API

### `GET /position`

Returns the net position for every symbol seen in an accepted event. Symbols
whose position nets to zero are included; negative values mean a net short.

```bash
curl http://127.0.0.1:8000/position
```

```json
{
  "RELIANCE": 4500,
  "TCS": -3750,
  "HDFCBANK": 3000,
  "SBIN": 5000,
  "ASIANPAINT": -250
}
```

Those are the real values after the supplied `order_updates.csv`, abbreviated to
five of the twenty symbols.

### `POST /events`

```bash
curl -X POST http://127.0.0.1:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"evt-0001","symbol":"RELIANCE","transaction_type":"BUY","quantity":90}'
```

```json
{"status": "accepted", "event_id": "evt-0001"}
```

Sending it a second time:

```json
{"status": "duplicate", "event_id": "evt-0001"}
```

An invalid event:

```bash
curl -X POST http://127.0.0.1:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"evt-0002","symbol":"TCS","transaction_type":"HOLD","quantity":10}'
```

```json
{
  "status": "rejected",
  "reason": "transaction_type: must be exactly one of BUY, SELL; got 'HOLD'",
  "field": "transaction_type"
}
```

### `GET /health`

```json
{"status": "ok", "applied_events": 1000, "duplicate_events": 0, "symbols": 20}
```

Interactive API documentation is served at `/docs`.

---

## Known limitations and trade-offs

**In scope but bounded by design**

- **State is in memory only.** Restarting the Position service clears both the
  positions and the applied-ID set, so a replay after a restart would re-apply
  everything. Persistence is explicitly out of scope; the tests cover
  idempotency only within a single process lifetime.
- **Delivery is at-least-once, not exactly-once.** If the receiver applies an
  event but the response is lost, the retry is deduplicated by `event_id`, so
  the *observable effect* is exactly-once while the process lives. Genuine
  exactly-once across restarts would need durable storage.
- **An event that exhausts its retries is dropped, not queued.** It is logged at
  `ERROR`, counted as undelivered, and reflected in exit status 1. Positions are
  then incomplete, and the summary says so rather than reporting a clean run.

**Deliberate simplifications**

- **One event per request.** At 50/second the overhead is irrelevant, and it
  keeps per-event acknowledgement — and therefore per-event reporting — exact.
  Batching would be the first change if the rate rose by orders of magnitude.
- **A single global lock**, not per-symbol locks. It is held only for a couple of
  dictionary operations, so contention is negligible at this scale, and one lock
  is far easier to reason about than a striped scheme.
- **`seen_event_ids` grows without bound**, in both services, at roughly 50 bytes
  per event. Fine for a bounded file; a long-lived feed would need a bounded
  structure with a time or count horizon.
- **Sends are sequential.** Delivery is one-at-a-time, which keeps CSV order
  intact and makes the rate limit trivially correct. Concurrent senders would be
  faster but would need explicit sequencing to preserve order.
- **No authentication, TLS, or rate limiting on the inbound API.** Out of scope;
  the service binds to localhost by default.
- **The producer's dedupe is per run.** Restarting the producer re-reads the file
  from the top; the receiver's dedupe is what keeps that harmless.

**Environment**

- Developed and tested on Windows 11 with Python 3.13. The code uses no
  platform-specific APIs; the test suite spawns subprocesses via
  `sys.executable`, so it should run unchanged on Linux and macOS.

---

## Repository layout

```
trading/
  events.py                 Event contract and validation (shared by both services)
  positions.py              Thread-safe in-memory position store
  csv_source.py             Incremental CSV reader
  throttle.py               Rate limiter
  transport.py              HTTP delivery with bounded retries
  order_update_service.py   Producer: read → validate → dedupe → throttle → send
  position_service.py       Consumer: FastAPI app, GET /position
  logging_setup.py          Shared log formatting
tests/                      188 tests (see the table above)
data/
  order_updates.csv         Supplied assessment data (synthetic)
  sample_invalid.csv        Malformed rows, for demonstrating the skip path
```

---

## Use of AI-assisted tools

This solution was developed with AI assistance (Claude). The assistant was used
to draft the implementation and test suite, and I directed the design decisions,
reviewed all output, and verified behaviour by running the services and the
tests. Specific points where testing changed the design are worth noting, since
they show what the verification actually caught:

- The first rate limiter used fixed-interval pacing alone. Measuring it showed a
  one-second window could hold 51 events at an exactly-aligned boundary, so the
  sliding-window guard was added to make the ceiling exact.
- A `null` request body originally fell through to FastAPI's default
  `{"detail": ...}` error shape while our own rejections used a different one.
  A test caught the inconsistency and it was normalised to a single shape.
- An early test assumed a NUL byte would raise `csv.Error`. It does not — it is
  read as an ordinary character. The test was rewritten around an oversized
  field, which does raise, and confirmed that reading resumes on the next line.

I am prepared to explain any part of the code and the reasoning behind each
design decision.
