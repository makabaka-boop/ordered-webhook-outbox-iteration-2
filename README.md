# Event Outbox + Independent Delivery Worker

Backend event ingress with a **persistent outbox** and an **independent
worker** that delivers events to HTTP targets.

## Acceptance

```bash
docker compose config --quiet      # compose file is valid
docker compose build               # build the image
docker compose run --rm verify     # one-shot verification service
```

The `verify` run stands up the full topology and executes 9 assertion
groups (ingress/dedup, FIFO, backoff/DLQ, DLQ replay, target isolation,
real timeout, signing, crash/restart, delivery-generation migration). Exit
code 0 = accepted.

## Architecture

```
 producers --HTTP--> [ api ] --writes--> +-----------------+
                                        | SQLite (volume) |  persistent outbox
 [ worker ] <--claims/dispatches--------+-----------------+
     | one dispatcher thread per target
     +--> target a (own secret)
     +--> target b (own secret)
     +--> target c (own secret)
```

* `app/api.py` — event ingress (`POST /v1/events`) + admin/introspection.
* `app/storage.py` — SQLite outbox (WAL), schema, claim/finish/replay.
* `app/worker.py` — independent process; per-target dispatcher threads.
* `app/config.py` — deployment-time whitelist, generation contracts and
  controllable clock.
* `app/signing.py` — HMAC-SHA256 over timestamp + original JSON bytes.
* `verify/` — fake HTTP target and the one-shot `verify` service.

Everything is Python 3.11 **standard library only**; the image needs no
package downloads at build time.

## Guarantees and semantics

* **Whitelist (deployment config only).** Target names, URLs and
  per-target secrets come exclusively from `TARGET_CONFIG`
  (`{"name": "URL|secret"}`). Ingestion to an unconfigured name is `404`;
  request data can never introduce or redirect a target.
* **Pinned delivery generations.** A target may preconfigure multiple
  generations in `TARGET_CONFIG`; the historical one-string form remains
  supported and is normalized to generation `default`. Each target's
  current generation id is stored in SQLite, while URLs and secrets remain
  deployment configuration. An admin activates a preconfigured generation
  with optimistic concurrency:

  ```
  POST /admin/targets/{target}/generations/{id}/activate
  {"expected_current_generation_id": "v1"}
  ```

  A mismatch returns `409` and changes nothing. Event de-duplication,
  current-generation lookup and insertion are one committed transaction.
  The resulting `events.generation_id` is immutable: retries, startup
  recovery and dead-letter replay all resolve that event's original URL and
  secret, so a database row can never be old while bytes go to the new
  endpoint. Activation during an in-flight attempt is ordered by durable
  state; the active attempt finishes under its claimed contract and later
  ingress binds the new one. If a bound generation is absent from worker
  configuration, the head stays scheduled, creates no attempt/failure and
  is reported at `GET worker:9100/control/blocked`; other targets continue.
  Restoring the contract resumes the queue.
* **Producer dedup by request id.** `X-Request-Id` is unique in the
  outbox. Re-posting the same id with the same body returns the original
  event (`duplicate: true`); same id with a different body is `409`.
* **Per-target signing.** Each delivery carries
  `X-Event-Id`, `X-Request-Id`, `X-Timestamp`, `X-Signature`, where

  ```
  X-Signature = HMAC_SHA256( target_secret,
                             X-Timestamp + "\n" + <exact ingestion bytes> )
  ```

  The signed body is the **raw JSON bytes as received** — never
  re-serialized (the verify suite checks this with unusual formatting).
* **Per-target FIFO.** Events are assigned a monotonic `seq` at
  insertion. A worker thread only claims the earliest active event for a
  target; while event N is in flight or parked in a retry backoff,
  events N+1… cannot be delivered. **Different targets are dispatched on
  independent threads and never block each other.**
* **Retry / dead-letter.** Non-2xx, timeout and connection errors are
  failed attempts. Retries happen after **1, 2, 4 seconds**; a **4th
  failure** moves the event to the dead-letter (`status=dead`), which
  releases the next event. All attempts (number, timestamps, status
  code/error) are persisted in `attempts`.
* **At-least-once, explicitly not exactly-once.** If the worker dies
  after the target accepts bytes but before the outcome is persisted,
  the restarted worker requeues the in-flight event and delivers it
  again — a duplicate, never a loss. The crashed attempt stays in
  history with `finished_at = NULL`. Consumers must dedupe on
  `X-Event-Id`/`X-Request-Id`.
* **DLQ replay keeps identity and history.** `POST
  /admin/events/{id}/replay` re-schedules a dead event with the same
  event id, request id, body and `seq`; old attempts remain, the new
  attempts continue the numbering (5, 6…) under a fresh 4-attempt budget
  and `replayed_count` is incremented.

## Verification details

The suite drives a **virtual clock** (`POST /admin/clock`,
`/admin/clock/advance`) so the 1/2/4 s schedule is deterministic and
fast, while real OS socket timeouts (worker timeout 1 s vs. a fake
target that sleeps 2 s) and real worker crashes are exercised in wall
time. The crash test arms the worker (`POST /control/crash`) to
`os._exit(17)` immediately after a successful write; Compose's
`restart: unless-stopped` brings it back, and the duplicated receipt plus
the half-open attempt row are asserted.

## HTTP surface

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/v1/events` | ingest (`X-Target`, `X-Request-Id`, JSON body) |
| GET | `/v1/events/{id}` | event state |
| GET | `/v1/events/{id}/attempts` | full attempt history |
| GET/POST | `/admin/clock`, POST `/admin/clock/advance` | controllable clock |
| GET | `/admin/events?status=dead` | introspection |
| GET | `/admin/targets`, `/admin/targets/{name}` | current/preconfigured generations |
| POST | `/admin/targets/{name}/generations/{id}/activate` | CAS generation activation |
| POST | `/admin/events/{id}/replay` | dead-letter replay |
| GET | `/control/blocked` (worker) | missing-generation head reports |
| POST | `/control/crash` (worker) | crash injection for tests |

## Local development without Docker

```bash
# pip-free: python3 -m app.api / app.worker need TARGET_CONFIG + DB_PATH
python3 -m app.api
python3 -m app.worker
python3 verify/fake_target.py
python3 verify/verify.py
```
