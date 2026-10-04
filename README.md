# Event Outbox + Independent Delivery Worker

Backend event ingress with a **persistent outbox** and an **independent
worker** that delivers events to HTTP targets — with **delivery
generations** so receivers can migrate to a new address and rotate their
signing key without breaking the delivery contract of events already in
the backlog.

## Acceptance

```bash
docker compose config --quiet      # compose file is valid
docker compose build               # build the image
docker compose run --rm verify     # one-shot verification service
```

The `verify` run stands up the full topology and executes 13 assertion
groups (ingress/dedup, FIFO, backoff/DLQ, DLQ replay, target isolation,
real timeout, signing, generation activation vs. in-flight delivery,
activation CAS, DLQ replay across a migration, missing-generation
blockage, crash/restart). Exit code 0 = accepted.

## Architecture

```
 producers --HTTP--> [ api ] --writes--> +-----------------+
                                        | SQLite (volume) |  persistent outbox
 [ worker ] <--claims/dispatches--------+-----------------+
     | one dispatcher thread per target
     +--> target a (legacy single generation)
     +--> target race, generation g1 -> old endpoint, old key
     +--> target race, generation g2 -> new endpoint, new key   ...
```

* `app/api.py` — event ingress (`POST /v1/events`) + admin/introspection
  (incl. generation activation).
* `app/storage.py` — SQLite outbox (WAL), schema, claim/finish/replay,
  generation pin + activation CAS.
* `app/worker.py` — independent process; per-target dispatcher threads,
  per-event generation resolution, blocked-head reporting.
* `app/config.py` — deployment whitelist (legacy + generation format),
  reloadable config provider, controllable clock.
* `app/signing.py` — HMAC-SHA256 over timestamp + original JSON bytes.
* `verify/` — fake HTTP endpoints and the one-shot `verify` service.

Everything is Python 3.11 **standard library only**; the image needs no
package downloads at build time.

## Guarantees and semantics

* **Whitelist (deployment config only).** Target names, generation URLs
  and secrets come exclusively from `TARGET_CONFIG` /
  `TARGET_CONFIG_FILE`. Ingestion to an unconfigured name is `404`;
  request data can never introduce or redirect a target.
* **Delivery generations.** Each target has one or more *pre-configured*
  generations `{url, secret}` and exactly one *active* generation
  (persistent runtime state, initialized to the config's `initial`).
  An event pins the active `generation_id` **inside the same transaction**
  as the request-id dedup check and the insert. The worker always
  delivers with the URL+secret of the *pinned* generation and signs the
  original raw bytes with it — retries, dead-letter replays and crash
  requeues never switch to a newer generation, and the database can never
  record "old generation" while bytes go to the new endpoint.
* **Activation is compare-and-swap.** `POST
  /admin/targets/{name}/generations/activate` with
  `{"generation": "g2", "expected_current": "g1"}` succeeds only if the
  persisted current generation equals `expected_current` (else `409`,
  nothing written). Only pre-configured generations can be activated
  (else `404`). Activation — successful or failed — **never rewrites
  stored events**; it only decides which generation *future* ingests pin.
* **Missing old-generation config = blockage, not failure.** If the
  worker's configuration (temporarily) lacks a pinned generation — e.g.
  mid rolling deploy — the claim is reverted exactly (no failed attempt,
  no schedule change), the head event stays in place and the blockage is
  reported on `GET /control/status`. The event is not skipped and not
  counted as a delivery failure; other targets keep flowing; the queue
  drains in order once the config converges.
* **Legacy config stays compatible.** `"name": "URL|secret"` entries
  behave as a single generation named `legacy`; databases written before
  generations existed migrate transparently (`generation_id NULL`
  resolves to the target's initial generation).
* **Producer dedup by request id.** `X-Request-Id` is unique in the
  outbox. Re-posting the same id with the same body returns the original
  event (`duplicate: true`) with its *original* pinned generation; same
  id with a different body is `409`.
* **Per-target signing.** Each delivery carries
  `X-Event-Id`, `X-Request-Id`, `X-Timestamp`, `X-Signature`, where

  ```
  X-Signature = HMAC_SHA256( pinned_generation_secret,
                             X-Timestamp + "\n" + <exact ingestion bytes> )
  ```

  The signed body is the **raw JSON bytes as received** — never
  re-serialized (the verify suite checks this with unusual formatting).
* **Per-target FIFO.** Events are assigned a monotonic `seq` at
  insertion. A worker thread only claims the earliest active event for a
  target; while event N is in flight or parked in a retry backoff,
  events N+1… cannot be delivered — even across a generation switch.
  **Different targets are dispatched on independent threads and never
  block each other.**
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
* **DLQ replay keeps identity, history and generation.** `POST
  /admin/events/{id}/replay` re-schedules a dead event with the same
  event id, request id, body, `seq` and pinned `generation_id`; old
  attempts remain, the new attempts continue the numbering (5, 6…)
  under a fresh 4-attempt budget and `replayed_count` is incremented.

## Configuration

`TARGET_CONFIG` (JSON env var) or `TARGET_CONFIG_FILE` (path to a JSON
document, re-read on change — used by the worker so rolling config
deploys converge without a restart). Per target, either format:

```jsonc
{
  // legacy single-generation (unchanged):
  "a": "http://a:8080/receive|secret-a",

  // pre-configured delivery generations:
  "billing": {
    "initial": "gen-2025",
    "generations": {
      "gen-2025": {"url": "http://billing-old:8080/", "secret": "old-key"},
      "gen-2026": {"url": "http://billing-new:9090/", "secret": "new-key"}
    }
  }
}
```

## Verification details

The suite drives a **virtual clock** (`POST /admin/clock`,
`/admin/clock/advance`) so the 1/2/4 s schedule is deterministic and
fast, while real OS socket timeouts (worker timeout 1 s vs. a fake
endpoint that sleeps 2 s) and real worker crashes are exercised in wall
time. Multi-generation targets point each generation at its **own fake
endpoint with its own secret**, so old/new receivers are truly
independent. The worker reads its config from a **file** that the suite
rewrites mid-run to simulate a rollout in which the old generation's
config has not reached the worker yet. The crash test arms the worker
(`POST /control/crash`) to `os._exit(17)` immediately after a successful
write; Compose's `restart: unless-stopped` brings it back, and the
duplicated receipt plus the half-open attempt row are asserted.

## HTTP surface

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/v1/events` | ingest (`X-Target`, `X-Request-Id`, JSON body) |
| GET | `/v1/events/{id}` | event state (incl. pinned `generation_id`) |
| GET | `/v1/events/{id}/attempts` | full attempt history |
| GET/POST | `/admin/clock`, POST `/admin/clock/advance` | controllable clock |
| GET | `/admin/events?status=dead` | introspection |
| POST | `/admin/events/{id}/replay` | dead-letter replay (keeps generation) |
| GET | `/admin/targets`, `/admin/targets/{name}` | configured + active generations |
| POST | `/admin/targets/{name}/generations/activate` | CAS activation of a pre-configured generation |
| GET | `/control/status` (worker) | per-target dispatcher state, blocked heads |
| POST | `/control/crash` (worker) | crash injection for tests |

## Local development without Docker

```bash
# pip-free: python3 -m app.api / app.worker need TARGET_CONFIG(+_FILE) + DB_PATH
python3 -m app.api
python3 -m app.worker
python3 verify/fake_target.py
python3 verify/verify.py
```
