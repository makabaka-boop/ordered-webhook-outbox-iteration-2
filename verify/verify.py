#!/usr/bin/env python3
"""One-shot verification service.

Run with: docker compose run --rm verify

It exercises the whole stack (api + independent worker + shared outbox +
fake HTTP endpoints) using a controllable virtual clock so the 1/2/4 s
retry schedule is deterministic and fast. Timeouts and worker crashes are
real wall-clock events.

Checks (in order):
  1. whitelist enforcement, malformed input and producer request-id dedup
  2. per-target FIFO ordering: a retrying head event blocks its successors
  3. non-2xx -> retries at exactly 1, 2, 4 s; 4th failure -> dead-letter
  4. DLQ replay keeps identity + attempt history, fresh budget continues
     attempt numbering (5..)
  5. independent targets: one target's retries/dead-letter never block
     another target
  6. real socket timeout behaves like any other failed attempt
  7. signed delivery over the original raw JSON bytes (with target secret)
  8. delivery generations: activation while an event is in flight -- the
     in-flight/retrying event keeps its pinned old endpoint+key, only new
     events use the new generation (double fake endpoints)
  9. activation is compare-and-swap (expected_current), admin surface,
     request-id dedup keeps the originally pinned generation
 10. DLQ replay after a generation migration still uses the pinned old
     endpoint+key; new events use the new one
 11. old-generation config temporarily missing at the worker: head event
     is kept and reported blocked, never failed, never skipped; other
     targets proceed; queue drains in order once config converges
 12. worker crash/restart mid-generation: pinned generation survives the
     restart; activation state is persistent across restarts
 13. worker crash after receipt -> duplicate on restart, no event lost;
     the system explicitly provides at-least-once, never exactly-once
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
from config import parse_targets  # noqa: E402  (shared generation contract)

API = os.environ.get("API_URL", "http://api:8080")
FAKE = os.environ.get("FAKE_URL", "http://fake-target:8090")
WORKER_CTRL = os.environ.get("WORKER_CONTROL_URL", "http://worker:9100")
WORKER_CONFIG_FILE = os.environ.get(
    "WORKER_CONFIG_FILE", "/worker-config/targets.json"
)

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
_results: list[tuple[str, bool, str]] = []

BASE_TS = 2_000_000_000  # fixed virtual-clock epoch used across the suite

# The full deployment config (same document the api/fake-target run with)
# and the worker's *partial* variant: target "d" lacks its old generation
# "d1", simulating a config rollout that has not reached the worker yet.
FULL_CONFIG = json.loads(os.environ["TARGET_CONFIG"])
PARTIAL_CONFIG = json.loads(json.dumps(FULL_CONFIG))
PARTIAL_CONFIG["d"] = {
    "initial": "d2",
    "generations": {"d2": FULL_CONFIG["d"]["generations"]["d2"]},
}
PARSED_TARGETS = parse_targets(os.environ["TARGET_CONFIG"])


def gen_secret(target: str, generation: str) -> str:
    return PARSED_TARGETS[target]["generations"][generation]["secret"]


class TimePusher:
    """Drives the virtual clock from the outside while the suite waits.

    Virtual time advances 0.1 s per 0.04 s of wall time (2.5x), in steps of
    0.1 s. It starts *paused*: tests that need an exact signature timestamp
    freeze time, post the event, then resume so delivery can proceed. This
    keeps the documented 1/2/4 s backoff deterministic in logical seconds
    while socket timeouts/crashes remain genuine wall-clock events.
    """

    STEP = 0.1
    PERIOD = 0.04

    def __init__(self):
        self._cond = threading.Condition()
        self._running = False
        self._inflight = 0
        self._t = threading.Thread(target=self._loop, daemon=True)
        self._t.start()

    def _loop(self):
        while True:
            with self._cond:
                while not self._running:
                    self._cond.wait()
                self._inflight += 1
            try:
                http("POST", API + "/admin/clock/advance",
                     json.dumps({"delta": self.STEP}).encode(),
                     {"Content-Type": "application/json"}, expect=200)
            except Exception:
                pass
            finally:
                with self._cond:
                    self._inflight -= 1
                    if self._inflight == 0:
                        self._cond.notify_all()
            time.sleep(self.PERIOD)

    def pause(self):
        """Stop advancing and wait for any in-flight advance to land."""
        with self._cond:
            self._running = False
            while self._inflight:
                self._cond.wait()

    def resume(self):
        with self._cond:
            self._running = True
            self._cond.notify_all()


PUSHER = TimePusher()


# ---------------------------------------------------------------- helpers

def http(method: str, url: str, body: bytes | None = None,
         headers: dict | None = None, expect: int | None = None,
         raw: bool = False):
    req = urllib.request.Request(url, data=body, method=method,
                                 headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = resp.read()
            code = resp.status
    except urllib.error.HTTPError as e:
        payload = e.read()
        code = e.code
    if expect is not None and code != expect:
        raise AssertionError(f"{method} {url}: expected {expect}, got {code}: "
                             f"{payload[:300]!r}")
    if raw:
        return code, payload
    return code, (json.loads(payload) if payload else None)


def wait_for(url: str, timeout: float = 60.0) -> None:
    deadline = time.time() + timeout
    while True:
        try:
            http("GET", url, expect=200)
            return
        except Exception:
            if time.time() > deadline:
                raise
            time.sleep(0.2)


def set_virtual_clock(t: float) -> None:
    http("POST", API + "/admin/clock",
         json.dumps({"mode": "virtual", "virtual_now": t}).encode(),
         {"Content-Type": "application/json"}, expect=200)


def post_event(target: str, request_id: str, payload: bytes,
               ctype: str = "application/json"):
    return http(
        "POST", API + "/v1/events", payload,
        {"Content-Type": ctype, "X-Target": target,
         "X-Request-Id": request_id}, expect=202,
    )[1]


def get_event(event_id: str) -> dict:
    return http("GET", f"{API}/v1/events/{event_id}", expect=200)[1]


def get_attempts(event_id: str) -> list[dict]:
    return http("GET", f"{API}/v1/events/{event_id}/attempts",
                expect=200)[1]


def wait_event(event_id: str, status: str, timeout: float = 30.0) -> dict:
    PUSHER.resume()
    deadline = time.time() + timeout
    while time.time() < deadline:
        ev = get_event(event_id)
        if ev["status"] == status:
            return ev
        time.sleep(0.1)
    raise AssertionError(
        f"event {event_id[:8]} did not reach {status}; "
        f"last={get_event(event_id)}"
    )


def freeze(t: int) -> None:
    """Pause the time pusher and pin the virtual clock to t."""
    PUSHER.pause()
    set_virtual_clock(t)


def behavior(endpoint: str, spec: dict) -> None:
    http("POST", f"{FAKE}/t/{endpoint}/behavior",
         json.dumps(spec).encode(), {"Content-Type": "application/json"},
         expect=200)


def receipts(endpoint: str) -> list[dict]:
    return http("GET", f"{FAKE}/t/{endpoint}/receipts", expect=200)[1][
        "receipts"
    ]


def clear(endpoint: str) -> None:
    http("DELETE", f"{FAKE}/t/{endpoint}/receipts", expect=200)


def activate(target: str, generation: str, expected_current: str,
             expect: int = 200):
    return http(
        "POST", f"{API}/admin/targets/{target}/generations/activate",
        json.dumps({"generation": generation,
                    "expected_current": expected_current}).encode(),
        {"Content-Type": "application/json"}, expect=expect,
    )[1]


def worker_status() -> dict:
    return http("GET", WORKER_CTRL + "/control/status", expect=200)[1]


def wait_worker(cond, timeout: float = 30.0) -> dict:
    """Poll the worker's /control/status until cond(status) holds."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            st = worker_status()
            last = st
            if cond(st):
                return st
        except Exception:
            pass
        time.sleep(0.2)
    raise AssertionError(f"worker condition not met; last status={last}")


def write_worker_config(cfg: dict) -> None:
    """Atomically replace the worker's config file (rollout simulation)."""
    tmp = WORKER_CONFIG_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh)
    os.replace(tmp, WORKER_CONFIG_FILE)


def converge_worker_config() -> None:
    """Point the worker at the partial config and wait until it agrees.

    Makes the suite independent of whatever a previous run left in the
    shared config file: target "d" starts without its old generation "d1".
    """
    write_worker_config(PARTIAL_CONFIG)
    expected = {
        "a": ["legacy"], "b": ["legacy"], "c": ["legacy"],
        "race": ["g1", "g2", "g3"], "dlq": ["h1", "h2"], "d": ["d2"],
    }
    wait_worker(
        lambda st: {
            name: t["configured_generations"]
            for name, t in st["targets"].items()
        } == expected,
        timeout=30,
    )


def check(name: str, fn):
    try:
        fn()
        _results.append((name, True, ""))
        print(f"  {PASS} {name}")
    except Exception as exc:
        _results.append((name, False, str(exc)))
        print(f"  {FAIL} {name}: {exc}")


def set_behavior_and_clock(endpoint: str, spec: dict, t: int):
    behavior(endpoint, spec)
    freeze(t)


# ------------------------------------------------------------------ tests

def test_input_and_dedup():
    set_virtual_clock(BASE_TS)
    # unknown target -> 404 (whitelist is deployment config only)
    code, body = http(
        "POST", API + "/v1/events", b'{"x":1}',
        {"Content-Type": "application/json", "X-Target": "nope",
         "X-Request-Id": "r-x"},
    )
    assert code == 404, body
    # non-JSON body -> 400
    code, _ = http(
        "POST", API + "/v1/events", b"not json",
        {"Content-Type": "application/json", "X-Target": "a",
         "X-Request-Id": "r-bad"},
    )
    assert code == 400
    # missing header -> 400
    code, _ = http("POST", API + "/v1/events", b"{}",
                   {"Content-Type": "application/json", "X-Target": "a"})
    assert code == 400

    first = post_event("a", "req-dedup-1", b'{"order": 7}')
    assert first["generation_id"] == "legacy"  # legacy config -> one gen
    again = post_event("a", "req-dedup-1", b'{"order": 7}')
    assert again["duplicate"] is True and again["event_id"] == first["event_id"]
    # same request id, different raw body -> conflict
    code, body = http(
        "POST", API + "/v1/events", b'{"order": 8}',
        {"Content-Type": "application/json", "X-Target": "a",
         "X-Request-Id": "req-dedup-1"},
    )
    assert code == 409, body
    wait_event(first["event_id"], "delivered")

    # dedup also holds *after* delivery: no second event/attempt is created
    after = post_event("a", "req-dedup-1", b'{"order": 7}')
    assert after["duplicate"] is True
    assert after["event_id"] == first["event_id"]
    assert after["status"] == "delivered", after


def test_fifo_ordering():
    clear("a")
    t = BASE_TS + 100
    set_behavior_and_clock("a", {"fail_first": 2, "fail_status": 503}, t)
    # e1 fails twice (1s, 2s backoff) then succeeds. e2/e3 are posted
    # before e1 is delivered and must not overtake it.
    e1 = post_event("a", "fifo-1", b'{"n":1}')["event_id"]
    e2 = post_event("a", "fifo-2", b'{"n":2}')["event_id"]
    e3 = post_event("a", "fifo-3", b'{"n":3}')["event_id"]
    wait_event(e3, "delivered", timeout=40)
    rs = receipts("a")
    ids = [r["event_id"] for r in rs]
    # every event delivered in seq order; first 3 receipts are e1 attempts
    assert ids[:3] == [e1, e1, e1], ids
    assert ids[-2:] == [e2, e3], ids
    # no receipt of e2/e3 appeared while e1 was still retrying: the first
    # time we see e2, e1 must already have a 200 receipt behind it
    first_e2 = ids.index(e2)
    assert any(r["event_id"] == e1 and r["responded"] == 200
               for r in rs[:first_e2]), rs


def test_retry_backoff_and_dead_letter():
    clear("b")
    t = BASE_TS + 200
    set_behavior_and_clock("b", {"fail_first": 99, "fail_status": 500}, t)
    eid = post_event("b", "dlq-1", b'{"msg":"boom"}')["event_id"]
    wait_event(eid, "dead", timeout=40)
    ev = get_event(eid)
    attempts = get_attempts(eid)
    assert ev["attempts"] == 4 and len(attempts) == 4, ev
    starts = [int(a["started_at"]) for a in attempts]
    # nominal schedule: gaps of exactly 1, 2, 4 seconds. Allow up to 0.25s
    # of clock quantization on a loaded CI machine (the policy is 1/2/4).
    gaps = [b - a for a, b in zip(starts, starts[1:])]
    for got, want in zip(gaps, [1, 2, 4]):
        assert want <= got <= want + 0.25, (starts, gaps)
    assert all(a["ok"] == 0 and a["status_code"] == 500 for a in attempts)
    assert [a["attempt_no"] for a in attempts] == [1, 2, 3, 4]
    # event identity preserved in death
    assert receipts("b")[0]["request_id"] == "dlq-1"


def test_dlq_replay():
    # reuse the dead event from the previous test
    dead = http("GET", API + "/admin/events?status=dead", expect=200)[1]
    eid = next(e["event_id"] for e in dead if e["request_id"] == "dlq-1")
    behavior("b", {"fail_first": 1, "fail_status": 502})  # 1 more fail, then ok
    t = BASE_TS + 300
    freeze(t)
    row = http("POST", f"{API}/admin/events/{eid}/replay",
               b"", expect=200)[1]
    assert row["status"] == "scheduled" and row["replayed_count"] == 1, row
    assert row["max_attempts"] == 8, row
    wait_event(eid, "delivered", timeout=40)
    attempts = get_attempts(eid)
    # full history kept, new attempts continue at 5, with a fresh budget
    assert [a["attempt_no"] for a in attempts] == [1, 2, 3, 4, 5, 6], attempts
    assert attempts[4]["status_code"] == 502
    assert attempts[5]["ok"] == 1
    ev = get_event(eid)
    assert ev["event_id"] == eid and ev["request_id"] == "dlq-1"
    assert ev["replayed_count"] == 1


def test_target_independence():
    clear("a"); clear("c")
    t = BASE_TS + 400
    # c is permanently failing; a is healthy
    behavior("c", {"fail_first": 99, "fail_status": 503})
    behavior("a", {"fail_first": 0})
    freeze(t)
    c1 = post_event("c", "iso-c1", b'{"z":1}')["event_id"]
    a1 = post_event("a", "iso-a1", b'{"z":2}')["event_id"]
    a2 = post_event("a", "iso-a2", b'{"z":3}')["event_id"]
    # a's events sail through while c retries
    wait_event(a2, "delivered", timeout=20)
    assert [r["event_id"] for r in receipts("a")] == [a1, a2]
    # c keeps retrying independently and eventually dead-letters
    wait_event(c1, "dead", timeout=40)
    assert len(receipts("c")) == 4
    # replay c1 so it does not linger as dead for later runs
    http("POST", f"{API}/admin/events/{c1}/replay", b"", expect=200)
    behavior("c", {"fail_first": 0})
    wait_event(c1, "delivered", timeout=20)


def test_real_timeout():
    clear("b")
    t = BASE_TS + 500
    # first attempt: fake sleeps 2s, worker times out at ~1s but the fake
    # records the bytes; afterwards it answers 200.
    behavior("b", {"timeout_first": 1, "sleep": 2.0})
    freeze(t)
    eid = post_event("b", "timeout-1", b'{"slow":true}')["event_id"]
    wait_event(eid, "delivered", timeout=30)
    attempts = get_attempts(eid)
    assert len(attempts) == 2, attempts
    a1 = attempts[0]
    assert a1["ok"] == 0 and a1["status_code"] is None
    assert "timeout" in (a1["error"] or "")
    assert attempts[1]["ok"] == 1
    rs = receipts("b")
    assert len(rs) == 2, rs
    # give the orphaned slow handler thread time to finish its sleep and
    # discover the worker has gone; its final write state is incidental,
    # what matters is that the signed bytes arrived despite the timeout
    deadline = time.time() + 2.5
    while receipts("b")[0]["responded"] is None and time.time() < deadline:
        time.sleep(0.1)
    rs = receipts("b")
    assert rs[0]["responded"] in (200, "client_gone", None), rs[0]
    assert rs[1]["responded"] == 200
    # target got the signed payload even though the first attempt "timed out"
    assert rs[0]["sig_valid"] and rs[0]["event_id"] == eid


def test_signature_and_raw_bytes():
    clear("a")
    sig_ts = BASE_TS + 600
    freeze(sig_ts)
    # unusual but valid JSON formatting + unicode; delivered bytes must be
    # identical to ingestion bytes
    raw = b'{\n  "\xe6\x97\xa5\xe6\x9c\xac": [1, 2, 3]\n}'
    assert json.loads(raw)  # sanity: still valid JSON
    eid = post_event("a", "sig-1", raw)["event_id"]
    wait_event(eid, "delivered", timeout=15)
    r = receipts("a")[-1]
    assert base64.b64decode(r["body_b64"]) == raw, "raw bytes mutated"
    assert r["sig_valid"] is True
    assert int(r["timestamp"]) == sig_ts
    assert r["request_id"] == "sig-1" and r["event_id"] == eid
    # signature is reproducible locally with the target's own secret and
    # over timestamp + "\n" + raw bytes
    secret = gen_secret("a", "legacy")
    expect = hmac.new(secret.encode(), str(sig_ts).encode() + b"\n" + raw,
                      hashlib.sha256).hexdigest()
    assert r["signature"] == expect
    # a *different* target's secret must not validate this signature
    wrong = hmac.new((secret + "-other").encode(),
                     str(sig_ts).encode() + b"\n" + raw,
                     hashlib.sha256).hexdigest()
    assert wrong != r["signature"]
    # and neither does signing over re-serialized bytes
    reserialized = json.dumps(json.loads(raw), separators=(",", ":")).encode()
    assert reserialized != raw
    bad = hmac.new(secret.encode(),
                   str(sig_ts).encode() + b"\n" + reserialized,
                   hashlib.sha256).hexdigest()
    assert bad != r["signature"]


def test_activation_vs_inflight_race():
    """Activation while an event is in flight: persistent state decides.

    e1 pins g1 at ingest; its first attempt is genuinely in flight (the
    old endpoint sleeps past the worker timeout) when g2 is activated.
    The retry must still go to the *old* endpoint with the *old* key --
    the database can never say "g1" while bytes go to the g2 endpoint.
    """
    clear("race-g1"); clear("race-g2")
    t = BASE_TS + 800
    behavior("race-g1", {"timeout_first": 1, "sleep": 2.0})
    behavior("race-g2", {"fail_first": 0})
    freeze(t)
    raw1, raw2 = b'{"gen":"old"}', b'{"gen":"new"}'
    e1 = post_event("race", "gen-race-1", raw1)["event_id"]
    assert get_event(e1)["generation_id"] == "g1"
    # wait until attempt 1 has reached the old endpoint (in flight now)
    deadline = time.time() + 10
    while time.time() < deadline and not receipts("race-g1"):
        time.sleep(0.05)
    assert receipts("race-g1"), "attempt 1 never reached the old endpoint"

    # CAS failures while e1 is in flight change nothing
    body = activate("race", "g2", "bogus", expect=409)
    assert body["current"] == "g1", body
    activate("race", "g9", "g1", expect=404)        # not pre-configured
    activate("no-such-target", "g2", "g1", expect=404)
    # the real activation, racing the in-flight delivery
    body = activate("race", "g2", "g1", expect=200)
    assert body["previous"] == "g1" and body["current"] == "g2", body
    # a retry of the same activation now fails (current moved)...
    activate("race", "g2", "g1", expect=409)
    # ...while re-activating the current generation is an idempotent no-op
    activate("race", "g2", "g2", expect=200)

    # new events pin the new generation; stored events keep theirs
    e2 = post_event("race", "gen-race-2", raw2)["event_id"]
    assert get_event(e1)["generation_id"] == "g1"
    assert get_event(e2)["generation_id"] == "g2"
    # request-id dedup after activation returns the original event with
    # its original pinned generation -- it is never re-pinned
    dup = post_event("race", "gen-race-1", raw1)
    assert dup["duplicate"] is True and dup["event_id"] == e1
    assert dup["generation_id"] == "g1", dup
    code, _ = http("POST", API + "/v1/events", b'{"gen":"CHANGED"}',
                   {"Content-Type": "application/json", "X-Target": "race",
                    "X-Request-Id": "gen-race-1"})
    assert code == 409

    wait_event(e1, "delivered", timeout=30)
    wait_event(e2, "delivered", timeout=30)
    r1, r2 = receipts("race-g1"), receipts("race-g2")
    # old endpoint saw exactly e1's two attempts; new endpoint only e2
    assert [r["event_id"] for r in r1] == [e1, e1], r1
    assert [r["event_id"] for r in r2] == [e2], r2
    assert all(r["sig_valid"] for r in r1 + r2)
    # each receipt verifies under its own generation's secret only
    for r, raw, good, bad in (
        (r1[-1], raw1, gen_secret("race", "g1"), gen_secret("race", "g2")),
        (r2[0], raw2, gen_secret("race", "g2"), gen_secret("race", "g1")),
    ):
        ts = int(r["timestamp"])
        want = hmac.new(good.encode(), str(ts).encode() + b"\n" + raw,
                        hashlib.sha256).hexdigest()
        notwant = hmac.new(bad.encode(), str(ts).encode() + b"\n" + raw,
                           hashlib.sha256).hexdigest()
        assert r["signature"] == want and r["signature"] != notwant
    # the in-flight first attempt is honestly recorded as a timeout
    a1, a2 = get_attempts(e1), get_attempts(e2)
    assert a1[0]["ok"] == 0 and "timeout" in (a1[0]["error"] or "")
    assert a1[1]["ok"] == 1
    # FIFO across generations: e2 was not attempted before e1 completed
    assert a2[0]["started_at"] >= get_event(e1)["delivered_at"]


def test_activation_cas_and_admin_surface():
    # admin surface: configured generations + persisted current (no secrets)
    targets = http("GET", API + "/admin/targets", expect=200)[1]
    by_name = {t["target"]: t for t in targets}
    assert by_name["race"]["generations"] == ["g1", "g2", "g3"]
    assert by_name["race"]["current"] == "g2"  # activated by previous test
    assert by_name["a"]["generations"] == ["legacy"]
    assert by_name["a"]["current"] == "legacy"
    assert "secret" not in json.dumps(by_name).lower()
    one = http("GET", API + "/admin/targets/race", expect=200)[1]
    assert one["current"] == "g2"
    http("GET", API + "/admin/targets/nope", expect=404)
    # legacy single-generation targets have no other generation to activate
    activate("a", "g2", "legacy", expect=404)
    # malformed activation body -> 400
    code, _ = http("POST", API + "/admin/targets/race/generations/activate",
                   json.dumps({"generation": "g3"}).encode(),
                   {"Content-Type": "application/json"})
    assert code == 400
    # wrong expected_current -> 409, persisted state unchanged
    body = activate("race", "g3", "g1", expect=409)
    assert body["current"] == "g2"
    assert http("GET", API + "/admin/targets/race", expect=200)[1][
        "current"] == "g2"
    # correct CAS activates g3
    body = activate("race", "g3", "g2", expect=200)
    assert body["previous"] == "g2" and body["current"] == "g3"
    # events ingested now pin g3 and go to the g3 endpoint with its key
    clear("race-g3")
    behavior("race-g3", {"fail_first": 0})  # explicit: never inherit state
    freeze(BASE_TS + 850)
    eid = post_event("race", "gen-cas-1", b'{"stage":"g3"}')["event_id"]
    assert get_event(eid)["generation_id"] == "g3"
    wait_event(eid, "delivered", timeout=20)
    rs = receipts("race-g3")
    assert [r["event_id"] for r in rs] == [eid] and rs[0]["sig_valid"]


def test_dlq_replay_keeps_pinned_generation():
    clear("dlq-g1"); clear("dlq-g2")
    t = BASE_TS + 900
    behavior("dlq-g1", {"fail_first": 99, "fail_status": 500})
    behavior("dlq-g2", {"fail_first": 0})
    freeze(t)
    eid = post_event("dlq", "gen-dlq-1", b'{"dlq":true}')["event_id"]
    assert get_event(eid)["generation_id"] == "h1"
    wait_event(eid, "dead", timeout=40)
    assert len(receipts("dlq-g1")) == 4 and receipts("dlq-g2") == []
    # the receiver migrates *after* the event died
    activate("dlq", "h2", "h1", expect=200)
    # replay: the fresh attempts must still use the pinned h1 contract
    behavior("dlq-g1", {"fail_first": 1, "fail_status": 502})
    freeze(t + 50)
    row = http("POST", f"{API}/admin/events/{eid}/replay", b"", expect=200)[1]
    assert row["generation_id"] == "h1" and row["replayed_count"] == 1, row
    wait_event(eid, "delivered", timeout=40)
    attempts = get_attempts(eid)
    assert [a["attempt_no"] for a in attempts] == [1, 2, 3, 4, 5, 6]
    assert attempts[4]["status_code"] == 502 and attempts[5]["ok"] == 1
    rs1, rs2 = receipts("dlq-g1"), receipts("dlq-g2")
    assert len(rs1) == 6 and all(r["event_id"] == eid for r in rs1)
    assert all(r["sig_valid"] for r in rs1)
    assert rs2 == [], "replayed event leaked to the new endpoint"
    # brand-new events on the same target use the new generation
    e2 = post_event("dlq", "gen-dlq-2", b'{"dlq":"new"}')["event_id"]
    assert get_event(e2)["generation_id"] == "h2"
    wait_event(e2, "delivered", timeout=20)
    assert [r["event_id"] for r in receipts("dlq-g2")] == [e2]
    assert receipts("dlq-g2")[0]["sig_valid"]


def test_missing_generation_blocks_head():
    # worker runs the partial config: target d's old generation d1 absent
    write_worker_config(PARTIAL_CONFIG)
    wait_worker(lambda st: st["targets"].get("d", {})
                .get("configured_generations") == ["d2"])
    clear("d"); clear("d2")
    t = BASE_TS + 1000
    behavior("d", {"fail_first": 0})
    behavior("d2", {"fail_first": 0})
    freeze(t)
    e1 = post_event("d", "gen-block-1", b'{"n":1}')["event_id"]
    e2 = post_event("d", "gen-block-2", b'{"n":2}')["event_id"]
    assert get_event(e1)["generation_id"] == "d1"
    # the worker claims the head, cannot resolve d1, parks it and says so
    wait_worker(lambda st: st["targets"]["d"]["blocked"]
                and st["targets"]["d"]["missing_generation"] == "d1"
                and st["targets"]["d"]["head_event_id"] == e1)
    # not a delivery failure: no attempts, no error records, head kept
    ev1 = get_event(e1)
    assert ev1["status"] == "scheduled" and ev1["attempts"] == 0, ev1
    assert get_attempts(e1) == []
    assert receipts("d") == [] and receipts("d2") == []
    # nothing is skipped: e2 waits behind e1
    assert get_event(e2)["status"] == "scheduled"
    # other targets are unaffected by d's blockage
    ea = post_event("a", "gen-block-other", b'{"n":3}')["event_id"]
    wait_event(ea, "delivered", timeout=20)
    # config converges (d1 reappears) -> the queue drains in order
    write_worker_config(FULL_CONFIG)
    wait_event(e1, "delivered", timeout=30)
    wait_event(e2, "delivered", timeout=30)
    rs = receipts("d")
    assert [r["event_id"] for r in rs] == [e1, e2], rs
    assert all(r["sig_valid"] for r in rs)
    # exactly one real attempt each -- the blockage left no trace
    assert get_event(e1)["attempts"] == 1 and get_event(e2)["attempts"] == 1
    assert receipts("d2") == []
    # and the target can now be migrated for new events
    activate("d", "d2", "d1", expect=200)
    e3 = post_event("d", "gen-block-3", b'{"n":4}')["event_id"]
    assert get_event(e3)["generation_id"] == "d2"
    wait_event(e3, "delivered", timeout=20)
    assert [r["event_id"] for r in receipts("d2")] == [e3]
    assert receipts("d2")[0]["sig_valid"]


def test_restart_keeps_pinned_generation():
    clear("race-g3")
    t = BASE_TS + 1100
    # first attempt fails fast, parking the event at t+1 while we arm the
    # crash; the successful second attempt kills the worker mid-generation
    behavior("race-g3", {"fail_first": 1, "fail_status": 503})
    freeze(t)
    eid = post_event("race", "gen-restart-1", b'{"crash":"gen"}')["event_id"]
    assert get_event(eid)["generation_id"] == "g3"
    deadline = time.time() + 10
    while time.time() < deadline:
        if get_attempts(eid):
            break
        time.sleep(0.05)
    assert get_attempts(eid), "first attempt never recorded"
    http("POST", WORKER_CTRL + "/control/crash",
         json.dumps({"event_id": eid}).encode(),
         {"Content-Type": "application/json"}, expect=200)
    # the restart requeues the in-flight event; the duplicate delivery
    # still uses the pinned g3 endpoint+key
    wait_event(eid, "delivered", timeout=60)
    rs = receipts("race-g3")
    assert [r["event_id"] for r in rs] == [eid] * 3, rs
    assert all(r["sig_valid"] for r in rs)
    attempts = get_attempts(eid)
    crashed = [a for a in attempts if a["finished_at"] is None]
    assert len(crashed) == 1 and crashed[0]["attempt_no"] == 2, attempts
    assert attempts[-1]["ok"] == 1
    # activation state is persistent: after the restart new events still
    # pin g3, not the config's initial generation
    e2 = post_event("race", "gen-restart-2", b'{"crash":"after"}')["event_id"]
    assert get_event(e2)["generation_id"] == "g3"
    wait_event(e2, "delivered", timeout=30)
    assert receipts("race-g3")[-1]["event_id"] == e2


def test_worker_crash_restart():
    clear("b")
    t = BASE_TS + 1200
    # First attempt fails fast while the clock is frozen, parking the event
    # at not_before=t+1 -- a deterministic window in which to arm the crash
    # before the (successful) second attempt.
    behavior("b", {"fail_first": 1, "fail_status": 503})
    freeze(t)
    eid = post_event("b", "crash-1", b'{"v":42}')["event_id"]
    # wait until the first attempt is recorded, then arm
    deadline = time.time() + 10
    while time.time() < deadline:
        if get_attempts(eid):
            break
        time.sleep(0.05)
    assert get_attempts(eid), "first attempt never recorded"
    http("POST", WORKER_CTRL + "/control/crash",
         json.dumps({"event_id": eid}).encode(),
         {"Content-Type": "application/json"}, expect=200)

    # event is eventually delivered (restart policy brings the worker back;
    # it requeues the in-flight event on startup)
    wait_event(eid, "delivered", timeout=45)
    rs = receipts("b")
    # receipt 1: fast 503, receipt 2: 200 + immediate crash,
    # receipt 3: post-restart duplicate 200.
    assert len(rs) == 3, f"expected fail+duplicate deliveries, got {len(rs)}"
    assert [r["event_id"] for r in rs] == [eid] * 3
    assert all(r["sig_valid"] for r in rs)
    assert all(
        base64.b64decode(rs[0]["body_b64"]) == base64.b64decode(r["body_b64"])
        for r in rs
    )
    # history honestly records the first failure and the crashed attempt
    # (finished_at NULL) followed by the successful post-restart attempt
    attempts = get_attempts(eid)
    assert attempts[0]["ok"] == 0 and attempts[0]["status_code"] == 503
    crashed = [a for a in attempts if a["finished_at"] is None]
    assert len(crashed) == 1 and crashed[0]["attempt_no"] == 2, attempts
    assert attempts[-1]["ok"] == 1
    # NOTE: duplicate happened by design -> guarantee is at-least-once.
    print("\n    [semantics] duplicate after crash is allowed and observed; "
          "system provides at-least-once, not exactly-once")


def main() -> int:
    print("Waiting for services...")
    wait_for(API + "/healthz")
    wait_for(FAKE + "/healthz")
    wait_for(WORKER_CTRL + "/healthz")
    # make repeated acceptance runs against a persistent volume idempotent
    http("POST", API + "/admin/reset", b"", expect=200)
    print("Outbox reset. Converging worker config...")
    converge_worker_config()
    print("All services reachable. Running checks:\n")

    check("ingress validation + request-id dedup", test_input_and_dedup)
    check("per-target FIFO under retry", test_fifo_ordering)
    check("1/2/4s backoff and 4th-failure dead-letter",
          test_retry_backoff_and_dead_letter)
    check("DLQ replay preserves identity & attempt history", test_dlq_replay)
    check("independent targets do not block each other",
          test_target_independence)
    check("real socket timeout is a failed attempt", test_real_timeout)
    check("signature over timestamp + original raw JSON bytes",
          test_signature_and_raw_bytes)
    check("generation activation vs in-flight delivery",
          test_activation_vs_inflight_race)
    check("activation CAS + admin surface + ingress pinning",
          test_activation_cas_and_admin_surface)
    check("DLQ replay keeps the pinned old generation",
          test_dlq_replay_keeps_pinned_generation)
    check("missing old-generation config blocks head, never fails it",
          test_missing_generation_blocks_head)
    check("worker restart preserves pinned generation",
          test_restart_keeps_pinned_generation)
    check("worker crash -> duplicate on restart, zero event loss",
          test_worker_crash_restart)

    print()
    failed = [n for n, ok, _ in _results if not ok]
    # leave the shared worker config file as the suite expects to find it
    # (partial: without d's old generation), so reruns and git stay clean
    try:
        write_worker_config(PARTIAL_CONFIG)
    except OSError:
        pass
    if failed:
        print(f"{FAIL} {len(failed)}/{len(_results)} checks failed:")
        for n, ok, err in _results:
            if not ok:
                print(f"  - {n}: {err}")
        return 1
    print(f"{PASS} all {len(_results)} checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
