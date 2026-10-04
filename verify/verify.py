#!/usr/bin/env python3
"""One-shot verification service.

Run with: docker compose run --rm verify

It exercises the whole stack (api + independent worker + shared outbox +
fake HTTP targets) using a controllable virtual clock so the 1/2/4 s
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
  8. worker crash after receipt -> duplicate on restart, no event lost;
     the system explicitly provides at-least-once, never exactly-once
  9. preconfigured delivery generations: CAS activation, activation/in-flight
     ordering, generation-pinned retries/DLQ/restart, dual endpoint signing,
     missing-contract blocking and other-target progress
"""

from __future__ import annotations

import base64
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

from config import load_targets

API = os.environ.get("API_URL", "http://api:8080")
FAKE = os.environ.get("FAKE_URL", "http://fake-target:8090")
WORKER_CTRL = os.environ.get("WORKER_CONTROL_URL", "http://worker:9100")

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
_results: list[tuple[str, bool, str]] = []

BASE_TS = 2_000_000_000  # fixed virtual-clock epoch used across the suite
TARGET_CONFIG = load_targets(os.environ.get("TARGET_CONFIG"))


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


def behavior(target: str, spec: dict) -> None:
    http("POST", f"{FAKE}/t/{target}/behavior",
         json.dumps(spec).encode(), {"Content-Type": "application/json"},
         expect=200)


def receipts(target: str) -> list[dict]:
    return http("GET", f"{FAKE}/t/{target}/receipts", expect=200)[1][
        "receipts"
    ]


def clear(target: str) -> None:
    http("DELETE", f"{FAKE}/t/{target}/receipts", expect=200)


def generation_behavior(target: str, generation: str, spec: dict) -> None:
    http(
        "POST",
        f"{FAKE}/t/{target}/generations/{generation}/behavior",
        json.dumps(spec).encode(),
        {"Content-Type": "application/json"},
        expect=200,
    )


def generation_receipts(target: str, generation: str) -> list[dict]:
    return http(
        "GET", f"{FAKE}/t/{target}/generations/{generation}/receipts",
        expect=200,
    )[1]["receipts"]


def clear_generation(target: str, generation: str) -> None:
    http(
        "DELETE",
        f"{FAKE}/t/{target}/generations/{generation}/receipts",
        expect=200,
    )


def activate_generation(target: str, generation: str,
                        expected: str | None = None,
                        expect: int = 200):
    body = {} if expected is None else \
        {"expected_current_generation_id": expected}
    return http(
        "POST",
        f"{API}/admin/targets/{target}/generations/{generation}/activate",
        json.dumps(body).encode(),
        {"Content-Type": "application/json"},
        expect=expect,
    )


def get_target(target: str) -> dict:
    return http("GET", f"{API}/admin/targets/{target}", expect=200)[1]


def get_blocked() -> list[dict]:
    return http("GET", WORKER_CTRL + "/control/blocked", expect=200)[1][
        "blocked"
    ]


def control_post(path: str, body: dict, expect: int = 200):
    return http(
        "POST", WORKER_CTRL + path, json.dumps(body).encode(),
        {"Content-Type": "application/json"}, expect=expect,
    )


def wait_until(predicate, timeout: float = 10.0, interval: float = 0.05,
               message: str = "condition"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {message}; last={last}")



def check(name: str, fn):
    try:
        fn()
        _results.append((name, True, ""))
        print(f"  {PASS} {name}")
    except Exception as exc:
        _results.append((name, False, str(exc)))
        print(f"  {FAIL} {name}: {exc}")


def set_behavior_and_clock(target: str, spec: dict, t: int):
    behavior(target, spec)
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
    import hashlib
    import hmac
    secret = TARGET_SECRETS["a"]
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


def test_worker_crash_restart():
    clear("b")
    t = BASE_TS + 700
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


def test_delivery_generations():
    """Preconfigured generations pin each event's URL, key and lifecycle."""
    target_name = "m"
    v1_secret = TARGET_CONFIG[target_name]["generations"]["v1"]["secret"]
    v2_secret = TARGET_CONFIG[target_name]["generations"]["v2"]["secret"]
    clear_generation(target_name, "v1")
    clear_generation(target_name, "v2")
    clear("a")

    target = get_target(target_name)
    assert target["current_generation_id"] == "v1", target
    assert [g["id"] for g in target["generations"]] == ["v1", "v2"]

    # CAS activation and whitelist checks. A failed activation must not
    # change durable current state.
    code, body = activate_generation(
        target_name, "v2", expected="v0", expect=409
    )
    assert body["current"] == "v1", body
    assert get_target(target_name)["current_generation_id"] == "v1"
    code, _ = http(
        "POST",
        f"{API}/admin/targets/{target_name}/generations/v9/activate",
        b"{}", {"Content-Type": "application/json"},
    )
    assert code == 404

    t = BASE_TS + 800
    freeze(t)
    # The first old attempt reaches v1 and blocks in a real 2-second target
    # sleep while the worker's 1-second socket timeout fires.
    generation_behavior(
        target_name, "v1",
        {"timeout_first": 1, "sleep": 2.0},
    )
    slow = post_event(target_name, "gen-slow", b'{"phase":"inflight"}')
    slow_id = slow["event_id"]
    assert slow["generation_id"] == "v1"
    wait_until(lambda: len(get_attempts(slow_id)) == 1,
               message="first v1 attempt started")

    # These requests complete and bind v1 while the first event is still
    # genuinely in flight on the old endpoint.
    queued_old = post_event(
        target_name, "gen-old-queued", b'{"phase":"old-queued"}'
    )["event_id"]
    old_dead_ids = [
        post_event(target_name, f"gen-old-dead-{i}",
                   b'{"phase":"old-dead"}')["event_id"]
        for i in range(3)
    ]

    # Activation is persisted while the first request is in flight. The old
    # attempt must finish as v1; only subsequent ingress binds v2.
    activated = activate_generation(target_name, "v2", expected="v1")[1]
    assert activated["current_generation_id"] == "v2"

    # Request-id de-duplication is evaluated against the original stored
    # generation despite activation. Same bytes returns the same event;
    # different bytes conflicts.
    duplicate = post_event(target_name, "gen-slow",
                           b'{"phase":"inflight"}')
    assert duplicate["duplicate"] is True
    assert duplicate["event_id"] == slow_id
    assert duplicate["generation_id"] == "v1"
    code, _ = http(
        "POST", API + "/v1/events", b'{"phase":"changed"}',
        {"Content-Type": "application/json", "X-Target": target_name,
         "X-Request-Id": "gen-slow"},
    )
    assert code == 409

    new1 = post_event(target_name, "gen-new-1", b'{"phase":"new"}')
    assert new1["generation_id"] == "v2"
    duplicate_new = post_event(target_name, "gen-new-1",
                               b'{"phase":"new"}')
    assert duplicate_new["duplicate"] is True
    assert duplicate_new["generation_id"] == "v2"

    # The timeout attempt reached the old URL/key and is not rerouted. It
    # remains one attempt; backoff is anchored to frozen logical time.
    wait_until(
        lambda: get_event(slow_id)["status"] == "scheduled" and
        len(get_attempts(slow_id)) == 1,
        timeout=5,
        message="old in-flight attempt time out without a new attempt",
    )
    old_first = wait_until(
        lambda: generation_receipts(target_name, "v1") or None,
        timeout=5, message="old endpoint receive in-flight request",
    )[0]
    assert old_first["event_id"] == slow_id
    assert old_first["generation_id"] == "v1"
    assert old_first["url_generation_id"] == "v1"
    assert old_first["sig_valid"] is True

    # The first two due v1 events are the slow one (whose timeout attempt is
    # already spent) and the queued event; let those succeed and fail every
    # later backlog event to create three old-generation dead letters.
    generation_behavior(
        target_name, "v1",
        {"succeed_first": 2, "fail_first": 99, "fail_status": 500},
    )
    set_virtual_clock(t + 1)
    wait_event(slow_id, "delivered", timeout=20)
    wait_event(queued_old, "delivered", timeout=20)
    for dead_id in old_dead_ids:
        wait_event(dead_id, "dead", timeout=40)
    wait_event(new1["event_id"], "delivered", timeout=20)

    v1_receipts = generation_receipts(target_name, "v1")
    v2_receipts = generation_receipts(target_name, "v2")
    assert [r["event_id"] for r in v2_receipts] == [new1["event_id"]]
    assert all(r["generation_id"] == "v2" and
               r["url_generation_id"] == "v2" and
               r["sig_valid"] for r in v2_receipts)
    assert all(r["generation_id"] == "v1" and
               r["url_generation_id"] == "v1" and
               r["sig_valid"] for r in v1_receipts)
    assert base64.b64decode(v2_receipts[0]["body_b64"]) == \
        b'{"phase":"new"}'
    assert get_event(slow_id)["generation_id"] == "v1"
    assert get_event(new1["event_id"])["generation_id"] == "v2"
    local_sig = __import__("hmac").new(
        v2_secret.encode(),
        str(v2_receipts[0]["timestamp"]).encode() +
        b"\n" + b'{"phase":"new"}',
        __import__("hashlib").sha256,
    ).hexdigest()
    assert v2_receipts[0]["signature"] == local_sig
    assert v1_secret != v2_secret

    # DLQ replay of an old event remains old, including its retry attempts.
    replay_id = old_dead_ids[0]
    before_count = len(generation_receipts(target_name, "v1"))
    generation_behavior(
        target_name, "v1",
        {"fail_first": 1, "fail_status": 502},
    )
    freeze(BASE_TS + 900)
    replay_row = http(
        "POST", f"{API}/admin/events/{replay_id}/replay", b"", expect=200
    )[1]
    assert replay_row["generation_id"] == "v1"
    wait_event(replay_id, "delivered", timeout=30)
    replay_attempts = get_attempts(replay_id)
    assert [a["attempt_no"] for a in replay_attempts] == \
        [1, 2, 3, 4, 5, 6]
    assert replay_attempts[4]["status_code"] == 502
    assert replay_attempts[5]["ok"] == 1
    assert all(
        r["event_id"] != replay_id or r["generation_id"] == "v1"
        for r in generation_receipts(target_name, "v1") +
        generation_receipts(target_name, "v2")
    )
    assert len(generation_receipts(target_name, "v2")) == 1
    assert len(generation_receipts(target_name, "v1")) == before_count + 2

    # Temporarily missing old contract: replay is retained at the head, no
    # attempt is added/failed, and another target still advances.
    blocked_id = old_dead_ids[1]
    generation_behavior(target_name, "v1", {"fail_first": 0})
    freeze(BASE_TS + 950)
    # Remove the worker's old contract first: replay stores no new attempt
    # and the re-scheduled head is deterministically retained.
    removed = TARGET_CONFIG[target_name]["generations"]["v1"]
    control_post("/control/test/generations/remove",
                 {"target": target_name, "generation_id": "v1"})
    http("POST", f"{API}/admin/events/{blocked_id}/replay", b"", expect=200)
    blocked = wait_until(
        lambda: [b for b in get_blocked() if b["event_id"] == blocked_id],
        timeout=5, message="missing-generation block report",
    )[0]
    assert blocked["generation_id"] == "v1"
    assert blocked["reason"] == "generation configuration missing"
    time.sleep(0.4)
    assert get_event(blocked_id)["status"] == "scheduled"
    assert len(get_attempts(blocked_id)) == 4
    v1_before_restore = len(generation_receipts(target_name, "v1"))

    other = post_event("a", "gen-other-target-progress",
                       b'{"ok":true}')["event_id"]
    wait_event(other, "delivered", timeout=10)
    assert receipts("a")[-1]["event_id"] == other

    # Restoring the same deployment contract unblocks without changing the
    # event's bound generation.
    control_post("/control/test/generations/restore",
                 {"target": target_name, "generation": removed})
    wait_event(blocked_id, "delivered", timeout=20)
    assert get_event(blocked_id)["generation_id"] == "v1"
    assert len(generation_receipts(target_name, "v1")) == \
        v1_before_restore + 1
    assert [b for b in get_blocked() if b["event_id"] == blocked_id] == []

    # A replayed old event that crashes after receipt is recovered using the
    # old endpoint/secret after worker restart.
    crash_id = old_dead_ids[2]
    generation_behavior(target_name, "v1", {"fail_first": 0})
    freeze(BASE_TS + 1000)
    http("POST", f"{API}/admin/events/{crash_id}/replay", b"", expect=200)
    wait_until(lambda: get_event(crash_id)["status"] == "scheduled",
               message="crash replay scheduled")
    http("POST", WORKER_CTRL + "/control/crash",
         json.dumps({"event_id": crash_id}).encode(),
         {"Content-Type": "application/json"}, expect=200)
    wait_event(crash_id, "delivered", timeout=45)
    crash_receipts = [
        r for r in generation_receipts(target_name, "v1")
        if r["event_id"] == crash_id and
        int(r["timestamp"]) >= BASE_TS + 1000
    ]
    assert len(crash_receipts) == 2, crash_receipts
    assert all(r["generation_id"] == "v1" and r["sig_valid"]
               for r in crash_receipts)
    assert all(r["generation_id"] == "v1" for r in crash_receipts)
    assert get_event(crash_id)["generation_id"] == "v1"


TARGET_SECRETS = json.loads(os.environ.get("TARGET_SECRETS_JSON", "{}"))


def main() -> int:
    print("Waiting for services...")
    wait_for(API + "/healthz")
    wait_for(FAKE + "/healthz")
    wait_for(WORKER_CTRL + "/healthz")
    # make repeated acceptance runs against a persistent volume idempotent
    http("POST", API + "/admin/reset", b"", expect=200)
    print("Outbox reset. All services reachable. Running checks:\n")

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
    check("worker crash -> duplicate on restart, zero event loss",
          test_worker_crash_restart)
    check("pinned delivery generations across activation and restart",
          test_delivery_generations)

    print()
    failed = [n for n, ok, _ in _results if not ok]
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
