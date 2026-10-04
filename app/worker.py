"""Independent delivery worker.

Runs in its own container/process and talks to the same SQLite outbox as
the API. Concurrency model:

* one dispatcher thread **per configured target** -> different targets
  never block each other;
* within a target the worker only ever holds the *head* active event. A
  row is claimed in a ``BEGIN IMMEDIATE`` transaction only when no earlier
  row for that target is scheduled-due or in flight, so event N+1 is
  never delivered while event N is still retrying;
* each event carries the delivery generation bound at ingress. The worker
  resolves that generation's URL and secret for every attempt. Retries,
  crash recovery and dead-letter replay can never switch to a generation
  activated after the event was stored;
* if the deployment configuration for an event's generation is temporarily
  absent (for example during a rolling configuration update), the event
  stays at the head with no attempt row and no failure count. The
  dispatcher reports a block and continues polling; other targets are
  unaffected;
* failure policy: a non-2xx response, timeout or connection error counts
  as one failed attempt. Attempts 1-3 are retried after 1, 2, 4 seconds
  (logical, controllable clock). A 4th failure moves the event to the
  dead-letter table-state (status='dead'), which unblocks the next event;
* crash semantics: an in-flight claim that is never finished (process
  killed) is requeued at the next worker startup and sent again. Duplicate
  delivery is therefore allowed and documented -- guarantee is
  at-least-once, never "exactly once";
* DLQ replay preserves identity (same event/request id, body, seq and
  generation) and the full attempt history; retry attempts continue
  numbering from 5 under a fresh 4-attempt budget.

A small control HTTP server (CONTROL_PORT) exists for tests:
    POST /control/crash  {"event_id": "..."}
        the *next successful* delivery of that event os._exit()s the
        process after the target received the bytes (duplicate-on-restart
        simulation).
    GET  /control/blocked
        reports heads parked because a bound generation is not configured.
    POST /control/test/generations/remove
        removes an in-memory generation without changing deployment files.
    POST /control/test/generations/restore
        restores a generation removed above.
    GET  /healthz
"""

from __future__ import annotations

import copy
import http.client
import json
import os
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from config import Clock, load_targets
from signing import sign
from storage import Storage

POLL_INTERVAL = 0.03  # seconds of real time between claim polls
SOCKET_TIMEOUT = float(os.environ.get("DELIVERY_TIMEOUT", "1.0"))

# event ids that crash the process after a successful send (in-memory only,
# deliberately not persisted -- it models a real crash, not a durable flag)
_crash_after: set[str] = set()


class WorkerState:
    """Mutable, in-memory target configuration and block reports."""

    def __init__(self, targets: dict[str, dict]):
        self._lock = threading.RLock()
        self.targets = copy.deepcopy(targets)
        self.blocked: dict[tuple[str, str], dict[str, Any]] = {}

    def resolve(self, target: str, generation_id: str | None) -> dict | None:
        with self._lock:
            spec = self.targets.get(target)
            if spec is None or generation_id is None:
                return None
            return spec["generations"].get(generation_id)

    def report_blocked(self, event: dict, reason: str) -> None:
        with self._lock:
            key = (event["target"], event["id"])
            self.blocked[key] = {
                "event_id": event["id"],
                "request_id": event["request_id"],
                "target": event["target"],
                "generation_id": event["generation_id"],
                "seq": event.get("seq"),
                "reason": reason,
                "reported_at": time.time(),
            }

    def clear_blocked(self, target: str, event_id: str) -> None:
        with self._lock:
            self.blocked.pop((target, event_id), None)

    def blocked_list(self) -> list[dict]:
        with self._lock:
            return list(self.blocked.values())

    def remove_generation(self, target: str, generation_id: str) -> dict | None:
        with self._lock:
            spec = self.targets.get(target)
            if spec is None:
                return None
            return spec["generations"].pop(generation_id, None)

    def restore_generation(self, target: str, generation: dict) -> bool:
        with self._lock:
            spec = self.targets.get(target)
            if spec is None or generation["id"] in spec["generations"]:
                return False
            spec["generations"][generation["id"]] = generation
            return True


def deliver_once(
    url: str,
    secret: str,
    event: dict,
    timestamp: int,
) -> tuple[bool, int | None, str | None]:
    """One blocking HTTP attempt. Returns (ok, status_code, error)."""
    signature = sign(secret, timestamp, event["payload"])
    parsed = urlparse(url)
    conn_cls = http.client.HTTPSConnection if parsed.scheme == "https" \
        else http.client.HTTPConnection
    conn = conn_cls(parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80),
                    timeout=SOCKET_TIMEOUT)
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    headers = {
        "Content-Type": "application/json",
        "Content-Length": str(len(event["payload"])),
        "X-Event-Id": event["id"],
        "X-Request-Id": event["request_id"],
        "X-Generation-Id": event["generation_id"] or "",
        "X-Timestamp": str(timestamp),
        "X-Signature": signature,
    }
    try:
        conn.request("POST", path, body=event["payload"], headers=headers)
        try:
            resp = conn.getresponse()
            status = resp.status
            # drain (and discard) the response body so the connection can close
            resp.read()
        finally:
            conn.close()
        if 200 <= status < 300:
            return True, status, None
        return False, status, f"non-2xx response: {status}"
    except (socket.timeout, TimeoutError):
        return False, None, f"timeout after {SOCKET_TIMEOUT}s"
    except (OSError, http.client.HTTPException) as exc:
        return False, None, f"{type(exc).__name__}: {exc}"


class Dispatcher:
    def __init__(self, name: str, state: WorkerState,
                 storage: Storage, clock: Clock):
        self.name = name
        self.state = state
        self.storage = storage
        self.clock = clock

    def run(self) -> None:
        sys.stderr.write(f"[worker] dispatcher for target '{self.name}' up\n")
        while True:
            time.sleep(POLL_INTERVAL)
            try:
                self._tick()
            except Exception:
                sys.stderr.write(f"[worker] tick failed for {self.name}:\n")
                import traceback
                traceback.print_exc()

    def _tick(self) -> None:
        now = self.clock.now()
        # Peek the FIFO gate before creating an attempt row. If its bound
        # contract is absent from this process's deployment config, retain
        # the row unchanged and report the block.
        head = self.storage.peek_due_head(self.name, now)
        if head is None:
            return
        generation = self.state.resolve(self.name, head["generation_id"])
        if generation is None:
            self._block(head)
            return

        event = self.storage.claim_next(self.name, now, wall_now=time.time())
        if event is None:
            return

        generation = self.state.resolve(self.name, event["generation_id"])
        if generation is None:
            # Contract removal happened between peek and claim. No HTTP was
            # sent, so erase the tentative attempt and park the unchanged
            # head rather than fabricating a delivery failure.
            self.storage.cancel_fresh_claim(event["id"], event["attempt_no"], now)
            head = self.storage.peek_due_head(self.name, now)
            if head is not None:
                self._block(head)
            return

        self.state.clear_blocked(self.name, event["id"])
        ts = int(now)
        sys.stderr.write(
            f"[worker] -> {self.name} gen={event['generation_id']} "
            f"event={event['id'][:8]} attempt={event['attempt_no']}\n"
        )
        ok, status, error = deliver_once(
            generation["url"], generation["secret"], event, ts
        )
        if ok and event["id"] in _crash_after:
            # Deterministic crash *after* the target accepted the bytes
            # but *before* the attempt is recorded: the restarted worker
            # re-sends, producing a real duplicate while never losing the
            # event.
            _crash_after.discard(event["id"])
            sys.stderr.write(
                f"[worker] CRASH after delivering event={event['id'][:8]}\n"
            )
            sys.stderr.flush()
            os._exit(17)
        new_status = self.storage.finish_attempt(
            event["id"], event["attempt_no"], self.clock.now(),
            claim_now=now,
            ok=ok, status_code=status, error=error,
        )
        outcome = "delivered" if ok else new_status
        sys.stderr.write(
            f"[worker] <- {self.name} gen={event['generation_id']} "
            f"event={event['id'][:8]} attempt={event['attempt_no']} "
            f"-> {outcome} (status={status}, err={error})\n"
        )

    def _block(self, head) -> None:
        event = {
            "id": head["id"],
            "request_id": head["request_id"],
            "target": self.name,
            "generation_id": head["generation_id"],
            "seq": head["seq"],
        }
        previous = next(
            (b for b in self.state.blocked_list()
             if b["event_id"] == event["id"]),
            None,
        )
        self.state.report_blocked(event, "generation configuration missing")
        if previous is None:
            sys.stderr.write(
                f"[worker] blocked target={self.name} "
                f"event={event['id'][:8]} gen={event['generation_id']}: "
                "delivery contract missing; head retained with no attempt\n"
            )


def make_control_server(state: WorkerState, storage: Storage, clock: Clock):
    class Control(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass

        def _reply(self, code: int, obj) -> None:
            data = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _body(self) -> dict:
            length = int(self.headers.get("Content-Length", "0"))
            return json.loads(self.rfile.read(length) or b"{}")

        def do_GET(self):
            path = urlparse(self.path).path
            if path == "/healthz":
                self._reply(200, {"ok": True})
            elif path == "/control/blocked":
                self._reply(200, {"blocked": state.blocked_list()})
            else:
                self._reply(404, {"error": "not found"})

        def do_POST(self):
            path = urlparse(self.path).path
            try:
                if path == "/control/crash":
                    body = self._body()
                    event_id = body.get("event_id")
                    if not event_id:
                        self._reply(400, {"error": "event_id required"})
                        return
                    _crash_after.add(event_id)
                    self._reply(200, {"armed": event_id})
                    return
                if path == "/control/test/generations/remove":
                    body = self._body()
                    target = body.get("target")
                    generation_id = body.get("generation_id")
                    removed = state.remove_generation(target, generation_id)
                    if removed is None:
                        self._reply(404, {"error": "generation not configured"})
                    else:
                        self._reply(200, {"removed": removed})
                    return
                if path == "/control/test/generations/restore":
                    body = self._body()
                    generation = body.get("generation")
                    if not isinstance(generation, dict) or \
                            not generation.get("id") or \
                            not generation.get("url") or \
                            not generation.get("secret"):
                        self._reply(400, {"error": "generation required"})
                        return
                    ok = state.restore_generation(body["target"], generation)
                    if not ok:
                        self._reply(409, {"error": "target missing or generation exists"})
                    else:
                        self._reply(200, {"restored": generation["id"]})
                    return
            except (ValueError, TypeError):
                self._reply(400, {"error": "invalid JSON"})
                return
            self._reply(404, {"error": "not found"})

    class ControlServer(ThreadingHTTPServer):
        allow_reuse_address = True
        daemon_threads = True

    return ControlServer(
        ("0.0.0.0", int(os.environ.get("CONTROL_PORT", "9100"))), Control
    )


def main() -> None:
    db_path = os.environ["DB_PATH"]
    storage = Storage(db_path)
    clock = Clock(storage)
    targets = load_targets(os.environ.get("TARGET_CONFIG"))
    state = WorkerState(targets)

    # Recover anything the dead predecessor had in flight (at-least-once).
    recovered = storage.requeue_inflight(clock.now())
    if recovered:
        sys.stderr.write(f"[worker] requeued {recovered} in-flight event(s)\n")

    control = make_control_server(state, storage, clock)
    threading.Thread(target=control.serve_forever, daemon=True).start()

    threads = []
    for name in targets:
        t = threading.Thread(
            target=Dispatcher(name, state, storage, clock).run,
            name=f"dispatch-{name}", daemon=True,
        )
        t.start()
        threads.append(t)
    sys.stderr.write(f"[worker] up, dispatching {len(targets)} target(s)\n")
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
