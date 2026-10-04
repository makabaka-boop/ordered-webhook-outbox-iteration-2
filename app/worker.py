"""Independent delivery worker.

Runs in its own container/process and talks to the same SQLite outbox as
the API. Concurrency model:

* one dispatcher thread **per configured target** -> different targets
  never block each other;
* within a target the worker only ever holds the *head* active event. A
  row is claimed in a ``BEGIN IMMEDIATE`` transaction only when no earlier
  row for that target is scheduled-due or in flight, so event N+1 is
  never delivered while event N is still retrying;
* failure policy: a non-2xx response, timeout or connection error counts
  as one failed attempt. Attempts 1-3 are retried after 1, 2, 4 seconds
  (logical, controllable clock). A 4th failure moves the event to the
  dead-letter table-state (status='dead'), which unblocks the next event;
* crash semantics: an in-flight claim that is never finished (process
  killed) is requeued at the next worker startup and sent again. Duplicate
  delivery is therefore allowed and documented -- guarantee is
  at-least-once, never "exactly once";
* DLQ replay preserves identity (same event/request id, same body, same
  seq) and the full attempt history; retry attempts continue numbering
  from 5 under a fresh 4-attempt budget.

A small control HTTP server (CONTROL_PORT) exists for tests:
    POST /control/crash  {"event_id": "..."}
        the *next successful* delivery of that event os._exit()s the
        process after the target received the bytes (duplicate-on-restart
        simulation).
    GET  /healthz
"""

from __future__ import annotations

import http.client
import json
import os
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from config import Clock, load_targets
from signing import sign
from storage import Storage

POLL_INTERVAL = 0.03  # seconds of real time between claim polls
SOCKET_TIMEOUT = float(os.environ.get("DELIVERY_TIMEOUT", "1.0"))

# event ids that crash the process after a successful send (in-memory only,
# deliberately not persisted -- it models a real crash, not a durable flag)
_crash_after: set[str] = set()


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
    def __init__(self, name: str, spec: dict, storage: Storage, clock: Clock):
        self.name = name
        self.url = spec["url"]
        self.secret = spec["secret"]
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
        event = self.storage.claim_next(self.name, now, wall_now=time.time())
        if event is None:
            return
        ts = int(now)
        sys.stderr.write(
            f"[worker] -> {self.name} event={event['id'][:8]} "
            f"attempt={event['attempt_no']}\n"
        )
        ok, status, error = deliver_once(self.url, self.secret, event, ts)
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
            f"[worker] <- {self.name} event={event['id'][:8]} "
            f"attempt={event['attempt_no']} -> {outcome} "
            f"(status={status}, err={error})\n"
        )


def make_control_server(storage: Storage, clock: Clock):
    class Control(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass

        def _reply(self, code: int, obj) -> None:
            data = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if urlparse(self.path).path == "/healthz":
                self._reply(200, {"ok": True})
            else:
                self._reply(404, {"error": "not found"})

        def do_POST(self):
            if urlparse(self.path).path != "/control/crash":
                self._reply(404, {"error": "not found"})
                return
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length) or b"{}")
            event_id = body.get("event_id")
            if not event_id:
                self._reply(400, {"error": "event_id required"})
                return
            _crash_after.add(event_id)
            self._reply(200, {"armed": event_id})

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

    # Recover anything the dead predecessor had in flight (at-least-once).
    recovered = storage.requeue_inflight(clock.now())
    if recovered:
        sys.stderr.write(f"[worker] requeued {recovered} in-flight event(s)\n")

    control = make_control_server(storage, clock)
    threading.Thread(target=control.serve_forever, daemon=True).start()

    threads = []
    for name, spec in targets.items():
        t = threading.Thread(
            target=Dispatcher(name, spec, storage, clock).run,
            name=f"dispatch-{name}", daemon=True,
        )
        t.start()
        threads.append(t)
    sys.stderr.write(f"[worker] up, dispatching {len(targets)} target(s)\n")
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
