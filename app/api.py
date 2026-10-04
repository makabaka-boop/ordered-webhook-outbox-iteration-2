"""Event ingress HTTP API (+ small admin surface).

Public API
----------
POST /v1/events
    Headers: X-Target (whitelisted name), X-Request-Id (idempotency key),
             Content-Type: application/json
    Body:    arbitrary raw JSON bytes (the exact bytes are stored and later
             signed/delivered -- never re-serialized)
    The current target generation id is fixed in the same transaction as
    request-id de-duplication and insertion. 202 accepted; 400 malformed;
    404 target not whitelisted; 409 same request id reused with a different
    body; 503 current generation contract is temporarily unavailable.
GET  /v1/events/{id}
GET  /v1/events/{id}/attempts

Admin (internal network only)
-----------------------------
GET/POST /admin/clock                       virtual clock control for tests
GET      /admin/events                      list events
GET      /admin/targets                     whitelist + current generations
GET      /admin/targets/{name}
POST     /admin/targets/{name}/generations/{id}/activate
GET      /admin/events?status=dead
POST     /admin/events/{id}/replay
GET      /healthz
"""

from __future__ import annotations

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from config import Clock, load_targets
from storage import Storage

MAX_BODY = 1 << 20  # 1 MiB


def make_handler(targets: dict, clock: Clock, storage: Storage):
    class Handler(BaseHTTPRequestHandler):
        server_version = "OutboxIngress/1.0"

        def log_message(self, fmt, *args):
            sys.stderr.write("[api] %s - %s\n" % (self.address_string(), fmt % args))

        # ------------------------------------------------------------ utils

        def _json(self, code: int, obj) -> None:
            data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _read_body(self, allow_empty: bool = False) -> bytes | None:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self._json(400, {"error": "invalid Content-Length"})
                return None
            if length <= 0:
                if allow_empty:
                    return b""
                self._json(400, {"error": "empty body"})
                return None
            if length > MAX_BODY:
                self._json(413, {"error": "body too large"})
                return None
            return self.rfile.read(length)

        # ------------------------------------------------------------- GET

        def do_GET(self):
            path = urlparse(self.path).path
            qs = urlparse(self.path).query
            try:
                if path == "/healthz":
                    self._json(200, {"ok": True, "clock": clock.state()})
                elif path == "/admin/clock":
                    self._json(200, clock.state())
                elif path == "/admin/events":
                    status = None
                    for part in qs.split("&"):
                        if part.startswith("status="):
                            status = part.split("=", 1)[1]
                    rows = storage.list_events()
                    if status:
                        rows = [r for r in rows if r["status"] == status]
                    self._json(200, [_event_dict(r) for r in rows])
                elif path == "/admin/targets":
                    self._json(200, [_target_dict(n, targets, storage)
                                     for n in targets])
                elif path.startswith("/admin/targets/"):
                    name = path[len("/admin/targets/"):]
                    if name in targets:
                        self._json(200, _target_dict(name, targets, storage))
                    else:
                        self._json(404, {"error": "unknown target"})
                elif path.startswith("/v1/events/"):
                    rest = path[len("/v1/events/"):]
                    if rest.endswith("/attempts"):
                        event_id = rest[: -len("/attempts")]
                        ev = storage.get_event(event_id)
                        if ev is None:
                            self._json(404, {"error": "unknown event"})
                            return
                        attempts = storage.list_attempts(event_id)
                        self._json(200, [dict(a) for a in attempts])
                    else:
                        ev = storage.get_event(rest)
                        if ev is None:
                            self._json(404, {"error": "unknown event"})
                            return
                        self._json(200, _event_dict(ev))
                else:
                    self._json(404, {"error": "not found"})
            except Exception as exc:  # never drop the connection
                self._json(500, {"error": str(exc)})

        # ------------------------------------------------------------ POST

        def do_POST(self):
            path = urlparse(self.path).path
            try:
                if path == "/v1/events":
                    self._ingest()
                elif path == "/admin/reset":
                    storage.reset_test_state()
                    storage.ensure_targets(targets, clock.now())
                    clock.invalidate()
                    self._json(200, {"reset": True})
                elif path == "/admin/clock":
                    self._set_clock()
                elif path == "/admin/clock/advance":
                    self._advance_clock()
                elif path.startswith("/admin/targets/") and \
                        "/generations/" in path and path.endswith("/activate"):
                    self._activate_generation(path)
                elif path.startswith("/admin/events/") and path.endswith("/replay"):
                    event_id = path[len("/admin/events/"): -len("/replay")]
                    row = storage.replay_dead(event_id, clock.now())
                    if row is None:
                        self._json(404, {"error": "event not in dead-letter"})
                    else:
                        self._json(200, _event_dict(row))
                else:
                    self._json(404, {"error": "not found"})
            except Exception as exc:
                self._json(500, {"error": str(exc)})

        def _ingest(self):
            target_name = self.headers.get("X-Target", "").strip()
            request_id = self.headers.get("X-Request-Id", "").strip()
            ctype = self.headers.get("Content-Type", "")
            if not target_name or not request_id:
                self._json(
                    400,
                    {"error": "X-Target and X-Request-Id headers are required"},
                )
                return
            if target_name not in targets:
                self._json(
                    404,
                    {
                        "error": "target is not in the deployment whitelist",
                        "target": target_name,
                    },
                )
                return
            if "application/json" not in ctype.lower():
                self._json(400, {"error": "Content-Type must be application/json"})
                return
            body = self._read_body()
            if body is None:
                return
            try:
                json.loads(body)
            except (ValueError, UnicodeDecodeError):
                self._json(400, {"error": "body must be valid JSON"})
                return

            result = storage.insert_event(
                request_id,
                target_name,
                body,
                clock.now(),
                set(targets[target_name]["generations"]),
            )
            if result["kind"] == "duplicate":
                self._json(
                    202,
                    {
                        "event_id": result["event_id"],
                        "status": result["status"],
                        "generation_id": result["generation_id"],
                        "duplicate": True,
                    },
                )
                return
            if result["kind"] == "conflict":
                self._json(
                    409,
                    {
                        "error": "X-Request-Id already used with a different body",
                        "event_id": result["event_id"],
                    },
                )
                return
            if result["kind"] == "missing_generation" or \
                    result["generation_id"] not in \
                    targets[target_name]["generations"]:
                self._json(
                    503,
                    {
                        "error": "current delivery generation is not configured",
                        "target": target_name,
                        "generation_id": result.get("generation_id"),
                    },
                )
                return
            self._json(
                202,
                {
                    "event_id": result["event_id"],
                    "status": result["status"],
                    "generation_id": result["generation_id"],
                },
            )

        def _activate_generation(self, path: str) -> None:
            middle = path[len("/admin/targets/"): -len("/activate")]
            target_name, marker, generation_id = middle.partition("/generations/")
            if not marker or not target_name or not generation_id:
                self._json(404, {"error": "not found"})
                return
            if target_name not in targets:
                self._json(404, {"error": "unknown target"})
                return
            if generation_id not in targets[target_name]["generations"]:
                self._json(
                    404,
                    {
                        "error": "generation is not preconfigured",
                        "target": target_name,
                        "generation_id": generation_id,
                    },
                )
                return

            body = self._read_body(allow_empty=True)
            if body is None:
                return
            try:
                req = json.loads(body or b"{}")
            except ValueError:
                self._json(400, {"error": "invalid JSON"})
                return
            expected = req.get("expected_current_generation_id")
            if expected is None:
                expected = req.get("expected_generation_id")
            if not isinstance(expected, str) or not expected:
                self._json(
                    400,
                    {
                        "error": "expected_current_generation_id is required "
                        "and must be a non-empty string"
                    },
                )
                return

            row, conflict = storage.activate_generation(
                target_name, generation_id, expected, clock.now()
            )
            if conflict is not None:
                self._json(
                    409,
                    {
                        "error": "current generation differs from expected",
                        "target": target_name,
                        "expected": expected,
                        "current": conflict,
                    },
                )
                return
            self._json(200, _target_dict(target_name, targets, storage))

        def _set_clock(self):
            body = self._read_body()
            if body is None:
                return
            try:
                req = json.loads(body)
            except ValueError:
                self._json(400, {"error": "invalid JSON"})
                return
            mode = req.get("mode", "real")
            if mode not in ("real", "virtual"):
                self._json(400, {"error": "mode must be real|virtual"})
                return
            vn = req.get("virtual_now")
            state = clock.set(mode, float(vn) if vn is not None else None)
            self._json(200, state)

        def _advance_clock(self):
            body = self._read_body()
            if body is None:
                return
            try:
                req = json.loads(body)
                delta = float(req.get("delta", 0))
            except (ValueError, TypeError):
                self._json(400, {"error": "invalid delta"})
                return
            state = storage.advance_clock(delta)
            clock.invalidate()
            self._json(200, state)

    return Handler


def _target_dict(name: str, targets: dict, storage: Storage) -> dict:
    target = targets[name]
    row = storage.get_target_state(name)
    current = row["generation_id"] if row else target["initial_generation_id"]
    order = target.get("generation_order") or list(target["generations"])
    return {
        "target": name,
        "current_generation_id": current,
        "generations": [
            {
                "id": generation_id,
                "url": target["generations"][generation_id]["url"],
                "current": generation_id == current,
            }
            for generation_id in order
        ],
    }


def _event_dict(row) -> dict:
    return {
        "event_id": row["id"],
        "request_id": row["request_id"],
        "target": row["target"],
        "generation_id": row["generation_id"],
        "status": row["status"],
        "seq": row["seq"],
        "attempts": row["attempts"],
        "max_attempts": row["max_attempts"],
        "created_at": row["created_at"],
        "not_before": row["not_before"],
        "delivered_at": row["delivered_at"],
        "dead_at": row["dead_at"],
        "replayed_count": row["replayed_count"],
    }


def main():
    db_path = os.environ["DB_PATH"]
    targets = load_targets(os.environ.get("TARGET_CONFIG"))
    storage = Storage(db_path)
    clock = Clock(storage)
    # Seed current generation state before accepting traffic. Existing rows
    # are preserved; this is also the single-generation backward-compat path.
    storage.ensure_targets(targets, clock.now())
    port = int(os.environ.get("API_PORT", "8080"))
    httpd = ThreadingHTTPServer(
        ("0.0.0.0", port), make_handler(targets, clock, storage)
    )
    sys.stderr.write(
        f"[api] listening on :{port}, targets={list(targets)}\n"
    )
    httpd.serve_forever()


if __name__ == "__main__":
    main()
