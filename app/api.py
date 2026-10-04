"""Event ingress HTTP API (+ small admin surface).

Public API
----------
POST /v1/events
    Headers: X-Target (whitelisted name), X-Request-Id (idempotency key),
             Content-Type: application/json
    Body:    arbitrary raw JSON bytes (the exact bytes are stored and later
             signed/delivered -- never re-serialized)
    202 accepted; 400 malformed; 404 target not whitelisted; 409 same
    request id reused with a different body.
    The stored event pins the target's *current delivery generation*
    (``generation_id`` in the response) inside the same transaction as the
    request-id dedup check and the insert.
GET  /v1/events/{id}
GET  /v1/events/{id}/attempts

Admin (internal network only)
-----------------------------
GET/POST /admin/clock            virtual clock control for tests
GET      /admin/events           list events (?status=dead supported)
POST     /admin/events/{id}/replay
GET      /admin/targets          whitelist + configured/active generations
GET      /admin/targets/{name}   (secrets are never exposed)
POST     /admin/targets/{name}/generations/activate
         {"generation": "g2", "expected_current": "g1"}
         Compare-and-swap activation of a *pre-configured* generation:
         200 on success, 404 unknown target/generation, 409 when the
         persisted current generation differs from ``expected_current``.
         Activation -- successful or failed -- never rewrites stored
         events; only events ingested afterwards pin the new generation.
GET      /healthz
"""

from __future__ import annotations

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from config import Clock, TargetConfigProvider
from storage import Storage

MAX_BODY = 1 << 20  # 1 MiB


def make_handler(provider: TargetConfigProvider, clock: Clock, storage: Storage):
    class Handler(BaseHTTPRequestHandler):
        server_version = "OutboxIngress/1.1"

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

        def _read_body(self) -> bytes | None:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self._json(400, {"error": "invalid Content-Length"})
                return None
            if length <= 0:
                self._json(400, {"error": "empty body"})
                return None
            if length > MAX_BODY:
                self._json(413, {"error": "body too large"})
                return None
            return self.rfile.read(length)

        def _target_view(self, name: str) -> dict:
            return {
                "target": name,
                "generations": provider.configured_generations(name),
                "initial": provider.default_generation(name),
                "current": storage.get_current_generation(
                    name, provider.default_generation(name), clock.now()
                ),
            }

        # ------------------------------------------------------------- GET

        def do_GET(self):
            path = urlparse(self.path).path
            qs = urlparse(self.path).query
            try:
                if path == "/healthz":
                    self._json(200, {"ok": True, "clock": clock.state()})
                elif path == "/admin/clock":
                    self._json(200, clock.state())
                elif path == "/admin/targets":
                    self._json(
                        200,
                        [self._target_view(n) for n in provider.targets()],
                    )
                elif path.startswith("/admin/targets/"):
                    name = path[len("/admin/targets/"):]
                    if "/" in name or not provider.has_target(name):
                        self._json(404, {"error": "unknown target"})
                    else:
                        self._json(200, self._target_view(name))
                elif path == "/admin/events":
                    status = None
                    for part in qs.split("&"):
                        if part.startswith("status="):
                            status = part.split("=", 1)[1]
                    rows = storage.list_events()
                    if status:
                        rows = [r for r in rows if r["status"] == status]
                    self._json(200, [_event_dict(r) for r in rows])
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
                    clock.invalidate()
                    self._json(200, {"reset": True})
                elif path == "/admin/clock":
                    self._set_clock()
                elif path == "/admin/clock/advance":
                    self._advance_clock()
                elif path.startswith("/admin/targets/") and path.endswith(
                    "/generations/activate"
                ):
                    name = path[
                        len("/admin/targets/"): -len("/generations/activate")
                    ]
                    self._activate_generation(name)
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
            if not provider.has_target(target_name):
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

            # Dedup + generation pin + insert are one transaction: a
            # repeated request id returns the original row (with its
            # original pinned generation, even if a newer generation has
            # been activated since); a collision with a different body is
            # a client error.
            row, is_new = storage.ingest_event(
                request_id, target_name, body, clock.now(),
                provider.default_generation(target_name),
            )
            if not is_new:
                if bytes(row["payload"]) != body:
                    self._json(
                        409,
                        {
                            "error": "X-Request-Id already used with a "
                            "different body",
                            "event_id": row["id"],
                        },
                    )
                    return
                self._json(
                    202,
                    {
                        "event_id": row["id"],
                        "status": row["status"],
                        "generation_id": row["generation_id"],
                        "duplicate": True,
                    },
                )
                return
            self._json(
                202,
                {
                    "event_id": row["id"],
                    "status": row["status"],
                    "generation_id": row["generation_id"],
                },
            )

        def _activate_generation(self, name: str):
            if not provider.has_target(name):
                self._json(404, {"error": "unknown target", "target": name})
                return
            body = self._read_body()
            if body is None:
                return
            try:
                req = json.loads(body)
            except ValueError:
                self._json(400, {"error": "invalid JSON"})
                return
            generation = req.get("generation")
            expected = req.get("expected_current")
            if not isinstance(generation, str) or not generation \
                    or not isinstance(expected, str) or not expected:
                self._json(
                    400,
                    {"error": "generation and expected_current are required"},
                )
                return
            if generation not in provider.configured_generations(name):
                self._json(
                    404,
                    {
                        "error": "generation is not pre-configured for "
                        "this target",
                        "target": name,
                        "generation": generation,
                        "configured": provider.configured_generations(name),
                    },
                )
                return
            ok, current = storage.activate_generation(
                name, expected, generation,
                provider.default_generation(name), clock.now(),
            )
            if not ok:
                # CAS failure: nothing was written; stored events and the
                # persisted current generation are untouched.
                self._json(
                    409,
                    {
                        "error": "current generation does not match "
                        "expected_current",
                        "target": name,
                        "current": current,
                    },
                )
                return
            self._json(
                200,
                {"target": name, "previous": expected, "current": generation},
            )

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


def _event_dict(row) -> dict:
    return {
        "event_id": row["id"],
        "request_id": row["request_id"],
        "target": row["target"],
        "status": row["status"],
        "seq": row["seq"],
        "generation_id": row["generation_id"],
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
    provider = TargetConfigProvider.from_env()
    storage = Storage(db_path)
    clock = Clock(storage)
    port = int(os.environ.get("API_PORT", "8080"))
    httpd = ThreadingHTTPServer(("0.0.0.0", port), make_handler(provider, clock, storage))
    sys.stderr.write(f"[api] listening on :{port}, targets={provider.targets()}\n")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
