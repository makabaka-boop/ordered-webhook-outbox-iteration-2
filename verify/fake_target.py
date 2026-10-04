"""Fake HTTP target(s) for verification.

One process impersonates every whitelisted target and every preconfigured
delivery generation. A legacy one-generation target receives deliveries at
``/t/<name>/receive`` and uses generation id ``default``. Multi-generation
targets use explicit paths::

    /t/<name>/generations/<generation>/receive
    /t/<name>/generations/<generation>/behavior
    /t/<name>/generations/<generation>/receipts

The worker may also post to the plain target path for multi-generation
targets; in that case the ``X-Generation-Id`` header selects the signing
secret. This lets a receipt prove both URL routing and the bound generation.

Per generation it supports scripted behavior:
    {"fail_first": N, "fail_status": 500}   first N requests fail, rest 200
    {"timeout_first": N, "sleep": 2.0}      first N requests sleep (the
                                            worker times out but the bytes
                                            were received and are recorded)
Every request -- including ones whose client gave up -- is recorded as a
receipt: headers, exact raw body and HMAC validity.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from config import DEFAULT_GENERATION_ID, load_targets
from signing import verify

_lock = threading.RLock()
# name -> {"receipts": [...], "behaviors": {generation: {...}}}
_state: dict[str, dict] = {}


def _bucket(name: str) -> dict:
    bucket = _state.setdefault(
        name,
        {
            "receipts": [],
            "behaviors": {},
        },
    )
    return bucket


def _behavior(name: str, generation_id: str) -> dict:
    with _lock:
        behaviors = _bucket(name)["behaviors"]
        return behaviors.setdefault(
            generation_id,
            {"fail_first": 0, "timeout_first": 0, "sleep": 0, "counter": 0},
        )


class Handler(BaseHTTPRequestHandler):
    server_version = "FakeTarget/1.0"

    def log_message(self, fmt, *args):
        sys.stderr.write("[fake] " + (fmt % args) + "\n")

    def _reply(self, code: int, obj=None, body: bytes | None = None) -> bool:
        """Write a response; returns False if the client was already gone."""
        if body is None:
            body = json.dumps(obj or {}).encode()
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return True
        except (BrokenPipeError, ConnectionResetError, OSError):
            return False  # client (worker) already timed out -- expected

    def _parse_target_path(self, suffix: str):
        """Return (name, generation_id) for receipts/behavior paths."""
        path = urlparse(self.path).path
        marker = "/generations/"
        if path.startswith("/t/") and marker in path and \
                path.endswith("/" + suffix):
            middle = path[len("/t/"): -len("/" + suffix)]
            name, gen_marker, generation_id = middle.partition(marker)
            if gen_marker and name in TARGETS and \
                    generation_id in TARGETS[name]["generations"]:
                return name, generation_id
        if path.startswith("/t/") and path.endswith("/" + suffix):
            name = path[len("/t/"): -len("/" + suffix)]
            if name in TARGETS:
                return name, DEFAULT_GENERATION_ID
        return None, None

    # --------------------------------------------------------------- GET

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/healthz":
            self._reply(200, {"ok": True, "targets": list(_state)})
            return
        name, generation_id = self._parse_target_path("receipts")
        if name:
            with _lock:
                receipts = [
                    dict(r) for r in _bucket(name)["receipts"]
                    if r["generation_id"] == generation_id
                ]
            self._reply(200, {"receipts": receipts})
            return
        self._reply(404, {"error": "not found"})

    def do_DELETE(self):
        name, generation_id = self._parse_target_path("receipts")
        if name:
            with _lock:
                bucket = _bucket(name)
                bucket["receipts"] = [
                    r for r in bucket["receipts"]
                    if r["generation_id"] != generation_id
                ]
                bucket["behaviors"][generation_id] = {
                    "fail_first": 0,
                    "timeout_first": 0,
                    "sleep": 0,
                    "counter": 0,
                }
            self._reply(200, {"cleared": name, "generation_id": generation_id})
            return
        self._reply(404, {"error": "not found"})

    # -------------------------------------------------------------- POST

    def do_POST(self):
        path = urlparse(self.path).path
        if path.endswith("/behavior"):
            name, generation_id = self._parse_target_path("behavior")
            if not name:
                self._reply(404, {"error": "unknown target or generation"})
                return
            length = int(self.headers.get("Content-Length", "0"))
            behavior = json.loads(self.rfile.read(length) or b"{}")
            behavior.setdefault("fail_first", 0)
            behavior.setdefault("timeout_first", 0)
            behavior.setdefault("sleep", 0)
            with _lock:
                current = _behavior(name, generation_id)
                current.clear()
                current.update(behavior)
                current["counter"] = 0
            response = dict(behavior)
            response["counter"] = 0
            self._reply(200, {"behavior": response,
                              "generation_id": generation_id})
            return

        name, path_generation = self._parse_receive_path(path)
        if not name:
            self._reply(404, {"error": "not found"})
            return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b""
        ts = self.headers.get("X-Timestamp", "")
        sig = self.headers.get("X-Signature", "")
        event_id = self.headers.get("X-Event-Id", "")
        request_id = self.headers.get("X-Request-Id", "")
        header_generation = self.headers.get("X-Generation-Id", "")
        generation_id = path_generation or header_generation
        generation = TARGETS[name]["generations"].get(generation_id)
        if generation is None:
            self._reply(404, {"error": "unknown generation"})
            return
        sig_ok = verify(generation["secret"], int(ts or "0"), raw, sig)

        with _lock:
            behavior_state = _behavior(name, generation_id)
            behavior = dict(behavior_state)
            n = behavior_state["counter"]
            behavior_state["counter"] = n + 1

        sleep_for = 0.0
        if n < behavior.get("timeout_first", 0):
            sleep_for = float(behavior.get("sleep", 2.0))
        fail = n >= behavior.get("succeed_first", 0) and \
            n - behavior.get("succeed_first", 0) < \
            behavior.get("fail_first", 0)
        status = int(behavior.get("fail_status", 500)) if fail else 200

        # Record the receipt *before* sleeping: even when the worker gives
        # up, the target demonstrably received the signed bytes.
        receipt = {
            "seq": n + 1,
            "received_at": time.time(),
            "target": name,
            "generation_id": generation_id,
            "url_generation_id": path_generation,
            "event_id": event_id,
            "request_id": request_id,
            "timestamp": ts,
            "signature": sig,
            "sig_valid": sig_ok,
            "body_b64": base64.b64encode(raw).decode("ascii"),
            "responded": None,
        }
        with _lock:
            _bucket(name)["receipts"].append(receipt)

        if not sig_ok:
            receipt["responded"] = 401
            self._reply(401, {"error": "bad signature"})
            return

        if sleep_for:
            time.sleep(sleep_for)
        receipt["responded"] = status
        if not self._reply(status, {"ok": status == 200, "n": n + 1}):
            # client (worker) already timed out -- bytes were still received
            # and the signed receipt stands; the broken write is recorded.
            receipt["responded"] = "client_gone"

    def _parse_receive_path(self, path: str) -> tuple[str | None, str | None]:
        marker = "/generations/"
        if path.startswith("/t/") and marker in path and \
                path.endswith("/receive"):
            middle = path[len("/t/"): -len("/receive")]
            name, _, generation_id = middle.partition(marker)
            if name in TARGETS and \
                    generation_id in TARGETS[name]["generations"]:
                return name, generation_id
        if path.startswith("/t/") and path.endswith("/receive"):
            name = path[len("/t/"): -len("/receive")]
            if name in TARGETS:
                return name, DEFAULT_GENERATION_ID
        return None, None


TARGETS = load_targets(os.environ.get("TARGET_CONFIG"))


def main() -> None:
    port = int(os.environ.get("FAKE_PORT", "8090"))
    for name in TARGETS:
        _bucket(name)
    httpd = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    sys.stderr.write(
        f"[fake] listening on :{port}, targets={list(TARGETS)}\n"
    )
    httpd.serve_forever()


if __name__ == "__main__":
    main()
