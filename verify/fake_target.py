"""Fake HTTP target(s) for verification.

One process impersonates every receive endpoint of every whitelisted
target -- including every *delivery generation* of multi-generation
targets. A generation with URL ``http://fake-target:8090/t/<ep>/receive``
is served as endpoint ``<ep>`` with that generation's own signing secret,
so "old endpoint / new endpoint" migrations are exercised as two fully
independent fake endpoints with independent receipt logs.

Per endpoint it supports scripted behavior:
    {"fail_first": N, "fail_status": 500}   first N requests fail, rest 200
    {"timeout_first": N, "sleep": 2.0}      first N requests sleep (the
                                            worker times out but the bytes
                                            were received and are recorded)
Every request -- including ones whose client gave up -- is recorded as a
receipt: headers, exact raw body and HMAC validity (checked against the
endpoint's own generation secret). Admin API:
    GET    /t/<ep>/receipts
    DELETE /t/<ep>/receipts
    POST   /t/<ep>/behavior
    GET    /healthz
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

from config import parse_targets
from signing import verify

_lock = threading.Lock()
# endpoint -> {"receipts": [...], "behavior": {...}, "counter": int}
_state: dict[str, dict] = {}


def _bucket(name: str) -> dict:
    return _state.setdefault(
        name, {"receipts": [], "behavior": {"fail_first": 0}, "counter": 0}
    )


def endpoint_secrets(parsed_targets: dict) -> dict[str, str]:
    """Map receive-endpoint name -> signing secret.

    Walks every generation of every configured target and derives the
    endpoint name from the URL path convention ``/t/<endpoint>/receive``.
    """
    endpoints: dict[str, str] = {}
    for tname, tspec in parsed_targets.items():
        for gid, gen in tspec["generations"].items():
            path = urlparse(gen["url"]).path
            if not (path.startswith("/t/") and path.endswith("/receive")):
                raise ValueError(
                    f"target {tname} generation {gid}: URL path {path!r} "
                    "does not match /t/<endpoint>/receive"
                )
            endpoint = path[len("/t/"): -len("/receive")]
            endpoints[endpoint] = gen["secret"]
    return endpoints


class Handler(BaseHTTPRequestHandler):
    server_version = "FakeTarget/1.1"

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

    # --------------------------------------------------------------- GET

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/healthz":
            self._reply(200, {"ok": True, "endpoints": list(_state)})
            return
        prefix, ok = self._endpoint_path(path, "receipts")
        if ok:
            with _lock:
                self._reply(200, {"receipts": list(_bucket(prefix)["receipts"])})
            return
        self._reply(404, {"error": "not found"})

    def do_DELETE(self):
        path = urlparse(self.path).path
        prefix, ok = self._endpoint_path(path, "receipts")
        if ok:
            with _lock:
                _bucket(prefix)["receipts"] = []
                _bucket(prefix)["counter"] = 0
            self._reply(200, {"cleared": prefix})
            return
        self._reply(404, {"error": "not found"})

    # -------------------------------------------------------------- POST

    def do_POST(self):
        path = urlparse(self.path).path
        if path.endswith("/behavior"):
            prefix = path[len("/t/"): -len("/behavior")]
            if prefix not in ENDPOINTS:
                self._reply(404, {"error": "unknown endpoint"})
                return
            length = int(self.headers.get("Content-Length", "0"))
            behavior = json.loads(self.rfile.read(length) or b"{}")
            behavior.setdefault("fail_first", 0)
            with _lock:
                bucket = _bucket(prefix)
                bucket["behavior"] = behavior
                bucket["counter"] = 0
            self._reply(200, {"behavior": behavior})
            return

        if not path.startswith("/t/") or not path.endswith("/receive"):
            self._reply(404, {"error": "not found"})
            return
        name = path[len("/t/"): -len("/receive")]
        if name not in ENDPOINTS:
            self._reply(404, {"error": "unknown endpoint"})
            return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b""
        ts = self.headers.get("X-Timestamp", "")
        sig = self.headers.get("X-Signature", "")
        event_id = self.headers.get("X-Event-Id", "")
        request_id = self.headers.get("X-Request-Id", "")
        sig_ok = verify(ENDPOINTS[name], int(ts or "0"), raw, sig)

        with _lock:
            bucket = _bucket(name)
            behavior = dict(bucket["behavior"])
            n = bucket["counter"]
            bucket["counter"] = n + 1

        sleep_for = 0.0
        if n < behavior.get("timeout_first", 0):
            sleep_for = float(behavior.get("sleep", 2.0))
        fail = n < behavior.get("fail_first", 0)
        status = int(behavior.get("fail_status", 500)) if fail else 200

        # Record the receipt *before* sleeping: even when the worker gives
        # up, the target demonstrably received the signed bytes.
        receipt = {
            "seq": n + 1,
            "received_at": time.time(),
            "event_id": event_id,
            "request_id": request_id,
            "timestamp": ts,
            "signature": sig,
            "sig_valid": sig_ok,
            "body_b64": base64.b64encode(raw).decode("ascii"),
            "responded": None,
        }
        with _lock:
            bucket["receipts"].append(receipt)

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

    def _endpoint_path(self, path: str, suffix: str) -> tuple[str, bool]:
        if path.startswith("/t/") and path.endswith("/" + suffix):
            name = path[len("/t/"): -len("/" + suffix)]
            if name in ENDPOINTS:
                return name, True
        return "", False


ENDPOINTS = endpoint_secrets(parse_targets(os.environ.get("TARGET_CONFIG")))


def main() -> None:
    port = int(os.environ.get("FAKE_PORT", "8090"))
    for name in ENDPOINTS:
        _bucket(name)
    httpd = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    sys.stderr.write(f"[fake] listening on :{port}, endpoints={list(ENDPOINTS)}\n")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
