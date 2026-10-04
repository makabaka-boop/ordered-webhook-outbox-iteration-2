"""Deploy-time configuration and the controllable clock.

Targets (URL + per-target signing secret) are configured **at deployment
time** through the ``TARGET_CONFIG`` environment variable, e.g.::

    TARGET_CONFIG={"billing":"http://billing:8080|s3crEt-billing", ...}

Ingestion accepts a target name only if it is a key of this whitelist; the
URL and secret are never taken from request data.

The clock is real by default. Tests run in *virtual* mode: ``now()`` reads
an integer-second epoch stored in the database, advanced explicitly via
``POST /admin/clock``. Retry scheduling (1/2/4 s) is therefore
instantaneously testable while network socket timeouts remain wall-clock
based (a timeout is a real OS condition and must be tested honestly).
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Optional


def load_targets(raw: Optional[str]) -> dict[str, dict[str, str]]:
    """Parse ``{"name": "URL|secret"}`` into ``{name: {url, secret}}``."""
    if not raw:
        return {}
    data = json.loads(raw)
    targets: dict[str, dict[str, str]] = {}
    for name, spec in data.items():
        if "|" not in spec:
            raise ValueError(
                f"TARGET_CONFIG[{name}] must be 'URL|secret'"
            )
        url, secret = spec.split("|", 1)
        if not url or not secret:
            raise ValueError(f"TARGET_CONFIG[{name}] url and secret required")
        targets[name] = {"url": url, "secret": secret}
    return targets


class Clock:
    """now() source shared by API and worker, cached briefly to spare SQLite."""

    def __init__(self, storage):
        self._storage = storage
        self._cache: dict | None = None
        self._cached_at = 0.0
        self._lock = threading.Lock()

    def now(self) -> float:
        with self._lock:
            wall = time.time()
            if wall - self._cached_at > 0.005 or self._cache is None:
                self._cache = self._storage.get_clock()
                self._cached_at = wall
            if self._cache.get("mode") == "virtual":
                return float(self._cache["virtual_now"])
            return wall

    def state(self) -> dict:
        self.invalidate()
        return self._storage.get_clock()

    def invalidate(self) -> None:
        with self._lock:
            self._cache = None
            self._cached_at = 0.0

    def set(self, mode: str, virtual_now: Optional[float] = None) -> dict:
        state = self._storage.set_clock(mode, virtual_now)
        with self._lock:
            self._cache = state
            self._cached_at = time.time()
        return state
