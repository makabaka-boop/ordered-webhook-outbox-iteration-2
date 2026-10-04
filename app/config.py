"""Deploy-time configuration and the controllable clock.

Targets (URL + per-target signing secret) are configured **at deployment
time** through the ``TARGET_CONFIG`` environment variable.  A target with one
generation uses the compact historical form::

    TARGET_CONFIG={"billing":"http://billing:8080|s3crEt-billing"}

A target that is being migrated can preconfigure several delivery
generations.  Only the initial/current generation is used until an admin
activates another one::

    TARGET_CONFIG={
      "billing": {
        "initial_generation_id": "v1",
        "generations": [
          {"id": "v1", "url": "http://billing-old:8080|ignored",
           "secret": "old-secret"},
          {"id": "v2", "url": "http://billing-new:8080",
           "secret": "new-secret"}
        ]
      }
    }

Ingestion accepts a target name only if it is a key of this whitelist; the
URL and secret are never taken from request data.  The current generation id
is persisted, while URLs and secrets remain deployment configuration.

The clock is real by default. Tests run in *virtual* mode: ``now()`` reads
an integer-second epoch stored in the database, advanced explicitly via
``POST /admin/clock``. Retry scheduling (1/2/4 s) is therefore
instantaneously testable while network socket timeouts remain wall-clock
based (a timeout is a real OS condition and must be tested honestly).
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Optional

DEFAULT_GENERATION_ID = "default"


def load_targets(raw: Optional[str]) -> dict[str, dict[str, Any]]:
    """Parse deployment target configuration.

    The normalized shape is::

        {name: {
          "initial_generation_id": "v1",
          "generations": {
              "v1": {"id": "v1", "url": "...", "secret": "..."}
          }
        }}
    """
    if not raw:
        return {}
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("TARGET_CONFIG must be a JSON object")

    targets: dict[str, dict[str, Any]] = {}
    for name, spec in data.items():
        if not name:
            raise ValueError("target names must be non-empty")
        if isinstance(spec, str):
            url, secret = _split_legacy_spec(name, spec)
            generation = {
                "id": DEFAULT_GENERATION_ID,
                "url": url,
                "secret": secret,
            }
            targets[name] = {
                "initial_generation_id": DEFAULT_GENERATION_ID,
                "generations": {DEFAULT_GENERATION_ID: generation},
            }
        elif isinstance(spec, dict):
            targets[name] = _load_generation_target(name, spec)
        else:
            raise ValueError(f"TARGET_CONFIG[{name}] must be a string or object")
    return targets


def _split_legacy_spec(name: str, spec: str) -> tuple[str, str]:
    if "|" not in spec:
        raise ValueError(f"TARGET_CONFIG[{name}] must be 'URL|secret'")
    url, secret = spec.split("|", 1)
    if not url or not secret:
        raise ValueError(f"TARGET_CONFIG[{name}] url and secret required")
    return url, secret


def _load_generation_target(
    name: str, spec: dict[str, Any]
) -> dict[str, Any]:
    raw_generations = spec.get("generations")
    if isinstance(raw_generations, dict):
        items = raw_generations.items()
    elif isinstance(raw_generations, list):
        items = ((g.get("id"), g) for g in raw_generations)
    else:
        raise ValueError(
            f"TARGET_CONFIG[{name}].generations must be a list or object"
        )

    generations: dict[str, dict[str, str]] = {}
    configured_order: list[str] = []
    for generation_id, generation in items:
        if not isinstance(generation, dict):
            raise ValueError(f"TARGET_CONFIG[{name}] generation must be an object")
        generation_id = str(generation_id or generation.get("id") or "").strip()
        if not generation_id:
            raise ValueError(f"TARGET_CONFIG[{name}] generation id required")
        if generation_id in generations:
            raise ValueError(
                f"TARGET_CONFIG[{name}] duplicate generation {generation_id}"
            )
        url = generation.get("url", "")
        secret = generation.get("secret", "")
        if not url or not secret:
            raise ValueError(
                f"TARGET_CONFIG[{name}][{generation_id}] url and secret required"
            )
        generations[generation_id] = {
            "id": generation_id,
            "url": url,
            "secret": secret,
        }
        configured_order.append(generation_id)

    if not generations:
        raise ValueError(f"TARGET_CONFIG[{name}] requires at least one generation")

    initial = str(
        spec.get("initial_generation_id")
        or spec.get("active_generation_id")
        or spec.get("current_generation_id")
        or configured_order[0]
    )
    if initial not in generations:
        raise ValueError(
            f"TARGET_CONFIG[{name}] initial generation {initial!r} is not configured"
        )
    return {
        "initial_generation_id": initial,
        # Preserve configuration order for admin responses.
        "generation_order": configured_order,
        "generations": generations,
    }


def generation_ids(spec: dict[str, Any]) -> set[str]:
    return set(spec["generations"])


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
