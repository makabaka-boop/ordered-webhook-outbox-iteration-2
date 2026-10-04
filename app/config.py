"""Deploy-time configuration and the controllable clock.

Targets are configured **at deployment time** through the ``TARGET_CONFIG``
environment variable (or a ``TARGET_CONFIG_FILE`` JSON document). Two
formats are supported per target, freely mixed:

*Legacy single-generation* (unchanged, fully compatible)::

    TARGET_CONFIG={"billing": "http://billing:8080|s3crEt-billing", ...}

*Pre-configured delivery generations*::

    {"billing": {
        "initial": "gen-2025",
        "generations": {
            "gen-2025": {"url": "http://billing-old:8080/",
                         "secret": "old-key"},
            "gen-2026": {"url": "http://billing-new:9090/",
                         "secret": "new-key"}
        }}}

Both parse into the same contract::

    {name: {"initial": <gen_id>,
            "generations": {<gen_id>: {"url": ..., "secret": ...}}}}

A legacy entry becomes a single generation named ``legacy``. Generation ids
are deployment-chosen strings; the *currently active* generation per target
is persistent runtime state (see storage.target_generations), initialized to
``initial`` and changed only by the admin activation endpoint. Every stored
event pins the generation id that was current when it was ingested, and the
worker always delivers with the URL+secret of the *pinned* generation --
never the current one.

Ingestion accepts a target name only if it is in this whitelist; URLs and
secrets are never taken from request data.

The clock is real by default. Tests run in *virtual* mode: ``now()`` reads
an integer-second epoch stored in the database, advanced explicitly via
``POST /admin/clock``. Retry scheduling (1/2/4 s) is therefore
instantaneously testable while network socket timeouts remain wall-clock
based (a timeout is a real OS condition and must be tested honestly).
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from typing import Optional

# Generation id used for targets configured with the legacy "URL|secret"
# single-generation format.
LEGACY_GENERATION = "legacy"


def parse_targets(raw: Optional[str]) -> dict[str, dict]:
    """Parse TARGET_CONFIG JSON into the shared generation contract.

    Accepts legacy ``{"name": "URL|secret"}`` entries and generation-map
    entries ``{"name": {"initial": g, "generations": {g: {...}}}}`` in any
    mixture. Returns ``{name: {"initial": gid, "generations": {...}}}``.
    """
    if not raw:
        return {}
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("TARGET_CONFIG must be a JSON object")
    targets: dict[str, dict] = {}
    for name, spec in data.items():
        if isinstance(spec, str):
            # legacy: "URL|secret" -> one implicit generation
            if "|" not in spec:
                raise ValueError(
                    f"TARGET_CONFIG[{name}] must be 'URL|secret'"
                )
            url, secret = spec.split("|", 1)
            if not url or not secret:
                raise ValueError(
                    f"TARGET_CONFIG[{name}] url and secret required"
                )
            targets[name] = {
                "initial": LEGACY_GENERATION,
                "generations": {
                    LEGACY_GENERATION: {"url": url, "secret": secret}
                },
            }
        elif isinstance(spec, dict):
            gens = spec.get("generations")
            if not isinstance(gens, dict) or not gens:
                raise ValueError(
                    f"TARGET_CONFIG[{name}].generations must be a "
                    "non-empty object"
                )
            parsed_gens: dict[str, dict[str, str]] = {}
            for gid, g in gens.items():
                gid = str(gid)
                if not isinstance(g, dict) or not g.get("url") \
                        or not g.get("secret"):
                    raise ValueError(
                        f"TARGET_CONFIG[{name}].generations[{gid}] needs "
                        "url and secret"
                    )
                parsed_gens[gid] = {"url": str(g["url"]),
                                    "secret": str(g["secret"])}
            initial = spec.get("initial")
            if initial is None:
                if len(parsed_gens) == 1:
                    initial = next(iter(parsed_gens))
                else:
                    raise ValueError(
                        f"TARGET_CONFIG[{name}].initial is required when "
                        "more than one generation is configured"
                    )
            initial = str(initial)
            if initial not in parsed_gens:
                raise ValueError(
                    f"TARGET_CONFIG[{name}].initial={initial!r} is not a "
                    "configured generation"
                )
            targets[name] = {"initial": initial, "generations": parsed_gens}
        else:
            raise ValueError(
                f"TARGET_CONFIG[{name}] must be 'URL|secret' or a "
                "generations object"
            )
    return targets


class TargetConfigProvider:
    """The generation contract, shared by API, worker, fake target, verify.

    Sources (file wins when both are set):
      * ``TARGET_CONFIG``        -- static env JSON (default for api/fake)
      * ``TARGET_CONFIG_FILE``   -- path to a JSON document, re-read when
                                    its mtime/size changes (rolling config
                                    updates, e.g. a ConfigMap rollout)

    A file that is missing or momentarily invalid is *not* fatal: the last
    good config is kept, so a half-written rollout never empties the
    whitelist. Resolution is deliberately tolerant of gaps: ``resolve()``
    returns ``None`` for an unknown (target, generation) pair and the caller
    decides how to cope (the worker parks the head event and reports the
    blockage instead of failing the delivery).
    """

    def __init__(self, static_raw: Optional[str] = None,
                 file_path: Optional[str] = None):
        self._static_raw = static_raw
        self._file_path = file_path
        self._lock = threading.Lock()
        self._parsed: dict[str, dict] = {}
        self._file_sig: Optional[tuple] = None
        self.reload(force=True)

    @classmethod
    def from_env(cls) -> "TargetConfigProvider":
        return cls(
            static_raw=os.environ.get("TARGET_CONFIG"),
            file_path=os.environ.get("TARGET_CONFIG_FILE") or None,
        )

    def reload(self, force: bool = False) -> bool:
        """Re-read the config file if it changed. Returns True on change."""
        if not self._file_path:
            if force:
                with self._lock:
                    self._parsed = parse_targets(self._static_raw)
                return True
            return False
        try:
            st = os.stat(self._file_path)
        except OSError:
            if force and not self._parsed:
                # no file yet and nothing else to fall back to
                with self._lock:
                    self._parsed = parse_targets(self._static_raw)
            return False
        sig = (st.st_mtime_ns, st.st_size)
        if not force and sig == self._file_sig:
            return False
        try:
            with open(self._file_path, "r", encoding="utf-8") as fh:
                parsed = parse_targets(fh.read())
        except (OSError, ValueError) as exc:
            sys.stderr.write(f"[config] ignoring invalid config file: {exc}\n")
            return False
        with self._lock:
            self._parsed = parsed
            self._file_sig = sig
        sys.stderr.write(
            f"[config] loaded {len(parsed)} target(s) from "
            f"{self._file_path}\n"
        )
        return True

    # ------------------------------------------------------------ queries

    def targets(self) -> list[str]:
        with self._lock:
            return sorted(self._parsed)

    def has_target(self, name: str) -> bool:
        with self._lock:
            return name in self._parsed

    def configured_generations(self, name: str) -> list[str]:
        with self._lock:
            spec = self._parsed.get(name)
            return sorted(spec["generations"]) if spec else []

    def default_generation(self, name: str) -> Optional[str]:
        with self._lock:
            spec = self._parsed.get(name)
            return spec["initial"] if spec else None

    def resolve(self, name: str,
                generation_id: Optional[str]) -> Optional[dict]:
        """URL+secret for one pinned generation, or None if not configured.

        ``generation_id=None`` (rows written before generations existed)
        resolves to the target's initial generation -- the endpoint those
        rows were originally contracted to.
        """
        with self._lock:
            spec = self._parsed.get(name)
            if spec is None:
                return None
            gid = generation_id if generation_id is not None \
                else spec["initial"]
            gen = spec["generations"].get(gid)
            return dict(gen) if gen else None


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
