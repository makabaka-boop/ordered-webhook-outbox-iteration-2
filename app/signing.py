"""Target-specific request signing.

Every delivery is signed with the **target's own** deployment-configured
secret. The signed material is the timestamp plus the *original raw JSON
bytes* exactly as received at ingestion (never a re-serialized body):

    signed = timestamp + "\\n" + raw_body_bytes
    X-Signature = hex(HMAC-SHA256(secret, signed))

Delivered headers:
    X-Event-Id     stable event identity (survives retries / DLQ replay)
    X-Request-Id   producer's idempotency key
    X-Timestamp    Unix epoch seconds (ingest-time semantics: clock.now())
    X-Signature    hex HMAC-SHA256 as above
"""

from __future__ import annotations

import hashlib
import hmac


def sign(secret: str, timestamp: int, raw_body: bytes) -> str:
    msg = str(int(timestamp)).encode("ascii") + b"\n" + raw_body
    return hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()


def verify(secret: str, timestamp: int, raw_body: bytes, signature: str) -> bool:
    expected = sign(secret, timestamp, raw_body)
    return hmac.compare_digest(expected, signature or "")
