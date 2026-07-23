from __future__ import annotations

import hashlib
import json
import re
from typing import Any


CLOID_RE = re.compile(r"^0x[0-9a-f]{32}$")


def deterministic_cloid(*parts: Any, prefix: str = "hlct-v1") -> str:
    """Return a deterministic 128-bit Hyperliquid cloid as lowercase hex."""

    payload = json.dumps([prefix, *parts], sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.blake2s(payload.encode("utf-8"), digest_size=16).hexdigest()
    return "0x" + digest


def validate_cloid(cloid: str) -> str:
    lowered = cloid.lower()
    if not CLOID_RE.fullmatch(lowered):
        raise ValueError(f"invalid Hyperliquid cloid: {cloid!r}")
    return lowered
