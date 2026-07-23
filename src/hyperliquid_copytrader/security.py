from __future__ import annotations

from dataclasses import asdict, is_dataclass
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, cast


SENSITIVE_KEY_FRAGMENTS = (
    "api_private_key",
    "private_key",
    "secret",
    "token",
    "password",
    "passphrase",
    "seed",
    "mnemonic",
)


def is_sensitive_key(key: str) -> bool:
    lowered = key.lower()
    return any(fragment in lowered for fragment in SENSITIVE_KEY_FRAGMENTS)


def redact_secrets(value: Any) -> Any:
    """Recursively redact sensitive fields before persistence, logging, or diagnostics."""

    if is_dataclass(value) and not isinstance(value, type):
        value = asdict(cast(Any, value))
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            key_str = str(key)
            if is_sensitive_key(key_str):
                redacted[key_str] = "<redacted>" if item else ""
            else:
                redacted[key_str] = redact_secrets(item)
        return redacted
    if isinstance(value, (list, tuple, set)):
        return [redact_secrets(item) for item in value]
    if isinstance(value, Decimal):
        return value
    if isinstance(value, Enum):
        return value
    if isinstance(value, Path):
        return value
    return value
