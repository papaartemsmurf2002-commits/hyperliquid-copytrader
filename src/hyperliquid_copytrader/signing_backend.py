from __future__ import annotations

from importlib import metadata as importlib_metadata
from typing import Any


REQUIRED_SIGNING_BACKEND = "eth_keys.backends.coincurve.CoinCurveECCBackend"
REQUIRED_COINCURVE_VERSION = "19.0.1"
_GLOBAL_BACKEND = object()


def signing_backend_identity(*, backend: Any = _GLOBAL_BACKEND) -> dict[str, Any]:
    """Return the effective eth-keys backend, not merely an installed package."""

    if backend is _GLOBAL_BACKEND:
        from eth_keys import keys

        backend = keys.backend
    backend_type = type(backend)
    qualified_name = f"{backend_type.__module__}.{backend_type.__qualname__}"
    try:
        coincurve_version = importlib_metadata.version("coincurve")
    except importlib_metadata.PackageNotFoundError:
        coincurve_version = "missing"
    return {
        "qualified_name": qualified_name,
        "coincurve_version": coincurve_version,
        "required_qualified_name": REQUIRED_SIGNING_BACKEND,
        "required_coincurve_version": REQUIRED_COINCURVE_VERSION,
        "required_backend_active": (
            qualified_name == REQUIRED_SIGNING_BACKEND
            and coincurve_version == REQUIRED_COINCURVE_VERSION
        ),
    }


def require_release_signing_backend(*, backend: Any = _GLOBAL_BACKEND) -> None:
    identity = signing_backend_identity(backend=backend)
    if identity["required_backend_active"] is not True:
        raise RuntimeError(
            "release signing backend is unavailable or stale: "
            f"{identity['qualified_name']} coincurve={identity['coincurve_version']}"
        )
