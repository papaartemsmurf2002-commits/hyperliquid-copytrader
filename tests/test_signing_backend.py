from __future__ import annotations

import pytest
from eth_keys.backends.coincurve import CoinCurveECCBackend
from eth_keys.backends.native import NativeECCBackend
from eth_keys.datatypes import PrivateKey
from eth_utils import keccak

from hyperliquid_copytrader import signing_backend


def test_release_signing_backend_identity_is_exact() -> None:
    identity = signing_backend.signing_backend_identity()

    assert identity == {
        "qualified_name": signing_backend.REQUIRED_SIGNING_BACKEND,
        "coincurve_version": signing_backend.REQUIRED_COINCURVE_VERSION,
        "required_qualified_name": signing_backend.REQUIRED_SIGNING_BACKEND,
        "required_coincurve_version": signing_backend.REQUIRED_COINCURVE_VERSION,
        "required_backend_active": True,
    }
    signing_backend.require_release_signing_backend()


def test_release_signing_backend_rejects_native_and_stale_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(RuntimeError, match="release signing backend"):
        signing_backend.require_release_signing_backend(backend=NativeECCBackend())

    original_version = signing_backend.importlib_metadata.version

    def stale_version(name: str) -> str:
        if name == "coincurve":
            return "0.0.0"
        return original_version(name)

    monkeypatch.setattr(signing_backend.importlib_metadata, "version", stale_version)
    with pytest.raises(RuntimeError, match="coincurve=0.0.0"):
        signing_backend.require_release_signing_backend(backend=CoinCurveECCBackend())


def test_native_and_coincurve_fixed_vector_match_and_recover() -> None:
    private_key = bytes.fromhex("11" * 32)
    digest = keccak(b"hlct-release-signing-backend-proof")
    native = PrivateKey(private_key, backend=NativeECCBackend()).sign_msg_hash(digest)
    coincurve = PrivateKey(private_key, backend=CoinCurveECCBackend()).sign_msg_hash(digest)

    assert (native.r, native.s, native.v) == (coincurve.r, coincurve.s, coincurve.v)
    assert native.recover_public_key_from_msg_hash(digest).to_checksum_address() == (
        coincurve.recover_public_key_from_msg_hash(digest).to_checksum_address()
    )
