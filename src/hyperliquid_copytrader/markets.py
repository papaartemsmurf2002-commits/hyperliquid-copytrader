from __future__ import annotations

import json
import re
from dataclasses import dataclass
from hashlib import sha256 as _sha256
from typing import Any, Iterable, Mapping


# Public testnet metadata includes legitimate option/volatility names such as ``$EVIV`` and
# ``BTC-29AUG25-120000-C``. Keep those exact exchange identities representable while still
# rejecting whitespace, colons, slashes, control characters, and path-like input.
_ASSET_COMPONENT_RE = re.compile(r"^[A-Za-z0-9$][A-Za-z0-9_$*.-]{0,31}$")
# Hyperliquid testnet currently includes the official DEX name ``i<3fl``. DEX names are
# therefore not limited to asset-symbol characters. Keep the accepted set explicit and
# path/colon/whitespace-free while preserving that real wire identity.
_DEX_COMPONENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.<>-]{0,15}$")
MARKET_UNIVERSE_MANIFEST_VERSION = 1
MARKET_UNIVERSE_NETWORKS = frozenset({"mainnet", "testnet"})


class MarketIdentityError(ValueError):
    """Raised when a market name cannot be represented safely and unambiguously."""


@dataclass(frozen=True, slots=True)
class MarketIdentity:
    """Canonical identity for a default-perp or DEX-qualified perpetual market."""

    asset: str
    dex: str | None = None

    def __post_init__(self) -> None:
        _validate_component(self.asset, name="asset")
        if self.asset.islower():
            raise MarketIdentityError(
                "canonical all-lowercase asset component must be uppercase; mixed-case exchange "
                "asset names are preserved exactly"
            )
        if self.dex is not None:
            if not self.dex:
                raise MarketIdentityError("DEX component must not be empty")
            _validate_component(self.dex, name="DEX")

    @property
    def canonical(self) -> str:
        return self.asset if self.dex is None else f"{self.dex}:{self.asset}"

    @property
    def is_default_perp(self) -> bool:
        return self.dex is None

    def __str__(self) -> str:
        return self.canonical


@dataclass(frozen=True, slots=True)
class FrozenMarketSpec:
    """Launch-time identity and sizing precision for one active perpetual market."""

    symbol: str
    sz_decimals: int

    def __post_init__(self) -> None:
        if not is_canonical_market(self.symbol):
            raise MarketIdentityError("frozen market symbol must be canonical")
        if isinstance(self.sz_decimals, bool) or not isinstance(self.sz_decimals, int):
            raise MarketIdentityError("market sz_decimals must be an integer")
        if not 0 <= self.sz_decimals <= 18:
            raise MarketIdentityError("market sz_decimals must be between 0 and 18")

    def to_payload(self) -> dict[str, Any]:
        return {"symbol": self.symbol, "sz_decimals": self.sz_decimals}


@dataclass(frozen=True, slots=True)
class FrozenMarketUniverseManifest:
    """Immutable unsigned catalog identity used to bind one execution run."""

    version: int
    network: str
    observed_ms: int
    dexes: tuple[str, ...]
    markets: tuple[FrozenMarketSpec, ...]
    sha256: str

    def __post_init__(self) -> None:
        if self.version != MARKET_UNIVERSE_MANIFEST_VERSION:
            raise MarketIdentityError(
                f"market universe manifest version must be {MARKET_UNIVERSE_MANIFEST_VERSION}"
            )
        if self.network not in MARKET_UNIVERSE_NETWORKS:
            raise MarketIdentityError("market universe network must be mainnet or testnet")
        if self.observed_ms <= 0:
            raise MarketIdentityError("market universe observed_ms must be positive")
        if self.dexes != _sorted_unique_dexes(self.dexes):
            raise MarketIdentityError("market universe DEXes must be canonical, unique, and sorted")
        if not self.dexes or self.dexes[0] != "":
            raise MarketIdentityError("market universe must include the default perp DEX")
        if self.markets != _sorted_unique_market_specs(self.markets):
            raise MarketIdentityError(
                "market universe entries must be canonical, unique, and sorted"
            )
        expected = market_catalog_fingerprint(
            network=self.network,
            dexes=self.dexes,
            markets=self.markets,
        )
        if self.sha256 != expected:
            raise MarketIdentityError("market universe SHA256 does not match its frozen content")

    def to_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "network": self.network,
            "observed_ms": self.observed_ms,
            "dexes": list(self.dexes),
            "symbols": list(self.symbols),
            "markets": [market.to_payload() for market in self.markets],
            "sha256": self.sha256,
        }

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(market.symbol for market in self.markets)

    def market(self, symbol: Any) -> FrozenMarketSpec:
        canonical = canonical_market(symbol)
        for market in self.markets:
            if market.symbol == canonical:
                return market
        raise MarketIdentityError(f"market {canonical} is not present in the frozen universe")

    @classmethod
    def from_payload(cls, payload: Any) -> FrozenMarketUniverseManifest:
        """Parse and fully revalidate a persisted immutable launch manifest."""

        if not isinstance(payload, Mapping):
            raise MarketIdentityError("market universe manifest must be an object")
        raw_markets = payload.get("markets")
        if not isinstance(raw_markets, list):
            raise MarketIdentityError("market universe manifest markets must be an array")
        markets: list[FrozenMarketSpec] = []
        for index, item in enumerate(raw_markets):
            if not isinstance(item, Mapping):
                raise MarketIdentityError(f"market universe manifest market {index} is malformed")
            symbol = item.get("symbol")
            sz_decimals = item.get("sz_decimals")
            if not isinstance(symbol, str):
                raise MarketIdentityError(
                    f"market universe manifest market {index} symbol must be a string"
                )
            if isinstance(sz_decimals, bool) or not isinstance(sz_decimals, int):
                raise MarketIdentityError(
                    f"market universe manifest market {index} sz_decimals must be an integer"
                )
            markets.append(
                FrozenMarketSpec(
                    symbol=symbol,
                    sz_decimals=sz_decimals,
                )
            )
        raw_dexes = payload.get("dexes")
        if not isinstance(raw_dexes, list):
            raise MarketIdentityError("market universe manifest dexes must be an array")
        version = payload.get("version")
        network = payload.get("network")
        observed_ms = payload.get("observed_ms")
        manifest_sha256 = payload.get("sha256")
        if isinstance(version, bool) or not isinstance(version, int):
            raise MarketIdentityError("market universe manifest version must be an integer")
        if not isinstance(network, str):
            raise MarketIdentityError("market universe manifest network must be a string")
        if isinstance(observed_ms, bool) or not isinstance(observed_ms, int):
            raise MarketIdentityError("market universe manifest observed_ms must be an integer")
        if not isinstance(manifest_sha256, str):
            raise MarketIdentityError("market universe manifest sha256 must be a string")
        manifest = cls(
            version=version,
            network=network,
            observed_ms=observed_ms,
            dexes=tuple(raw_dexes),
            markets=tuple(markets),
            sha256=manifest_sha256,
        )
        raw_symbols = payload.get("symbols")
        if raw_symbols is not None and (
            not isinstance(raw_symbols, list) or tuple(raw_symbols) != manifest.symbols
        ):
            raise MarketIdentityError(
                "market universe manifest symbols do not match its market entries"
            )
        return manifest


@dataclass(frozen=True, slots=True)
class MarketPrecisionDrift:
    symbol: str
    expected_sz_decimals: int
    observed_sz_decimals: int

    def to_payload(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "expected_sz_decimals": self.expected_sz_decimals,
            "observed_sz_decimals": self.observed_sz_decimals,
        }


@dataclass(frozen=True, slots=True)
class MarketUniverseDrift:
    """Deterministic difference between a frozen manifest and a fresh observation."""

    expected_sha256: str
    observed_sha256: str
    expected_network: str
    observed_network: str
    added_dexes: tuple[str, ...]
    removed_dexes: tuple[str, ...]
    added_symbols: tuple[str, ...]
    removed_symbols: tuple[str, ...]
    precision_changes: tuple[MarketPrecisionDrift, ...]

    @property
    def changed(self) -> bool:
        return self.expected_sha256 != self.observed_sha256

    def to_payload(self) -> dict[str, Any]:
        return {
            "changed": self.changed,
            "expected_sha256": self.expected_sha256,
            "observed_sha256": self.observed_sha256,
            "expected_network": self.expected_network,
            "observed_network": self.observed_network,
            "added_dexes": list(self.added_dexes),
            "removed_dexes": list(self.removed_dexes),
            "added_symbols": list(self.added_symbols),
            "removed_symbols": list(self.removed_symbols),
            "precision_changes": [change.to_payload() for change in self.precision_changes],
        }


def parse_market(value: Any) -> MarketIdentity:
    """Parse and canonicalize a market name.

    Existing default-perp symbols remain case-insensitive and canonicalize to uppercase. HIP-3
    markets contain exactly one colon, preserve the case-sensitive exchange DEX prefix, and
    canonicalize the asset component to uppercase.
    """

    if not isinstance(value, str):
        raise MarketIdentityError("market must be a string")
    raw = value.strip()
    if not raw:
        raise MarketIdentityError("market must not be empty")
    colon_count = raw.count(":")
    if colon_count > 1:
        raise MarketIdentityError("market must contain at most one colon")

    if colon_count == 0:
        _validate_component(raw, name="asset")
        return MarketIdentity(asset=_canonical_asset(raw))

    dex, asset = raw.split(":", 1)
    if not dex:
        raise MarketIdentityError("DEX component must not be empty")
    if not asset:
        raise MarketIdentityError("asset component must not be empty")
    _validate_component(dex, name="DEX")
    _validate_component(asset, name="asset")
    return MarketIdentity(asset=_canonical_asset(asset), dex=dex)


def canonical_market(value: Any) -> str:
    """Return the canonical wire/storage form for a valid market name."""

    return parse_market(value).canonical


def canonical_market_symbol(value: Any) -> str:
    """Compatibility-friendly explicit name for :func:`canonical_market`."""

    return canonical_market(value)


def market_dex(value: Any) -> str:
    """Return the canonical DEX prefix, or an empty string for the default perp DEX."""

    return parse_market(value).dex or ""


def qualify_market_symbol(dex: Any, coin: Any) -> str:
    """Canonicalize *coin* and attach *dex* when the response coin is unqualified.

    Hyperliquid responses are not fully uniform: a DEX-scoped payload may return either
    ``AAPL`` or ``xyz:AAPL``. A conflicting embedded prefix is rejected instead of being
    silently rewritten.
    """

    if not isinstance(dex, str):
        raise MarketIdentityError("DEX must be a string")
    normalized_dex = dex.strip()
    if normalized_dex:
        _validate_component(normalized_dex, name="DEX")
    identity = parse_market(coin)
    if identity.dex is not None:
        if normalized_dex and identity.dex != normalized_dex:
            raise MarketIdentityError(
                f"market DEX {identity.dex!r} conflicts with response DEX {normalized_dex!r}"
            )
        return identity.canonical
    if not normalized_dex:
        return identity.canonical
    return MarketIdentity(asset=identity.asset, dex=normalized_dex).canonical


def is_valid_market(value: Any) -> bool:
    """Return whether *value* can be parsed as an unambiguous market identity."""

    try:
        parse_market(value)
    except MarketIdentityError:
        return False
    return True


def is_canonical_market(value: Any) -> bool:
    """Return whether *value* is valid and already in canonical form."""

    if not isinstance(value, str):
        return False
    try:
        return canonical_market(value) == value
    except MarketIdentityError:
        return False


def active_perp_market_universe(
    meta_payload: Mapping[str, Any],
    *,
    dex: str = "",
) -> tuple[str, ...]:
    """Return the immutable active market set represented by one ``meta`` response.

    DEX-scoped names may arrive qualified or unqualified. Conflicting or duplicate wire
    identities are rejected so a full-coverage run cannot silently trade an ambiguous catalog.
    """

    if not isinstance(dex, str) or dex != dex.strip():
        raise MarketIdentityError("metadata DEX must be an exact string without surrounding space")
    if dex:
        _validate_component(dex, name="DEX")

    universe = meta_payload.get("universe")
    if not isinstance(universe, list):
        raise MarketIdentityError("market metadata must contain a universe array")
    symbols: set[str] = set()
    seen_identities: set[str] = set()
    for item in universe:
        if not isinstance(item, Mapping):
            raise MarketIdentityError("market metadata universe entries must be objects")
        raw_name = item.get("name")
        if not isinstance(raw_name, str) or not raw_name or raw_name != raw_name.strip():
            raise MarketIdentityError("market metadata contains an empty or inexact name")
        is_delisted = item.get("isDelisted", False)
        if not isinstance(is_delisted, bool):
            raise MarketIdentityError("market metadata isDelisted must be a boolean")
        try:
            symbol = qualify_market_symbol(dex, raw_name)
        except MarketIdentityError as exc:
            label = dex or "<default>"
            raise MarketIdentityError(
                f"market metadata DEX {label!r} contains invalid name {raw_name!r}: {exc}"
            ) from exc
        if market_dex(symbol) != dex:
            raise MarketIdentityError(f"market {symbol!r} conflicts with metadata DEX {dex!r}")
        if symbol in seen_identities:
            raise MarketIdentityError(f"market metadata contains duplicate market {symbol}")
        seen_identities.add(symbol)
        if is_delisted:
            continue
        symbols.add(symbol)
    return tuple(sorted(symbols, key=lambda symbol: (market_dex(symbol), symbol)))


def perp_dex_names(payload: Any) -> tuple[str, ...]:
    """Validate and return the API-order DEX names from ``perpDexs``.

    ``allPerpMetas`` is aligned to this wire order, so this function deliberately does not sort.
    The leading null represents Hyperliquid's default perpetual DEX and is returned as ``""``.
    """

    if not isinstance(payload, list) or not payload:
        raise MarketIdentityError("perpDexs response must be a non-empty array")
    if payload[0] is not None:
        raise MarketIdentityError("perpDexs response must start with the default null DEX")

    names = [""]
    seen = {""}
    for index, item in enumerate(payload[1:], start=1):
        if not isinstance(item, Mapping):
            raise MarketIdentityError(f"perpDexs entry {index} must be an object")
        name = item.get("name")
        if not isinstance(name, str) or not name or name != name.strip():
            raise MarketIdentityError(f"perpDexs entry {index} has an empty or inexact name")
        _validate_component(name, name="DEX")
        if name in seen:
            raise MarketIdentityError(f"perpDexs response contains duplicate DEX {name}")
        names.append(name)
        seen.add(name)
    return tuple(names)


def build_frozen_market_universe_manifest(
    *,
    network: str,
    observed_ms: int,
    perp_dexs_payload: Any,
    all_perp_metas_payload: Any,
) -> FrozenMarketUniverseManifest:
    """Build a fail-closed launch catalog from the two authoritative public info responses."""

    canonical_network = _canonical_network(network)
    if isinstance(observed_ms, bool) or not isinstance(observed_ms, int) or observed_ms <= 0:
        raise MarketIdentityError("market universe observed_ms must be a positive integer")

    wire_dexes = perp_dex_names(perp_dexs_payload)
    if not isinstance(all_perp_metas_payload, list) or not all_perp_metas_payload:
        raise MarketIdentityError("allPerpMetas response must be a non-empty array")
    if len(all_perp_metas_payload) != len(wire_dexes):
        raise MarketIdentityError(
            "perpDexs and allPerpMetas responses must have the same number of entries"
        )

    markets: list[FrozenMarketSpec] = []
    seen_symbols: set[str] = set()
    for index, (dex, meta_payload) in enumerate(
        zip(wire_dexes, all_perp_metas_payload, strict=True)
    ):
        if not isinstance(meta_payload, Mapping):
            raise MarketIdentityError(f"allPerpMetas entry {index} must be an object")
        active_symbols = active_perp_market_universe(meta_payload, dex=dex)
        universe = meta_payload["universe"]
        entries_by_symbol: dict[str, Mapping[str, Any]] = {}
        for raw_entry in universe:
            # active_perp_market_universe has already validated every entry and its status.
            assert isinstance(raw_entry, Mapping)
            symbol = qualify_market_symbol(dex, raw_entry["name"])
            if raw_entry.get("isDelisted", False):
                continue
            entries_by_symbol[symbol] = raw_entry

        for symbol in active_symbols:
            if symbol in seen_symbols:
                raise MarketIdentityError(
                    f"allPerpMetas response contains duplicate market identity {symbol}"
                )
            entry = entries_by_symbol[symbol]
            sz_decimals = entry.get("szDecimals")
            if isinstance(sz_decimals, bool) or not isinstance(sz_decimals, int):
                raise MarketIdentityError(f"market {symbol} szDecimals must be an integer")
            markets.append(FrozenMarketSpec(symbol=symbol, sz_decimals=sz_decimals))
            seen_symbols.add(symbol)

    frozen_dexes = _sorted_unique_dexes(wire_dexes)
    frozen_markets = _sorted_unique_market_specs(markets)
    sha256 = market_catalog_fingerprint(
        network=canonical_network,
        dexes=frozen_dexes,
        markets=frozen_markets,
    )
    return FrozenMarketUniverseManifest(
        version=MARKET_UNIVERSE_MANIFEST_VERSION,
        network=canonical_network,
        observed_ms=observed_ms,
        dexes=frozen_dexes,
        markets=frozen_markets,
        sha256=sha256,
    )


def market_catalog_fingerprint(
    *,
    network: str,
    dexes: Iterable[str],
    markets: Iterable[FrozenMarketSpec],
) -> str:
    """Hash the immutable catalog content; observation time is intentionally excluded."""

    payload = {
        "version": MARKET_UNIVERSE_MANIFEST_VERSION,
        "network": _canonical_network(network),
        "dexes": list(_sorted_unique_dexes(dexes)),
        "markets": [market.to_payload() for market in _sorted_unique_market_specs(markets)],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return _sha256(encoded).hexdigest()


def compare_market_universes(
    expected: FrozenMarketUniverseManifest,
    observed: FrozenMarketUniverseManifest,
) -> MarketUniverseDrift:
    """Return a deterministic, non-adopting comparison of two validated manifests."""

    expected_precision = {market.symbol: market.sz_decimals for market in expected.markets}
    observed_precision = {market.symbol: market.sz_decimals for market in observed.markets}
    precision_changes = tuple(
        MarketPrecisionDrift(
            symbol=symbol,
            expected_sz_decimals=expected_precision[symbol],
            observed_sz_decimals=observed_precision[symbol],
        )
        for symbol in sorted(
            expected_precision.keys() & observed_precision.keys(),
            key=lambda candidate: (
                market_dex(candidate) != "",
                market_dex(candidate),
                candidate,
            ),
        )
        if expected_precision[symbol] != observed_precision[symbol]
    )
    return MarketUniverseDrift(
        expected_sha256=expected.sha256,
        observed_sha256=observed.sha256,
        expected_network=expected.network,
        observed_network=observed.network,
        added_dexes=_sorted_set_difference(observed.dexes, expected.dexes),
        removed_dexes=_sorted_set_difference(expected.dexes, observed.dexes),
        added_symbols=_sorted_symbol_difference(observed.symbols, expected.symbols),
        removed_symbols=_sorted_symbol_difference(expected.symbols, observed.symbols),
        precision_changes=precision_changes,
    )


def market_universe_fingerprint(symbols: Iterable[str]) -> str:
    """Hash a canonical, order-independent market set for a durable run manifest."""

    canonical = sorted({canonical_market_symbol(symbol) for symbol in symbols})
    if not canonical:
        raise MarketIdentityError("market universe must not be empty")
    return _sha256("\n".join(canonical).encode("utf-8")).hexdigest()


def _canonical_network(value: Any) -> str:
    if not isinstance(value, str):
        raise MarketIdentityError("market universe network must be mainnet or testnet")
    canonical = value.strip().lower()
    if canonical not in MARKET_UNIVERSE_NETWORKS:
        raise MarketIdentityError("market universe network must be mainnet or testnet")
    return canonical


def _dex_sort_key(dex: str) -> tuple[bool, str]:
    return (dex != "", dex)


def _market_sort_key(market: FrozenMarketSpec) -> tuple[bool, str, str]:
    dex = market_dex(market.symbol)
    return (dex != "", dex, market.symbol)


def _sorted_unique_dexes(values: Iterable[str]) -> tuple[str, ...]:
    dexes: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str) or value != value.strip():
            raise MarketIdentityError("market universe DEXes must be exact strings")
        if value:
            _validate_component(value, name="DEX")
        if value in seen:
            raise MarketIdentityError(f"market universe contains duplicate DEX {value}")
        dexes.append(value)
        seen.add(value)
    return tuple(sorted(dexes, key=_dex_sort_key))


def _sorted_unique_market_specs(
    values: Iterable[FrozenMarketSpec],
) -> tuple[FrozenMarketSpec, ...]:
    markets: list[FrozenMarketSpec] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, FrozenMarketSpec):
            raise MarketIdentityError("market universe entries must be FrozenMarketSpec values")
        if value.symbol in seen:
            raise MarketIdentityError(f"market universe contains duplicate market {value.symbol}")
        markets.append(value)
        seen.add(value.symbol)
    if not markets:
        raise MarketIdentityError("market universe must not be empty")
    return tuple(sorted(markets, key=_market_sort_key))


def _sorted_set_difference(left: Iterable[str], right: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(set(left) - set(right), key=_dex_sort_key))


def _sorted_symbol_difference(left: Iterable[str], right: Iterable[str]) -> tuple[str, ...]:
    difference = set(left) - set(right)
    return tuple(
        sorted(
            difference,
            key=lambda symbol: (
                market_dex(symbol) != "",
                market_dex(symbol),
                symbol,
            ),
        )
    )


def _validate_component(value: str, *, name: str) -> None:
    pattern = _DEX_COMPONENT_RE if name == "DEX" else _ASSET_COMPONENT_RE
    if not pattern.fullmatch(value):
        allowed = (
            "ASCII letters, digits, underscores, dots, angle brackets, or hyphens"
            if name == "DEX"
            else "ASCII letters, digits, dollar signs, asterisks, underscores, dots, or hyphens"
        )
        limit = 16 if name == "DEX" else 32
        start = "an ASCII letter or digit" if name == "DEX" else "an ASCII letter, digit, or $"
        raise MarketIdentityError(
            f"{name} component must start with {start} and contain at most {limit} {allowed}"
        )


def _canonical_asset(value: str) -> str:
    return value.upper() if value.islower() else value
