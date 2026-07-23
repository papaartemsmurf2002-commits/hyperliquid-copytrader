from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass, replace
from decimal import Decimal, InvalidOperation
from enum import Enum
from hashlib import sha256
from time import time_ns
from typing import Any, Mapping, Protocol

from .markets import (
    FrozenMarketSpec,
    FrozenMarketUniverseManifest,
    MarketIdentityError,
    build_frozen_market_universe_manifest,
    canonical_market,
    market_dex,
    perp_dex_names,
    qualify_market_symbol,
)

from .journal_writer import JournalWriter


class PublicMarketInfoClient(Protocol):
    """Minimal unsigned Hyperliquid info client required for catalog discovery."""

    def info(self, payload: dict[str, Any]) -> Any: ...


class MarketReadiness(str, Enum):
    READY = "READY"
    NO_CONTEXT = "NO_CONTEXT"
    HALTED = "HALTED"
    DELISTED = "DELISTED"
    UNTRUSTED = "UNTRUSTED"


@dataclass(frozen=True, slots=True)
class CatalogMarket:
    symbol: str
    dex: str
    asset_id: int
    dex_index: int
    universe_index: int
    sz_decimals: int
    max_leverage: int
    readiness: MarketReadiness
    margin_mode: str = "cross"
    collateral_token: int = 0
    margin_table_id: int | None = None
    is_delisted: bool = False
    context_observed_ms: int = 0
    oracle_px: Decimal | None = None
    mark_px: Decimal | None = None
    book_revision: int = 0
    book_observed_ms: int = 0
    tombstoned_from_revision: str = ""
    removal_tombstone: bool = False
    pending_asset_id: int | None = None
    pending_universe_index: int | None = None
    pending_sz_decimals: int | None = None
    pending_max_leverage: int | None = None
    pending_margin_mode: str | None = None
    pending_collateral_token: int | None = None
    pending_margin_table_id: int | None = None
    pending_identity_revision: str = ""

    def __post_init__(self) -> None:
        if canonical_market(self.symbol) != self.symbol or market_dex(self.symbol) != self.dex:
            raise MarketIdentityError("catalog market identity is not canonical")
        if self.asset_id < 0 or self.dex_index < 0 or self.universe_index < 0:
            raise MarketIdentityError("catalog market indices must be non-negative")
        if not 0 <= self.sz_decimals <= 18:
            raise MarketIdentityError("catalog szDecimals must be between 0 and 18")
        if self.max_leverage < 1:
            raise MarketIdentityError("catalog maxLeverage must be positive")
        if self.margin_mode not in {"cross", "noCross", "strictIsolated"}:
            raise MarketIdentityError("catalog marginMode is unsupported")
        if self.collateral_token < 0:
            raise MarketIdentityError("catalog collateralToken must be non-negative")
        if self.margin_table_id is not None and self.margin_table_id < 0:
            raise MarketIdentityError("catalog marginTableId must be non-negative")
        pending_values = (
            self.pending_asset_id,
            self.pending_universe_index,
            self.pending_sz_decimals,
            self.pending_max_leverage,
        )
        if self.pending_identity_revision:
            if any(value is None for value in pending_values):
                raise MarketIdentityError("pending catalog identity is incomplete")
            if min(int(value) for value in pending_values if value is not None) < 0:
                raise MarketIdentityError("pending catalog identity is invalid")
            pending_sz_decimals = self.pending_sz_decimals
            pending_max_leverage = self.pending_max_leverage
            assert pending_sz_decimals is not None
            assert pending_max_leverage is not None
            if not 0 <= pending_sz_decimals <= 18:
                raise MarketIdentityError("pending catalog precision is invalid")
            if pending_max_leverage < 1:
                raise MarketIdentityError("pending catalog leverage is invalid")
            if self.pending_margin_mode not in {"cross", "noCross", "strictIsolated"}:
                raise MarketIdentityError("pending catalog marginMode is unsupported")
            if self.pending_collateral_token is None or self.pending_collateral_token < 0:
                raise MarketIdentityError("pending catalog collateralToken is invalid")
            if self.pending_margin_table_id is not None and self.pending_margin_table_id < 0:
                raise MarketIdentityError("pending catalog marginTableId is invalid")
        elif any(value is not None for value in pending_values) or any(
            value is not None
            for value in (
                self.pending_margin_mode,
                self.pending_collateral_token,
                self.pending_margin_table_id,
            )
        ):
            raise MarketIdentityError("pending catalog identity has no revision")
        for price in (self.oracle_px, self.mark_px):
            if price is not None and (not price.is_finite() or price <= 0):
                raise MarketIdentityError("catalog context price must be finite and positive")

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["readiness"] = self.readiness.value
        payload["oracle_px"] = _decimal_text(self.oracle_px)
        payload["mark_px"] = _decimal_text(self.mark_px)
        return payload


@dataclass(frozen=True, slots=True)
class CatalogRevision:
    sequence: int
    revision_id: str
    policy_version: str
    network: str
    observed_ms: int
    wire_dexes: tuple[str, ...]
    markets: tuple[CatalogMarket, ...]
    snapshot_sha256: str
    dex_bracket_before_sha256: str
    dex_bracket_after_sha256: str

    def __post_init__(self) -> None:
        if self.sequence < 1 or not self.revision_id or not self.policy_version:
            raise MarketIdentityError("catalog revision identity is invalid")
        symbols = tuple(market.symbol for market in self.markets)
        if len(symbols) != len(set(symbols)):
            raise MarketIdentityError("catalog revision contains duplicate symbols")
        asset_ids = tuple(
            market.asset_id
            for market in self.markets
            if not market.is_delisted and market.readiness is not MarketReadiness.UNTRUSTED
        )
        if len(asset_ids) != len(set(asset_ids)):
            raise MarketIdentityError("catalog revision contains duplicate active asset IDs")

    def market(self, symbol: str) -> CatalogMarket | None:
        canonical = canonical_market(symbol)
        return next((market for market in self.markets if market.symbol == canonical), None)

    def to_payload(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "revision_id": self.revision_id,
            "policy_version": self.policy_version,
            "network": self.network,
            "observed_ms": self.observed_ms,
            "wire_dexes": list(self.wire_dexes),
            "markets": [market.to_payload() for market in self.markets],
            "snapshot_sha256": self.snapshot_sha256,
            "dex_bracket_before_sha256": self.dex_bracket_before_sha256,
            "dex_bracket_after_sha256": self.dex_bracket_after_sha256,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> CatalogRevision:
        markets: list[CatalogMarket] = []
        raw_markets = payload.get("markets")
        if not isinstance(raw_markets, list):
            raise MarketIdentityError("persisted catalog markets are malformed")
        for raw in raw_markets:
            if not isinstance(raw, Mapping):
                raise MarketIdentityError("persisted catalog market is malformed")
            values = dict(raw)
            values["readiness"] = MarketReadiness(str(values["readiness"]))
            values["collateral_token"] = int(values.get("collateral_token") or 0)
            if values.get("margin_table_id") not in {None, ""}:
                values["margin_table_id"] = int(values["margin_table_id"])
            else:
                values["margin_table_id"] = None
            if values.get("pending_identity_revision"):
                # Catalogs persisted before these pending fields existed can
                # only have had unchanged margin/collateral semantics.
                values.setdefault("pending_margin_mode", values.get("margin_mode", "cross"))
                values.setdefault("pending_collateral_token", values["collateral_token"])
                values.setdefault("pending_margin_table_id", values["margin_table_id"])
            pending_collateral = values.get("pending_collateral_token")
            values["pending_collateral_token"] = (
                None if pending_collateral in {None, ""} else int(str(pending_collateral))
            )
            pending_table = values.get("pending_margin_table_id")
            values["pending_margin_table_id"] = (
                None if pending_table in {None, ""} else int(str(pending_table))
            )
            for field_name in ("oracle_px", "mark_px"):
                value = values.get(field_name)
                values[field_name] = None if value in {None, ""} else Decimal(str(value))
            markets.append(CatalogMarket(**values))
        revision = cls(
            sequence=int(payload["sequence"]),
            revision_id=str(payload["revision_id"]),
            policy_version=str(payload["policy_version"]),
            network=str(payload["network"]),
            observed_ms=int(payload["observed_ms"]),
            wire_dexes=tuple(str(item) for item in payload["wire_dexes"]),
            markets=tuple(markets),
            snapshot_sha256=str(payload["snapshot_sha256"]),
            dex_bracket_before_sha256=str(payload["dex_bracket_before_sha256"]),
            dex_bracket_after_sha256=str(payload["dex_bracket_after_sha256"]),
        )
        expected_snapshot = _revision_identity_snapshot_sha256(revision)
        if revision.snapshot_sha256 != expected_snapshot:
            raise MarketIdentityError(
                "persisted catalog identity payload does not match snapshot hash"
            )
        expected_revision_id = f"catalog-{revision.sequence:06d}-{expected_snapshot[:16]}"
        if revision.revision_id != expected_revision_id:
            raise MarketIdentityError(
                "persisted catalog revision ID does not match identity snapshot"
            )
        for name, value in (
            ("dex bracket before", revision.dex_bracket_before_sha256),
            ("dex bracket after", revision.dex_bracket_after_sha256),
        ):
            if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
                raise MarketIdentityError(f"persisted catalog {name} hash is invalid")
        return revision


def _canonical_payload_sha256(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(encoded).hexdigest()


def _revision_identity_snapshot_sha256(revision: CatalogRevision) -> str:
    identities: list[dict[str, Any]] = []
    sortable: list[tuple[int, int, CatalogMarket]] = []
    for market in revision.markets:
        if market.removal_tombstone:
            continue
        asset_id = market.asset_id if market.pending_asset_id is None else market.pending_asset_id
        universe_index = (
            market.universe_index
            if market.pending_universe_index is None
            else market.pending_universe_index
        )
        dex_index = 0 if asset_id < 100_000 else (asset_id - 100_000) // 10_000
        if dex_index >= len(revision.wire_dexes):
            raise MarketIdentityError("persisted catalog asset ID has no DEX owner")
        expected_dex = revision.wire_dexes[dex_index]
        if market.dex != expected_dex:
            raise MarketIdentityError("persisted catalog DEX/index mapping is invalid")
        sortable.append((dex_index, universe_index, market))
    for dex_index, universe_index, market in sorted(sortable, key=lambda item: (item[0], item[1])):
        asset_id = (
            market.asset_id if market.pending_asset_id is None else int(market.pending_asset_id)
        )
        identities.append(
            {
                "symbol": market.symbol,
                "dex": market.dex,
                "asset_id": asset_id,
                "dex_index": dex_index,
                "universe_index": universe_index,
                "sz_decimals": (
                    market.sz_decimals
                    if market.pending_sz_decimals is None
                    else int(market.pending_sz_decimals)
                ),
                "max_leverage": (
                    market.max_leverage
                    if market.pending_max_leverage is None
                    else int(market.pending_max_leverage)
                ),
                "collateral_token": (
                    market.collateral_token
                    if market.pending_collateral_token is None
                    else market.pending_collateral_token
                ),
                "margin_mode": (
                    market.margin_mode
                    if market.pending_margin_mode is None
                    else market.pending_margin_mode
                ),
                "margin_table_id": (
                    market.margin_table_id
                    if not market.pending_identity_revision
                    else market.pending_margin_table_id
                ),
                "is_delisted": market.is_delisted,
            }
        )
    return _canonical_payload_sha256(
        {
            "network": revision.network,
            "policy_version": revision.policy_version,
            "wire_dexes": list(revision.wire_dexes),
            "markets": identities,
        }
    )


def _hip3_asset_id(dex_index: int, universe_index: int) -> int:
    if dex_index == 0:
        return universe_index
    # Hyperliquid indexes perpDexs with the native/default DEX at zero. HIP-3
    # asset IDs retain that wire index rather than renumbering only HIP-3 DEXes.
    return 100_000 + dex_index * 10_000 + universe_index


def build_dynamic_catalog_revision(
    *,
    network: str,
    policy_version: str,
    sequence: int,
    observed_ms: int,
    dexes_before_payload: Any,
    all_perp_metas_payload: Any,
    dexes_after_payload: Any,
    previous: CatalogRevision | None = None,
    retain_symbols: set[str] | frozenset[str] = frozenset(),
) -> CatalogRevision:
    """Build a coherent identity revision while retaining delisted identities."""

    wire_before = perp_dex_names(dexes_before_payload)
    wire_after = perp_dex_names(dexes_after_payload)
    if wire_before != wire_after:
        raise MarketIdentityError("perp DEX catalog changed during dynamic snapshot")
    if not isinstance(all_perp_metas_payload, list) or len(all_perp_metas_payload) != len(
        wire_before
    ):
        raise MarketIdentityError("allPerpMetas is not aligned with perpDexs")
    if sequence < 1 or observed_ms <= 0:
        raise MarketIdentityError("catalog sequence and observation time must be positive")
    markets: list[CatalogMarket] = []
    symbols: set[str] = set()
    asset_ids: set[int] = set()
    identity_payload: list[dict[str, Any]] = []
    for dex_index, (dex, raw_meta) in enumerate(
        zip(wire_before, all_perp_metas_payload, strict=True)
    ):
        if not isinstance(raw_meta, Mapping):
            raise MarketIdentityError(f"allPerpMetas entry {dex_index} must be an object")
        universe = raw_meta.get("universe")
        if not isinstance(universe, list):
            raise MarketIdentityError(f"DEX {dex or '<default>'} universe must be an array")
        collateral_token = raw_meta.get("collateralToken", 0)
        if isinstance(collateral_token, bool) or not isinstance(collateral_token, int):
            raise MarketIdentityError(f"DEX {dex or '<default>'} has invalid collateralToken")
        for universe_index, raw in enumerate(universe):
            if not isinstance(raw, Mapping):
                raise MarketIdentityError("catalog universe entries must be objects")
            name = raw.get("name")
            if not isinstance(name, str) or not name or name != name.strip():
                raise MarketIdentityError("catalog market name must be an exact non-empty string")
            symbol = qualify_market_symbol(dex, name)
            if symbol in symbols:
                raise MarketIdentityError(f"duplicate canonical catalog symbol {symbol}")
            sz_decimals = raw.get("szDecimals")
            max_leverage = raw.get("maxLeverage")
            if isinstance(sz_decimals, bool) or not isinstance(sz_decimals, int):
                raise MarketIdentityError(f"market {symbol} has invalid szDecimals")
            if isinstance(max_leverage, bool) or not isinstance(max_leverage, int):
                raise MarketIdentityError(f"market {symbol} has invalid maxLeverage")
            is_delisted = raw.get("isDelisted", False)
            if not isinstance(is_delisted, bool):
                raise MarketIdentityError(f"market {symbol} has invalid isDelisted")
            raw_margin_mode = raw.get("marginMode")
            if raw_margin_mode is None:
                margin_mode = "strictIsolated" if raw.get("onlyIsolated") is True else "cross"
            elif isinstance(raw_margin_mode, str):
                margin_mode = raw_margin_mode
            else:
                raise MarketIdentityError(f"market {symbol} has invalid marginMode")
            if margin_mode not in {"cross", "noCross", "strictIsolated"}:
                raise MarketIdentityError(f"market {symbol} has unsupported marginMode")
            margin_table_id = raw.get("marginTableId")
            if margin_table_id is not None and (
                isinstance(margin_table_id, bool) or not isinstance(margin_table_id, int)
            ):
                raise MarketIdentityError(f"market {symbol} has invalid marginTableId")
            asset_id = _hip3_asset_id(dex_index, universe_index)
            if asset_id in asset_ids:
                raise MarketIdentityError(f"duplicate catalog asset ID {asset_id}")
            readiness = MarketReadiness.DELISTED if is_delisted else MarketReadiness.NO_CONTEXT
            market = CatalogMarket(
                symbol=symbol,
                dex=dex,
                asset_id=asset_id,
                dex_index=dex_index,
                universe_index=universe_index,
                sz_decimals=sz_decimals,
                max_leverage=max_leverage,
                readiness=readiness,
                margin_mode=margin_mode,
                collateral_token=collateral_token,
                margin_table_id=margin_table_id,
                is_delisted=is_delisted,
            )
            markets.append(market)
            symbols.add(symbol)
            asset_ids.add(asset_id)
            identity_payload.append(
                {
                    "symbol": symbol,
                    "dex": dex,
                    "asset_id": asset_id,
                    "dex_index": dex_index,
                    "universe_index": universe_index,
                    "sz_decimals": sz_decimals,
                    "max_leverage": max_leverage,
                    "margin_mode": margin_mode,
                    "collateral_token": collateral_token,
                    "margin_table_id": margin_table_id,
                    "is_delisted": is_delisted,
                }
            )
    snapshot_payload = {
        "network": network.strip().lower(),
        "policy_version": policy_version,
        "wire_dexes": list(wire_before),
        "markets": identity_payload,
    }
    snapshot_hash = _canonical_payload_sha256(snapshot_payload)
    revision_id = f"catalog-{sequence:06d}-{snapshot_hash[:16]}"
    revision = CatalogRevision(
        sequence=sequence,
        revision_id=revision_id,
        policy_version=policy_version,
        network=network.strip().lower(),
        observed_ms=observed_ms,
        wire_dexes=wire_before,
        markets=tuple(markets),
        snapshot_sha256=snapshot_hash,
        dex_bracket_before_sha256=_canonical_payload_sha256(dexes_before_payload),
        dex_bracket_after_sha256=_canonical_payload_sha256(dexes_after_payload),
    )
    if previous is None:
        return revision
    retained = {canonical_market(symbol) for symbol in retain_symbols}
    prior = {market.symbol: market for market in previous.markets}
    updated: list[CatalogMarket] = []
    current_symbols: set[str] = set()
    for market in revision.markets:
        current_symbols.add(market.symbol)
        old = prior.get(market.symbol)
        identity_mutated = old is not None and (
            old.dex,
            old.asset_id,
            old.sz_decimals,
            old.max_leverage,
            old.margin_mode,
            old.collateral_token,
            old.margin_table_id,
        ) != (
            market.dex,
            market.asset_id,
            market.sz_decimals,
            market.max_leverage,
            market.margin_mode,
            market.collateral_token,
            market.margin_table_id,
        )
        if identity_mutated and market.symbol in retained and old is not None:
            market = replace(
                old,
                readiness=MarketReadiness.UNTRUSTED,
                tombstoned_from_revision=previous.revision_id,
                pending_asset_id=market.asset_id,
                pending_universe_index=market.universe_index,
                pending_sz_decimals=market.sz_decimals,
                pending_max_leverage=market.max_leverage,
                pending_margin_mode=market.margin_mode,
                pending_collateral_token=market.collateral_token,
                pending_margin_table_id=market.margin_table_id,
                pending_identity_revision=revision.revision_id,
            )
        updated.append(market)
    for old in previous.markets:
        if old.symbol in current_symbols or old.symbol not in retained:
            continue
        updated.append(
            replace(
                old,
                readiness=MarketReadiness.DELISTED,
                is_delisted=True,
                tombstoned_from_revision=previous.revision_id,
                removal_tombstone=True,
            )
        )
    return replace(
        revision,
        markets=tuple(sorted(updated, key=lambda item: (item.dex != "", item.dex, item.symbol))),
    )


def resolve_public_market_universe(
    client: PublicMarketInfoClient,
    *,
    network: str,
    observed_ms: int | None = None,
) -> FrozenMarketUniverseManifest:
    """Resolve one stable catalog using exactly three unsigned public info requests.

    A second ``perpDexs`` read brackets ``allPerpMetas``. If the aligned DEX list changes
    while the snapshot is being assembled, the result is discarded instead of auto-adopted.
    """

    dexes_before = client.info({"type": "perpDexs"})
    all_metas = client.info({"type": "allPerpMetas"})
    dexes_after = client.info({"type": "perpDexs"})
    if perp_dex_names(dexes_before) != perp_dex_names(dexes_after):
        raise MarketIdentityError("perp DEX catalog changed during unsigned snapshot")
    if observed_ms is None:
        observed_ms = time_ns() // 1_000_000
    return build_frozen_market_universe_manifest(
        network=network,
        observed_ms=observed_ms,
        perp_dexs_payload=dexes_before,
        all_perp_metas_payload=all_metas,
    )


def _catalog_diffs(
    previous: CatalogRevision | None,
    current: CatalogRevision,
) -> list[dict[str, Any]]:
    old = {} if previous is None else {market.symbol: market for market in previous.markets}
    new = {market.symbol: market for market in current.markets}
    changes: list[tuple[str, str, dict[str, Any]]] = []
    for symbol in sorted(new.keys() - old.keys()):
        changes.append(("added", symbol, {"after": new[symbol].to_payload()}))
    for symbol in sorted(old.keys() - new.keys()):
        changes.append(
            (
                ("archived_removed_tombstone" if old[symbol].removal_tombstone else "removed"),
                symbol,
                {"before": old[symbol].to_payload()},
            )
        )
    for symbol in sorted(old.keys() & new.keys()):
        before = old[symbol]
        after = new[symbol]
        if after.removal_tombstone and not before.removal_tombstone:
            changes.append(
                (
                    "removed",
                    symbol,
                    {"before": before.to_payload(), "after": after.to_payload()},
                )
            )
        elif before.is_delisted != after.is_delisted:
            changes.append(
                (
                    "delisted" if after.is_delisted else "relisted",
                    symbol,
                    {"before": before.to_payload(), "after": after.to_payload()},
                )
            )
        identity_before = (
            before.dex,
            before.asset_id,
            before.sz_decimals,
            before.max_leverage,
            before.margin_mode,
            before.collateral_token,
            before.margin_table_id,
        )
        identity_after = (
            after.dex,
            (after.asset_id if after.pending_asset_id is None else after.pending_asset_id),
            (after.sz_decimals if after.pending_sz_decimals is None else after.pending_sz_decimals),
            (
                after.max_leverage
                if after.pending_max_leverage is None
                else after.pending_max_leverage
            ),
            after.margin_mode if after.pending_margin_mode is None else after.pending_margin_mode,
            (
                after.collateral_token
                if after.pending_collateral_token is None
                else after.pending_collateral_token
            ),
            (
                after.margin_table_id
                if not after.pending_identity_revision
                else after.pending_margin_table_id
            ),
        )
        if identity_before != identity_after:
            changes.append(
                (
                    "identity_mutation",
                    symbol,
                    {"before": before.to_payload(), "after": after.to_payload()},
                )
            )
    from_revision_id = "" if previous is None else previous.revision_id
    result: list[dict[str, Any]] = []
    for change_class, symbol, detail in changes:
        identity = f"{from_revision_id}|{current.revision_id}|{change_class}|{symbol}"
        result.append(
            {
                "diff_id": "catalog-diff-" + sha256(identity.encode("utf-8")).hexdigest()[:32],
                "from_revision_id": from_revision_id,
                "to_revision_id": current.revision_id,
                "change_class": change_class,
                "canonical_market": symbol,
                **detail,
            }
        )
    return result


class MarketCatalogActor:
    """Fleet-wide owner of dynamic native and HIP-3 identity/context revisions."""

    def __init__(
        self,
        *,
        client: PublicMarketInfoClient,
        network: str,
        policy_version: str,
        journal: JournalWriter,
        context_max_age_ms: int = 5_000,
        identity_max_age_ms: int = 120_000,
    ) -> None:
        if not policy_version:
            raise ValueError("catalog policy version is required")
        if context_max_age_ms <= 0:
            raise ValueError("catalog context maximum age must be positive")
        if identity_max_age_ms < 60_000:
            raise ValueError("catalog identity maximum age must cover one refresh cadence")
        self.client = client
        self.network = network
        self.policy_version = policy_version
        self.journal = journal
        self.context_max_age_ms = context_max_age_ms
        self.identity_max_age_ms = identity_max_age_ms
        self._current: CatalogRevision | None = None
        self._lock = asyncio.Lock()
        # Keep the three unsigned identity calls as one serialized bracket, but
        # never hold the state lock while a budgeted/network call can wait.  The
        # context/book consumer must remain able to apply frames during a slow
        # metadata refresh.
        self._refresh_lock = asyncio.Lock()
        self._subscribers: set[asyncio.Queue[CatalogRevision]] = set()
        self._historical_asset_ids: dict[int, str] = {}
        self._refresh_reasons: asyncio.Queue[str] = asyncio.Queue(1)
        self._refresh_count = 0
        self._immediate_refresh_count = 0
        self._refresh_coalesced = 0
        self._last_refresh_reason = ""
        self._publication_coalesced = 0
        self._last_refresh_error = ""
        self._last_identity_success_ms = 0
        self._context_frame_count = 0
        self._aligned_context_frame_count = 0
        self._last_context_alignment_healthy = False
        self._context_anomaly_count = 0
        self._last_context_anomalies: tuple[str, ...] = ()

    @property
    def current(self) -> CatalogRevision | None:
        return self._current

    def record_refresh_error(self, error: BaseException | str) -> None:
        self._last_refresh_error = (
            error if isinstance(error, str) else f"{type(error).__name__}: {error}"
        )

    def identity_healthy(self, now_ms: int | None = None) -> bool:
        if now_ms is None:
            now_ms = time_ns() // 1_000_000
        return bool(
            self._current is not None
            and not self._last_refresh_error
            and self._last_identity_success_ms > 0
            and 0 <= now_ms - self._last_identity_success_ms <= self.identity_max_age_ms
        )

    @property
    def context_frame_count(self) -> int:
        return self._context_frame_count

    @property
    def aligned_context_frame_count(self) -> int:
        return self._aligned_context_frame_count

    def restore(
        self,
        revision_payload: Mapping[str, Any] | None,
        asset_history: list[Mapping[str, Any]],
    ) -> None:
        if self._current is not None:
            raise MarketIdentityError("catalog actor can only restore before refresh")
        historical: dict[int, str] = {}
        for row in asset_history:
            asset_id = int(row["asset_id"])
            symbol = canonical_market(str(row["canonical_market"]))
            prior = historical.get(asset_id)
            if prior is not None and prior != symbol:
                raise MarketIdentityError("persisted asset ID history contains reuse")
            historical[asset_id] = symbol
        self._historical_asset_ids = historical
        if revision_payload is None:
            return
        revision = CatalogRevision.from_payload(revision_payload)
        if revision.network != self.network or revision.policy_version != self.policy_version:
            raise MarketIdentityError("persisted catalog policy/network fence failed")
        revision = replace(
            revision,
            markets=tuple(
                replace(
                    market,
                    readiness=(
                        market.readiness
                        if market.readiness in {MarketReadiness.DELISTED, MarketReadiness.UNTRUSTED}
                        else MarketReadiness.NO_CONTEXT
                    ),
                    context_observed_ms=0,
                    book_revision=0,
                    book_observed_ms=0,
                )
                for market in revision.markets
            ),
        )
        self._current = revision
        for market in revision.markets:
            prior = self._historical_asset_ids.get(market.asset_id)
            if prior is not None and prior != market.symbol:
                if market.readiness is not MarketReadiness.UNTRUSTED:
                    raise MarketIdentityError("persisted catalog revision reuses an asset ID")
                continue
            self._historical_asset_ids[market.asset_id] = market.symbol

    def subscribe(self, *, capacity: int = 8) -> asyncio.Queue[CatalogRevision]:
        queue: asyncio.Queue[CatalogRevision] = asyncio.Queue(capacity)
        self._subscribers.add(queue)
        if self._current is not None:
            queue.put_nowait(self._current)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[CatalogRevision]) -> None:
        self._subscribers.discard(queue)

    def request_immediate_refresh(self, reason: str) -> bool:
        if not reason:
            raise ValueError("catalog refresh reason must be non-empty")
        self._last_refresh_reason = reason
        try:
            self._refresh_reasons.put_nowait(reason)
        except asyncio.QueueFull:
            self._refresh_coalesced += 1
            return False
        self._immediate_refresh_count += 1
        return True

    async def next_refresh_reason(self) -> str:
        return await self._refresh_reasons.get()

    async def refresh(
        self,
        *,
        active_markets: set[str] | None = None,
        observed_ms: int | None = None,
    ) -> CatalogRevision:
        active = {canonical_market(symbol) for symbol in (active_markets or set())}
        async with self._refresh_lock:
            before = await asyncio.to_thread(self.client.info, {"type": "perpDexs"})
            metas = await asyncio.to_thread(self.client.info, {"type": "allPerpMetas"})
            after = await asyncio.to_thread(self.client.info, {"type": "perpDexs"})
            if observed_ms is None:
                observed_ms = time_ns() // 1_000_000
        async with self._lock:
            candidate = build_dynamic_catalog_revision(
                network=self.network,
                policy_version=self.policy_version,
                sequence=1 if self._current is None else self._current.sequence + 1,
                observed_ms=observed_ms,
                dexes_before_payload=before,
                all_perp_metas_payload=metas,
                dexes_after_payload=after,
            )
            if (
                self._current is not None
                and candidate.snapshot_sha256 == self._current.snapshot_sha256
                and not any(
                    (market.pending_identity_revision or market.removal_tombstone)
                    and market.symbol not in active
                    for market in self._current.markets
                )
            ):
                self._refresh_count += 1
                self._last_refresh_error = ""
                self._last_identity_success_ms = observed_ms
                return self._current
            candidate = self._apply_tombstones_and_mutation_blocks(candidate, active)
            diffs = _catalog_diffs(self._current, candidate)
            await self.journal.submit(
                "append_catalog_revision",
                revision_id=candidate.revision_id,
                policy_version=candidate.policy_version,
                snapshot_sha256=candidate.snapshot_sha256,
                dex_bracket_before_sha256=candidate.dex_bracket_before_sha256,
                dex_bracket_after_sha256=candidate.dex_bracket_after_sha256,
                payload=candidate.to_payload(),
                diffs=diffs,
            )
            for market in candidate.markets:
                prior = self._historical_asset_ids.get(market.asset_id)
                if prior is None:
                    self._historical_asset_ids[market.asset_id] = market.symbol
                if market.pending_asset_id is not None:
                    pending_owner = self._historical_asset_ids.get(market.pending_asset_id)
                    if pending_owner is None:
                        self._historical_asset_ids[market.pending_asset_id] = market.symbol
            self._current = candidate
            self._refresh_count += 1
            self._last_refresh_error = ""
            self._last_identity_success_ms = observed_ms
            self._publish(candidate)
            return candidate

    def _apply_tombstones_and_mutation_blocks(
        self,
        candidate: CatalogRevision,
        active_markets: set[str],
    ) -> CatalogRevision:
        prior = self._current
        old = {} if prior is None else {market.symbol: market for market in prior.markets}
        updated: list[CatalogMarket] = []
        current_symbols: set[str] = set()
        for market in candidate.markets:
            current_symbols.add(market.symbol)
            previous = old.get(market.symbol)
            historical_owner = self._historical_asset_ids.get(market.asset_id)
            reused_id = historical_owner is not None and historical_owner != market.symbol
            identity_mutated = previous is not None and (
                previous.dex,
                previous.asset_id,
                previous.sz_decimals,
                previous.max_leverage,
                previous.margin_mode,
                previous.collateral_token,
                previous.margin_table_id,
            ) != (
                market.dex,
                market.asset_id,
                market.sz_decimals,
                market.max_leverage,
                market.margin_mode,
                market.collateral_token,
                market.margin_table_id,
            )
            if identity_mutated and market.symbol in active_markets and previous is not None:
                # Keep the last accepted execution identity.  The newly observed
                # identity is evidence to reconcile, never an identity that can
                # be signed against while state is active.
                market = replace(
                    previous,
                    readiness=MarketReadiness.UNTRUSTED,
                    tombstoned_from_revision=prior.revision_id if prior is not None else "",
                    pending_asset_id=market.asset_id,
                    pending_universe_index=market.universe_index,
                    pending_sz_decimals=market.sz_decimals,
                    pending_max_leverage=market.max_leverage,
                    pending_margin_mode=market.margin_mode,
                    pending_collateral_token=market.collateral_token,
                    pending_margin_table_id=market.margin_table_id,
                    pending_identity_revision=candidate.revision_id,
                )
            elif reused_id:
                market = replace(
                    market,
                    readiness=MarketReadiness.UNTRUSTED,
                    pending_asset_id=market.asset_id,
                    pending_universe_index=market.universe_index,
                    pending_sz_decimals=market.sz_decimals,
                    pending_max_leverage=market.max_leverage,
                    pending_margin_mode=market.margin_mode,
                    pending_collateral_token=market.collateral_token,
                    pending_margin_table_id=market.margin_table_id,
                    pending_identity_revision=candidate.revision_id,
                )
            elif (
                previous is not None
                and not market.is_delisted
                and not previous.pending_identity_revision
            ):
                # A coherent unchanged identity may retain independently validated context/book.
                market = replace(
                    market,
                    readiness=previous.readiness,
                    context_observed_ms=previous.context_observed_ms,
                    oracle_px=previous.oracle_px,
                    mark_px=previous.mark_px,
                    book_revision=previous.book_revision,
                    book_observed_ms=previous.book_observed_ms,
                )
            updated.append(market)
        if prior is not None:
            for removed in prior.markets:
                if removed.symbol in current_symbols:
                    continue
                if removed.symbol not in active_markets:
                    continue
                updated.append(
                    replace(
                        removed,
                        readiness=MarketReadiness.DELISTED,
                        is_delisted=True,
                        tombstoned_from_revision=prior.revision_id,
                        removal_tombstone=True,
                    )
                )
        return replace(
            candidate,
            markets=tuple(
                sorted(updated, key=lambda market: (market.dex != "", market.dex, market.symbol))
            ),
        )

    async def observe_all_dex_contexts(self, payload: Any, *, observed_ms: int) -> CatalogRevision:
        """Atomically validate an aligned aggregate context frame and publish local states."""

        async with self._lock:
            current = self._current
            if current is None:
                raise MarketIdentityError("catalog context arrived before an identity revision")
            contexts_by_dex = _parse_all_dex_contexts(payload)
            expected = set(current.wire_dexes)
            anomalies: list[str] = [
                f"unexpected_dex:{dex}" for dex in sorted(set(contexts_by_dex) - expected)
            ]
            markets_by_dex: dict[str, list[CatalogMarket]] = {dex: [] for dex in current.wire_dexes}
            for market in current.markets:
                if market.removal_tombstone:
                    continue
                markets_by_dex[market.dex].append(market)
            replacements: dict[str, CatalogMarket] = {}
            for dex in current.wire_dexes:
                markets = markets_by_dex[dex]
                contexts = contexts_by_dex.get(dex)
                if contexts is None:
                    anomalies.append(f"missing_dex:{dex or '<default>'}")
                    for market in markets:
                        replacements[market.symbol] = replace(
                            market,
                            readiness=(
                                market.readiness
                                if market.readiness
                                in {MarketReadiness.DELISTED, MarketReadiness.UNTRUSTED}
                                else MarketReadiness.NO_CONTEXT
                            ),
                            context_observed_ms=0,
                        )
                    continue
                aligned: dict[int, CatalogMarket] = {}
                for market in markets:
                    universe_index = (
                        market.pending_universe_index
                        if market.pending_identity_revision
                        and market.pending_universe_index is not None
                        else market.universe_index
                    )
                    if universe_index in aligned:
                        anomalies.append(
                            f"identity_collision:{dex or '<default>'}:{universe_index}"
                        )
                    aligned[universe_index] = market
                if set(aligned) != set(range(len(contexts))):
                    anomalies.append(f"length_mismatch:{dex or '<default>'}")
                    for market in markets:
                        replacements[market.symbol] = replace(
                            market,
                            readiness=(
                                market.readiness
                                if market.readiness
                                in {MarketReadiness.DELISTED, MarketReadiness.UNTRUSTED}
                                else MarketReadiness.NO_CONTEXT
                            ),
                            context_observed_ms=0,
                        )
                    continue
                for universe_index, raw in enumerate(contexts):
                    market = aligned[universe_index]
                    if market.pending_identity_revision:
                        # Consume the candidate slot to prove aggregate alignment,
                        # but retain the last accepted execution context until the
                        # active identity has been independently reconciled.
                        replacements[market.symbol] = market
                        continue
                    if not isinstance(raw, Mapping):
                        anomalies.append(f"invalid_context:{dex or '<default>'}:{universe_index}")
                        replacements[market.symbol] = replace(
                            market,
                            readiness=MarketReadiness.NO_CONTEXT,
                            context_observed_ms=0,
                        )
                        continue
                    halted = raw.get("isHalted") is True or raw.get("isMarketOpen") is False
                    oracle = _optional_positive_decimal(raw.get("oraclePx"))
                    mark = _optional_positive_decimal(raw.get("markPx"))
                    if market.is_delisted:
                        readiness = MarketReadiness.DELISTED
                    elif market.readiness is MarketReadiness.UNTRUSTED:
                        readiness = MarketReadiness.UNTRUSTED
                    elif halted:
                        readiness = MarketReadiness.HALTED
                    elif (market.dex and oracle is None) or (oracle is None and mark is None):
                        readiness = MarketReadiness.NO_CONTEXT
                    elif market.book_revision > 0:
                        readiness = MarketReadiness.READY
                    else:
                        readiness = MarketReadiness.NO_CONTEXT
                    replacements[market.symbol] = replace(
                        market,
                        readiness=readiness,
                        context_observed_ms=observed_ms,
                        oracle_px=oracle,
                        mark_px=mark,
                    )
            revised = replace(
                current,
                markets=tuple(
                    replacements.get(market.symbol, market) for market in current.markets
                ),
            )
            frame_sha256 = _canonical_payload_sha256(payload)
            transitions = self._readiness_transitions(
                current,
                revised,
                transition_class="aggregate_context_readiness",
                observed_ms=observed_ms,
                frame_sha256=frame_sha256,
            )
            await self.journal.submit("append_catalog_market_transitions", transitions=transitions)
            if anomalies:
                self.request_immediate_refresh(
                    "aggregate_context_anomaly:" + ",".join(anomalies[:8])
                )
                await self.journal.submit(
                    "append_control_audit",
                    control="catalog_context_alignment",
                    status="market_local_refresh_requested",
                    detail=",".join(anomalies),
                    payload={
                        "revision_id": current.revision_id,
                        "frame_sha256": frame_sha256,
                        "anomalies": anomalies,
                    },
                    created_ms=observed_ms,
                )
            self._last_context_anomalies = tuple(anomalies)
            self._context_anomaly_count += len(anomalies)
            self._context_frame_count += 1
            self._last_context_alignment_healthy = not anomalies
            if not anomalies:
                self._aligned_context_frame_count += 1
            self._current = revised
            if transitions:
                self._publish(revised)
            return revised

    async def observe_active_context(self, payload: Any, *, observed_ms: int) -> CatalogRevision:
        """Apply one official activeAssetCtx frame without widening its failure."""

        async with self._lock:
            current = self._current
            if current is None:
                raise MarketIdentityError(
                    "active market context arrived before an identity revision"
                )
            if not isinstance(payload, Mapping):
                raise MarketIdentityError("active market context frame is malformed")
            try:
                symbol = canonical_market(str(payload.get("coin") or ""))
            except ValueError as exc:
                raise MarketIdentityError("active market context has no market") from exc
            market = current.market(symbol)
            if market is None:
                self.request_immediate_refresh(f"unknown_active_context_market:{symbol}")
                raise MarketIdentityError(f"unknown active context market {symbol}")
            raw_context = payload.get("ctx")
            if not isinstance(raw_context, Mapping):
                updated = (
                    market
                    if market.readiness
                    in {
                        MarketReadiness.HALTED,
                        MarketReadiness.DELISTED,
                        MarketReadiness.UNTRUSTED,
                    }
                    else replace(
                        market,
                        readiness=MarketReadiness.NO_CONTEXT,
                        context_observed_ms=0,
                    )
                )
                revised = replace(
                    current,
                    markets=tuple(
                        updated if item.symbol == symbol else item for item in current.markets
                    ),
                )
                frame_sha256 = _canonical_payload_sha256(payload)
                transitions = self._readiness_transitions(
                    current,
                    revised,
                    transition_class="active_context_malformed",
                    observed_ms=observed_ms,
                    frame_sha256=frame_sha256,
                )
                await self.journal.submit(
                    "append_catalog_market_transitions", transitions=transitions
                )
                self.request_immediate_refresh(f"malformed_active_context:{symbol}")
                self._context_frame_count += 1
                self._context_anomaly_count += 1
                self._last_context_alignment_healthy = False
                self._last_context_anomalies = (f"malformed_active_context:{symbol}",)
                self._current = revised
                if transitions:
                    self._publish(revised)
                return revised
            oracle = _optional_positive_decimal(raw_context.get("oraclePx"))
            mark = _optional_positive_decimal(raw_context.get("markPx"))
            halted = raw_context.get("isHalted") is True or raw_context.get("isMarketOpen") is False
            if market.is_delisted:
                readiness = MarketReadiness.DELISTED
            elif market.readiness is MarketReadiness.UNTRUSTED:
                readiness = MarketReadiness.UNTRUSTED
            elif halted:
                readiness = MarketReadiness.HALTED
            elif (market.dex and oracle is None) or (oracle is None and mark is None):
                readiness = MarketReadiness.NO_CONTEXT
            elif market.book_revision > 0:
                readiness = MarketReadiness.READY
            else:
                readiness = MarketReadiness.NO_CONTEXT
            updated = replace(
                market,
                readiness=readiness,
                context_observed_ms=observed_ms,
                oracle_px=oracle,
                mark_px=mark,
            )
            revised = replace(
                current,
                markets=tuple(
                    updated if item.symbol == symbol else item for item in current.markets
                ),
            )
            frame_sha256 = _canonical_payload_sha256(payload)
            transitions = self._readiness_transitions(
                current,
                revised,
                transition_class="active_context_readiness",
                observed_ms=observed_ms,
                frame_sha256=frame_sha256,
            )
            await self.journal.submit("append_catalog_market_transitions", transitions=transitions)
            self._context_frame_count += 1
            self._aligned_context_frame_count += 1
            self._last_context_alignment_healthy = True
            self._last_context_anomalies = ()
            self._current = revised
            if transitions:
                self._publish(revised)
            return revised

    async def invalidate_market_context(
        self, symbol: str, *, observed_ms: int, reason: str
    ) -> CatalogRevision:
        """Fence an unsubscribed active-market context before it can be reused."""

        async with self._lock:
            current = self._current
            if current is None:
                raise MarketIdentityError("market context invalidation has no catalog")
            canonical = canonical_market(symbol)
            market = current.market(canonical)
            if market is None:
                self.request_immediate_refresh(f"unknown_context_invalidation_market:{canonical}")
                raise MarketIdentityError(f"unknown catalog market {canonical}")
            if market.readiness in {
                MarketReadiness.DELISTED,
                MarketReadiness.UNTRUSTED,
                MarketReadiness.HALTED,
            }:
                return current
            updated = replace(
                market,
                readiness=MarketReadiness.NO_CONTEXT,
                context_observed_ms=0,
            )
            revised = replace(
                current,
                markets=tuple(
                    updated if item.symbol == canonical else item for item in current.markets
                ),
            )
            frame = {"market": canonical, "reason": reason, "observed_ms": observed_ms}
            transitions = self._readiness_transitions(
                current,
                revised,
                transition_class="active_context_unsubscribed",
                observed_ms=observed_ms,
                frame_sha256=_canonical_payload_sha256(frame),
            )
            await self.journal.submit("append_catalog_market_transitions", transitions=transitions)
            self._current = revised
            if transitions:
                self._publish(revised)
            return revised

    async def invalidate_all_contexts(
        self, *, observed_ms: int, reason: str, frame_payload: Any
    ) -> CatalogRevision:
        """Fail closed per market when an aggregate frame cannot be classified."""

        async with self._lock:
            current = self._current
            if current is None:
                raise MarketIdentityError("catalog context arrived before an identity revision")
            revised = replace(
                current,
                markets=tuple(
                    market
                    if market.readiness
                    in {
                        MarketReadiness.DELISTED,
                        MarketReadiness.UNTRUSTED,
                        MarketReadiness.HALTED,
                    }
                    else replace(
                        market,
                        readiness=MarketReadiness.NO_CONTEXT,
                        context_observed_ms=0,
                    )
                    for market in current.markets
                ),
            )
            frame_sha256 = _canonical_payload_sha256(frame_payload)
            transitions = self._readiness_transitions(
                current,
                revised,
                transition_class="aggregate_context_unclassified",
                observed_ms=observed_ms,
                frame_sha256=frame_sha256,
            )
            await self.journal.submit("append_catalog_market_transitions", transitions=transitions)
            self._context_anomaly_count += 1
            self._last_context_anomalies = (reason,)
            self._last_context_alignment_healthy = False
            self._current = revised
            if transitions:
                self._publish(revised)
            return revised

    async def observe_book(
        self,
        symbol: str,
        *,
        book_revision: int,
        observed_ms: int,
        now_ms: int | None = None,
    ) -> CatalogMarket:
        async with self._lock:
            current = self._current
            if current is None:
                raise MarketIdentityError("book arrived before catalog identity")
            canonical = canonical_market(symbol)
            market = current.market(canonical)
            if market is None:
                self.request_immediate_refresh(f"unknown_book_market:{canonical}")
                raise MarketIdentityError(f"unknown catalog market {canonical}")
            if book_revision <= market.book_revision:
                raise MarketIdentityError("book revision must advance")
            reference_now = observed_ms if now_ms is None else now_ms
            context_fresh = (
                market.context_observed_ms > 0
                and 0 <= reference_now - market.context_observed_ms <= self.context_max_age_ms
            )
            readiness = market.readiness
            if (
                readiness
                not in {
                    MarketReadiness.HALTED,
                    MarketReadiness.DELISTED,
                    MarketReadiness.UNTRUSTED,
                }
                and context_fresh
                and (market.oracle_px is not None or market.mark_px is not None)
            ):
                readiness = MarketReadiness.READY
            elif readiness not in {
                MarketReadiness.HALTED,
                MarketReadiness.DELISTED,
                MarketReadiness.UNTRUSTED,
            }:
                readiness = MarketReadiness.NO_CONTEXT
            updated = replace(
                market,
                readiness=readiness,
                book_revision=book_revision,
                book_observed_ms=observed_ms,
            )
            revised = replace(
                current,
                markets=tuple(
                    updated if item.symbol == canonical else item for item in current.markets
                ),
            )
            transitions = self._readiness_transitions(
                current,
                revised,
                transition_class="book_readiness",
                observed_ms=observed_ms,
                frame_sha256=_canonical_payload_sha256(
                    {
                        "symbol": canonical,
                        "book_revision": book_revision,
                        "observed_ms": observed_ms,
                    }
                ),
            )
            await self.journal.submit("append_catalog_market_transitions", transitions=transitions)
            self._current = revised
            if updated.readiness is not market.readiness:
                self._publish(revised)
            return updated

    @staticmethod
    def _readiness_transitions(
        before: CatalogRevision,
        after: CatalogRevision,
        *,
        transition_class: str,
        observed_ms: int,
        frame_sha256: str,
    ) -> list[dict[str, Any]]:
        old = {market.symbol: market for market in before.markets}
        transitions: list[dict[str, Any]] = []
        for market in after.markets:
            previous = old.get(market.symbol)
            if previous is None or previous.readiness is market.readiness:
                continue
            identity = (
                f"{after.revision_id}|{market.symbol}|{transition_class}|"
                f"{previous.readiness.value}|{market.readiness.value}|"
                f"{observed_ms}|{frame_sha256}"
            )
            transitions.append(
                {
                    "transition_id": "catalog-market-"
                    + sha256(identity.encode("utf-8")).hexdigest()[:32],
                    "revision_id": after.revision_id,
                    "canonical_market": market.symbol,
                    "transition_class": transition_class,
                    "from_readiness": previous.readiness.value,
                    "to_readiness": market.readiness.value,
                    "observed_ms": observed_ms,
                    "frame_sha256": frame_sha256,
                    "asset_id": market.asset_id,
                    "dex": market.dex,
                    "context_observed_ms": market.context_observed_ms,
                    "book_revision": market.book_revision,
                }
            )
        return transitions

    def entry_market(self, symbol: str, *, denied_symbols: set[str] | None = None) -> CatalogMarket:
        current = self._current
        canonical = canonical_market(symbol)
        if current is None:
            raise MarketIdentityError("catalog has no accepted revision")
        market = current.market(canonical)
        if market is None:
            self.request_immediate_refresh(f"unknown_source_market:{canonical}")
            raise MarketIdentityError(f"unknown catalog market {canonical}")
        denied = {canonical_market(item) for item in (denied_symbols or set())}
        if canonical in denied:
            raise MarketIdentityError(f"market {canonical} is denied by frozen policy")
        if market.readiness is not MarketReadiness.READY:
            raise MarketIdentityError(
                f"market {canonical} is not entry-ready: {market.readiness.value}"
            )
        return market

    def health(self) -> dict[str, Any]:
        now_ms = time_ns() // 1_000_000
        return {
            "healthy": self.identity_healthy(now_ms),
            "revision_id": "" if self._current is None else self._current.revision_id,
            "snapshot_sha256": "" if self._current is None else self._current.snapshot_sha256,
            "refresh_count": self._refresh_count,
            "immediate_refresh_count": self._immediate_refresh_count,
            "refresh_coalesced": self._refresh_coalesced,
            "last_refresh_reason": self._last_refresh_reason,
            "publication_coalesced": self._publication_coalesced,
            "pending_refresh_reasons": self._refresh_reasons.qsize(),
            "last_refresh_error": self._last_refresh_error,
            "last_identity_success_ms": self._last_identity_success_ms,
            "identity_age_ms": (
                None
                if self._last_identity_success_ms <= 0
                else max(0, now_ms - self._last_identity_success_ms)
            ),
            "identity_max_age_ms": self.identity_max_age_ms,
            "context_frame_count": self._context_frame_count,
            "aligned_context_frame_count": self._aligned_context_frame_count,
            "last_context_alignment_healthy": self._last_context_alignment_healthy,
            "context_anomaly_count": self._context_anomaly_count,
            "last_context_anomalies": list(self._last_context_anomalies),
            "market_states": {}
            if self._current is None
            else {
                state.value: sum(1 for market in self._current.markets if market.readiness is state)
                for state in MarketReadiness
            },
        }

    def _publish(self, revision: CatalogRevision) -> None:
        for queue in tuple(self._subscribers):
            if queue.full():
                self._publication_coalesced += 1
                raise RuntimeError(
                    "catalog publication queue overflow would lose a lifecycle transition"
                )
            queue.put_nowait(revision)


def _parse_all_dex_contexts(payload: Any) -> dict[str, list[Any]]:
    if isinstance(payload, Mapping):
        if "ctxs" in payload:
            return _parse_all_dex_contexts(payload["ctxs"])
        result: dict[str, list[Any]] = {}
        for raw_dex, raw_contexts in payload.items():
            dex = "" if raw_dex in {"", "default"} else str(raw_dex)
            if not isinstance(raw_contexts, list):
                raise MarketIdentityError("aggregate DEX contexts must be arrays")
            result[dex] = raw_contexts
        return result
    if isinstance(payload, list):
        result = {}
        for item in payload:
            if isinstance(item, Mapping):
                raw_dex = item.get("dex", "")
                contexts = item.get("contexts", item.get("ctxs"))
            elif isinstance(item, (list, tuple)) and len(item) == 2:
                raw_dex, contexts = item
            else:
                raise MarketIdentityError("aggregate context entries must be DEX objects or pairs")
            dex = "" if raw_dex in {None, "", "default"} else str(raw_dex)
            if not isinstance(contexts, list) or dex in result:
                raise MarketIdentityError("aggregate context DEX entry is malformed")
            result[dex] = contexts
        return result
    raise MarketIdentityError("aggregate contexts must be an object or array")


def _optional_positive_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


@dataclass(frozen=True, slots=True)
class LastGoodMarketContext:
    """Last independently valid mark and mid observations for one frozen market."""

    symbol: str
    mark_px: Decimal | None = None
    mark_observed_ms: int | None = None
    mid_px: Decimal | None = None
    mid_observed_ms: int | None = None

    def __post_init__(self) -> None:
        if canonical_market(self.symbol) != self.symbol:
            raise MarketIdentityError("last-good market symbol must be canonical")
        _validate_price_pair(self.mark_px, self.mark_observed_ms, name="mark")
        _validate_price_pair(self.mid_px, self.mid_observed_ms, name="mid")
        if self.mark_px is None and self.mid_px is None:
            raise MarketIdentityError("last-good market context must contain a mark or mid")

    def to_payload(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "mark_px": _decimal_text(self.mark_px),
            "mark_observed_ms": self.mark_observed_ms,
            "mid_px": _decimal_text(self.mid_px),
            "mid_observed_ms": self.mid_observed_ms,
        }


@dataclass(frozen=True, slots=True)
class FrozenReductionMarketContext:
    """Frozen precision plus a fresh-enough last-good public price for risk reduction."""

    symbol: str
    sz_decimals: int
    reference_px: Decimal
    price_source: str
    price_observed_ms: int

    def __post_init__(self) -> None:
        FrozenMarketSpec(self.symbol, self.sz_decimals)
        if self.price_source not in {"mark", "mid"}:
            raise MarketIdentityError("reduction price_source must be mark or mid")
        _validate_price_pair(self.reference_px, self.price_observed_ms, name="reference")

    def to_payload(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "sz_decimals": self.sz_decimals,
            "reference_px": _decimal_text(self.reference_px),
            "price_source": self.price_source,
            "price_observed_ms": self.price_observed_ms,
        }


class FrozenMarketContextProvider:
    """Mutable last-good price cache strictly bounded to one immutable launch manifest."""

    def __init__(self, manifest: FrozenMarketUniverseManifest):
        if not isinstance(manifest, FrozenMarketUniverseManifest):
            raise TypeError("manifest must be a FrozenMarketUniverseManifest")
        self.manifest = manifest
        self._contexts: dict[str, LastGoodMarketContext] = {}

    def observe(
        self,
        symbol: Any,
        *,
        observed_ms: int,
        mark_px: Any | None = None,
        mid_px: Any | None = None,
    ) -> LastGoodMarketContext:
        """Record positive finite public prices without erasing an omitted last-good field."""

        market = self.manifest.market(symbol)
        if mark_px is None and mid_px is None:
            raise MarketIdentityError("market context observation must contain a mark or mid")
        _validate_observed_ms(observed_ms, name="market context")
        mark = _positive_decimal(mark_px, name="mark_px") if mark_px is not None else None
        mid = _positive_decimal(mid_px, name="mid_px") if mid_px is not None else None
        previous = self._contexts.get(market.symbol)

        previous_mark = previous.mark_px if previous is not None else None
        previous_mark_ms = previous.mark_observed_ms if previous is not None else None
        previous_mid = previous.mid_px if previous is not None else None
        previous_mid_ms = previous.mid_observed_ms if previous is not None else None

        next_mark, next_mark_ms = _merge_observation(
            previous_mark,
            previous_mark_ms,
            mark,
            observed_ms if mark is not None else None,
            name="mark",
        )
        next_mid, next_mid_ms = _merge_observation(
            previous_mid,
            previous_mid_ms,
            mid,
            observed_ms if mid is not None else None,
            name="mid",
        )
        context = LastGoodMarketContext(
            symbol=market.symbol,
            mark_px=next_mark,
            mark_observed_ms=next_mark_ms,
            mid_px=next_mid,
            mid_observed_ms=next_mid_ms,
        )
        self._contexts[market.symbol] = context
        return context

    def context(self, symbol: Any) -> LastGoodMarketContext | None:
        market = self.manifest.market(symbol)
        return self._contexts.get(market.symbol)

    def reduction_context(
        self,
        symbol: Any,
        *,
        now_ms: int,
        max_age_ms: int,
    ) -> FrozenReductionMarketContext:
        """Return safe reduction inputs, even if the market vanished from a fresh catalog."""

        market = self.manifest.market(symbol)
        _validate_observed_ms(now_ms, name="reduction now")
        if isinstance(max_age_ms, bool) or not isinstance(max_age_ms, int) or max_age_ms < 0:
            raise MarketIdentityError("reduction max_age_ms must be a non-negative integer")
        context = self._contexts.get(market.symbol)
        if context is None:
            raise MarketIdentityError(f"market {market.symbol} has no last-good price context")

        candidates: list[tuple[int, int, str, Decimal]] = []
        if context.mark_px is not None and context.mark_observed_ms is not None:
            if 0 <= now_ms - context.mark_observed_ms <= max_age_ms:
                candidates.append((context.mark_observed_ms, 1, "mark", context.mark_px))
        if context.mid_px is not None and context.mid_observed_ms is not None:
            if 0 <= now_ms - context.mid_observed_ms <= max_age_ms:
                candidates.append((context.mid_observed_ms, 0, "mid", context.mid_px))
        if not candidates:
            raise MarketIdentityError(
                f"market {market.symbol} has no price within reduction max_age_ms"
            )
        observed, _, source, price = max(candidates)
        return FrozenReductionMarketContext(
            symbol=market.symbol,
            sz_decimals=market.sz_decimals,
            reference_px=price,
            price_source=source,
            price_observed_ms=observed,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "manifest_sha256": self.manifest.sha256,
            "contexts": [
                self._contexts[symbol].to_payload()
                for symbol in self.manifest.symbols
                if symbol in self._contexts
            ],
        }


def _positive_decimal(value: Any, *, name: str) -> Decimal:
    if isinstance(value, bool):
        raise MarketIdentityError(f"{name} must be a positive finite decimal")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise MarketIdentityError(f"{name} must be a positive finite decimal") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise MarketIdentityError(f"{name} must be a positive finite decimal")
    return parsed


def _validate_observed_ms(value: Any, *, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise MarketIdentityError(f"{name} observed_ms must be a positive integer")


def _validate_price_pair(
    price: Decimal | None,
    observed_ms: int | None,
    *,
    name: str,
) -> None:
    if (price is None) != (observed_ms is None):
        raise MarketIdentityError(f"{name} price and observed_ms must be present together")
    if price is not None:
        if not isinstance(price, Decimal) or not price.is_finite() or price <= 0:
            raise MarketIdentityError(f"{name} price must be a positive finite Decimal")
        _validate_observed_ms(observed_ms, name=name)


def _merge_observation(
    previous: Decimal | None,
    previous_ms: int | None,
    observed: Decimal | None,
    observed_ms: int | None,
    *,
    name: str,
) -> tuple[Decimal | None, int | None]:
    if observed is None:
        return previous, previous_ms
    assert observed_ms is not None
    if previous_ms is not None and observed_ms < previous_ms:
        raise MarketIdentityError(f"{name} observation moved backwards")
    if previous_ms == observed_ms and previous is not None and observed != previous:
        raise MarketIdentityError(f"{name} observation conflicts at the same timestamp")
    return observed, observed_ms


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.normalize(), "f")
