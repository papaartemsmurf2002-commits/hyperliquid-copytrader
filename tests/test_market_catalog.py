from __future__ import annotations

import asyncio
import json
import threading
from copy import deepcopy
from decimal import Decimal
from typing import Any

import pytest

from hyperliquid_copytrader.market_catalog import (
    CatalogRevision,
    FrozenMarketContextProvider,
    MarketCatalogActor,
    MarketReadiness,
    build_dynamic_catalog_revision,
    resolve_public_market_universe,
)
from hyperliquid_copytrader.markets import (
    MarketIdentityError,
    build_frozen_market_universe_manifest,
    compare_market_universes,
)
from hyperliquid_copytrader.persistence import SQLiteStore


class FakePublicInfoClient:
    def __init__(self, responses: list[Any]):
        self._responses = iter(responses)
        self.calls: list[dict[str, Any]] = []

    def info(self, payload: dict[str, Any]) -> Any:
        self.calls.append(dict(payload))
        return next(self._responses)


class JournalStub:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def submit(self, operation: str, **payload: Any) -> None:
        self.calls.append((operation, payload))


class BlockingRefreshClient:
    def __init__(self, responses: list[Any], *, block_call: int) -> None:
        self._responses = iter(responses)
        self.block_call = block_call
        self.calls: list[dict[str, Any]] = []
        self.blocked = threading.Event()
        self.release = threading.Event()
        self._lock = threading.Lock()

    def info(self, payload: dict[str, Any]) -> Any:
        with self._lock:
            self.calls.append(dict(payload))
            call_number = len(self.calls)
            response = next(self._responses)
        if call_number == self.block_call:
            self.blocked.set()
            if not self.release.wait(2):
                raise TimeoutError("test refresh was not released")
        return response


def _meta(*entries: dict[str, Any]) -> dict[str, Any]:
    return {"universe": list(entries)}


def _manifest(
    *entries: dict[str, Any],
    observed_ms: int = 100,
):
    return build_frozen_market_universe_manifest(
        network="mainnet",
        observed_ms=observed_ms,
        perp_dexs_payload=[None],
        all_perp_metas_payload=[_meta(*entries)],
    )


def test_public_catalog_resolution_uses_only_exact_bounded_unsigned_info_calls():
    dexs = [None, {"name": "xyz"}]
    metas = [
        _meta({"name": "BTC", "szDecimals": 5}),
        _meta({"name": "SKHY", "szDecimals": 3}),
    ]
    client = FakePublicInfoClient([dexs, metas, dexs])

    manifest = resolve_public_market_universe(client, network="mainnet", observed_ms=123)

    assert client.calls == [
        {"type": "perpDexs"},
        {"type": "allPerpMetas"},
        {"type": "perpDexs"},
    ]
    assert manifest.symbols == ("BTC", "xyz:SKHY")
    assert all(set(call) == {"type"} for call in client.calls)
    assert all(call["type"] not in {"exchange", "order", "action"} for call in client.calls)


def test_dynamic_catalog_preserves_margin_and_collateral_semantics() -> None:
    revision = build_dynamic_catalog_revision(
        network="mainnet",
        policy_version="dynamic-v1",
        sequence=1,
        observed_ms=100,
        dexes_before_payload=[None, {"name": "xyz"}],
        all_perp_metas_payload=[
            _meta({"name": "BTC", "szDecimals": 5, "maxLeverage": 50}),
            {
                "universe": [
                    {
                        "name": "XYZ100",
                        "szDecimals": 3,
                        "maxLeverage": 10,
                        "marginMode": "strictIsolated",
                        "marginTableId": 10,
                    }
                ],
                "collateralToken": 7,
            },
        ],
        dexes_after_payload=[None, {"name": "xyz"}],
    )

    native = revision.market("BTC")
    hip3 = revision.market("xyz:XYZ100")
    assert native is not None and (native.margin_mode, native.collateral_token) == ("cross", 0)
    assert hip3 is not None
    assert (hip3.margin_mode, hip3.collateral_token, hip3.margin_table_id) == (
        "strictIsolated",
        7,
        10,
    )


def test_public_catalog_resolution_rejects_dex_change_during_snapshot():
    client = FakePublicInfoClient(
        [
            [None, {"name": "xyz"}],
            [_meta({"name": "BTC", "szDecimals": 5})],
            [None, {"name": "flx"}],
        ]
    )

    with pytest.raises(MarketIdentityError, match="changed during unsigned snapshot"):
        resolve_public_market_universe(client, network="mainnet", observed_ms=123)


def test_public_catalog_resolution_rejects_meta_and_asset_context_pair_shape():
    dexs = [None]
    client = FakePublicInfoClient(
        [
            dexs,
            [[_meta({"name": "BTC", "szDecimals": 5}), [{"markPx": "100"}]]],
            dexs,
        ]
    )

    with pytest.raises(MarketIdentityError, match="must be an object"):
        resolve_public_market_universe(client, network="mainnet", observed_ms=123)


def test_hip3_asset_ids_retain_the_wire_dex_index() -> None:
    dexs = [None, {"name": "xyz"}, {"name": "flx"}]
    revision = build_dynamic_catalog_revision(
        network="mainnet",
        policy_version="dynamic-v1",
        sequence=1,
        observed_ms=123,
        dexes_before_payload=dexs,
        all_perp_metas_payload=[
            _meta({"name": "BTC", "szDecimals": 5, "maxLeverage": 20}),
            _meta({"name": "XYZ100", "szDecimals": 3, "maxLeverage": 10}),
            _meta({"name": "TSLA", "szDecimals": 2, "maxLeverage": 10}),
        ],
        dexes_after_payload=dexs,
    )

    assert revision.market("BTC").asset_id == 0
    assert revision.market("xyz:XYZ100").asset_id == 110_000
    assert revision.market("flx:TSLA").asset_id == 120_000


@pytest.mark.asyncio
async def test_live_all_dex_context_wrapper_and_pair_shape_is_accepted() -> None:
    dexs = [None, {"name": "xyz"}]
    actor = MarketCatalogActor(
        client=FakePublicInfoClient(
            [
                dexs,
                [
                    _meta({"name": "BTC", "szDecimals": 5, "maxLeverage": 20}),
                    _meta({"name": "XYZ100", "szDecimals": 3, "maxLeverage": 10}),
                ],
                dexs,
            ]
        ),
        network="mainnet",
        policy_version="dynamic-v1",
        journal=JournalStub(),
    )
    await actor.refresh(observed_ms=100)

    revision = await actor.observe_all_dex_contexts(
        {
            "ctxs": [
                ["", [{"oraclePx": "50000", "markPx": "50001"}]],
                ["xyz", [{"oraclePx": "100", "markPx": "101"}]],
            ]
        },
        observed_ms=200,
    )

    assert revision.market("BTC").mark_px == Decimal("50001")
    assert revision.market("xyz:XYZ100").oracle_px == Decimal("100")


@pytest.mark.asyncio
async def test_blocked_identity_refresh_does_not_starve_context_or_book_frames() -> None:
    dexs = [None]
    metas = [_meta({"name": "BTC", "szDecimals": 5, "maxLeverage": 20})]
    client = BlockingRefreshClient(
        [dexs, metas, dexs, dexs, metas, dexs],
        block_call=4,
    )
    actor = MarketCatalogActor(
        client=client,
        network="mainnet",
        policy_version="dynamic-v1",
        journal=JournalStub(),
    )
    await actor.refresh(observed_ms=100)

    refresh_task = asyncio.create_task(actor.refresh(observed_ms=300))
    assert await asyncio.to_thread(client.blocked.wait, 1)
    revised = await asyncio.wait_for(
        actor.observe_all_dex_contexts(
            {"ctxs": [["", [{"oraclePx": "50000", "markPx": "50001"}]]]},
            observed_ms=200,
        ),
        timeout=0.25,
    )
    assert revised.market("BTC").context_observed_ms == 200
    ready = await asyncio.wait_for(
        actor.observe_book("BTC", book_revision=1, observed_ms=201),
        timeout=0.25,
    )
    assert ready.readiness is MarketReadiness.READY

    client.release.set()
    committed = await refresh_task
    assert committed.market("BTC").context_observed_ms == 200
    assert committed.market("BTC").book_revision == 1
    assert committed.market("BTC").readiness is MarketReadiness.READY


@pytest.mark.asyncio
async def test_concurrent_identity_refreshes_keep_three_call_brackets_serialized() -> None:
    dexs = [None]
    metas = [_meta({"name": "BTC", "szDecimals": 5, "maxLeverage": 20})]
    client = BlockingRefreshClient(
        [dexs, metas, dexs, dexs, metas, dexs],
        block_call=1,
    )
    actor = MarketCatalogActor(
        client=client,
        network="mainnet",
        policy_version="dynamic-v1",
        journal=JournalStub(),
    )

    first = asyncio.create_task(actor.refresh(observed_ms=100))
    assert await asyncio.to_thread(client.blocked.wait, 1)
    second = asyncio.create_task(actor.refresh(observed_ms=200))
    await asyncio.sleep(0.05)
    assert client.calls == [{"type": "perpDexs"}]
    client.release.set()
    await asyncio.gather(first, second)

    assert client.calls == [
        {"type": "perpDexs"},
        {"type": "allPerpMetas"},
        {"type": "perpDexs"},
        {"type": "perpDexs"},
        {"type": "allPerpMetas"},
        {"type": "perpDexs"},
    ]


@pytest.mark.asyncio
async def test_invalid_context_frame_cannot_be_repromoted_by_a_fresh_book() -> None:
    dexs = [None]
    actor = MarketCatalogActor(
        client=FakePublicInfoClient(
            [
                dexs,
                [_meta({"name": "BTC", "szDecimals": 5, "maxLeverage": 20})],
                dexs,
            ]
        ),
        network="mainnet",
        policy_version="dynamic-v1",
        journal=JournalStub(),
    )
    await actor.refresh(observed_ms=100)
    await actor.observe_all_dex_contexts(
        {"ctxs": [["", [{"oraclePx": "50000", "markPx": "50001"}]]]},
        observed_ms=200,
    )
    ready = await actor.observe_book("BTC", book_revision=1, observed_ms=201)
    assert ready.readiness is MarketReadiness.READY

    invalid = await actor.observe_all_dex_contexts({"ctxs": []}, observed_ms=202)
    assert invalid.market("BTC").readiness is MarketReadiness.NO_CONTEXT
    assert invalid.market("BTC").context_observed_ms == 0
    still_blocked = await actor.observe_book("BTC", book_revision=2, observed_ms=203)
    assert still_blocked.readiness is MarketReadiness.NO_CONTEXT

    await actor.observe_all_dex_contexts(
        {"ctxs": [["", [{"oraclePx": "50002", "markPx": "50003"}]]]},
        observed_ms=204,
    )
    assert (
        await actor.observe_book("BTC", book_revision=3, observed_ms=205)
    ).readiness is MarketReadiness.READY
    invalidated = await actor.invalidate_all_contexts(
        observed_ms=206,
        reason="malformed_frame",
        frame_payload="not-json-object",
    )
    assert invalidated.market("BTC").context_observed_ms == 0
    assert (
        await actor.observe_book("BTC", book_revision=4, observed_ms=207)
    ).readiness is MarketReadiness.NO_CONTEXT


@pytest.mark.asyncio
async def test_active_market_context_is_native_hip3_and_failure_local() -> None:
    dexs = [None, {"name": "xyz"}]
    actor = MarketCatalogActor(
        client=FakePublicInfoClient(
            [
                dexs,
                [
                    _meta({"name": "BTC", "szDecimals": 5, "maxLeverage": 20}),
                    _meta({"name": "XYZ100", "szDecimals": 3, "maxLeverage": 10}),
                ],
                dexs,
            ]
        ),
        network="mainnet",
        policy_version="dynamic-v1",
        journal=JournalStub(),
    )
    await actor.refresh(observed_ms=100)
    await actor.observe_active_context(
        {"coin": "BTC", "ctx": {"oraclePx": "50000", "markPx": "50001"}},
        observed_ms=200,
    )
    await actor.observe_book("BTC", book_revision=1, observed_ms=201)
    await actor.observe_active_context(
        {"coin": "xyz:XYZ100", "ctx": {"oraclePx": "100", "markPx": "101"}},
        observed_ms=202,
    )
    await actor.observe_book("xyz:XYZ100", book_revision=1, observed_ms=203)

    malformed = await actor.observe_active_context({"coin": "BTC", "ctx": "bad"}, observed_ms=204)

    assert malformed.market("BTC").readiness is MarketReadiness.NO_CONTEXT
    assert malformed.market("BTC").context_observed_ms == 0
    assert malformed.market("xyz:XYZ100").readiness is MarketReadiness.READY
    assert malformed.market("xyz:XYZ100").oracle_px == Decimal("100")
    assert actor.health()["context_anomaly_count"] == 1
    assert actor.health()["pending_refresh_reasons"] == 1

    restored = await actor.observe_active_context(
        {"coin": "BTC", "ctx": {"oraclePx": "50002", "markPx": "50003"}},
        observed_ms=205,
    )
    assert restored.market("BTC").readiness is MarketReadiness.READY
    invalidated = await actor.invalidate_market_context(
        "BTC", observed_ms=206, reason="subscription_removed"
    )
    assert invalidated.market("BTC").readiness is MarketReadiness.NO_CONTEXT
    assert invalidated.market("xyz:XYZ100").readiness is MarketReadiness.READY


@pytest.mark.asyncio
async def test_catalog_identity_error_is_not_cleared_by_context_and_expires() -> None:
    dexs = [None]
    actor = MarketCatalogActor(
        client=FakePublicInfoClient(
            [
                dexs,
                [_meta({"name": "BTC", "szDecimals": 5, "maxLeverage": 20})],
                dexs,
            ]
        ),
        network="mainnet",
        policy_version="dynamic-v1",
        journal=JournalStub(),
        identity_max_age_ms=60_000,
    )
    await actor.refresh(observed_ms=100)
    assert actor.identity_healthy(60_100) is True
    assert actor.identity_healthy(60_101) is False
    actor.record_refresh_error("metadata unavailable")
    await actor.observe_all_dex_contexts(
        {"ctxs": [["", [{"oraclePx": "50000", "markPx": "50001"}]]]},
        observed_ms=101,
    )
    assert actor.identity_healthy(101) is False


def test_immediate_catalog_refresh_requests_are_coalesced() -> None:
    actor = MarketCatalogActor(
        client=FakePublicInfoClient([]),
        network="mainnet",
        policy_version="dynamic-v1",
        journal=JournalStub(),
    )

    accepted = [actor.request_immediate_refresh(f"alignment:{index}") for index in range(1_000)]

    assert accepted.count(True) == 1
    health = actor.health()
    assert health["pending_refresh_reasons"] == 1
    assert health["refresh_coalesced"] == 999
    assert health["last_refresh_reason"] == "alignment:999"


def test_persisted_catalog_identity_payload_is_hash_verified() -> None:
    revision = build_dynamic_catalog_revision(
        network="mainnet",
        policy_version="dynamic-v1",
        sequence=1,
        observed_ms=100,
        dexes_before_payload=[None],
        all_perp_metas_payload=[_meta({"name": "BTC", "szDecimals": 5, "maxLeverage": 20})],
        dexes_after_payload=[None],
    )
    tampered = deepcopy(revision.to_payload())
    tampered["markets"][0]["sz_decimals"] = 3

    with pytest.raises(MarketIdentityError, match="does not match snapshot hash"):
        CatalogRevision.from_payload(tampered)


def test_delisted_drift_blocks_adoption_but_frozen_reduction_context_remains_available():
    launch = _manifest(
        {"name": "BTC", "szDecimals": 5},
        {"name": "OLD", "szDecimals": 2},
    )
    provider = FrozenMarketContextProvider(launch)
    provider.observe("OLD", observed_ms=1_000, mark_px="10", mid_px="9.9")
    provider.observe("OLD", observed_ms=1_100, mark_px="10.1")
    fresh = _manifest(
        {"name": "BTC", "szDecimals": 5},
        {"name": "OLD", "szDecimals": 2, "isDelisted": True},
        observed_ms=1_200,
    )

    drift = compare_market_universes(launch, fresh)
    reduction = provider.reduction_context("OLD", now_ms=1_150, max_age_ms=200)

    assert drift.changed is True
    assert drift.removed_symbols == ("OLD",)
    assert "OLD" not in fresh.symbols
    assert reduction.symbol == "OLD"
    assert reduction.sz_decimals == 2
    assert reduction.reference_px == Decimal("10.1")
    assert reduction.price_source == "mark"
    assert reduction.price_observed_ms == 1_100
    assert json.loads(json.dumps(provider.to_payload())) == {
        "manifest_sha256": launch.sha256,
        "contexts": [
            {
                "symbol": "OLD",
                "mark_px": "10.1",
                "mark_observed_ms": 1_100,
                "mid_px": "9.9",
                "mid_observed_ms": 1_000,
            }
        ],
    }


def test_reduction_context_rejects_stale_future_or_missing_prices():
    manifest = _manifest({"name": "BTC", "szDecimals": 5})
    provider = FrozenMarketContextProvider(manifest)
    with pytest.raises(MarketIdentityError, match="no last-good"):
        provider.reduction_context("BTC", now_ms=200, max_age_ms=100)

    provider.observe("BTC", observed_ms=100, mid_px="50000")
    with pytest.raises(MarketIdentityError, match="no price within"):
        provider.reduction_context("BTC", now_ms=201, max_age_ms=100)
    with pytest.raises(MarketIdentityError, match="no price within"):
        provider.reduction_context("BTC", now_ms=99, max_age_ms=100)


def test_last_good_provider_rejects_invalid_out_of_order_or_conflicting_observations():
    manifest = _manifest({"name": "BTC", "szDecimals": 5})
    provider = FrozenMarketContextProvider(manifest)
    provider.observe("BTC", observed_ms=100, mark_px="50000")

    with pytest.raises(MarketIdentityError, match="moved backwards"):
        provider.observe("BTC", observed_ms=99, mark_px="50001")
    with pytest.raises(MarketIdentityError, match="conflicts at the same timestamp"):
        provider.observe("BTC", observed_ms=100, mark_px="50001")
    with pytest.raises(MarketIdentityError, match="positive finite"):
        provider.observe("BTC", observed_ms=101, mid_px="NaN")
    with pytest.raises(MarketIdentityError, match="not present"):
        provider.observe("ETH", observed_ms=101, mid_px="3000")

    assert provider.context("BTC").mark_px == Decimal("50000")


@pytest.mark.asyncio
async def test_active_identity_mutation_retains_last_accepted_execution_identity():
    dexs = [None]
    client = FakePublicInfoClient(
        [
            dexs,
            [_meta({"name": "BTC", "szDecimals": 5, "maxLeverage": 20})],
            dexs,
            dexs,
            [_meta({"name": "BTC", "szDecimals": 3, "maxLeverage": 10})],
            dexs,
        ]
    )
    journal = JournalStub()
    actor = MarketCatalogActor(
        client=client,
        network="mainnet",
        policy_version="dynamic-v1",
        journal=journal,
    )
    first = await actor.refresh(observed_ms=100)

    second = await actor.refresh(active_markets={"BTC"}, observed_ms=200)

    before = first.market("BTC")
    after = second.market("BTC")
    assert before is not None and after is not None
    assert (after.asset_id, after.sz_decimals, after.max_leverage) == (
        before.asset_id,
        before.sz_decimals,
        before.max_leverage,
    )
    assert after.readiness is MarketReadiness.UNTRUSTED
    assert after.pending_asset_id == 0
    assert after.pending_sz_decimals == 3
    assert after.pending_max_leverage == 10
    assert after.pending_identity_revision == second.revision_id
    diffs = journal.calls[-1][1]["diffs"]
    assert [item["change_class"] for item in diffs] == ["identity_mutation"]

    aligned = await actor.observe_all_dex_contexts(
        {"ctxs": [["", [{"oraclePx": "50000", "markPx": "50001"}]]]},
        observed_ms=201,
    )
    retained = aligned.market("BTC")
    assert retained is not None
    assert retained.readiness is MarketReadiness.UNTRUSTED
    assert retained.asset_id == before.asset_id
    assert retained.sz_decimals == before.sz_decimals


@pytest.mark.asyncio
async def test_active_margin_and_collateral_mutation_is_blocked_and_round_trips():
    dexs = [None]
    before_meta = {
        "collateralToken": 0,
        "universe": [
            {
                "name": "BTC",
                "szDecimals": 5,
                "maxLeverage": 20,
                "marginMode": "cross",
                "marginTableId": 1,
            }
        ],
    }
    after_meta = {
        "collateralToken": 1,
        "universe": [
            {
                "name": "BTC",
                "szDecimals": 5,
                "maxLeverage": 20,
                "marginMode": "strictIsolated",
                "marginTableId": 2,
            }
        ],
    }
    journal = JournalStub()
    actor = MarketCatalogActor(
        client=FakePublicInfoClient([dexs, [before_meta], dexs, dexs, [after_meta], dexs]),
        network="mainnet",
        policy_version="dynamic-v1",
        journal=journal,
    )
    first = await actor.refresh(observed_ms=100)

    second = await actor.refresh(active_markets={"BTC"}, observed_ms=200)

    before = first.market("BTC")
    after = second.market("BTC")
    assert before is not None and after is not None
    assert (after.margin_mode, after.collateral_token, after.margin_table_id) == (
        before.margin_mode,
        before.collateral_token,
        before.margin_table_id,
    )
    assert after.readiness is MarketReadiness.UNTRUSTED
    assert after.pending_margin_mode == "strictIsolated"
    assert after.pending_collateral_token == 1
    assert after.pending_margin_table_id == 2
    assert [item["change_class"] for item in journal.calls[-1][1]["diffs"]] == ["identity_mutation"]
    restored = CatalogRevision.from_payload(second.to_payload())
    assert restored.market("BTC") == after


@pytest.mark.asyncio
async def test_removed_market_keeps_tombstone_and_explicit_removed_evidence():
    dexs = [None]
    client = FakePublicInfoClient(
        [
            dexs,
            [_meta({"name": "BTC", "szDecimals": 5, "maxLeverage": 20})],
            dexs,
            dexs,
            [_meta()],
            dexs,
        ]
    )
    journal = JournalStub()
    actor = MarketCatalogActor(
        client=client,
        network="mainnet",
        policy_version="dynamic-v1",
        journal=journal,
    )
    first = await actor.refresh(observed_ms=100)

    second = await actor.refresh(active_markets={"BTC"}, observed_ms=200)

    removed = second.market("BTC")
    assert removed is not None
    assert removed.asset_id == first.market("BTC").asset_id
    assert removed.removal_tombstone is True
    assert removed.readiness is MarketReadiness.DELISTED
    assert removed.tombstoned_from_revision == first.revision_id
    diffs = journal.calls[-1][1]["diffs"]
    assert [item["change_class"] for item in diffs] == ["removed"]


def test_lightweight_refresh_retains_removed_exposure_as_close_only_tombstone():
    first = build_dynamic_catalog_revision(
        network="mainnet",
        policy_version="continuous-ws-v1",
        sequence=1,
        observed_ms=100,
        dexes_before_payload=[None],
        all_perp_metas_payload=[_meta({"name": "BTC", "szDecimals": 5, "maxLeverage": 20})],
        dexes_after_payload=[None],
    )
    second = build_dynamic_catalog_revision(
        network="mainnet",
        policy_version="continuous-ws-v1",
        sequence=2,
        observed_ms=200,
        dexes_before_payload=[None],
        all_perp_metas_payload=[_meta()],
        dexes_after_payload=[None],
        previous=first,
        retain_symbols={"BTC"},
    )
    retained = second.market("BTC")
    assert retained is not None
    assert retained.removal_tombstone is True
    assert retained.is_delisted is True
    assert retained.readiness is MarketReadiness.DELISTED


def test_catalog_revision_and_never_reuse_history_restore_durably(tmp_path):
    revision = build_dynamic_catalog_revision(
        network="mainnet",
        policy_version="dynamic-v1",
        sequence=1,
        observed_ms=100,
        dexes_before_payload=[None],
        all_perp_metas_payload=[_meta({"name": "BTC", "szDecimals": 5, "maxLeverage": 20})],
        dexes_after_payload=[None],
    )
    payload = revision.to_payload()
    store = SQLiteStore(tmp_path / "execution.sqlite3")
    store.append_catalog_revision(
        revision_id=revision.revision_id,
        policy_version=revision.policy_version,
        snapshot_sha256=revision.snapshot_sha256,
        dex_bracket_before_sha256=revision.dex_bracket_before_sha256,
        dex_bracket_after_sha256=revision.dex_bracket_after_sha256,
        payload=payload,
    )

    recovery = store.fast_runtime_recovery(generation="generation-1")
    restored_actor = MarketCatalogActor(
        client=FakePublicInfoClient([]),
        network="mainnet",
        policy_version="dynamic-v1",
        journal=JournalStub(),
    )
    restored_actor.restore(recovery["catalog_revision"], recovery["catalog_asset_history"])
    assert restored_actor.current is not None
    assert restored_actor.current.revision_id == revision.revision_id

    conflicting = deepcopy(payload)
    conflicting["revision_id"] = "catalog-000002-conflict"
    conflicting["snapshot_sha256"] = "c" * 64
    conflicting["markets"][0]["symbol"] = "ETH"
    conflicting["markets"][0].update(
        {
            "readiness": "UNTRUSTED",
            "pending_asset_id": 0,
            "pending_universe_index": 0,
            "pending_sz_decimals": 5,
            "pending_max_leverage": 20,
            "pending_identity_revision": conflicting["revision_id"],
        }
    )
    store.append_catalog_revision(
        revision_id=conflicting["revision_id"],
        policy_version="dynamic-v1",
        snapshot_sha256=conflicting["snapshot_sha256"],
        dex_bracket_before_sha256=revision.dex_bracket_before_sha256,
        dex_bracket_after_sha256=revision.dex_bracket_after_sha256,
        payload=conflicting,
    )
    history = store.fast_runtime_recovery(generation="generation-1")["catalog_asset_history"]
    assert history == [
        {
            "asset_id": 0,
            "canonical_market": "BTC",
            "first_revision_id": revision.revision_id,
        }
    ]
    store.close()
