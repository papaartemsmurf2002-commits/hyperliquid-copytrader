from __future__ import annotations

import json

import pytest

from hyperliquid_copytrader.markets import (
    MARKET_UNIVERSE_MANIFEST_VERSION,
    FrozenMarketSpec,
    FrozenMarketUniverseManifest,
    MarketIdentity,
    MarketIdentityError,
    active_perp_market_universe,
    build_frozen_market_universe_manifest,
    canonical_market,
    compare_market_universes,
    is_canonical_market,
    is_valid_market,
    market_catalog_fingerprint,
    market_universe_fingerprint,
    parse_market,
    perp_dex_names,
    qualify_market_symbol,
)


@pytest.mark.parametrize(
    ("raw", "canonical", "dex", "asset"),
    [
        ("BTC", "BTC", None, "BTC"),
        (" btc ", "BTC", None, "BTC"),
        ("kpepe", "KPEPE", None, "KPEPE"),
        ("kPEPE", "kPEPE", None, "kPEPE"),
        ("$eviv", "$EVIV", None, "$EVIV"),
        ("hb:BTC-29AUG25-120000-C", "hb:BTC-29AUG25-120000-C", "hb", "BTC-29AUG25-120000-C"),
        ("xyz:AAPL", "xyz:AAPL", "xyz", "AAPL"),
        ("XYZ:aapl", "XYZ:AAPL", "XYZ", "AAPL"),
    ],
)
def test_parse_market_returns_canonical_identity(raw, canonical, dex, asset):
    market = parse_market(raw)

    assert market.canonical == canonical
    assert str(market) == canonical
    assert market.dex == dex
    assert market.asset == asset
    assert market.is_default_perp is (dex is None)


@pytest.mark.parametrize(
    "raw",
    [
        None,
        1,
        "",
        "   ",
        ":AAPL",
        "xyz:",
        "xyz:AAPL:PERP",
        "xyz:AAPL/USD",
        "xy z:AAPL",
        "xyz:.AAPL",
        "_xyz:AAPL",
        "xyz:_AAPL",
        "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFG",
    ],
)
def test_parse_market_rejects_empty_ambiguous_or_unsafe_values(raw):
    with pytest.raises(MarketIdentityError):
        parse_market(raw)
    assert is_valid_market(raw) is False


def test_canonical_helpers_distinguish_valid_legacy_case_from_canonical_form():
    assert canonical_market("btc") == "BTC"
    assert canonical_market("XYZ:aapl") == "XYZ:AAPL"
    assert is_canonical_market("BTC") is True
    assert is_canonical_market("xyz:AAPL") is True
    assert is_canonical_market("btc") is False
    assert is_canonical_market("XYZ:aapl") is False


def test_official_testnet_dex_name_with_angle_bracket_is_preserved():
    assert canonical_market("I<3FL:foo") == "I<3FL:FOO"
    assert qualify_market_symbol("i<3fl", "FOO") == "i<3fl:FOO"


def test_active_perp_market_universe_is_dex_qualified_sorted_and_excludes_delisted():
    payload = {
        "universe": [
            {"name": "SKHY", "szDecimals": 2},
            {"name": "xyz:JP225", "szDecimals": 1},
            {"name": "OLD", "isDelisted": True},
        ]
    }

    universe = active_perp_market_universe(payload, dex="xyz")

    assert universe == ("xyz:JP225", "xyz:SKHY")
    assert market_universe_fingerprint(universe) == market_universe_fingerprint(reversed(universe))


def test_active_perp_market_universe_rejects_duplicate_or_malformed_metadata():
    with pytest.raises(MarketIdentityError, match="duplicate market BTC"):
        active_perp_market_universe({"universe": [{"name": "BTC"}, {"name": "btc"}]})
    with pytest.raises(MarketIdentityError, match="universe array"):
        active_perp_market_universe({})


def test_frozen_full_perp_catalog_is_sorted_serializable_and_time_independent():
    dexs = [None, {"name": "xyz"}, {"name": "KNETIQ"}]
    metas = [
        {
            "universe": [
                {"name": "SOL", "szDecimals": 2},
                {"name": "BTC", "szDecimals": 5},
                {"name": "OLD", "szDecimals": 1, "isDelisted": True},
            ]
        },
        {
            "universe": [
                {"name": "xyz:SKHY", "szDecimals": 4},
                {"name": "SKHX", "szDecimals": 3},
            ]
        },
        {"universe": [{"name": "KNETIQ:INJ", "szDecimals": 2}]},
    ]

    first = build_frozen_market_universe_manifest(
        network="MAINNET",
        observed_ms=1_000,
        perp_dexs_payload=dexs,
        all_perp_metas_payload=metas,
    )
    later = build_frozen_market_universe_manifest(
        network="mainnet",
        observed_ms=2_000,
        perp_dexs_payload=dexs,
        all_perp_metas_payload=metas,
    )

    assert first.version == MARKET_UNIVERSE_MANIFEST_VERSION
    assert first.network == "mainnet"
    assert first.dexes == ("", "KNETIQ", "xyz")
    assert first.symbols == ("BTC", "SOL", "KNETIQ:INJ", "xyz:SKHX", "xyz:SKHY")
    assert tuple(market.sz_decimals for market in first.markets) == (5, 2, 2, 3, 4)
    assert "OLD" not in first.symbols
    assert first.sha256 == later.sha256
    assert len(first.sha256) == 64
    assert json.loads(json.dumps(first.to_payload()))["markets"][0] == {
        "symbol": "BTC",
        "sz_decimals": 5,
    }
    assert compare_market_universes(first, later).changed is False
    assert (
        market_catalog_fingerprint(
            network=first.network,
            dexes=first.dexes,
            markets=reversed(first.markets),
        )
        == first.sha256
    )


def test_frozen_catalog_round_trips_from_persisted_payload():
    manifest = build_frozen_market_universe_manifest(
        network="mainnet",
        observed_ms=123,
        perp_dexs_payload=[None, {"name": "xyz"}],
        all_perp_metas_payload=[
            {"universe": [{"name": "BTC", "szDecimals": 5}]},
            {"universe": [{"name": "AAPL", "szDecimals": 2}]},
        ],
    )

    restored = FrozenMarketUniverseManifest.from_payload(
        json.loads(json.dumps(manifest.to_payload()))
    )

    assert restored == manifest


def test_frozen_catalog_rejects_persisted_symbol_or_hash_tampering():
    manifest = build_frozen_market_universe_manifest(
        network="mainnet",
        observed_ms=123,
        perp_dexs_payload=[None],
        all_perp_metas_payload=[{"universe": [{"name": "BTC", "szDecimals": 5}]}],
    )
    payload = manifest.to_payload()
    payload["symbols"] = ["ETH"]
    with pytest.raises(MarketIdentityError, match="symbols do not match"):
        FrozenMarketUniverseManifest.from_payload(payload)

    payload = manifest.to_payload()
    payload["sha256"] = "0" * 64
    with pytest.raises(MarketIdentityError, match="SHA256"):
        FrozenMarketUniverseManifest.from_payload(payload)


def test_catalog_drift_reports_add_remove_and_precision_change_deterministically():
    expected = build_frozen_market_universe_manifest(
        network="mainnet",
        observed_ms=1,
        perp_dexs_payload=[None, {"name": "xyz"}],
        all_perp_metas_payload=[
            {"universe": [{"name": "BTC", "szDecimals": 5}]},
            {"universe": [{"name": "AAPL", "szDecimals": 3}]},
        ],
    )
    observed = build_frozen_market_universe_manifest(
        network="mainnet",
        observed_ms=2,
        perp_dexs_payload=[None, {"name": "flx"}],
        all_perp_metas_payload=[
            {"universe": [{"name": "BTC", "szDecimals": 4}]},
            {"universe": [{"name": "GOLD", "szDecimals": 2}]},
        ],
    )

    drift = compare_market_universes(expected, observed)

    assert drift.changed is True
    assert drift.added_dexes == ("flx",)
    assert drift.removed_dexes == ("xyz",)
    assert drift.added_symbols == ("flx:GOLD",)
    assert drift.removed_symbols == ("xyz:AAPL",)
    assert [change.to_payload() for change in drift.precision_changes] == [
        {
            "symbol": "BTC",
            "expected_sz_decimals": 5,
            "observed_sz_decimals": 4,
        }
    ]
    assert json.loads(json.dumps(drift.to_payload()))["changed"] is True


@pytest.mark.parametrize(
    ("dexs", "metas", "match"),
    [
        (
            [None, {"name": "xyz"}],
            [{"universe": [{"name": "BTC", "szDecimals": 5}]}],
            "same number",
        ),
        ([None, {"name": "xyz"}, {"name": "xyz"}], [{}, {}, {}], "duplicate DEX"),
        ([None], [[{"universe": []}, []]], "must be an object"),
        ([None], [{"universe": [{"name": "xyz:AAPL", "szDecimals": 2}]}], "conflicts"),
        (
            [None, {"name": "xyz"}],
            [
                {"universe": [{"name": "BTC", "szDecimals": 5}]},
                {"universe": [{"name": "other:AAPL", "szDecimals": 2}]},
            ],
            "conflicts",
        ),
        ([None], [{"universe": [{"name": "BTC", "szDecimals": True}]}], "integer"),
        (
            [None],
            [{"universe": [{"name": "BTC", "szDecimals": 2, "isDelisted": "yes"}]}],
            "boolean",
        ),
    ],
)
def test_frozen_catalog_rejects_malformed_duplicate_or_conflicting_wire_shapes(dexs, metas, match):
    with pytest.raises(MarketIdentityError, match=match):
        build_frozen_market_universe_manifest(
            network="mainnet",
            observed_ms=1,
            perp_dexs_payload=dexs,
            all_perp_metas_payload=metas,
        )


def test_perp_dex_names_preserves_wire_order_for_all_perp_meta_alignment():
    assert perp_dex_names([None, {"name": "xyz"}, {"name": "KNETIQ"}]) == (
        "",
        "xyz",
        "KNETIQ",
    )
    with pytest.raises(MarketIdentityError, match="default null"):
        perp_dex_names([{"name": "default"}])


def test_frozen_market_spec_rejects_noncanonical_symbol_or_unsafe_precision():
    with pytest.raises(MarketIdentityError, match="canonical"):
        FrozenMarketSpec("btc", 5)
    with pytest.raises(MarketIdentityError, match="between"):
        FrozenMarketSpec("BTC", 19)


@pytest.mark.parametrize(
    "market",
    [
        lambda: MarketIdentity(asset="btc"),
        lambda: MarketIdentity(asset="AAPL", dex="bad dex"),
        lambda: MarketIdentity(asset="AAPL", dex=""),
        lambda: MarketIdentity(asset="AAPL/USD", dex="xyz"),
    ],
)
def test_market_identity_constructor_enforces_canonical_invariants(market):
    with pytest.raises(MarketIdentityError):
        market()
