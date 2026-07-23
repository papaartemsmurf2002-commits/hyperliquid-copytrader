from __future__ import annotations

from decimal import Decimal

import pytest

from hyperliquid_copytrader.account_stream import (
    AccountStream,
    AccountStreamError,
    follower_subscription_specs,
    source_subscription_specs,
)


SOURCE = "0x1111111111111111111111111111111111111111"
FOLLOWER = "0x2222222222222222222222222222222222222222"
NOW = 1_000_000


def _position_state(
    time_ms: int,
    *positions: tuple[str, str],
    account_value: str | None = None,
) -> dict:
    state = {
        "time": time_ms,
        "assetPositions": [
            {
                "position": {
                    "coin": coin,
                    "szi": size,
                    "entryPx": "100",
                    "leverage": {"type": "cross", "value": 5},
                }
            }
            for coin, size in positions
        ],
    }
    if account_value is not None:
        state["marginSummary"] = {"accountValue": account_value}
    return state


def _positions(user: str, *states: tuple[str, dict]) -> dict:
    return {
        "channel": "allDexsClearinghouseState",
        "data": {"user": user, "clearinghouseStates": [list(state) for state in states]},
    }


def _spot(user: str, *, total: str = "100", hold: str = "2", time_ms: int = NOW) -> dict:
    return {
        "channel": "spotState",
        "data": {
            "user": user,
            "time": time_ms,
            "balances": [
                {"coin": "PURR", "token": 1, "total": "5", "hold": "0"},
                {"coin": "o458", "total": "0", "hold": "0"},
                {"coin": "USDC", "token": 0, "total": total, "hold": hold},
            ],
        },
    }


def _fill(
    *,
    tx_hash: str = "0xabc",
    tid: int = 7,
    size: str = "0.5",
    coin: str = "BTC",
    time_ms: int = NOW,
) -> dict:
    return {
        "hash": tx_hash,
        "tid": tid,
        "time": time_ms,
        "coin": coin,
        "side": "B",
        "sz": size,
        "px": "100",
        "startPosition": "0",
        "oid": 42,
    }


def _fills(user: str, *, snapshot: bool, fills: list[dict]) -> dict:
    return {
        "channel": "userFills",
        "data": {"user": user, "isSnapshot": snapshot, "fills": fills},
    }


def _twap_fills(user: str, *, snapshot: bool, fills: list[dict]) -> dict:
    return {
        "channel": "userTwapSliceFills",
        "data": {
            "user": user,
            "isSnapshot": snapshot,
            "twapSliceFills": [{"twapId": index, "fill": fill} for index, fill in enumerate(fills)],
        },
    }


def _open_orders(user: str, orders: list[dict]) -> dict:
    return {"channel": "openOrders", "data": {"user": user, "orders": orders}}


def _order(*, oid: int = 11, cloid: str | None = None, coin: str = "BTC") -> dict:
    order = {
        "coin": coin,
        "side": "B",
        "sz": "0.1",
        "limitPx": "99",
        "oid": oid,
        "reduceOnly": False,
    }
    if cloid is not None:
        order["cloid"] = cloid
    return order


def _complete_baseline(stream: AccountStream, epoch: int, *, start_ms: int = NOW) -> int:
    messages = (
        _positions(SOURCE, ("", _position_state(start_ms))),
        _spot(SOURCE, time_ms=start_ms),
        _fills(SOURCE, snapshot=True, fills=[]),
        _twap_fills(SOURCE, snapshot=True, fills=[]),
        _positions(FOLLOWER, ("", _position_state(start_ms))),
        _spot(FOLLOWER, time_ms=start_ms),
        _open_orders(FOLLOWER, []),
        _fills(FOLLOWER, snapshot=True, fills=[]),
    )
    for offset, message in enumerate(messages, 1):
        stream.apply(message, epoch=epoch, received_ms=start_ms + offset)
    return start_ms + len(messages)


def test_subscription_specs_are_minimal_and_account_scoped() -> None:
    assert source_subscription_specs(SOURCE.upper()) == (
        {"type": "allDexsClearinghouseState", "user": SOURCE},
        {"type": "spotState", "user": SOURCE},
        {"type": "userFills", "user": SOURCE, "aggregateByTime": False},
        {"type": "userTwapSliceFills", "user": SOURCE},
    )
    assert tuple(spec["type"] for spec in follower_subscription_specs(FOLLOWER)) == (
        "allDexsClearinghouseState",
        "spotState",
        "openOrders",
        "orderUpdates",
        "userFills",
    )
    assert tuple(
        spec["type"] for spec in source_subscription_specs(SOURCE, account_mode="standard")
    ) == ("allDexsClearinghouseState", "userFills", "userTwapSliceFills")


def test_standard_source_uses_each_relevant_dex_equity_once_and_needs_no_spot() -> None:
    stream = AccountStream(
        source=SOURCE,
        follower=FOLLOWER,
        source_account_mode="standard",
        source_markets=("BTC", "ETH", "xyz:FOO"),
        follower_markets=("BTC", "ETH", "xyz:FOO"),
    )
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(
        _positions(
            SOURCE,
            ("", _position_state(NOW, ("BTC", "1"), account_value="100")),
            ("xyz", _position_state(NOW, ("FOO", "-2"), account_value="50")),
        ),
        epoch=epoch,
        received_ms=NOW,
    )
    stream.apply(
        _fills(SOURCE, snapshot=True, fills=[]),
        epoch=epoch,
        received_ms=NOW + 1,
    )
    stream.apply(
        _twap_fills(SOURCE, snapshot=True, fills=[]),
        epoch=epoch,
        received_ms=NOW + 2,
    )

    assert stream.source.baseline_complete is True
    assert stream.source.collateral is None
    assert stream.source.perp_equity_by_dex == {"": Decimal("100"), "xyz": Decimal("50")}
    assert stream.source.perp_equity_total == Decimal("150")


def test_standard_source_waits_for_coherent_equity_when_dynamic_scope_adds_dex() -> None:
    stream = AccountStream(
        source=SOURCE,
        follower=FOLLOWER,
        source_account_mode="standard",
        source_markets=("BTC",),
        follower_markets=("BTC",),
    )
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(
        _positions(SOURCE, ("", _position_state(NOW, account_value="100"))),
        epoch=epoch,
        received_ms=NOW,
    )
    stream.apply(_fills(SOURCE, snapshot=True, fills=[]), epoch=epoch, received_ms=NOW + 1)
    stream.apply(
        _twap_fills(SOURCE, snapshot=True, fills=[]),
        epoch=epoch,
        received_ms=NOW + 2,
    )
    assert stream.source.baseline_complete is True

    stream.set_market_scope(
        source_markets=("BTC", "xyz:FOO"),
        follower_markets=("BTC", "xyz:FOO"),
    )
    assert stream.source.baseline_complete is False
    assert stream.source.perp_equity_by_dex == {}

    stream.apply(
        _fills(
            SOURCE,
            snapshot=False,
            fills=[_fill(coin="xyz:FOO", time_ms=NOW + 3)],
        ),
        epoch=epoch,
        received_ms=NOW + 3,
    )
    assert stream.source.baseline_complete is False

    stream.apply(
        _positions(
            SOURCE,
            ("", _position_state(NOW + 4, account_value="100")),
            ("xyz", _position_state(NOW + 4, ("FOO", "0.5"), account_value="50")),
        ),
        epoch=epoch,
        received_ms=NOW + 4,
    )

    assert stream.source.baseline_complete is True
    assert stream.source.perp_equity_by_dex == {"": Decimal("100"), "xyz": Decimal("50")}
    assert stream.source.perp_equity_total == Decimal("150")


def test_standard_source_same_dex_market_addition_keeps_complete_equity() -> None:
    stream = AccountStream(
        source=SOURCE,
        follower=FOLLOWER,
        source_account_mode="standard",
        source_markets=("BTC",),
    )
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(
        _positions(SOURCE, ("", _position_state(NOW, account_value="100"))),
        epoch=epoch,
        received_ms=NOW,
    )
    stream.apply(_fills(SOURCE, snapshot=True, fills=[]), epoch=epoch, received_ms=NOW + 1)
    stream.apply(
        _twap_fills(SOURCE, snapshot=True, fills=[]),
        epoch=epoch,
        received_ms=NOW + 2,
    )

    stream.set_market_scope(
        source_markets=("BTC", "ETH"),
        follower_markets=("BTC", "ETH"),
    )

    assert stream.source.baseline_complete is True
    assert stream.source.perp_equity_total == Decimal("100")


def test_standard_source_rejects_nonflat_dex_without_positive_equity() -> None:
    stream = AccountStream(
        source=SOURCE,
        follower=FOLLOWER,
        source_account_mode="standard",
        source_markets=("BTC",),
    )
    epoch = stream.begin_connection(received_ms=NOW - 1)

    with pytest.raises(AccountStreamError, match="nonflat with no equity"):
        stream.apply(
            _positions(
                SOURCE,
                ("", _position_state(NOW, ("BTC", "1"), account_value="0")),
            ),
            epoch=epoch,
            received_ms=NOW,
        )


def test_live_fill_advances_position_from_start_position_without_waiting_for_snapshot() -> None:
    stream = AccountStream(source=SOURCE, follower=FOLLOWER)
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(_fills(SOURCE, snapshot=True, fills=[]), epoch=epoch, received_ms=NOW)

    stream.apply(
        _fills(SOURCE, snapshot=False, fills=[_fill()]),
        epoch=epoch,
        received_ms=NOW + 1,
    )

    assert stream.source.positions["BTC"].size == Decimal("0.5")


def test_queued_frame_received_before_pong_remains_valid() -> None:
    stream = AccountStream(source=SOURCE, follower=FOLLOWER)
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.note_connection_activity(epoch=epoch, received_ms=NOW + 1)

    update = stream.apply(
        _fills(SOURCE, snapshot=True, fills=[]),
        epoch=epoch,
        received_ms=NOW,
    )

    assert update.initial_snapshot is True
    assert stream.source.last_connection_activity_ms == NOW + 1


def test_delayed_position_snapshot_cannot_rewind_newer_fill_truth() -> None:
    stream = AccountStream(source=SOURCE, follower=FOLLOWER)
    epoch = stream.begin_connection(received_ms=99)
    stream.apply(
        _positions(SOURCE, ("", _position_state(100))),
        epoch=epoch,
        received_ms=100,
    )
    stream.apply(_fills(SOURCE, snapshot=True, fills=[]), epoch=epoch, received_ms=101)
    stream.apply(
        _fills(SOURCE, snapshot=False, fills=[_fill(time_ms=200, size="1")]),
        epoch=epoch,
        received_ms=200,
    )

    with pytest.raises(AccountStreamError, match="moved backwards"):
        stream.apply(
            _positions(SOURCE, ("", _position_state(150))),
            epoch=epoch,
            received_ms=201,
        )

    assert stream.source.positions["BTC"].size == Decimal("1")
    assert stream.source.component_received_ms["positions"] == 200


def test_unrelated_dex_time_does_not_rewind_a_newer_relevant_dex() -> None:
    stream = AccountStream(
        source=SOURCE,
        follower=FOLLOWER,
        source_account_mode="standard",
        source_markets=("xyz:FOO",),
    )
    epoch = stream.begin_connection(received_ms=99)
    stream.apply(
        _positions(
            SOURCE,
            ("", _position_state(100, account_value="100")),
            ("xyz", _position_state(100, account_value="10")),
        ),
        epoch=epoch,
        received_ms=100,
    )
    stream.apply(_fills(SOURCE, snapshot=True, fills=[]), epoch=epoch, received_ms=101)
    stream.apply(_twap_fills(SOURCE, snapshot=True, fills=[]), epoch=epoch, received_ms=102)
    stream.apply(
        _fills(
            SOURCE,
            snapshot=False,
            fills=[_fill(coin="xyz:FOO", time_ms=200, size="1")],
        ),
        epoch=epoch,
        received_ms=200,
    )

    stream.apply(
        _positions(
            SOURCE,
            ("", _position_state(100, account_value="100")),
            ("xyz", _position_state(210, ("FOO", "1"), account_value="10")),
        ),
        epoch=epoch,
        received_ms=211,
    )

    assert stream.source.positions["xyz:FOO"].size == Decimal("1")
    assert stream.source.perp_equity_by_dex == {"xyz": Decimal("10")}


def test_perp_market_scope_ignores_account_wide_spot_fills() -> None:
    stream = AccountStream(
        source=SOURCE,
        follower=FOLLOWER,
        source_markets=("BTC",),
        follower_markets=("BTC",),
    )
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(_fills(SOURCE, snapshot=True, fills=[]), epoch=epoch, received_ms=NOW)
    spot = _fill(tx_hash="0xspot", tid=19, size="1")
    spot["coin"] = "@107"
    spot["startPosition"] = "10"

    update = stream.apply(
        _fills(SOURCE, snapshot=False, fills=[spot]),
        epoch=epoch,
        received_ms=NOW + 1,
    )

    assert update.new_fills == ()
    assert stream.source.positions == {}
    assert stream.source.fills == ()


def test_conflicting_multi_fill_frame_is_atomic_and_does_not_seed_dedupe() -> None:
    stream = AccountStream(source=SOURCE, follower=FOLLOWER)
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(_fills(SOURCE, snapshot=True, fills=[]), epoch=epoch, received_ms=NOW)
    first = _fill(tx_hash="0xfirst", tid=1, size="0.5")
    broken = _fill(tx_hash="0xbroken", tid=2, size="0.5")
    broken["startPosition"] = "9"

    with pytest.raises(AccountStreamError, match="does not match current"):
        stream.apply(
            _fills(SOURCE, snapshot=False, fills=[first, broken]),
            epoch=epoch,
            received_ms=NOW + 1,
        )

    assert stream.source.positions == {}
    assert stream.source.fills == ()
    accepted = stream.apply(
        _fills(SOURCE, snapshot=False, fills=[first]),
        epoch=epoch,
        received_ms=NOW + 2,
    )
    assert len(accepted.new_fills) == 1
    assert stream.source.positions["BTC"].size == Decimal("0.5")


def test_twap_and_ordinary_fill_share_dedupe_and_position_continuity() -> None:
    stream = AccountStream(source=SOURCE, follower=FOLLOWER)
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(_fills(SOURCE, snapshot=True, fills=[]), epoch=epoch, received_ms=NOW)
    stream.apply(
        _twap_fills(SOURCE, snapshot=True, fills=[]),
        epoch=epoch,
        received_ms=NOW + 1,
    )
    fill = _fill()

    twap = stream.apply(
        {
            "channel": "userTwapSliceFills",
            "data": {
                "user": SOURCE,
                "isSnapshot": False,
                "twapSliceFills": [{"twapId": 7, "fill": fill}],
            },
        },
        epoch=epoch,
        received_ms=NOW + 2,
    )
    ordinary = stream.apply(
        _fills(SOURCE, snapshot=False, fills=[fill]),
        epoch=epoch,
        received_ms=NOW + 3,
    )

    assert len(twap.new_fills) == 1
    assert ordinary.new_fills == ()
    assert ordinary.duplicate_fills == 1
    assert stream.source.positions["BTC"].size == Decimal("0.5")


def test_user_fills_missing_snapshot_flag_is_incremental_after_baseline() -> None:
    stream = AccountStream(source=SOURCE, follower=FOLLOWER)
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(_fills(SOURCE, snapshot=True, fills=[]), epoch=epoch, received_ms=NOW)

    update = stream.apply(
        {
            "channel": "userFills",
            "data": {"user": SOURCE, "fills": [_fill()]},
        },
        epoch=epoch,
        received_ms=NOW + 1,
    )

    assert len(update.new_fills) == 1
    assert update.new_fills[0].is_snapshot is False


def test_user_fills_missing_snapshot_flag_cannot_create_initial_baseline() -> None:
    stream = AccountStream(source=SOURCE, follower=FOLLOWER)
    epoch = stream.begin_connection(received_ms=NOW - 1)

    with pytest.raises(AccountStreamError, match="before the initial snapshot"):
        stream.apply(
            {
                "channel": "userFills",
                "data": {"user": SOURCE, "fills": []},
            },
            epoch=epoch,
            received_ms=NOW,
        )


@pytest.mark.parametrize("twap_first", [False, True])
def test_user_and_twap_snapshots_are_idempotent_in_either_arrival_order(
    twap_first: bool,
) -> None:
    stream = AccountStream(source=SOURCE, follower=FOLLOWER)
    epoch = stream.begin_connection(received_ms=1_000)
    fill = _fill(tx_hash="0xdual", tid=44, time_ms=900)
    user_snapshot = {
        "channel": "userFills",
        "data": {"user": SOURCE, "isSnapshot": True, "fills": [fill]},
    }
    twap_snapshot = {
        "channel": "userTwapSliceFills",
        "data": {
            "user": SOURCE,
            "isSnapshot": True,
            "twapSliceFills": [{"fill": fill, "twapId": 9}],
        },
    }
    first, second = (twap_snapshot, user_snapshot) if twap_first else (user_snapshot, twap_snapshot)

    first_update = stream.apply(first, epoch=epoch, received_ms=1_001)
    second_update = stream.apply(second, epoch=epoch, received_ms=1_002)

    assert first_update.initial_snapshot is True
    assert second_update.initial_snapshot is True
    assert first_update.new_fills == ()
    assert second_update.new_fills == ()
    assert second_update.duplicate_fills == 1
    assert len(stream.source.fills) == 1


def test_native_hip3_positions_and_token_zero_usdc_form_current_baseline() -> None:
    stream = AccountStream(source=SOURCE, follower=FOLLOWER)
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(
        _positions(
            SOURCE,
            ("", _position_state(NOW, ("btc", "1"))),
            ("xyz", _position_state(NOW + 1, ("FOO", "-2"))),
        ),
        epoch=epoch,
        received_ms=NOW,
    )
    stream.apply(_spot(SOURCE), epoch=epoch, received_ms=NOW + 1)
    stream.apply(_fills(SOURCE, snapshot=True, fills=[]), epoch=epoch, received_ms=NOW + 2)
    stream.apply(
        _twap_fills(SOURCE, snapshot=True, fills=[]),
        epoch=epoch,
        received_ms=NOW + 3,
    )
    stream.apply(
        _positions(FOLLOWER, ("", _position_state(NOW))),
        epoch=epoch,
        received_ms=NOW + 4,
    )
    stream.apply(_spot(FOLLOWER), epoch=epoch, received_ms=NOW + 5)
    stream.apply(_open_orders(FOLLOWER, []), epoch=epoch, received_ms=NOW + 6)
    final = stream.apply(
        _fills(FOLLOWER, snapshot=True, fills=[]), epoch=epoch, received_ms=NOW + 7
    )

    assert stream.source.positions == {
        "BTC": stream.source.positions["BTC"],
        "xyz:FOO": stream.source.positions["xyz:FOO"],
    }
    assert stream.source.positions["BTC"].size == Decimal("1")
    assert stream.source.positions["xyz:FOO"].size == Decimal("-2")
    assert stream.source.collateral is not None
    assert stream.source.collateral.total == Decimal("100")
    assert stream.source.collateral.hold == Decimal("2")
    assert stream.source.collateral.available == Decimal("98")
    assert stream.baseline_ready is True
    assert final.baseline_ready is True
    assert stream.is_fresh(now_ms=NOW + 50, max_age_ms=100) is True
    assert stream.is_fresh(now_ms=NOW + 200, max_age_ms=100) is False


def test_snapshot_fills_seed_identity_without_becoming_live_events() -> None:
    stream = AccountStream(source=SOURCE, follower=FOLLOWER)
    epoch = stream.begin_connection(received_ms=NOW - 1)

    baseline = stream.apply(
        _fills(SOURCE, snapshot=True, fills=[_fill()]),
        epoch=epoch,
        received_ms=NOW,
    )
    duplicate = stream.apply(
        _fills(SOURCE, snapshot=False, fills=[_fill()]),
        epoch=epoch,
        received_ms=NOW + 1,
    )
    live = stream.apply(
        _fills(SOURCE, snapshot=False, fills=[_fill(tx_hash="0xdef", tid=8)]),
        epoch=epoch,
        received_ms=NOW + 2,
    )

    assert baseline.initial_snapshot is True
    assert baseline.new_fills == ()
    assert duplicate.new_fills == ()
    assert duplicate.duplicate_fills == 1
    assert len(live.new_fills) == 1
    assert live.new_fills[0].identity == (SOURCE, "0xdef", "8")
    assert live.new_fills[0].signed_size == Decimal("0.5")


def test_fill_dedupe_uses_account_hash_tid_and_rejects_changed_payload_atomically() -> None:
    stream = AccountStream(source=SOURCE, follower=FOLLOWER)
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(_fills(SOURCE, snapshot=True, fills=[]), epoch=epoch, received_ms=NOW)
    original = stream.apply(
        _fills(SOURCE, snapshot=False, fills=[_fill()]),
        epoch=epoch,
        received_ms=NOW + 1,
    )

    with pytest.raises(AccountStreamError, match="fill identity changed payload"):
        stream.apply(
            _fills(SOURCE, snapshot=False, fills=[_fill(size="0.6")]),
            epoch=epoch,
            received_ms=NOW + 2,
        )
    assert stream.source.fills == original.new_fills

    stream.apply(_fills(FOLLOWER, snapshot=True, fills=[]), epoch=epoch, received_ms=NOW + 2)
    follower = stream.apply(
        _fills(FOLLOWER, snapshot=False, fills=[_fill()]),
        epoch=epoch,
        received_ms=NOW + 3,
    )
    assert len(follower.new_fills) == 1
    assert follower.new_fills[0].account == FOLLOWER


def test_follower_open_order_snapshot_updates_and_fills_are_current_state() -> None:
    cloid = "0x" + "a" * 32
    stream = AccountStream(source=SOURCE, follower=FOLLOWER)
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(
        _open_orders(FOLLOWER, [_order(oid=11, cloid=cloid, coin="xyz:FOO")]),
        epoch=epoch,
        received_ms=NOW,
    )
    assert stream.follower.open_orders[0].coin == "xyz:FOO"

    stream.apply(
        {
            "channel": "orderUpdates",
            "data": [
                {
                    "order": _order(oid=12),
                    "status": "open",
                    "statusTimestamp": NOW + 1,
                }
            ],
        },
        epoch=epoch,
        received_ms=NOW + 1,
    )
    assert {order.oid for order in stream.follower.open_orders} == {11, 12}

    stream.apply(
        {
            "channel": "orderUpdates",
            "data": [
                {
                    "order": {"coin": "BTC", "oid": 12, "sz": "0"},
                    "status": "filled",
                    "statusTimestamp": NOW + 2,
                }
            ],
        },
        epoch=epoch,
        received_ms=NOW + 2,
    )
    assert [order.oid for order in stream.follower.open_orders] == [11]

    stream.apply(_fills(FOLLOWER, snapshot=True, fills=[]), epoch=epoch, received_ms=NOW + 3)
    stream.apply(
        _fills(FOLLOWER, snapshot=False, fills=[_fill(tx_hash="0xfollower")]),
        epoch=epoch,
        received_ms=NOW + 4,
    )
    assert stream.follower.fills[-1].tx_hash == "0xfollower"


def test_reconnect_clears_current_truth_rejects_old_epoch_and_retains_fill_dedupe() -> None:
    stream = AccountStream(source=SOURCE, follower=FOLLOWER)
    first_epoch = stream.begin_connection(received_ms=NOW - 1)
    _complete_baseline(stream, first_epoch)
    stream.apply(
        _fills(SOURCE, snapshot=False, fills=[_fill()]),
        epoch=first_epoch,
        received_ms=NOW + 8,
    )

    second_epoch = stream.begin_connection(received_ms=NOW + 9)
    assert stream.baseline_ready is False
    assert stream.source.positions == {}
    with pytest.raises(AccountStreamError, match="stale connection epoch"):
        stream.apply(
            _fills(SOURCE, snapshot=False, fills=[]),
            epoch=first_epoch,
            received_ms=NOW + 10,
        )
    snapshot = stream.apply(
        _fills(SOURCE, snapshot=True, fills=[_fill()]),
        epoch=second_epoch,
        received_ms=NOW + 10,
    )
    assert snapshot.new_fills == ()
    assert snapshot.duplicate_fills == 1


@pytest.mark.parametrize(
    "balances, match",
    [
        ([{"coin": "USDC", "token": 0, "total": "1"}] * 2, "exactly one token-0"),
        ([{"coin": "NOT-USDC", "token": 0, "total": "1"}], "not identified as USDC"),
        (
            [{"coin": "USDC", "token": 0, "total": "1", "hold": "2"}],
            "inconsistent",
        ),
    ],
)
def test_spot_collateral_rejects_ambiguous_or_inconsistent_token_zero(
    balances: list[dict], match: str
) -> None:
    stream = AccountStream(source=SOURCE, follower=FOLLOWER)
    epoch = stream.begin_connection(received_ms=NOW - 1)
    with pytest.raises(AccountStreamError, match=match):
        stream.apply(
            {
                "channel": "spotState",
                "data": {"user": SOURCE, "time": NOW, "balances": balances},
            },
            epoch=epoch,
            received_ms=NOW,
        )
    assert stream.source.collateral is None


def test_live_fills_before_snapshot_and_unknown_order_status_do_not_mutate_state() -> None:
    stream = AccountStream(source=SOURCE, follower=FOLLOWER)
    epoch = stream.begin_connection(received_ms=NOW - 1)
    with pytest.raises(AccountStreamError, match="before the initial fill snapshot"):
        stream.apply(
            _fills(SOURCE, snapshot=False, fills=[_fill()]),
            epoch=epoch,
            received_ms=NOW,
        )
    stream.apply(_open_orders(FOLLOWER, [_order()]), epoch=epoch, received_ms=NOW)
    with pytest.raises(AccountStreamError, match="unknown lifecycle status"):
        stream.apply(
            {
                "channel": "orderUpdates",
                "data": [
                    {
                        "order": {"coin": "BTC", "oid": 11},
                        "status": "futureMystery",
                        "statusTimestamp": NOW + 1,
                    }
                ],
            },
            epoch=epoch,
            received_ms=NOW + 1,
        )
    assert [order.oid for order in stream.follower.open_orders] == [11]
