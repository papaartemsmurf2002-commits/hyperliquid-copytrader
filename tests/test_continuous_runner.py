from __future__ import annotations

import asyncio
import json
import runpy
import sys
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest
from eth_account import Account

from hyperliquid_copytrader import continuous_runner
from hyperliquid_copytrader.continuous_config import (
    BoundContinuousPlan,
    BoundContinuousSlot,
    ContinuousPlan,
    ContinuousSlotConfig,
)
from hyperliquid_copytrader.continuous_runner import (
    FleetRunResult,
    JsonlMetrics,
    RecoveryHttpFills,
    STARTUP_HTTP_REQUESTS,
    STARTUP_HTTP_WEIGHT,
    StartupInfo,
    _WsStartupInfo,
    _build_lanes,
    build_startup_catalog,
    ensure_engine_identity,
    load_durable_catalog,
)
from hyperliquid_copytrader.desired_engine import NextAction
from hyperliquid_copytrader.market_catalog import MarketReadiness, build_dynamic_catalog_revision
from hyperliquid_copytrader.market_stream import MarketSubscriptionChange
from hyperliquid_copytrader.ws_actions import PostOutcome, PostResult, WsPostMux


def _address(index: int) -> str:
    return f"0x{index:040x}"


@pytest.mark.asyncio
async def test_startup_ws_info_retries_transient_read_only_server_error() -> None:
    class Socket:
        async def send(self, _message: str) -> None:
            raise AssertionError("startup heartbeat is not due")

    class Mux:
        write_timeout_s = 1.0

        def __init__(self) -> None:
            self.calls = 0

        async def post_info(self, payload, *, required_epoch, timeout_s):  # type: ignore[no-untyped-def]
            self.calls += 1
            assert payload == {"type": "openOrders", "user": _address(1)}
            assert required_epoch == 7
            assert timeout_s == 5.0
            if self.calls == 1:
                return PostResult(
                    1,
                    PostOutcome.REJECTED,
                    {"type": "error"},
                    "server_error_response",
                )
            return PostResult(
                2,
                PostOutcome.INFO,
                {
                    "type": "info",
                    "payload": {"type": "openOrders", "data": []},
                },
                "info_response",
            )

    delays: list[float] = []

    async def no_delay(seconds: float) -> None:
        delays.append(seconds)

    mux = Mux()
    bridge = _WsStartupInfo(
        loop=asyncio.get_running_loop(),
        mux=cast(Any, mux),
        socket=Socket(),
        epoch=7,
        timeout_s=5.0,
        retry_delay_s=0.5,
        sleep=no_delay,
    )

    response = await asyncio.to_thread(
        bridge,
        {"type": "openOrders", "user": _address(1)},
    )

    assert response == []
    assert bridge.logical_count == 1
    assert bridge.count == 2
    assert delays == [0.5]


@pytest.mark.asyncio
async def test_startup_ws_info_paces_weight_and_keeps_socket_alive() -> None:
    class Socket:
        def __init__(self) -> None:
            self.messages: list[str] = []

        async def send(self, message: str) -> None:
            self.messages.append(message)

    class Mux:
        write_timeout_s = 1.0

        async def post_info(self, payload, *, required_epoch, timeout_s):  # type: ignore[no-untyped-def]
            return PostResult(
                1,
                PostOutcome.INFO,
                {
                    "type": "info",
                    "payload": {"type": payload["type"], "data": []},
                },
                "info_response",
            )

    clock = [0.0]
    delays: list[float] = []

    async def advance(seconds: float) -> None:
        delays.append(seconds)
        clock[0] += seconds

    socket = Socket()
    bridge = _WsStartupInfo(
        loop=asyncio.get_running_loop(),
        mux=cast(Any, Mux()),
        socket=socket,
        epoch=7,
        timeout_s=5.0,
        weight_limit=20,
        weight_window_s=60.0,
        clock=lambda: clock[0],
        sleep=advance,
    )

    first = await asyncio.to_thread(
        bridge, {"type": "openOrders", "user": _address(1)}
    )
    second = await asyncio.to_thread(
        bridge, {"type": "openOrders", "user": _address(2)}
    )

    assert first == second == []
    assert delays == [25.0, 25.0, 10.0]
    assert socket.messages == ['{"method":"ping"}']
    assert bridge.logical_count == 2
    assert bridge.count == 2
    assert bridge.wire_weight == 40


def _bound(tmp_path: Path, *, count: int, real_keys: bool) -> BoundContinuousPlan:
    configs: list[ContinuousSlotConfig] = []
    slots: list[BoundContinuousSlot] = []
    for index in range(count):
        slot_id = f"slot{index + 1}"
        config = ContinuousSlotConfig(
            slot=slot_id,
            source_address=_address(100 + index),
            follower_account_address=_address(200 + index),
            credential_profile_id=slot_id,
            multiplier=Decimal("0.01"),
            max_order_notional_usd=Decimal("10"),
            max_gross_exposure_usd=Decimal("20"),
            max_open_positions=2,
            max_leverage=2,
            action_limit_per_minute=30,
            allowed_markets=("BTC",),
            enabled=True,
        )
        key_file = tmp_path / f"{slot_id}.key"
        if real_keys:
            wallet = Account.create(f"continuous-runner-{index}")
            key_file.write_text(wallet.key.hex(), encoding="utf-8")
            api_wallet = str(wallet.address).lower()
        else:
            api_wallet = _address(300 + index)
        configs.append(config)
        slots.append(
            BoundContinuousSlot(
                config=config,
                api_wallet_address=api_wallet,
                api_private_key_file=key_file,
                global_account_address=config.follower_account_address,
                expected_account_mode="unified",
            )
        )
    plan = ContinuousPlan(
        version=1,
        network="mainnet",
        runtime_id="runner-test",
        startup_baseline_only=True,
        max_combined_gross_usd=Decimal(20 * count),
        slots=tuple(configs),
        path=tmp_path / "plan.json",
        sha256="a" * 64,
    )
    return BoundContinuousPlan(plan, tuple(slots))


def test_monitor_only_lanes_never_open_private_key_files(tmp_path: Path) -> None:
    bound = _bound(tmp_path, count=2, real_keys=False)

    lanes, journals = _build_lanes(
        bound,
        engine_state_dir=tmp_path / "engine",
        armed=False,
    )
    try:
        assert all(not lane.signing_enabled for lane in lanes.values())
        assert all(not slot.api_private_key_file.exists() for slot in bound.slots)
    finally:
        for journal in journals:
            journal.close()


def test_stable_engine_reopens_each_signer_journal_instead_of_generation_state(
    tmp_path: Path,
) -> None:
    bound = _bound(tmp_path, count=2, real_keys=False)
    engine = tmp_path / "engine"
    lanes, journals = _build_lanes(bound, engine_state_dir=engine, armed=False)
    first = lanes["slot1"]
    first.journal.reserve_nonce(
        follower_account=first.follower_account,
        api_wallet=first.api_wallet_address,
        wall_ms=1_750_000_000_000,
    )
    for journal in journals:
        journal.close()

    restored, reopened = _build_lanes(bound, engine_state_dir=engine, armed=False)
    try:
        lane = restored["slot1"]
        assert (
            lane.journal.last_nonce(
                follower_account=lane.follower_account,
                api_wallet=lane.api_wallet_address,
            )
            == 1_750_000_000_000
        )
        assert sorted(path.name for path in (engine / "actions").glob("*.sqlite3")) == [
            "slot1.sqlite3",
            "slot2.sqlite3",
        ]
    finally:
        for journal in reopened:
            journal.close()


class _BarrierMux:
    def __init__(self, total: int) -> None:
        self.total = total
        self.arrived = 0
        self.release = asyncio.Event()

    async def post_action(self, signed, *, before_send, **_kwargs):  # type: ignore[no-untyped-def]
        self.arrived += 1
        request_id = self.arrived
        await before_send(request_id)
        if self.arrived == self.total:
            self.release.set()
        await asyncio.wait_for(self.release.wait(), timeout=2)
        return PostResult(
            request_id,
            PostOutcome.FILLED,
            {},
            "fake_fill",
            signed.ioc.expected_size,
        )


@pytest.mark.asyncio
async def test_ten_signer_lanes_reach_fake_send_without_cross_journal_locking(
    tmp_path: Path,
) -> None:
    bound = _bound(tmp_path, count=10, real_keys=True)
    lanes, journals = _build_lanes(
        bound,
        engine_state_dir=tmp_path / "engine",
        armed=True,
    )
    mux = _BarrierMux(10)
    try:
        attempts = await asyncio.gather(
            *(
                lane.execute_ioc(
                    action=NextAction(
                        desired_id=f"desired-{slot_id}",
                        market="BTC",
                        side="buy",
                        size=Decimal("0.001"),
                        reduce_only=False,
                        reason="test",
                    ),
                    asset_id=0,
                    limit_px=Decimal("100000"),
                    mux=cast(WsPostMux, mux),
                    required_epoch=1,
                )
                for slot_id, lane in lanes.items()
            )
        )
        assert mux.arrived == 10
        assert all(attempt.result.outcome is PostOutcome.FILLED for attempt in attempts)
    finally:
        for journal in journals:
            journal.close()


def test_ten_slot_normal_startup_http_surface_is_catalog_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    dexs = [None]

    def fake_http(_url: str, payload: dict[str, Any], *, timeout_s: float) -> Any:
        assert timeout_s == 10.0
        calls.append(dict(payload))
        if payload["type"] == "perpDexs":
            return dexs
        return [{"universe": [{"name": "BTC", "szDecimals": 5, "maxLeverage": 20}]}]

    monkeypatch.setattr(
        "hyperliquid_copytrader.continuous_runner._http_info",
        fake_http,
    )
    with StartupInfo("https://example.invalid") as info:
        catalog = build_startup_catalog(info, network="mainnet", observed_ms=123)

    assert catalog.market("BTC") is not None
    assert info.count == STARTUP_HTTP_REQUESTS
    assert info.logical_count == STARTUP_HTTP_REQUESTS
    assert info.weight == STARTUP_HTTP_WEIGHT
    assert STARTUP_HTTP_WEIGHT == 60
    assert calls == [
        {"type": "perpDexs"},
        {"type": "allPerpMetas"},
        {"type": "perpDexs"},
    ]


def test_durable_catalog_load_is_strict_and_round_trips(tmp_path: Path) -> None:
    path = tmp_path / "catalog.json"
    catalog = build_dynamic_catalog_revision(
        network="mainnet",
        policy_version="continuous-ws-v1",
        sequence=1,
        observed_ms=123,
        dexes_before_payload=[None],
        all_perp_metas_payload=[
            {"universe": [{"name": "BTC", "szDecimals": 5, "maxLeverage": 50}]}
        ],
        dexes_after_payload=[None],
    )
    path.write_text(json.dumps(catalog.to_payload()), encoding="utf-8")

    assert load_durable_catalog(path, network="mainnet") == catalog
    with pytest.raises(RuntimeError, match="different runtime policy"):
        load_durable_catalog(path, network="testnet")

    path.write_text("{not-json", encoding="utf-8")
    with pytest.raises(RuntimeError, match="unreadable or invalid"):
        load_durable_catalog(path, network="mainnet")

    path.write_text(json.dumps({"markets": []}), encoding="utf-8")
    with pytest.raises(RuntimeError, match="unreadable or invalid"):
        load_durable_catalog(path, network="mainnet")


def test_cold_restart_retains_removed_and_mutated_market_identity(tmp_path: Path) -> None:
    path = tmp_path / "catalog.json"
    previous = build_dynamic_catalog_revision(
        network="mainnet",
        policy_version="continuous-ws-v1",
        sequence=1,
        observed_ms=100,
        dexes_before_payload=[None],
        all_perp_metas_payload=[
            {
                "universe": [
                    {"name": "BTC", "szDecimals": 5, "maxLeverage": 50},
                    {"name": "ETH", "szDecimals": 4, "maxLeverage": 50},
                ]
            }
        ],
        dexes_after_payload=[None],
    )
    path.write_text(json.dumps(previous.to_payload()), encoding="utf-8")
    restored = load_durable_catalog(path, network="mainnet")
    assert restored is not None
    responses = iter(
        (
            [None],
            [{"universe": [{"name": "BTC", "szDecimals": 3, "maxLeverage": 20}]}],
            [None],
        )
    )

    current = build_startup_catalog(
        lambda _payload: next(responses),
        network="mainnet",
        observed_ms=200,
        previous=restored,
        retain_symbols={market.symbol for market in restored.markets},
    )

    btc = current.market("BTC")
    eth = current.market("ETH")
    assert btc is not None and btc.readiness is MarketReadiness.UNTRUSTED
    assert (btc.sz_decimals, btc.pending_sz_decimals) == (5, 3)
    assert eth is not None and eth.removal_tombstone is True
    assert eth.readiness is MarketReadiness.DELISTED


@pytest.mark.asyncio
async def test_unknown_market_fill_wakes_catalog_refresh_without_waiting_for_periodic_timer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    initial = build_dynamic_catalog_revision(
        network="mainnet",
        policy_version="continuous-ws-v1",
        sequence=1,
        observed_ms=100,
        dexes_before_payload=[None],
        all_perp_metas_payload=[
            {"universe": [{"name": "BTC", "szDecimals": 5, "maxLeverage": 50}]}
        ],
        dexes_after_payload=[None],
    )
    refreshed = build_dynamic_catalog_revision(
        network="mainnet",
        policy_version="continuous-ws-v1",
        sequence=2,
        observed_ms=200,
        dexes_before_payload=[None],
        all_perp_metas_payload=[
            {
                "universe": [
                    {"name": "BTC", "szDecimals": 5, "maxLeverage": 50},
                    {"name": "NEW", "szDecimals": 3, "maxLeverage": 20},
                ]
            }
        ],
        dexes_after_payload=[None],
    )
    requested = asyncio.Event()
    requested.set()
    stop = asyncio.Event()
    metrics: list[dict[str, Any]] = []

    class Runtime:
        catalog = initial
        catalog_retention_markets: frozenset[str] = frozenset()

        async def wait_for_catalog_refresh_request(self) -> None:
            await requested.wait()

        def clear_catalog_refresh_request(self) -> None:
            requested.clear()

        async def apply_catalog(self, catalog):  # type: ignore[no-untyped-def]
            self.catalog = catalog
            return MarketSubscriptionChange(("NEW",), ())

    class Driver:
        async def notify_market_change(self, change: MarketSubscriptionChange) -> None:
            assert change.added == ("NEW",)
            stop.set()

    class Metrics:
        def sink(self, payload: Mapping[str, Any]) -> None:
            metrics.append(dict(payload))

    class Info:
        logical_count = STARTUP_HTTP_REQUESTS
        count = STARTUP_HTTP_REQUESTS
        weight = STARTUP_HTTP_WEIGHT

        def __init__(self, _url: str) -> None:
            pass

        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *_args: Any) -> None:
            pass

    monkeypatch.setattr(continuous_runner, "StartupInfo", Info)
    monkeypatch.setattr(
        continuous_runner,
        "build_startup_catalog",
        lambda *_args, **_kwargs: refreshed,
    )
    monkeypatch.setattr(continuous_runner, "UNKNOWN_MARKET_REFRESH_COOLDOWN_S", 0.0)

    await asyncio.wait_for(
        continuous_runner._catalog_refresh_loop(
            stop,
            runtime=cast(Any, Runtime()),
            driver=cast(Any, Driver()),
            rest_url="https://example.invalid",
            network="mainnet",
            catalog_path=tmp_path / "catalog.json",
            durable_catalog_path=tmp_path / "durable-catalog.json",
            metrics=cast(Any, Metrics()),
            interval_s=60.0,
        ),
        timeout=1.0,
    )

    assert metrics[-1]["event"] == "catalog_refreshed"
    assert metrics[-1]["trigger"] == "unknown_market_fill"
    assert metrics[-1]["http_weight"] == STARTUP_HTTP_WEIGHT


def test_startup_catalog_retries_idempotent_timeouts_and_counts_wire_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    failures = 2

    def flaky_http(_url: str, payload: dict[str, Any], *, timeout_s: float) -> Any:
        nonlocal failures
        assert timeout_s == 10.0
        calls.append(payload["type"])
        if failures:
            failures -= 1
            raise TimeoutError("transient read timeout")
        if payload["type"] == "perpDexs":
            return [None]
        return [{"universe": [{"name": "BTC", "szDecimals": 5, "maxLeverage": 20}]}]

    monkeypatch.setattr(
        "hyperliquid_copytrader.continuous_runner._http_info",
        flaky_http,
    )
    monkeypatch.setattr("hyperliquid_copytrader.rest_throttle.time.sleep", lambda _s: None)

    with StartupInfo("https://example.invalid") as info:
        catalog = build_startup_catalog(info, network="mainnet", observed_ms=123)

    assert catalog.market("BTC") is not None
    assert info.logical_count == STARTUP_HTTP_REQUESTS
    assert info.count == 5
    assert info.weight == 100
    assert calls == ["perpDexs", "perpDexs", "perpDexs", "allPerpMetas", "perpDexs"]


def test_engine_identity_is_stable_and_refuses_plan_fork(tmp_path: Path) -> None:
    engine = tmp_path / "engine"
    first = ensure_engine_identity(
        engine,
        network="mainnet",
        runtime_id="fleet-one",
        plan_sha256="a" * 64,
    )
    second = ensure_engine_identity(
        engine,
        network="mainnet",
        runtime_id="fleet-one",
        plan_sha256="a" * 64,
    )
    assert first == second
    assert json.loads(first.read_text(encoding="utf-8"))["runtime_id"] == "fleet-one"
    with pytest.raises(RuntimeError, match="different plan identity"):
        ensure_engine_identity(
            engine,
            network="mainnet",
            runtime_id="fleet-one",
            plan_sha256="b" * 64,
        )


def test_preflight_identity_check_does_not_pin_a_failed_plan(tmp_path: Path) -> None:
    engine = tmp_path / "engine"
    checked = ensure_engine_identity(
        engine,
        network="mainnet",
        runtime_id="fleet-one",
        plan_sha256="a" * 64,
        create=False,
    )
    assert not checked.exists()

    created = ensure_engine_identity(
        engine,
        network="mainnet",
        runtime_id="fleet-one",
        plan_sha256="b" * 64,
    )
    assert json.loads(created.read_text(encoding="utf-8"))["plan_sha256"] == "b" * 64


@pytest.mark.asyncio
async def test_metrics_writer_drains_jsonl_on_its_own_thread(tmp_path: Path) -> None:
    metrics = JsonlMetrics(tmp_path / "metrics.jsonl", capacity=512)
    metrics.start()
    for index in range(200):
        metrics.sink({"event": "sample", "index": index})
    await metrics.close()

    rows = [json.loads(line) for line in metrics.path.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 200
    assert rows[0] == {"event": "sample", "index": 0}
    assert rows[-1] == {"event": "sample", "index": 199}


@pytest.mark.asyncio
async def test_recovery_http_serializes_and_cools_down_per_user(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[dict[str, Any]] = []

    class Metrics:
        def sink(self, event: Mapping[str, Any]) -> None:
            events.append(dict(event))

    clock = [10.0]
    calls: list[dict[str, Any]] = []
    throttle_calls: list[dict[str, Any]] = []

    def fake_http(_base_url: str, payload: Mapping[str, Any], *, timeout_s: float) -> list[Any]:
        assert timeout_s == 10.0
        calls.append(dict(payload))
        return []

    monkeypatch.setattr(continuous_runner, "_http_info", fake_http)

    def fake_throttle(label: str, fn, **kwargs: Any) -> Any:
        throttle_calls.append({"label": label, **kwargs})
        return fn()

    monkeypatch.setattr(continuous_runner, "call_with_rest_backoff", fake_throttle)
    recovery = RecoveryHttpFills(
        "https://example.invalid",
        cast(JsonlMetrics, Metrics()),
        minimum_interval_s=30.0,
        clock=lambda: clock[0],
    )
    kwargs = {"user": _address(1), "start_ms": 1, "end_ms": 2}

    assert await recovery(**kwargs) == []
    with pytest.raises(RuntimeError, match="cooldown"):
        await recovery(**kwargs)
    assert len(calls) == 1
    assert events[-1]["event"] == "recovery_http_cooldown"

    clock[0] = 40.0
    assert await recovery(**kwargs) == []
    assert len(calls) == 2
    assert [row["label"] for row in throttle_calls] == [
        "info:userFillsByTime",
        "info:userFillsByTime",
    ]
    assert all(row["attempts"] == 1 for row in throttle_calls)
    assert all(row["weight"] == 20 for row in throttle_calls)


@pytest.mark.asyncio
async def test_metrics_writer_failure_is_observable_and_shutdown_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "metrics.jsonl"
    original_open = Path.open

    def fail_target(path: Path, *args: Any, **kwargs: Any):
        if path == target:
            raise OSError("disk unavailable")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_target)
    metrics = JsonlMetrics(target)
    metrics.start()
    for _ in range(100):
        if metrics.failure is not None:
            break
        await asyncio.sleep(0.001)

    with pytest.raises(RuntimeError, match="metrics writer failed"):
        metrics.raise_if_failed()
    with pytest.raises(RuntimeError, match="metrics writer failed"):
        await asyncio.wait_for(metrics.close(), timeout=1)


@pytest.mark.asyncio
async def test_status_failure_sets_shared_stop_and_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()

    async def fail_status(_stop: asyncio.Event, **_kwargs: Any) -> None:
        raise OSError("status disk unavailable")

    monkeypatch.setattr(continuous_runner, "_status_loop", fail_status)
    with pytest.raises(OSError, match="status disk unavailable"):
        await continuous_runner._status_guard(stop)
    assert stop.is_set()


@pytest.mark.asyncio
async def test_armed_duration_closes_all_slots_before_setting_stop() -> None:
    stop = asyncio.Event()
    calls: list[str] = []

    class Driver:
        async def close_out_all(self, *, reason: str) -> None:
            assert stop.is_set() is False
            calls.append(reason)

    await continuous_runner._duration_stop(
        stop,
        cast(Any, Driver()),
        armed=True,
        duration_s=0,
    )

    assert calls == ["planned duration elapsed"]
    assert stop.is_set()


def test_runner_script_reports_success_with_canonical_status_field(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def fake_run(**_kwargs: Any) -> FleetRunResult:
        return FleetRunResult(
            status="stopped",
            armed=False,
            status_path=tmp_path / "status.json",
            metrics_path=tmp_path / "metrics.jsonl",
            engine_state_dir=tmp_path / "engine",
            startup_http_requests=3,
            startup_http_weight=60,
            metrics_dropped=0,
        )

    monkeypatch.setattr(
        "hyperliquid_copytrader.continuous_runner.run_continuous_fleet",
        fake_run,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_continuous_fleet.py",
            "--repo-root",
            str(tmp_path),
            "--plan",
            str(tmp_path / "plan.json"),
            "--state-dir",
            str(tmp_path / "run"),
            "--engine-state-dir",
            str(tmp_path / "engine"),
        ],
    )

    runpy.run_path(
        str(Path(__file__).parents[1] / "scripts" / "run_continuous_fleet.py"),
        run_name="__main__",
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "stopped"
    assert "state" not in payload
