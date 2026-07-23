from __future__ import annotations

import base64
import json
import re
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from threading import Event
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

from hyperliquid_copytrader.models import (
    ExecutionReport,
    FollowerIntent,
    IntentAction,
    IntentStatus,
    Mode,
    OpenOrder,
    Position,
    ReconcileSnapshot,
    SafeModeReason,
    SourceEvent,
    SourceEventType,
    now_ms,
)
from hyperliquid_copytrader.config import load_config
from hyperliquid_copytrader.run_history import RunHistoryService
from hyperliquid_copytrader.runtime import SlidingWindowRateLimiter
from hyperliquid_copytrader.safety import SafeModeController
from hyperliquid_copytrader.service import CopyTraderService
from hyperliquid_copytrader.web.app import create_app


TESTNET_API_KEY = "0x" + "1" * 64
FOLLOWER_ACCOUNT = "0xf000000000000000000000000000000000000000"


@pytest.fixture(autouse=True)
def source_wallet_env(monkeypatch):
    monkeypatch.setenv("HLCT_SOURCE_WALLET", "0xcf7c4feb434751146a48b895e96caeb15838f92c")


def _app_state(client: TestClient) -> Any:
    return cast(Any, client.app).state


def test_gui_dashboard_and_status_render(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    monkeypatch.setenv("HLCT_LEADERBOARD_ENABLE", "false")
    monkeypatch.setenv("HLCT_ADDRESS_ANALYTICS_ENABLE", "false")
    client = TestClient(create_app())
    html = client.get("/")
    assert html.status_code == 200
    assert "Hyperliquid Fleet & Analytics" in html.text
    assert 'id="continuous-fleet-console"' in html.text
    assert "Fleet controller is not configured" in html.text
    assert "30D Profit Leaderboard" in html.text
    assert "Address Analyzer" in html.text
    assert "Fleet accounts" in html.text
    assert 'href="/analytics"' in html.text
    assert "Guided Credential Setup" not in html.text
    assert "API wallet private key" not in html.text
    assert 'action="/controls/run-once"' not in html.text
    favicon = client.get("/favicon.ico")
    assert favicon.status_code == 204
    assert favicon.content == b""
    status = client.get("/api/status")
    assert status.status_code == 200
    assert status.json()["mode"] == "shadow"
    assert status.json()["preflight"]["passed"] is True
    assert status.json()["preflight"]["scope"] == "local_config"
    assert status.json()["preflight"]["signed_account_probe"] is False
    assert status.json()["source_health"]["event_count"] == 0
    assert status.json()["safe_mode"]["incident"]["severity"] == "normal"
    assert status.json()["ops"]["source_websocket_idle_timeout_ms"] == 55_000
    assert status.json()["ops"]["source_websocket_heartbeat_timeout_ms"] == 5_000
    assert status.json()["ops"]["connection_siren_after_ms"] == 30_000
    assert status.json()["ops"]["dashboard_control_max_per_minute"] == 20
    assert status.json()["recent_control_audit"] == []
    assert status.json()["ops"]["pending_source_reaction_count"] == 0
    assert status.json()["sizing"]["slippage_policy"]["reduce_only"]["guard"]
    assert status.json()["connection_integrity"]["status"] == "waiting_for_source"
    assert status.json()["sizing"]["mode"] == "not_calculated"
    assert status.json()["active_subaccount_assignment"]["status"] == "not_required"
    assert status.json()["account_context"]["status"] == "not_required"
    assert status.json()["subaccount_monitoring"][0]["slot"] == "primary"
    assert status.json()["runner"]["online"] is False
    assert status.json()["runner"]["control_authority"] is False
    assert status.json()["mainnet_follow_validation"]["verdict"] == "not_started"
    readiness = client.get("/api/readiness")
    assert readiness.status_code == 200
    assert readiness.json()["readiness_label"] == "shadow_operational"
    leaderboard = client.get("/api/leaderboard")
    assert leaderboard.status_code == 200
    assert leaderboard.json()["status"] == "disabled"
    filtered_leaderboard = client.get(
        "/api/leaderboard?limit=17&min_volume_usd=250000&min_account_value_usd=5000"
    )
    assert filtered_leaderboard.status_code == 200
    assert filtered_leaderboard.json()["limit"] == 17
    assert filtered_leaderboard.json()["active_volume_filter_usd"] == "250000"
    assert filtered_leaderboard.json()["active_account_value_filter_usd"] == "5000"
    address_analysis = client.get(
        "/api/address-analysis?address=0x1111111111111111111111111111111111111111"
    )
    assert address_analysis.status_code == 200
    assert address_analysis.json()["status"] == "disabled"
    metrics = client.get("/metrics")
    assert metrics.status_code == 200
    assert 'hlct_safe_mode_enabled{mode="shadow",reason="none"} 0' in metrics.text
    assert 'hlct_source_websocket_idle_timeout_seconds{mode="shadow"} 55' in metrics.text
    assert 'hlct_source_websocket_heartbeat_timeout_seconds{mode="shadow"} 5' in metrics.text


def test_restored_backup_analytics_page_is_full_and_monitor_only(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    client = TestClient(create_app(control_authority=True))

    page = client.get("/analytics")

    assert page.status_code == 200
    assert "Hyperliquid Analytics & Operations" in page.text
    assert "30D Profit Leaderboard" in page.text
    assert "Address Analyzer" in page.text
    assert "const ADDRESS_ANALYSIS_TIMEOUT_MS = 120000;" in page.text
    assert "Selected Chart Point" in page.text
    assert "Previous ranked trader" in page.text
    assert "Next ranked trader" in page.text
    assert "Recent Intent Journal" in page.text
    assert "Connection Integrity" in page.text
    assert "monitor only" in page.text
    assert 'action="/"' in page.text
    assert 'data-dashboard-view="analytics"' in page.text
    assert 'window.localStorage.getItem("hlct-analytics-dashboard-view")' in page.text


def test_gui_summarizes_active_acc7_follow_validation(monkeypatch, tmp_path):
    state_dir = tmp_path / "data" / "mainnet-canary"
    state_dir.mkdir(parents=True)
    monkeypatch.setenv("HLCT_DB_PATH", str(state_dir / "gui.sqlite3"))
    runs_dir = tmp_path / "data" / "runs"
    run_dir = runs_dir / "mainnet-acc7-20260712T003157Z"
    run_dir.mkdir(parents=True)
    (runs_dir / "mainnet-acc7-current-report.json").write_text(
        json.dumps(
            {
                "verdict": "in_progress",
                "generated_at": "2026-07-12T01:00:00+00:00",
                "acc7_gate_passed": False,
                "fleet_launch_ready": False,
                "fleet_boundary": "one-account evidence is not fleet proof",
                "window": {
                    "deadline_at": "2026-07-12T08:17:33+00:00",
                    "observed_seconds": 2500,
                    "expected_seconds": 28800,
                },
                "diagnostics": {"snapshot_count": 10, "all_healthy": True},
                "journal": {
                    "source_fill_events": 0,
                    "copy_intents": [],
                    "safe_mode_incidents": [],
                },
                "requirements": {"bounded_window_elapsed": False},
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "manifest.json").write_text(
        json.dumps({"deadline_at": "2026-07-12T08:17:33+00:00"}),
        encoding="utf-8",
    )
    (run_dir / "diagnostics.jsonl").write_text(
        json.dumps(
            {
                "observed_at": "2026-07-12T01:00:00+00:00",
                "healthy": True,
                "runner": {"online": True},
                "watchdog": {"ready": True, "detail": "pending=0 cancels=0 errors=0"},
                "safe_mode": {"enabled": False},
                "pending_intents": 0,
                "source": {"positions": []},
                "follower": {"positions": [], "open_orders": 0, "account_value": "24.99"},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "supervisor-heartbeat.json").write_text(
        json.dumps({"outcome": "running"}),
        encoding="utf-8",
    )

    client = TestClient(create_app())
    payload = client.get("/api/status").json()["mainnet_follow_validation"]

    assert payload["verdict"] == "in_progress"
    assert payload["progress"]["run"] == run_dir.name
    assert payload["progress"]["healthy"] is True
    assert payload["progress"]["runner_online"] is True
    assert payload["progress"]["watchdog_ready"] is True
    assert payload["progress"]["snapshot_count"] == 1
    assert payload["journal"]["copy_intent_count"] == 0


def test_gui_sets_security_headers(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    client = TestClient(create_app())

    response = client.get("/")

    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]
    assert "unsafe-inline" not in response.headers["content-security-policy"]
    nonce_match = re.search(
        r"script-src 'self' 'nonce-([^']+)'",
        response.headers["content-security-policy"],
    )
    assert nonce_match is not None
    nonce = nonce_match.group(1)
    assert f'<style nonce="{nonce}">' in response.text
    assert response.headers["cross-origin-opener-policy"] == "same-origin"
    assert response.headers["permissions-policy"].startswith("camera=()")
    assert response.headers["referrer-policy"] == "no-referrer"


def test_gui_exposes_redacted_run_history(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    client = TestClient(create_app(run_history_service=RunHistoryService(tmp_path / "runs")))

    response = client.get("/api/runs")

    assert response.status_code == 200
    assert response.json() == {
        "status": "empty",
        "report_count": 0,
        "ready_count": 0,
        "claimed_ready_count": 0,
        "signed_lifecycle_count": 0,
        "blocked_count": 0,
        "inconsistent_count": 0,
        "quality_issue_count": 0,
        "rows": [],
        "errors": [],
    }


def test_fleet_console_keeps_analytics_and_avoids_dynamic_inner_html():
    template = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "hyperliquid_copytrader"
        / "web"
        / "templates"
        / "index.html"
    ).read_text(encoding="utf-8")

    assert 'item.state === "RUNNING" && item.data_stale !== true' in template

    assert ".innerHTML" not in template
    assert "{{ operator_token }}" not in template
    assert 'id="continuous-fleet-console"' in template
    assert "/api/continuous/status" in template
    assert "/api/continuous/plan" in template
    assert "window.setInterval(refreshStatus,10000)" in template
    assert "30D Profit Leaderboard" in template
    assert "Address Analyzer" in template
    assert "Fleet accounts" in template
    assert "UPDATE_FLEET" in template
    assert "API wallet private key" not in template
    assert "textContent" in template


def test_market_universe_endpoint_is_unsigned_complete_and_cached(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))

    class FakeCatalogClient:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def info(self, payload: dict[str, Any]) -> Any:
            request_type = str(payload.get("type"))
            self.calls.append(request_type)
            if request_type == "perpDexs":
                return [None, {"name": "xyz"}]
            if request_type == "allPerpMetas":
                return [
                    {"universe": [{"name": "BTC", "szDecimals": 5}]},
                    {"universe": [{"name": "AAPL", "szDecimals": 3}]},
                ]
            raise AssertionError(f"unexpected info request {request_type}")

    fake = FakeCatalogClient()
    client = TestClient(create_app(market_catalog_client_factory=lambda _url: fake))

    first = client.get("/api/market-universe?network=mainnet")
    second = client.get("/api/market-universe?network=mainnet")

    assert first.status_code == 200
    payload = first.json()
    assert payload["symbols"] == ["BTC", "xyz:AAPL"]
    assert payload["active_market_count"] == 2
    assert payload["dex_count"] == 2
    assert len(payload["sha256"]) == 64
    assert payload["read_only_query"] is True
    assert payload["signed_action_performed"] is False
    assert payload["launch_catalog_is_refreshed_separately"] is True
    assert second.json()["sha256"] == payload["sha256"]
    assert fake.calls == ["perpDexs", "allPerpMetas", "perpDexs"]


def test_market_universe_endpoint_rejects_unknown_network(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    client = TestClient(create_app())

    response = client.get("/api/market-universe?network=production")

    assert response.status_code == 422
    assert response.json()["detail"] == "network must be mainnet or testnet"


def test_market_universe_is_cache_only_while_continuous_runner_is_online(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))

    class Controller:
        online = True

        def status(self) -> dict[str, bool]:
            return {"online": self.online}

    class CatalogClient:
        calls = 0

        def info(self, payload):  # type: ignore[no-untyped-def]
            self.calls += 1
            if payload["type"] == "perpDexs":
                return [None]
            return [{"universe": [{"name": "BTC", "szDecimals": 5}]}]

    controller = Controller()
    catalog = CatalogClient()
    app = create_app(
        continuous_launch_controller=cast(Any, controller),
        market_catalog_client_factory=lambda _url: cast(Any, catalog),
    )
    client = TestClient(app)

    blocked = client.get("/api/market-universe?network=mainnet")
    assert blocked.status_code == 503
    assert catalog.calls == 0

    controller.online = False
    loaded = client.get("/api/market-universe?network=mainnet")
    assert loaded.status_code == 200
    assert catalog.calls == 3

    timestamp, manifest = app.state.market_catalog_cache["mainnet"]
    app.state.market_catalog_cache["mainnet"] = (timestamp - 600, manifest)
    controller.online = True
    stale = client.get("/api/market-universe?network=mainnet")
    assert stale.status_code == 200
    assert stale.json()["cache_stale"] is True
    assert stale.json()["external_refresh_blocked_while_runner_online"] is True
    assert catalog.calls == 3


def test_address_analysis_endpoint_rejects_invalid_address(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    monkeypatch.setenv("HLCT_LEADERBOARD_ENABLE", "false")
    monkeypatch.setenv("HLCT_ADDRESS_ANALYTICS_ENABLE", "false")
    client = TestClient(create_app())

    response = client.get("/api/address-analysis?address=not-an-address")

    assert response.status_code == 422


def test_dashboard_exposes_preloaded_subaccount_assignments(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    monkeypatch.setenv("HLCT_LEADERBOARD_ENABLE", "false")
    monkeypatch.setenv(
        "HLCT_SUBACCOUNT_ASSIGNMENTS_JSON",
        (
            '[{"slot":"btc-copy","subaccount":"0xf000000000000000000000000000000000000000",'
            '"source_wallet":"0x1111111111111111111111111111111111111111",'
            '"mode":"testnet","enabled":true,"subaccount_verified":true,'
            '"operator_verified_at":"2026-07-07T00:00:00Z","note":"manual BTC source"}]'
        ),
    )
    client = TestClient(create_app())

    payload = client.get("/api/status?include_recent=false").json()

    rows = payload["subaccount_monitoring"]
    assert any(row["slot"] == "btc-copy" for row in rows)
    row = next(row for row in rows if row["slot"] == "btc-copy")
    assert row["status"] == "enabled"
    assert row["subaccount_verified"] is True
    assert row["operator_verified_at"] == "2026-07-07T00:00:00Z"
    assert row["verification"] == "verified 2026-07-07T00:00:00Z"


def test_dashboard_flags_unverified_enabled_subaccount_assignment(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    monkeypatch.setenv("HLCT_LEADERBOARD_ENABLE", "false")
    monkeypatch.setenv(
        "HLCT_SUBACCOUNT_ASSIGNMENTS_JSON",
        (
            '[{"slot":"btc-copy","subaccount":"0xf000000000000000000000000000000000000000",'
            '"source_wallet":"0x1111111111111111111111111111111111111111",'
            '"mode":"testnet","enabled":true,"note":"manual BTC source"}]'
        ),
    )
    client = TestClient(create_app())

    payload = client.get("/api/status?include_recent=false").json()

    row = next(row for row in payload["subaccount_monitoring"] if row["slot"] == "btc-copy")
    assert row["status"] == "blocked:unverified"
    assert row["subaccount_verified"] is False
    assert row["verification"] == "unverified"
    assert any(
        "btc-copy is enabled but subaccount_verified is not true" in blocker
        for blocker in payload["preflight"]["blockers"]
    )


def test_dashboard_exposes_active_subaccount_assignment_match(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_MODE", "testnet")
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    monkeypatch.setenv("HLCT_LEADERBOARD_ENABLE", "false")
    monkeypatch.setenv("HLCT_ADDRESS_ANALYTICS_ENABLE", "false")
    monkeypatch.setenv("HLCT_FOLLOWER_ACCOUNT_ADDRESS", FOLLOWER_ACCOUNT)
    monkeypatch.setenv("HLCT_API_PRIVATE_KEY", TESTNET_API_KEY)
    monkeypatch.setenv(
        "HLCT_SUBACCOUNT_ASSIGNMENTS_JSON",
        (
            f'[{{"slot":"btc-copy","subaccount":"{FOLLOWER_ACCOUNT}",'
            '"source_wallet":"0xcf7c4feb434751146a48b895e96caeb15838f92c",'
            '"mode":"testnet","enabled":true,"subaccount_verified":true,'
            '"operator_verified_at":"2026-07-07T00:00:00Z","note":"manual BTC source"}]'
        ),
    )
    client = TestClient(create_app())

    payload = client.get("/api/status?include_recent=false").json()
    readiness = client.get("/api/readiness").json()

    active_assignment = payload["active_subaccount_assignment"]
    assert active_assignment["status"] == "matched"
    assert active_assignment["matched_slot"] == "btc-copy"
    assert active_assignment["action_account"] == FOLLOWER_ACCOUNT
    assignment_check = next(
        check for check in readiness["checks"] if check["name"] == "active_subaccount_assignment"
    )
    assert assignment_check["passed"] is True
    assert "slot=btc-copy" in assignment_check["detail"]


def test_dashboard_exposes_intent_reason_and_latest_blocker(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    monkeypatch.setenv("HLCT_LEADERBOARD_ENABLE", "false")
    monkeypatch.setenv("HLCT_ADDRESS_ANALYTICS_ENABLE", "false")
    client = TestClient(create_app())
    created = now_ms()
    intent = FollowerIntent(
        intent_id="intent-blocked",
        cloid="0x11111111111111111111111111111111",
        action=IntentAction.OPEN,
        coin="BTC",
        side="buy",
        size=Decimal("0.01"),
        price=Decimal("50000"),
        reduce_only=False,
        mode=Mode.SHADOW,
        source_event_key="source-1",
        reason="move follower toward desired target; entry_slippage_bps=20",
        created_ms=created,
    )
    _app_state(client).service.store.append_intent(intent)
    _app_state(client).service.store.append_execution_report(
        ExecutionReport(
            report_id="blocked-report",
            intent_id=intent.intent_id,
            cloid=intent.cloid,
            status=IntentStatus.SKIPPED,
            exchange_status="blocked:risk_limit",
            exchange_ts_ms=created,
            payload={"detail": "would exceed max notional cap"},
        )
    )

    status = client.get("/api/status").json()

    row = status["recent_intents"][0]
    assert row["reason"] == "move follower toward desired target; entry_slippage_bps=20"
    assert row["outcome"] == "blocked"
    assert row["latest_exchange_status"] == "blocked:risk_limit"
    assert row["latest_report_detail"] == "would exceed max notional cap"


def test_status_endpoint_can_skip_recent_journal_rows(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    monkeypatch.setenv("HLCT_LEADERBOARD_ENABLE", "false")
    client = TestClient(create_app())

    def unexpected_recent(*_args, **_kwargs):
        raise AssertionError("recent journal rows should not be read for lightweight status")

    _app_state(client).service.store.recent = unexpected_recent
    _app_state(client).service.store.recent_intents = unexpected_recent
    _app_state(client).service.store.rebuild_runtime_state = unexpected_recent

    response = client.get("/api/status?include_recent=false")

    assert response.status_code == 200
    payload = response.json()
    assert payload["recent_source_events"] == []
    assert payload["recent_intents"] == []
    assert payload["runtime_state"]["latest_source_events"] == []
    assert payload["source_health"]["event_count"] == 0
    assert payload["reconciliation_status"]["source"]["status"] == "missing"
    assert payload["reconciliation_status"]["follower"]["status"] == "not_required"


def test_status_enriches_active_asset_data_events(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    monkeypatch.setenv("HLCT_LEADERBOARD_ENABLE", "false")
    client = TestClient(create_app())
    _app_state(client).service.store.append_source_event(
        SourceEvent(
            idempotency_key="active-asset-data",
            event_type=SourceEventType.LEVERAGE,
            exchange_ts_ms=7777,
            observed_ts_ms=7777,
            payload={
                "event_subtype": "active_asset_data",
                "coins": ["BTC"],
                "leverage": "cross:3",
                "available_to_trade": "0.5/0.75",
                "event_count": 1,
                "timestamp_source": "observed",
            },
        )
    )

    row = client.get("/api/status").json()["recent_source_events"][0]

    assert row["event_subtype"] == "active_asset_data"
    assert row["event_leverage"] == "cross:3"
    assert row["event_available_to_trade"] == "0.5/0.75"
    assert "payload_json" not in row


def test_dashboard_exposes_reconciliation_status_from_journal(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    monkeypatch.setenv("HLCT_LEADERBOARD_ENABLE", "false")
    monkeypatch.setenv("HLCT_ADDRESS_ANALYTICS_ENABLE", "false")
    client = TestClient(create_app())
    observed = now_ms()
    _app_state(client).service.store.append_source_event(
        SourceEvent(
            idempotency_key="source-snapshot",
            event_type=SourceEventType.SNAPSHOT,
            exchange_ts_ms=observed,
            observed_ts_ms=observed,
            payload={"event_subtype": "rest_snapshot", "coins": ["BTC"]},
        )
    )
    _app_state(client).service.store.append_reconcile_snapshot(
        ReconcileSnapshot(
            snapshot_id="follower-snapshot",
            account="0xf000000000000000000000000000000000000000",
            positions={"BTC": Position(coin="BTC", size=Decimal("0.1"), updated_ms=observed)},
            open_orders=[
                OpenOrder(
                    coin="BTC",
                    side="buy",
                    size=Decimal("0.1"),
                    price=Decimal("50000"),
                    updated_ms=observed,
                )
            ],
            observed_ms=observed,
            source="fake",
        )
    )

    payload = client.get("/api/status").json()

    status = payload["reconciliation_status"]
    assert status["source"]["status"] == "fresh"
    assert status["source"]["latest_key"] == "source-snapshot"
    assert status["follower"]["status"] == "fresh"
    assert status["follower"]["required"] is False
    assert status["follower"]["positions"] == 1
    assert status["follower"]["open_orders"] == 1
    assert status["blockers"] == []


def test_gui_blocks_cross_origin_control_post(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    client = TestClient(create_app(control_authority=True))
    response = client.post(
        "/controls/pause",
        headers={"origin": "http://evil.example"},
        data={"reason": "csrf"},
        follow_redirects=False,
    )
    assert response.status_code == 403
    audit_rows = client.get("/api/status").json()["recent_control_audit"]
    assert audit_rows[0]["control"] == "pause"
    assert audit_rows[0]["status"] == "denied"


def test_plain_gui_factory_is_monitor_only(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    client = TestClient(create_app(), base_url="http://testserver")

    response = client.post(
        "/controls/pause",
        headers={"origin": "http://testserver"},
        data={"reason": "should not be authoritative"},
        follow_redirects=False,
    )

    assert response.status_code == 409
    assert "monitor-only" in response.json()["detail"]
    status = client.get("/api/status").json()
    assert status["runner"]["control_authority"] is False
    assert status["safe_mode"]["enabled"] is False


def test_legacy_shadow_controls_are_not_rendered(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    html = TestClient(
        create_app(
            control_authority=True,
            continuous_launch_controller=cast(Any, object()),
        )
    ).get("/").text

    assert 'id="continuous-fleet-console"' in html
    assert 'id="run-generation"' in html
    assert 'id="followers-flat"' in html
    assert 'id="diagnostics-state"' in html
    assert "latest_price_deviation_bps" in html
    assert 'action="/controls/run-once"' not in html
    assert 'action="/controls/pause"' not in html
    assert 'action="/controls/testnet-smoke"' not in html


def test_monitor_only_gui_does_not_construct_exchange_executor(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    monkeypatch.setenv("HLCT_MODE", "testnet")
    monkeypatch.setenv("HLCT_FOLLOWER_ACCOUNT_ADDRESS", FOLLOWER_ACCOUNT)
    monkeypatch.setenv("HLCT_API_PRIVATE_KEY", TESTNET_API_KEY)

    app = create_app()

    assert app.state.control_authority is False
    assert app.state.service.execution_enabled is False
    assert app.state.service.execution_adapter is None


def test_integrated_gui_controls_refuse_live_mainnet_mode(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    monkeypatch.setenv("HLCT_MODE", "live")

    with pytest.raises(ValueError, match="disabled for live/mainnet"):
        create_app(control_authority=True)


def test_gui_status_refreshes_safe_mode_written_by_another_process(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    app = create_app()
    client = TestClient(app)
    external = SafeModeController(app.state.service.store)
    external.trip(SafeModeReason.WEBSOCKET_DISCONNECT, "external runner disconnected")

    status = client.get("/api/status?include_recent=false").json()

    assert status["safe_mode"]["enabled"] is True
    assert status["safe_mode"]["reason"] == "websocket_disconnect"
    assert status["safe_mode"]["revision"] == status["runtime_state"]["latest_safe_mode"]["seq"]


def test_gui_source_health_ignores_other_wallet_history(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    app = create_app()
    app.state.service.store.append_source_event(
        SourceEvent(
            idempotency_key="other-source-event",
            event_type=SourceEventType.FILL,
            source_wallet="0x" + "9" * 40,
            exchange_ts_ms=now_ms(),
            observed_ts_ms=now_ms(),
            payload={"event_subtype": "fill"},
        )
    )

    status = TestClient(app).get("/api/status").json()

    assert status["source_health"]["event_count"] == 0
    assert status["source_health"]["latest_age_ms"] is None
    assert status["recent_source_events"] == []


def test_integrated_console_worker_publishes_authoritative_heartbeat(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    service = CopyTraderService(load_config("shadow"))
    cycle_ran = Event()

    def fake_run_once():
        cycle_ran.set()
        return {"safe_mode": {"enabled": False}}

    monkeypatch.setattr(service, "run_once", fake_run_once)
    app = create_app(
        service=service,
        control_authority=True,
        start_worker=True,
        worker_interval_s=0.05,
    )

    with TestClient(app) as client:
        assert cycle_ran.wait(timeout=2)
        status = client.get("/api/status?include_recent=false").json()
        runner = status["runner"]
        assert runner["online"] is True
        assert runner["owner_instance"] is True
        assert runner["config_matches"] is True
        assert runner["control_authority"] is True
        assert runner["integrated_worker"] is True
        assert runner["cycle_count"] >= 1

    stopped = service.runner_status()
    assert stopped["online"] is False
    assert stopped["status"] == "stopped"


def test_monitor_process_reads_matching_runner_heartbeat(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    config = load_config("shadow")
    runner_service = CopyTraderService(config)
    monitor_service = CopyTraderService(config)
    try:
        runner_service.record_runner_heartbeat(
            status="idle",
            detail="external CLI runner idle",
            ttl_ms=60_000,
            cycle_completed=True,
        )
        with TestClient(create_app(service=monitor_service)) as client:
            runner = client.get("/api/status?include_recent=false").json()["runner"]

        assert runner["online"] is True
        assert runner["owner_instance"] is False
        assert runner["config_matches"] is True
        assert runner["control_authority"] is False
        assert runner["detail"] == "external CLI runner idle"
    finally:
        monitor_service.store.close()
        runner_service.store.close()


def test_gui_allows_same_origin_control_post(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    client = TestClient(create_app(control_authority=True), base_url="http://testserver")
    response = client.post(
        "/controls/pause",
        headers={"origin": "http://testserver"},
        data={"reason": "operator"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    audit_rows = client.get("/api/status").json()["recent_control_audit"]
    assert audit_rows[0]["control"] == "pause"
    assert audit_rows[0]["status"] == "success"


def test_gui_blocks_dns_rebinding_control_host_without_token(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    client = TestClient(create_app(control_authority=True), base_url="http://attacker.example")
    response = client.post(
        "/controls/pause",
        headers={"origin": "http://attacker.example"},
        data={"reason": "dns rebinding attempt"},
        follow_redirects=False,
    )
    assert response.status_code == 421
    assert "literal loopback Host" in response.text


def test_gui_blocks_remote_peer_spoofing_loopback_host_without_token(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    client = TestClient(
        create_app(control_authority=True),
        base_url="http://127.0.0.1",
        client=("203.0.113.10", 50000),
    )
    response = client.post(
        "/controls/pause",
        headers={"host": "127.0.0.1", "origin": "http://127.0.0.1"},
        data={"reason": "spoofed loopback authority"},
        follow_redirects=False,
    )
    assert response.status_code == 421
    assert "literal loopback Host" in response.text


def test_gui_refuses_non_loopback_startup_without_token(base_config, tmp_path):
    config = replace(base_config, host="0.0.0.0", db_path=tmp_path / "remote.sqlite3")
    service = CopyTraderService(config)

    with pytest.raises(ValueError, match="direct non-loopback GUI binding is unsupported"):
        create_app(service=service)


def test_tokenless_gui_rejects_non_loopback_host_on_read_routes(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_SOURCE_WALLET", "0xcf7c4feb434751146a48b895e96caeb15838f92c")
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "dns-rebind.sqlite3"))
    client = TestClient(create_app())

    response = client.get("/api/status", headers={"host": "attacker.example"})

    assert response.status_code == 421
    assert "loopback Host" in response.text


def test_gui_requires_operator_token_when_configured(monkeypatch, tmp_path):
    token = "a-very-long-operator-token"
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    monkeypatch.setenv("HLCT_GUI_TOKEN", token)
    client = TestClient(create_app(control_authority=True), base_url="http://testserver")
    missing = client.post(
        "/controls/pause",
        headers={"origin": "http://testserver"},
        data={"reason": "missing"},
        follow_redirects=False,
    )
    assert missing.status_code == 401
    assert "Basic" in missing.headers["www-authenticate"]
    authorization = "Basic " + base64.b64encode(f"operator:{token}".encode()).decode()
    ok = client.post(
        "/controls/pause",
        headers={"origin": "http://testserver", "authorization": authorization},
        data={"reason": "ok"},
        follow_redirects=False,
    )
    assert ok.status_code == 303


def test_gui_rate_limits_control_posts(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    monkeypatch.setenv("HLCT_DASHBOARD_CONTROL_MAX_PER_MINUTE", "1")
    client = TestClient(create_app(control_authority=True), base_url="http://testserver")

    first = client.post(
        "/controls/reconcile",
        headers={"origin": "http://testserver"},
        follow_redirects=False,
    )
    second = client.post(
        "/controls/reconcile",
        headers={"origin": "http://testserver"},
        follow_redirects=False,
    )

    assert first.status_code == 303
    assert second.status_code == 429
    audit_rows = client.get("/api/status").json()["recent_control_audit"]
    assert audit_rows[0]["status"] == "denied"
    assert "rate limit" in audit_rows[0]["detail"]


def test_gui_audits_structured_blocked_control_result(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    app = create_app(control_authority=True)
    app.state.service.run_once = lambda: {
        "passed": False,
        "safe_mode": {"enabled": True, "reason": "rest_lag", "detail": "source stale"},
    }
    client = TestClient(app, base_url="http://testserver")

    response = client.post(
        "/controls/run-once",
        headers={"origin": "http://testserver"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    audit = client.get("/api/status").json()["recent_control_audit"][0]
    assert audit["status"] == "blocked"
    assert "source stale" in audit["detail"]


def test_gui_bounds_analytics_queries_and_windows(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    app = create_app()
    app.state.analytics_query_rate_limiter = SlidingWindowRateLimiter(max_events=1)
    client = TestClient(app)

    first = client.get("/api/leaderboard")
    second = client.get("/api/leaderboard")

    assert first.status_code == 200
    assert second.status_code == 429
    app.state.analytics_query_rate_limiter = SlidingWindowRateLimiter(max_events=10)
    invalid = client.get(
        "/api/address-analysis?address=0x1111111111111111111111111111111111111111&window_days=366"
    )
    assert invalid.status_code == 422
    invalid_limit = client.get("/api/leaderboard?limit=101")
    assert invalid_limit.status_code == 422
    invalid_volume = client.get("/api/leaderboard?min_volume_usd=-1")
    assert invalid_volume.status_code == 422


def test_address_analyzer_is_cache_only_while_continuous_runner_is_online(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    calls: list[str] = []

    class Controller:
        online = True

        def status(self) -> dict[str, bool]:
            return {"online": self.online}

    class Analyzer:
        def local_snapshot(self, address: str, *, window_days: int | None = None) -> dict:
            calls.append(f"local:{address}:{window_days}")
            return {"status": "local_only_unavailable", "address": address}

        def analyze(
            self,
            address: str,
            *,
            force_refresh: bool = False,
            window_days: int | None = None,
        ) -> dict:
            calls.append(f"external:{address}:{force_refresh}:{window_days}")
            return {"status": "fresh", "address": address}

    controller = Controller()
    app = create_app(continuous_launch_controller=cast(Any, controller))
    app.state.address_analysis = Analyzer()
    client = TestClient(app)
    address = "0x1111111111111111111111111111111111111111"

    online = client.get(
        f"/api/address-analysis?address={address}&window_days=30&force_refresh=true"
    )
    controller.online = False
    offline = client.get(
        f"/api/address-analysis?address={address}&window_days=30&force_refresh=true"
    )

    assert online.json()["status"] == "local_only_unavailable"
    assert offline.json()["status"] == "fresh"
    assert calls == [
        f"local:{address}:30",
        f"external:{address}:True:30",
    ]


def test_leaderboard_is_cache_only_while_continuous_runner_is_online(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    calls: list[str] = []

    class Controller:
        online = True

        def status(self) -> dict[str, bool]:
            return {"online": self.online}

    class Leaderboard:
        def local_snapshot(self, **_filters: object) -> dict:
            calls.append("local")
            return {"status": "local_only_unavailable", "rows": []}

        def snapshot(self, **_filters: object) -> dict:
            calls.append("external")
            return {"status": "fresh", "rows": []}

    controller = Controller()
    app = create_app(continuous_launch_controller=cast(Any, controller))
    app.state.leaderboard = Leaderboard()
    client = TestClient(app)

    online = client.get("/api/leaderboard?force_refresh=true")
    controller.online = False
    offline = client.get("/api/leaderboard?force_refresh=true")

    assert online.json()["status"] == "local_only_unavailable"
    assert offline.json()["status"] == "fresh"
    assert calls == ["local", "external"]


def test_continuous_runner_panel_and_http_boundary(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    monkeypatch.delenv("HLCT_GUI_TOKEN", raising=False)
    calls: list[tuple[object, ...]] = []

    class ContinuousController:
        online = False

        def status(self) -> dict[str, object]:
            calls.append(("status",))
            return {
                "status": "running" if self.online else "idle",
                "online": self.online,
                "execution_enabled": self.online,
            }

        @staticmethod
        def preview() -> dict[str, object]:
            calls.append(("preview",))
            return {
                "launchable": True,
                "blockers": [],
                "acknowledgement": "LIVE_CONTINUOUS",
            }

        def start(self, *, acknowledgement: str) -> dict[str, object]:
            calls.append(("start", acknowledgement))
            self.online = True
            return self.status()

        @staticmethod
        def update_leaders(*, leaders: dict[str, str], acknowledgement: str) -> dict[str, object]:
            calls.append(("leaders", leaders, acknowledgement))
            return {"launchable": True, "plan": {"slots": []}}

        @staticmethod
        def update_fleet(
            *,
            slots: list[dict[str, object]],
            max_combined_gross_usd: object,
            acknowledgement: str,
        ) -> dict[str, object]:
            calls.append(("fleet", slots, max_combined_gross_usd, acknowledgement))
            return {"launchable": True, "plan": {"slots": slots}}

        def stop(self, *, acknowledgement: str) -> dict[str, object]:
            calls.append(("stop", acknowledgement))
            return {**self.status(), "status": "stopping"}

    controller = ContinuousController()
    app = create_app(continuous_launch_controller=cast(Any, controller))
    client = TestClient(
        app,
        base_url="http://127.0.0.1",
        client=("127.0.0.1", 50000),
    )
    origin = {"origin": "http://127.0.0.1"}

    html = client.get("/")
    preview = client.post("/api/continuous/preview", headers=origin, json={})
    leaders = client.post(
        "/api/continuous/leaders",
        headers=origin,
        json={
            "leaders": {"acc1": "0x" + "1" * 40},
            "acknowledgement": "UPDATE_LEADERS",
        },
    )
    fleet = client.post(
        "/api/continuous/plan",
        headers=origin,
        json={
            "slots": [],
            "max_combined_gross_usd": "30",
            "acknowledgement": "UPDATE_FLEET",
        },
    )
    start = client.post(
        "/api/continuous/start",
        headers=origin,
        json={"acknowledgement": "LIVE_CONTINUOUS"},
    )
    status = client.get("/api/continuous/status")
    stop = client.post(
        "/api/continuous/stop",
        headers=origin,
        json={"acknowledgement": "STOP_CONTINUOUS"},
    )

    assert html.status_code == 200
    assert 'id="continuous-fleet-console"' in html.text
    assert "runner?.alarm_threshold_ms" in html.text
    assert "last_successful_sync_ms" in html.text
    assert "requiredConnection(runner)" in html.text
    assert 'id="fleet-launch-panel"' not in html.text
    assert preview.json()["acknowledgement"] == "LIVE_CONTINUOUS"
    assert leaders.status_code == 200
    assert fleet.status_code == 200
    assert start.json()["online"] is True
    assert status.json()["status"] == "running"
    assert stop.json()["status"] == "stopping"
    assert ("preview",) in calls
    assert ("leaders", {"acc1": "0x" + "1" * 40}, "UPDATE_LEADERS") in calls
    assert ("fleet", [], "30", "UPDATE_FLEET") in calls
    assert ("start", "LIVE_CONTINUOUS") in calls
    assert ("stop", "STOP_CONTINUOUS") in calls


def test_gui_never_opens_signer_keys_when_rendering_profiles(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    source = "0x1000000000000000000000000000000000000001"
    owner = "0x2000000000000000000000000000000000000002"
    follower = "0x3000000000000000000000000000000000000003"
    signer = "0x4000000000000000000000000000000000000004"
    legacy_dir = tmp_path / ".secrets" / "operator-profile"
    fleet_dir = tmp_path / ".secrets" / "operator-profiles" / "acc1"
    legacy_dir.mkdir(parents=True)
    fleet_dir.mkdir(parents=True)
    legacy_key = legacy_dir / "api-wallet.key"
    fleet_key = fleet_dir / "api-wallet.key"
    legacy_key.write_text("must-not-be-opened", encoding="utf-8")
    fleet_key.write_text("must-not-be-opened", encoding="utf-8")
    public_fields = {
        "profile_label": "pilot",
        "network": "mainnet",
        "source_wallet": source,
        "global_account_address": owner,
        "subaccount_name": "acc1",
        "follower_account_address": follower,
        "api_wallet_address": signer,
        "expected_account_mode": "standard",
        "coin": "BTC",
    }
    (legacy_dir / "profile.json").write_text(
        json.dumps(
            {
                **public_fields,
                "profile_version": 2,
                "api_private_key_file": str(legacy_key.resolve()),
            }
        ),
        encoding="utf-8",
    )
    (fleet_dir / "profile.json").write_text(
        json.dumps(
            {
                **public_fields,
                "profile_version": 3,
                "profile_id": "acc1",
                "eligibility": "all_active_markets",
                "denied_symbols": [],
                "api_private_key_file": str(fleet_key.resolve()),
            }
        ),
        encoding="utf-8",
    )

    app = create_app(credential_root=tmp_path)
    app.state.settings_rate_limiter = SlidingWindowRateLimiter(max_events=100)
    original_read_text = Path.read_text

    def guarded_read_text(path: Path, *args, **kwargs):
        if path.name == "api-wallet.key":
            raise AssertionError("browser UI attempted to open signer-key content")
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", guarded_read_text)
    client = TestClient(app, base_url="http://testserver")

    assert client.get("/").status_code == 200
    legacy_status = client.get("/api/credentials")
    fleet_status = client.get("/api/credential-profiles")
    assert legacy_status.status_code == 200
    assert legacy_status.json()["credential_content_read"] is False
    assert fleet_status.status_code == 200
    assert fleet_status.json()["credential_content_read"] is False
    assert fleet_status.json()["profiles"][0]["status"] == "metadata_present"

    assert client.get("/api/credential-profiles/export").status_code == 404


def test_gui_never_rate_limits_emergency_pause(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    monkeypatch.setenv("HLCT_DASHBOARD_CONTROL_MAX_PER_MINUTE", "1")
    client = TestClient(create_app(control_authority=True), base_url="http://testserver")

    first = client.post(
        "/controls/pause",
        headers={"origin": "http://testserver"},
        data={"reason": "first emergency pause"},
        follow_redirects=False,
    )
    second = client.post(
        "/controls/pause",
        headers={"origin": "http://testserver"},
        data={"reason": "repeat emergency pause"},
        follow_redirects=False,
    )

    assert first.status_code == 303
    assert second.status_code == 303
    assert client.get("/api/status").json()["safe_mode"]["enabled"] is True


def test_gui_never_renders_configured_operator_token(monkeypatch, tmp_path):
    token = "a-very-long-operator-token"
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    monkeypatch.setenv("HLCT_GUI_TOKEN", token)
    client = TestClient(create_app())
    unauthorized = client.get("/")
    assert unauthorized.status_code == 401
    authorization = "Basic " + base64.b64encode(f"operator:{token}".encode()).decode()
    html = client.get("/", headers={"authorization": authorization})
    assert html.status_code == 200
    assert token not in html.text
    assert 'name="operator_token"' not in html.text
    api = client.get("/api/status", headers={"x-operator-token": token})
    assert api.status_code == 200


def test_gui_control_invalid_form_value_trips_safe_mode(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    client = TestClient(create_app(control_authority=True), base_url="http://testserver")
    response = client.post(
        "/controls/testnet-smoke",
        headers={"origin": "http://testserver"},
        data={"coin": "BTC", "size": "not-a-decimal"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    status = client.get("/api/status").json()
    assert status["safe_mode"]["enabled"] is True
    assert status["safe_mode"]["reason"] == "config_invalid"
    assert "GUI control testnet-smoke failed" in status["safe_mode"]["detail"]


def test_gui_active_smoke_invalid_form_value_trips_safe_mode(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    client = TestClient(create_app(control_authority=True), base_url="http://testserver")
    response = client.post(
        "/controls/testnet-active-smoke",
        headers={"origin": "http://testserver"},
        data={"coin": "BTC", "size": "not-a-decimal"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    status = client.get("/api/status").json()
    assert status["safe_mode"]["enabled"] is True
    assert status["safe_mode"]["reason"] == "config_invalid"
    assert "GUI control testnet-active-smoke failed" in status["safe_mode"]["detail"]


def test_gui_control_unexpected_exception_trips_safe_mode(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "gui.sqlite3"))
    client = TestClient(create_app(control_authority=True), base_url="http://testserver")

    def boom():
        raise RuntimeError("operator action exploded")

    _app_state(client).service.run_once = boom
    response = client.post(
        "/controls/run-once",
        headers={"origin": "http://testserver"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    status = client.get("/api/status").json()
    assert status["safe_mode"]["enabled"] is True
    assert status["safe_mode"]["reason"] == "config_invalid"
    assert "GUI control run-once failed: operator action exploded" in status["safe_mode"]["detail"]
