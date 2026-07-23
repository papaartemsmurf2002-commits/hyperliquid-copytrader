from __future__ import annotations

import json
from dataclasses import replace

from hyperliquid_copytrader.config import ExchangeConfig
from hyperliquid_copytrader.security import redact_secrets
from hyperliquid_copytrader.service import CopyTraderService

from .fixtures.fake_hyperliquid import FakeInfoClient


def test_redact_secrets_recursively_masks_sensitive_fields():
    payload = {
        "exchange": {"api_private_key": "0xabc", "account": "0x123"},
        "ops": {"gui_token": "secret-token", "nested": [{"password": "pw"}]},
    }
    redacted = redact_secrets(payload)
    assert redacted["exchange"]["api_private_key"] == "<redacted>"
    assert redacted["exchange"]["account"] == "0x123"
    assert redacted["ops"]["gui_token"] == "<redacted>"
    assert redacted["ops"]["nested"][0]["password"] == "<redacted>"


def test_config_revision_does_not_persist_plaintext_secrets(base_config, store):
    secret_key = "0x" + "a" * 64
    gui_token = "long-gui-token-for-tests"
    config = replace(
        base_config,
        exchange=ExchangeConfig(api_private_key=secret_key),
        ops=replace(base_config.ops, gui_token=gui_token),
    )
    service = CopyTraderService(config, store=store, info_client=FakeInfoClient())
    service.run_once()

    row = store.recent("config_revisions", 1)[0]
    payload = json.loads(row["payload_json"])
    assert payload["exchange"]["api_private_key"] == "<redacted>"
    assert payload["ops"]["gui_token"] == "<redacted>"
    assert secret_key not in row["payload_json"]
    assert gui_token not in row["payload_json"]
    assert service.security_audit()["passed"]


def test_security_audit_detects_existing_unredacted_sensitive_values(store):
    with store.lock:
        with store.conn:
            store.conn.execute(
                "INSERT INTO config_revisions(revision_id, payload_json, created_ms) VALUES (?, ?, ?)",
                (
                    "legacy",
                    json.dumps({"exchange": {"api_private_key": "plain-secret"}}),
                    1,
                ),
            )
    findings = store.sensitive_value_findings()
    assert findings == [
        {"table": "config_revisions", "seq": 1, "path": "$.exchange.api_private_key"}
    ]


def test_security_audit_detects_configured_secret_occurrence(base_config, store):
    secret_key = "0x" + "b" * 64
    config = replace(base_config, exchange=ExchangeConfig(api_private_key=secret_key))
    service = CopyTraderService(config, store=store, info_client=FakeInfoClient())
    with store.lock:
        with store.conn:
            store.conn.execute(
                "INSERT INTO execution_reports(report_id, intent_id, cloid, status, exchange_status, payload_json, created_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    "leak-report",
                    "intent",
                    "0x11111111111111111111111111111111",
                    "rejected",
                    "exception",
                    json.dumps({"error": secret_key}),
                    1,
                ),
            )
    audit = service.security_audit()
    assert not audit["passed"]
    assert audit["configured_secret_occurrences"] == [
        {"table": "execution_reports", "seq": 1, "column": "payload_json"}
    ]


def test_dashboard_security_audit_uses_short_lived_cache(base_config, store, monkeypatch):
    config = replace(
        base_config,
        ops=replace(base_config.ops, dashboard_security_audit_ttl_ms=60_000),
    )
    service = CopyTraderService(config, store=store, info_client=FakeInfoClient())
    calls = {"configured": 0, "sensitive": 0}

    def configured(values):
        calls["configured"] += 1
        return []

    def sensitive():
        calls["sensitive"] += 1
        return []

    monkeypatch.setattr(store, "find_text_occurrences", configured)
    monkeypatch.setattr(store, "sensitive_value_findings", sensitive)

    first = service.dashboard()["security"]
    second = service.dashboard()["security"]

    assert calls == {"configured": 1, "sensitive": 1}
    assert first["cached"] is False
    assert second["cached"] is True
    assert second["cache_ttl_ms"] == 60_000


def test_exact_security_audit_bypasses_dashboard_cache(base_config, store, monkeypatch):
    config = replace(
        base_config,
        ops=replace(base_config.ops, dashboard_security_audit_ttl_ms=60_000),
    )
    service = CopyTraderService(config, store=store, info_client=FakeInfoClient())
    sensitive_findings = [
        {"table": "config_revisions", "seq": 1, "path": "$.exchange.api_private_key"}
    ]
    calls = {"configured": 0, "sensitive": 0}

    def configured(values):
        calls["configured"] += 1
        return []

    def sensitive():
        calls["sensitive"] += 1
        return sensitive_findings if calls["sensitive"] > 1 else []

    monkeypatch.setattr(store, "find_text_occurrences", configured)
    monkeypatch.setattr(store, "sensitive_value_findings", sensitive)

    assert service.dashboard()["security"]["passed"] is True
    cached = service.dashboard()["security"]
    exact = service.security_audit()

    assert cached["passed"] is True
    assert cached["cached"] is True
    assert exact["passed"] is False
    assert exact["sensitive_value_findings"] == sensitive_findings
    assert calls == {"configured": 2, "sensitive": 2}


def test_readiness_and_metrics_use_lightweight_dashboard(base_config, store, monkeypatch):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())

    def fail_recent(*args, **kwargs):
        raise AssertionError("recent journal rows should not be loaded for readiness or metrics")

    def fail_rebuild(*args, **kwargs):
        raise AssertionError("full runtime rebuild should not be loaded for readiness or metrics")

    monkeypatch.setattr(store, "recent", fail_recent)
    monkeypatch.setattr(store, "recent_intents", fail_recent)
    monkeypatch.setattr(store, "rebuild_runtime_state", fail_rebuild)

    readiness = service.readiness()
    metrics = service.metrics_text()

    assert readiness["readiness_label"] == "shadow_operational"
    assert 'hlct_security_audit_cache_age_ms{mode="shadow"}' in metrics
