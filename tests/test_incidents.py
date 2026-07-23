from __future__ import annotations

from hyperliquid_copytrader.incidents import assert_guidance_complete, incident_guidance
from hyperliquid_copytrader.models import SafeModeReason
from hyperliquid_copytrader.service import CopyTraderService

from .fixtures.fake_hyperliquid import FakeInfoClient


def test_incident_guidance_covers_every_safe_mode_reason():
    assert_guidance_complete()
    for reason in SafeModeReason:
        guidance = incident_guidance(reason, enabled=True)
        assert guidance["reason"] == reason.value
        assert guidance["severity"]
        assert guidance["required_action"]
        assert guidance["resume_gate"]


def test_incident_guidance_reports_normal_when_safe_mode_is_clear():
    guidance = incident_guidance(SafeModeReason.ORDER_TIMEOUT, enabled=False)
    assert guidance["reason"] == "none"
    assert guidance["severity"] == "normal"
    assert guidance["blocks_new_risk"] is False
    assert guidance["required_action"] == "Monitor source and follower state."


def test_testnet_incident_guidance_has_no_enable_flag_blocker():
    guidance = incident_guidance(SafeModeReason.TESTNET_BLOCKED, enabled=True)

    assert "enable testnet" not in str(guidance["required_action"]).lower()
    assert "no extra testnet enable flag" in str(guidance["required_action"]).lower()


def test_dashboard_exposes_safe_mode_incident_guidance(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    service.safe_mode.trip(SafeModeReason.ORDER_TIMEOUT, "cloid-1 timed out")

    safe_mode = service.dashboard()["safe_mode"]

    assert safe_mode["enabled"] is True
    assert safe_mode["reason"] == "order_timeout"
    assert safe_mode["incident"]["severity"] == "critical"
    assert safe_mode["incident"]["blocks_new_risk"] is True
    assert "query exchange order status" in safe_mode["incident"]["required_action"]
