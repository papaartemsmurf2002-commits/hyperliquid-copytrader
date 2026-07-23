from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "select_risk_profile.py"
SPEC = importlib.util.spec_from_file_location("select_risk_profile", SCRIPT_PATH)
assert SPEC is not None
select_risk_profile = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = select_risk_profile
SPEC.loader.exec_module(select_risk_profile)


def source(slot: int, address: str, *, fills: int, simulations: list[dict]) -> dict:
    return {
        "address": address,
        "fills": fills,
        "slot": slot,
        "source_approx_roi_pct": "100",
        "simulations": simulations,
    }


def sim(
    leverage: int,
    *,
    net: str,
    min_equity: str = "40",
    zeroed: bool = False,
    copied: int = 10,
) -> dict:
    return {
        "strategy": f"risk_budget_50_cap_{leverage}x",
        "initial_equity_usd": "50",
        "ending_equity_usd": str(50 + float(net)),
        "net_pnl_usd": net,
        "max_drawdown_usd": "10",
        "min_equity_usd": min_equity,
        "max_effective_leverage": leverage,
        "copied_fills": copied,
        "skipped_min_notional_fills": 2,
        "capped_fills": 1,
        "copied_notional_usd": "100",
        "source_net_pnl_seen_usd": "100",
        "liquidated_or_zero_equity": zeroed,
    }


def test_selector_picks_positive_non_zeroed_active_sources():
    report = select_risk_profile.select_risk_profile(
        {
            "days": 180,
            "initial_equity_usd": "50",
            "min_notional_usd": "10",
            "sources": [
                source(
                    1,
                    "0x1111111111111111111111111111111111111111",
                    fills=200,
                    simulations=[sim(1, net="5"), sim(5, net="20", min_equity="30")],
                ),
                source(
                    2,
                    "0x2222222222222222222222222222222222222222",
                    fills=150,
                    simulations=[sim(1, net="7"), sim(2, net="6", min_equity="45")],
                ),
                source(
                    3,
                    "0x3333333333333333333333333333333333333333",
                    fills=20,
                    simulations=[sim(20, net="500", min_equity="49")],
                ),
                source(
                    4,
                    "0x4444444444444444444444444444444444444444",
                    fills=200,
                    simulations=[sim(1, net="10", zeroed=True), sim(2, net="-1")],
                ),
            ],
        },
        target_slots=2,
    )

    assert report["risk_profile_ready"] is True
    assert [row["source_address"] for row in report["selected"]] == [
        "0x1111111111111111111111111111111111111111",
        "0x2222222222222222222222222222222222222222",
    ]
    assert report["selected"][0]["cap_leverage"] == 5
    assert report["aggregate"]["zeroed_slots"] == 0
    disqualified = {
        row["source_address"]: row["decision"] for row in report["disqualified_sources"]
    }
    assert (
        "source fills 20 below active-source minimum"
        in disqualified["0x3333333333333333333333333333333333333333"]
    )
    assert (
        "no non-zeroed leverage cap produced net PnL above 0"
        in disqualified["0x4444444444444444444444444444444444444444"]
    )


def test_selector_blocks_when_backtest_uses_old_min_notional():
    report = select_risk_profile.select_risk_profile(
        {
            "days": 180,
            "initial_equity_usd": "50",
            "min_notional_usd": "1",
            "sources": [
                source(
                    1,
                    "0x1111111111111111111111111111111111111111",
                    fills=200,
                    simulations=[sim(1, net="5")],
                )
            ],
        },
        target_slots=1,
    )

    assert report["risk_profile_ready"] is False
    assert any("min_notional_usd must be 10" in blocker for blocker in report["blockers"])


def test_selector_cli_writes_report_and_slot_plan(tmp_path: Path):
    backtest_path = tmp_path / "backtest.json"
    out_path = tmp_path / "risk.json"
    slot_plan_path = tmp_path / "slot-plan.json"
    backtest_path.write_text(
        json.dumps(
            {
                "days": 180,
                "initial_equity_usd": "50",
                "min_notional_usd": "10",
                "sources": [
                    source(
                        1,
                        "0x1111111111111111111111111111111111111111",
                        fills=200,
                        simulations=[sim(1, net="5")],
                    )
                ],
            }
        ),
        encoding="utf-8",
    )

    exit_code = select_risk_profile.main(
        [
            "--backtest-json",
            str(backtest_path),
            "--target-slots",
            "1",
            "--out",
            str(out_path),
            "--slot-plan-out",
            str(slot_plan_path),
        ]
    )

    assert exit_code == 0
    report = json.loads(out_path.read_text(encoding="utf-8"))
    slot_plan = json.loads(slot_plan_path.read_text(encoding="utf-8"))
    assert report["risk_profile_ready"] is True
    assert slot_plan["slots"][0]["max_emergency_leverage"] == "1"
    assert slot_plan["slots"][0]["fixed_multiplier"] == "1"
