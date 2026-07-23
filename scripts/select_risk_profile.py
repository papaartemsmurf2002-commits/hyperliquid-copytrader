from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


RISK_PROFILE_SELECTOR_VERSION = 1
RISK_STRATEGY_RE = re.compile(r"^risk_budget_(?P<budget>\d+)_cap_(?P<leverage>\d+)x$")
DEFAULT_TARGET_SLOTS = 10
DEFAULT_MIN_SOURCE_FILLS = 100
DEFAULT_MIN_EQUITY_FLOOR_PCT = Decimal("25")
DEFAULT_MIN_NET_PNL_USD = Decimal("0")
DEFAULT_ALLOWED_COINS = ("BTC", "ETH", "SOL")


class RiskProfileInputError(RuntimeError):
    """Raised when a backtest artifact cannot produce a usable risk profile."""


@dataclass(frozen=True)
class StrategyCandidate:
    source_slot: int
    source_address: str
    strategy: str
    risk_budget_usd: Decimal
    cap_leverage: int
    initial_equity_usd: Decimal
    ending_equity_usd: Decimal
    net_pnl_usd: Decimal
    min_equity_usd: Decimal
    max_drawdown_usd: Decimal
    copied_fills: int
    skipped_min_notional_fills: int
    capped_fills: int
    liquidated_or_zero_equity: bool

    @property
    def max_notional_usd(self) -> Decimal:
        return self.risk_budget_usd * Decimal(self.cap_leverage)


def select_risk_profile(
    backtest: dict[str, Any],
    *,
    target_slots: int = DEFAULT_TARGET_SLOTS,
    min_source_fills: int = DEFAULT_MIN_SOURCE_FILLS,
    min_equity_floor_pct: Decimal = DEFAULT_MIN_EQUITY_FLOOR_PCT,
    min_net_pnl_usd: Decimal = DEFAULT_MIN_NET_PNL_USD,
) -> dict[str, Any]:
    if target_slots <= 0:
        raise RiskProfileInputError("target_slots must be positive")
    if min_source_fills < 0:
        raise RiskProfileInputError("min_source_fills cannot be negative")
    if min_equity_floor_pct < 0:
        raise RiskProfileInputError("min_equity_floor_pct cannot be negative")

    min_notional = decimal_optional(backtest.get("min_notional_usd"))
    sources = list_items(backtest.get("sources"))
    if not sources:
        raise RiskProfileInputError("backtest contains no sources")

    source_reports: list[dict[str, Any]] = []
    selected_pool: list[tuple[StrategyCandidate, dict[str, Any]]] = []
    for source in sources:
        source_report, candidate = evaluate_source(
            source,
            min_source_fills=min_source_fills,
            min_equity_floor_pct=min_equity_floor_pct,
            min_net_pnl_usd=min_net_pnl_usd,
        )
        source_reports.append(source_report)
        if candidate is not None:
            selected_pool.append((candidate, source_report))

    selected_pool.sort(
        key=lambda item: (
            item[0].net_pnl_usd,
            item[0].copied_fills,
            item[0].min_equity_usd,
            -item[0].max_drawdown_usd,
            -Decimal(item[0].cap_leverage),
        ),
        reverse=True,
    )
    selected = selected_pool[:target_slots]
    selected_ids = {candidate.source_address for candidate, _report in selected}
    for _candidate, report in selected_pool[target_slots:]:
        report["status"] = "candidate_not_selected"
        report["decision"] = "viable but outside top target slot count"

    selected_candidates = [candidate for candidate, _report in selected]
    aggregate = aggregate_candidates(selected_candidates)
    slot_plan = build_slot_plan(selected_candidates)
    ready = len(selected_candidates) == target_slots and all(
        not candidate.liquidated_or_zero_equity for candidate in selected_candidates
    )
    blockers: list[str] = []
    if len(selected_candidates) < target_slots:
        blockers.append(
            f"only {len(selected_candidates)} viable sources selected for target_slots={target_slots}"
        )
    if any(candidate.liquidated_or_zero_equity for candidate in selected_candidates):
        blockers.append("selected profile contains a zeroed/liquidated strategy path")
    if min_notional != Decimal("10"):
        blockers.append(
            "backtest min_notional_usd must be 10 to match current Hyperliquid perp minimum"
        )

    return {
        "risk_profile_selector_version": RISK_PROFILE_SELECTOR_VERSION,
        "read_only": True,
        "exchange_touched": False,
        "status": "risk_profile_ready" if ready and not blockers else "blocked",
        "risk_profile_ready": ready and not blockers,
        "target_slots": target_slots,
        "selected_slots": len(selected_candidates),
        "selection_policy": {
            "min_source_fills": min_source_fills,
            "min_equity_floor_pct": decimal_str(min_equity_floor_pct),
            "min_net_pnl_usd": decimal_str(min_net_pnl_usd),
            "strategy_family": "risk_budget_<budget>_cap_<leverage>x",
            "ranking": (
                "filter by activity, positive net PnL, no zeroed equity, min-equity floor; "
                "then rank by net PnL, copied fills, min equity, drawdown, and lower leverage"
            ),
        },
        "backtest": {
            "days": int_optional(backtest.get("days")),
            "initial_equity_usd": decimal_str(decimal_optional(backtest.get("initial_equity_usd"))),
            "min_notional_usd": decimal_str(min_notional),
            "source_count": len(sources),
        },
        "selected": [
            candidate_to_dict(candidate, rank=index + 1)
            for index, candidate in enumerate(selected_candidates)
        ],
        "aggregate": aggregate,
        "slot_plan": slot_plan,
        "source_reports": source_reports,
        "blockers": blockers,
        "disqualified_sources": [
            report for report in source_reports if report["status"] == "disqualified"
        ],
        "not_selected_sources": [
            report
            for report in source_reports
            if report["source_address"] not in selected_ids and report["status"] != "disqualified"
        ],
        "runtime_mapping": {
            "HLCT_BALANCE_SIZING_ENABLE": "true",
            "HLCT_FIXED_MULTIPLIER": "1",
            "HLCT_MAX_LEVERAGE": "per selected slot max_emergency_leverage",
            "HLCT_MAX_NOTIONAL_USD": "per selected slot max_notional_usd",
            "HLCT_MAX_GROSS_EXPOSURE_USD": "per selected slot max_gross_notional_usd",
        },
    }


def evaluate_source(
    source: dict[str, Any],
    *,
    min_source_fills: int,
    min_equity_floor_pct: Decimal,
    min_net_pnl_usd: Decimal,
) -> tuple[dict[str, Any], StrategyCandidate | None]:
    source_fills = int_optional(source.get("fills")) or 0
    risk_rows: list[StrategyCandidate] = []
    for item in list_items(source.get("simulations")):
        candidate = candidate_from_simulation(source, item)
        if candidate is not None:
            risk_rows.append(candidate)
    if not risk_rows:
        return source_report(
            source,
            status="disqualified",
            decision="no supported risk-budget strategies in backtest",
            candidates=[],
        ), None

    floor = risk_rows[0].initial_equity_usd * min_equity_floor_pct / Decimal("100")
    blockers: list[str] = []
    if source_fills < min_source_fills:
        blockers.append(
            f"source fills {source_fills} below active-source minimum {min_source_fills}"
        )

    viable = [
        candidate
        for candidate in risk_rows
        if not candidate.liquidated_or_zero_equity
        and candidate.min_equity_usd >= floor
        and candidate.net_pnl_usd > min_net_pnl_usd
    ]
    if not viable:
        non_zeroed = [
            candidate for candidate in risk_rows if not candidate.liquidated_or_zero_equity
        ]
        if all(candidate.liquidated_or_zero_equity for candidate in risk_rows):
            blockers.append("every tested leverage cap zeroed follower equity")
        elif not any(candidate.net_pnl_usd > min_net_pnl_usd for candidate in non_zeroed):
            blockers.append(f"no non-zeroed leverage cap produced net PnL above {min_net_pnl_usd}")
        elif not any(candidate.min_equity_usd >= floor for candidate in non_zeroed):
            blockers.append(f"no tested leverage cap kept min equity above {decimal_str(floor)}")
        else:
            blockers.append("no strategy satisfied all source-selection filters")

    if blockers:
        return source_report(
            source,
            status="disqualified",
            decision="; ".join(blockers),
            candidates=risk_rows,
            min_equity_floor_usd=floor,
        ), None

    selected = max(
        viable,
        key=lambda candidate: (
            candidate.net_pnl_usd,
            candidate.copied_fills,
            candidate.min_equity_usd,
            -candidate.max_drawdown_usd,
            -Decimal(candidate.cap_leverage),
        ),
    )
    return source_report(
        source,
        status="selected_candidate",
        decision=(
            f"selected {selected.cap_leverage}x because it maximized net PnL among "
            "positive, non-zeroed, active-source candidates above the min-equity floor"
        ),
        candidates=risk_rows,
        selected=selected,
        min_equity_floor_usd=floor,
    ), selected


def candidate_from_simulation(
    source: dict[str, Any],
    simulation: dict[str, Any],
) -> StrategyCandidate | None:
    strategy = clean(simulation.get("strategy"))
    match = RISK_STRATEGY_RE.fullmatch(strategy)
    if match is None:
        return None
    return StrategyCandidate(
        source_slot=int_optional(source.get("slot")) or 0,
        source_address=clean(source.get("address")).lower(),
        strategy=strategy,
        risk_budget_usd=decimal_required(match.group("budget"), field=f"{strategy}.budget"),
        cap_leverage=int(match.group("leverage")),
        initial_equity_usd=decimal_required(
            simulation.get("initial_equity_usd"), field="initial_equity_usd"
        ),
        ending_equity_usd=decimal_required(
            simulation.get("ending_equity_usd"), field="ending_equity_usd"
        ),
        net_pnl_usd=decimal_required(simulation.get("net_pnl_usd"), field="net_pnl_usd"),
        min_equity_usd=decimal_required(simulation.get("min_equity_usd"), field="min_equity_usd"),
        max_drawdown_usd=decimal_required(
            simulation.get("max_drawdown_usd"), field="max_drawdown_usd"
        ),
        copied_fills=int_optional(simulation.get("copied_fills")) or 0,
        skipped_min_notional_fills=int_optional(simulation.get("skipped_min_notional_fills")) or 0,
        capped_fills=int_optional(simulation.get("capped_fills")) or 0,
        liquidated_or_zero_equity=simulation.get("liquidated_or_zero_equity") is True,
    )


def source_report(
    source: dict[str, Any],
    *,
    status: str,
    decision: str,
    candidates: list[StrategyCandidate],
    selected: StrategyCandidate | None = None,
    min_equity_floor_usd: Decimal | None = None,
) -> dict[str, Any]:
    return {
        "source_slot": int_optional(source.get("slot")) or 0,
        "source_address": clean(source.get("address")).lower(),
        "source_fills": int_optional(source.get("fills")) or 0,
        "source_approx_roi_pct": decimal_str(decimal_optional(source.get("source_approx_roi_pct"))),
        "status": status,
        "decision": decision,
        "min_equity_floor_usd": decimal_str(min_equity_floor_usd),
        "selected_strategy": candidate_to_dict(selected) if selected is not None else None,
        "tested_strategies": [
            candidate_to_dict(candidate)
            for candidate in sorted(candidates, key=lambda item: item.cap_leverage)
        ],
    }


def candidate_to_dict(
    candidate: StrategyCandidate | None, *, rank: int | None = None
) -> dict[str, Any] | None:
    if candidate is None:
        return None
    payload: dict[str, Any] = {
        "source_slot": candidate.source_slot,
        "source_address": candidate.source_address,
        "strategy": candidate.strategy,
        "risk_budget_usd": decimal_str(candidate.risk_budget_usd),
        "cap_leverage": candidate.cap_leverage,
        "initial_equity_usd": decimal_str(candidate.initial_equity_usd),
        "ending_equity_usd": decimal_str(candidate.ending_equity_usd),
        "net_pnl_usd": decimal_str(candidate.net_pnl_usd),
        "min_equity_usd": decimal_str(candidate.min_equity_usd),
        "max_drawdown_usd": decimal_str(candidate.max_drawdown_usd),
        "max_notional_usd": decimal_str(candidate.max_notional_usd),
        "copied_fills": candidate.copied_fills,
        "skipped_min_notional_fills": candidate.skipped_min_notional_fills,
        "capped_fills": candidate.capped_fills,
        "liquidated_or_zero_equity": candidate.liquidated_or_zero_equity,
    }
    if rank is not None:
        payload = {"rank": rank, **payload}
    return payload


def aggregate_candidates(candidates: list[StrategyCandidate]) -> dict[str, Any]:
    return {
        "start_equity_usd": decimal_str(
            sum_decimal(candidate.initial_equity_usd for candidate in candidates)
        ),
        "ending_equity_usd": decimal_str(
            sum_decimal(candidate.ending_equity_usd for candidate in candidates)
        ),
        "net_pnl_usd": decimal_str(sum_decimal(candidate.net_pnl_usd for candidate in candidates)),
        "copied_fills": sum(candidate.copied_fills for candidate in candidates),
        "skipped_min_notional_fills": sum(
            candidate.skipped_min_notional_fills for candidate in candidates
        ),
        "capped_fills": sum(candidate.capped_fills for candidate in candidates),
        "zeroed_slots": sum(1 for candidate in candidates if candidate.liquidated_or_zero_equity),
        "worst_min_equity_usd": decimal_str(
            min((candidate.min_equity_usd for candidate in candidates), default=Decimal("0"))
        ),
        "max_slot_drawdown_usd": decimal_str(
            max((candidate.max_drawdown_usd for candidate in candidates), default=Decimal("0"))
        ),
    }


def build_slot_plan(candidates: list[StrategyCandidate]) -> dict[str, Any]:
    return {
        "version": 1,
        "environment": "testnet",
        "notes": (
            "Generated from read-only backtest risk-profile selection. Replace placeholder "
            "subaccount addresses, set enabled/subaccount_verified only after operator checks, "
            "and validate with scripts/validate_slot_plan.py before running the supervisor."
        ),
        "slots": [
            {
                "slot": f"slot-{index:02d}",
                "source_address": candidate.source_address,
                "subaccount_address": f"0xf{'0' * 36}{index:03x}",
                "mode": "testnet",
                "enabled": False,
                "subaccount_verified": False,
                "operator_verified_at": None,
                "sizing_policy": "fixed_risk_budget",
                "initial_budget_usd": decimal_str(candidate.risk_budget_usd),
                "fixed_risk_budget_usd": decimal_str(candidate.risk_budget_usd),
                "fixed_multiplier": "1",
                "allowed_coins": list(DEFAULT_ALLOWED_COINS),
                "denied_coins": [],
                "min_notional_usd": "10",
                "dust_policy": {"mode": "accumulate", "stale_after_ms": 300000},
                "entry_slippage_bps": "20",
                "reduce_only_slippage_bps": "300",
                "max_emergency_leverage": str(candidate.cap_leverage),
                "max_gross_notional_usd": decimal_str(candidate.max_notional_usd),
                "expected_margin_mode": "cross",
                "expected_account_mode": "perp",
                "equity_confidence_policy": "block_low",
                "note": (
                    f"Selected from {candidate.strategy}: net={decimal_str(candidate.net_pnl_usd)} "
                    f"min_equity={decimal_str(candidate.min_equity_usd)}"
                ),
            }
            for index, candidate in enumerate(candidates, start=1)
        ],
    }


def read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise RiskProfileInputError(f"could not read backtest JSON: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise RiskProfileInputError(f"backtest JSON is invalid: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise RiskProfileInputError("backtest JSON must be an object")
    return payload


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temp_path.replace(path)


def list_items(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def clean(value: Any) -> str:
    return str(value or "").strip()


def int_optional(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def decimal_optional(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def decimal_required(value: Any, *, field: str) -> Decimal:
    parsed = decimal_optional(value)
    if parsed is None:
        raise RiskProfileInputError(f"{field} must be a decimal value")
    return parsed


def decimal_str(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def sum_decimal(values: Any) -> Decimal:
    total = Decimal("0")
    for value in values:
        total += value
    return total


def parse_decimal_arg(value: str) -> Decimal:
    parsed = decimal_optional(value)
    if parsed is None:
        raise argparse.ArgumentTypeError(f"expected decimal value, got {value!r}")
    return parsed


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Select a per-source copy risk profile from read-only backtest artifacts."
    )
    parser.add_argument("--backtest-json", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--slot-plan-out", type=Path)
    parser.add_argument("--target-slots", type=int, default=DEFAULT_TARGET_SLOTS)
    parser.add_argument("--min-source-fills", type=int, default=DEFAULT_MIN_SOURCE_FILLS)
    parser.add_argument(
        "--min-equity-floor-pct",
        type=parse_decimal_arg,
        default=DEFAULT_MIN_EQUITY_FLOOR_PCT,
    )
    parser.add_argument(
        "--min-net-pnl-usd", type=parse_decimal_arg, default=DEFAULT_MIN_NET_PNL_USD
    )
    parser.add_argument(
        "--fail-on-blocked",
        action="store_true",
        help="Exit nonzero if fewer than target slots pass the risk-profile filters.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        report = select_risk_profile(
            read_json_object(args.backtest_json),
            target_slots=args.target_slots,
            min_source_fills=args.min_source_fills,
            min_equity_floor_pct=args.min_equity_floor_pct,
            min_net_pnl_usd=args.min_net_pnl_usd,
        )
    except RiskProfileInputError as exc:
        print(f"risk profile input error: {exc}", file=sys.stderr)
        return 2
    if args.out is not None:
        write_json(args.out, report)
    if args.slot_plan_out is not None:
        write_json(args.slot_plan_out, report["slot_plan"])
    print(
        json.dumps(
            {
                "status": report["status"],
                "risk_profile_ready": report["risk_profile_ready"],
                "selected_slots": report["selected_slots"],
                "target_slots": report["target_slots"],
                "aggregate": report["aggregate"],
                "blockers": report["blockers"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    if args.fail_on_blocked and not report["risk_profile_ready"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
