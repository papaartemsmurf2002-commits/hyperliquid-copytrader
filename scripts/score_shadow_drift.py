from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator


DRIFT_SCORER_VERSION = 1
SHADOW_TARGET_SNAPSHOT_VERSION = 1
DEFAULT_DRIFT_THRESHOLD_USD = Decimal("1")
DEFAULT_SAMPLE_SIZE = 50

EXECUTED_POLICY_DECISIONS = {
    "would_send_now",
    "would_send_accumulated_dust",
}


class DriftInputError(RuntimeError):
    """Raised when policy facts or follower snapshots cannot be scored."""


@dataclass
class TargetCoinState:
    coin: str
    signed_target_notional_usd: Decimal = Decimal("0")
    executed_actions: int = 0
    blocked_actions: int = 0
    skipped_actions: int = 0
    dust_actions: int = 0
    source_rows: int = 0
    decision_counts: Counter[str] = field(default_factory=Counter)

    def observe(
        self,
        *,
        decision: str,
        signed_target_delta: Decimal | None = None,
    ) -> None:
        self.decision_counts[decision] += 1
        self.source_rows += 1
        if signed_target_delta is not None:
            self.signed_target_notional_usd += signed_target_delta
            self.executed_actions += 1
        elif decision.startswith("blocked"):
            self.blocked_actions += 1
        elif decision.startswith("skipped"):
            self.skipped_actions += 1
        elif "dust" in decision:
            self.dust_actions += 1

    def as_dict(self) -> dict[str, Any]:
        signed = self.signed_target_notional_usd
        return {
            "coin": self.coin,
            "signed_target_notional_usd": decimal_str(signed),
            "abs_target_notional_usd": decimal_str(abs(signed)),
            "side": target_side(signed),
            "executed_actions": self.executed_actions,
            "blocked_actions": self.blocked_actions,
            "skipped_actions": self.skipped_actions,
            "dust_actions": self.dust_actions,
            "source_rows": self.source_rows,
            "decision_counts": counter_dict(self.decision_counts),
        }


@dataclass
class PolicyTarget:
    policy: str
    target_delta_by_coin: dict[str, Decimal] = field(default_factory=lambda: defaultdict(Decimal))
    coin_states: dict[str, TargetCoinState] = field(default_factory=dict)
    executed_actions: int = 0
    blocked_actions: int = 0
    skipped_actions: int = 0
    dust_actions: int = 0
    source_rows: int = 0
    decision_counts: Counter[str] = field(default_factory=Counter)

    def observe(self, fact: dict[str, Any]) -> None:
        decision = clean(fact.get("decision"))
        self.decision_counts[decision] += 1
        self.source_rows += 1
        metadata = metadata_of(fact)
        coin = clean(metadata.get("coin"))
        coin_state = self.coin_state(coin) if coin.lower() != "unknown" else None
        if decision in EXECUTED_POLICY_DECISIONS:
            amount = decimal_optional(metadata.get("executed_target_notional_usd")) or Decimal("0")
            signed = Decimal("0")
            if amount > 0:
                signed = signed_amount(amount, metadata.get("side"))
                self.target_delta_by_coin[coin] += signed
            self.executed_actions += 1
            if coin_state is not None:
                coin_state.observe(decision=decision, signed_target_delta=signed)
        elif decision.startswith("blocked"):
            self.blocked_actions += 1
            if coin_state is not None:
                coin_state.observe(decision=decision)
        elif decision.startswith("skipped"):
            self.skipped_actions += 1
            if coin_state is not None:
                coin_state.observe(decision=decision)
        elif "dust" in decision:
            self.dust_actions += 1
            if coin_state is not None:
                coin_state.observe(decision=decision)
        elif coin_state is not None:
            coin_state.observe(decision=decision)

    def coin_state(self, coin: str) -> TargetCoinState:
        return self.coin_states.setdefault(coin, TargetCoinState(coin=coin))

    def as_dict(self) -> dict[str, Any]:
        return {
            "policy": self.policy,
            "target_delta_by_coin": decimal_map(self.target_delta_by_coin),
            "target_positions": [
                state.as_dict()
                for state in sorted(self.coin_states.values(), key=lambda item: item.coin)
            ],
            "executed_actions": self.executed_actions,
            "blocked_actions": self.blocked_actions,
            "skipped_actions": self.skipped_actions,
            "dust_actions": self.dust_actions,
            "source_rows": self.source_rows,
            "decision_counts": counter_dict(self.decision_counts),
        }


@dataclass
class FollowerView:
    policy: str
    account_value_usd: Decimal | None
    positions_by_coin: dict[str, Decimal]
    open_orders_by_coin: dict[str, Decimal]
    raw_position_count: int
    raw_open_order_count: int

    def projected_by_coin(self) -> dict[str, Decimal]:
        result: dict[str, Decimal] = defaultdict(Decimal)
        for coin, value in self.positions_by_coin.items():
            result[coin] += value
        for coin, value in self.open_orders_by_coin.items():
            result[coin] += value
        return dict(result)

    def as_dict(self) -> dict[str, Any]:
        return {
            "policy": self.policy,
            "account_value_usd": decimal_str(self.account_value_usd),
            "positions_by_coin": decimal_map(self.positions_by_coin),
            "open_orders_by_coin": decimal_map(self.open_orders_by_coin),
            "projected_by_coin": decimal_map(self.projected_by_coin()),
            "raw_position_count": self.raw_position_count,
            "raw_open_order_count": self.raw_open_order_count,
        }


class ShadowDriftScorer:
    def __init__(
        self,
        *,
        policy_facts_path: Path,
        follower_snapshot_path: Path,
        drift_threshold_usd: Decimal,
        facts_out: Path | None,
        sample_size: int,
    ) -> None:
        self.policy_facts_path = policy_facts_path
        self.follower_snapshot_path = follower_snapshot_path
        self.drift_threshold_usd = drift_threshold_usd
        self.facts_out = facts_out
        self.sample_size = sample_size

        self.policy_targets: dict[str, PolicyTarget] = {}
        self.policy_fact_rows = 0
        self.global_fact_rows = 0
        self.fact_type_counts: Counter[str] = Counter()
        self.decision_counts: Counter[str] = Counter()
        self.slots: Counter[str] = Counter()
        self.source_addresses: Counter[str] = Counter()
        self.target_fact_samples: list[dict[str, Any]] = []

    def observe_policy_fact(self, fact: dict[str, Any]) -> None:
        self.fact_type_counts[clean(fact.get("fact_type"))] += 1
        self.slots[clean(fact.get("slot"))] += 1
        self.source_addresses[clean(fact.get("address")).lower()] += 1
        policy = fact.get("policy")
        if policy in (None, "", "unknown"):
            self.global_fact_rows += 1
            return
        policy_name = clean(policy)
        self.policy_fact_rows += 1
        target = self.policy_targets.setdefault(policy_name, PolicyTarget(policy=policy_name))
        target.observe(fact)
        self.decision_counts[clean(fact.get("decision"))] += 1
        if len(self.target_fact_samples) < self.sample_size:
            self.target_fact_samples.append(fact)

    def score(
        self, follower_snapshot: dict[str, Any]
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        recovery = recovery_state(follower_snapshot)
        policies = sorted(self.policy_targets)
        follower_views = {
            policy: follower_view_for_policy(follower_snapshot, policy=policy)
            for policy in policies
        }
        drift_facts: list[dict[str, Any]] = []
        drift_counts: Counter[str] = Counter()
        repair_counts: Counter[str] = Counter()

        for policy in policies:
            target = self.policy_targets[policy]
            follower = follower_views[policy]
            projected = follower.projected_by_coin()
            coins = sorted(
                set(target.target_delta_by_coin)
                | set(follower.positions_by_coin)
                | set(follower.open_orders_by_coin)
            )
            if not coins:
                if recovery["complete"]:
                    decision = "no_target_or_follower_exposure"
                    repair_intent = "no_repair_needed"
                    confidence = "high"
                    reason = "no policy target and no follower exposure"
                else:
                    decision = "blocked_recovery_incomplete"
                    repair_intent = "do_not_repair_until_recovery_complete"
                    confidence = "low"
                    reason = (
                        "recovery completion is not proven even though no target exposure exists"
                    )
                facts = [
                    self._drift_fact(
                        policy=policy,
                        coin="all",
                        target_delta=Decimal("0"),
                        follower_position=Decimal("0"),
                        follower_open_orders=Decimal("0"),
                        follower_projected=Decimal("0"),
                        recovery=recovery,
                        decision=decision,
                        repair_intent=repair_intent,
                        confidence=confidence,
                        reason=reason,
                    )
                ]
            else:
                facts = [
                    self._score_coin(
                        policy=policy,
                        coin=coin,
                        target_delta=target.target_delta_by_coin.get(coin, Decimal("0")),
                        follower_position=follower.positions_by_coin.get(coin, Decimal("0")),
                        follower_open_orders=follower.open_orders_by_coin.get(coin, Decimal("0")),
                        follower_projected=projected.get(coin, Decimal("0")),
                        recovery=recovery,
                    )
                    for coin in coins
                ]
            for fact in facts:
                drift_facts.append(fact)
                drift_counts[fact["decision"]] += 1
                repair_counts[fact["repair_intent"]] += 1

        report = {
            "drift_scorer_version": DRIFT_SCORER_VERSION,
            "read_only": True,
            "exchange_touched": False,
            "policy_facts_path": str(self.policy_facts_path),
            "follower_snapshot_path": str(self.follower_snapshot_path),
            "drift_facts_out": str(self.facts_out) if self.facts_out is not None else None,
            "slot": clean(follower_snapshot.get("slot")),
            "follower_subaccount": clean(follower_snapshot.get("follower_subaccount")),
            "captured_ms": int_optional(follower_snapshot.get("captured_ms")),
            "drift_threshold_usd": decimal_str(self.drift_threshold_usd),
            "input_counts": {
                "policy_fact_rows": self.policy_fact_rows,
                "global_fact_rows": self.global_fact_rows,
                "fact_types": counter_dict(self.fact_type_counts),
                "policy_decisions": counter_dict(self.decision_counts),
                "slots": counter_dict(self.slots),
                "source_addresses": counter_dict(self.source_addresses),
            },
            "recovery_completion": recovery,
            "shadow_target_snapshot": self.target_snapshot(),
            "policy_targets": {
                policy: target.as_dict() for policy, target in sorted(self.policy_targets.items())
            },
            "follower_views": {
                policy: view.as_dict() for policy, view in sorted(follower_views.items())
            },
            "drift_counts": counter_dict(drift_counts),
            "repair_intent_counts": counter_dict(repair_counts),
            "sample_target_facts": self.target_fact_samples,
            "sample_drift_facts": drift_facts[: self.sample_size],
        }
        return report, drift_facts

    def target_snapshot(self) -> dict[str, Any]:
        return {
            "shadow_target_snapshot_version": SHADOW_TARGET_SNAPSHOT_VERSION,
            "read_only": True,
            "exchange_touched": False,
            "policy_facts_path": str(self.policy_facts_path),
            "source": "policy_shadow_decision_facts",
            "slots": counter_dict(self.slots),
            "source_addresses": counter_dict(self.source_addresses),
            "input_counts": {
                "policy_fact_rows": self.policy_fact_rows,
                "global_fact_rows": self.global_fact_rows,
                "fact_types": counter_dict(self.fact_type_counts),
                "policy_decisions": counter_dict(self.decision_counts),
            },
            "policies": {
                policy: target.as_dict() for policy, target in sorted(self.policy_targets.items())
            },
            "caveats": [
                (
                    "targets are materialized from read-only policy decision facts and are not "
                    "exchange execution proof"
                ),
                (
                    "this is a first-class target-position artifact, but not yet a full "
                    "order/TWAP state engine"
                ),
            ],
        }

    def _score_coin(
        self,
        *,
        policy: str,
        coin: str,
        target_delta: Decimal,
        follower_position: Decimal,
        follower_open_orders: Decimal,
        follower_projected: Decimal,
        recovery: dict[str, Any],
    ) -> dict[str, Any]:
        projected_drift = target_delta - follower_projected
        position_drift = target_delta - follower_position
        if not recovery["complete"]:
            return self._drift_fact(
                policy=policy,
                coin=coin,
                target_delta=target_delta,
                follower_position=follower_position,
                follower_open_orders=follower_open_orders,
                follower_projected=follower_projected,
                recovery=recovery,
                decision="blocked_recovery_incomplete",
                repair_intent="do_not_repair_until_recovery_complete",
                confidence="low",
                reason="source backfill, follower refresh, and reconcile completion are not all proven",
            )
        if abs(projected_drift) <= self.drift_threshold_usd:
            if abs(position_drift) <= self.drift_threshold_usd:
                return self._drift_fact(
                    policy=policy,
                    coin=coin,
                    target_delta=target_delta,
                    follower_position=follower_position,
                    follower_open_orders=follower_open_orders,
                    follower_projected=follower_projected,
                    recovery=recovery,
                    decision="in_sync",
                    repair_intent="no_repair_needed",
                    confidence="high",
                    reason="follower projected exposure matches target within threshold",
                )
            return self._drift_fact(
                policy=policy,
                coin=coin,
                target_delta=target_delta,
                follower_position=follower_position,
                follower_open_orders=follower_open_orders,
                follower_projected=follower_projected,
                recovery=recovery,
                decision="projected_in_sync_position_pending",
                repair_intent="wait_for_open_orders_or_reconcile",
                confidence="medium",
                reason="open orders cover drift but current position is not yet at target",
            )
        if target_delta == 0 and follower_projected != 0:
            repair_intent = "would_reduce_or_close_follower_exposure"
        elif follower_projected == 0 and target_delta != 0:
            repair_intent = "would_open_or_place_follower_exposure"
        elif signs_conflict(target_delta, follower_projected):
            repair_intent = "would_reduce_flip_or_flatten_then_reopen"
        elif abs(target_delta) > abs(follower_projected):
            repair_intent = "would_increase_follower_exposure"
        else:
            repair_intent = "would_reduce_follower_exposure"
        return self._drift_fact(
            policy=policy,
            coin=coin,
            target_delta=target_delta,
            follower_position=follower_position,
            follower_open_orders=follower_open_orders,
            follower_projected=follower_projected,
            recovery=recovery,
            decision="drift_detected",
            repair_intent=repair_intent,
            confidence="medium",
            reason="follower projected exposure differs from policy target beyond threshold",
        )

    def _drift_fact(
        self,
        *,
        policy: str,
        coin: str,
        target_delta: Decimal,
        follower_position: Decimal,
        follower_open_orders: Decimal,
        follower_projected: Decimal,
        recovery: dict[str, Any],
        decision: str,
        repair_intent: str,
        confidence: str,
        reason: str,
    ) -> dict[str, Any]:
        projected_drift = target_delta - follower_projected
        position_drift = target_delta - follower_position
        payload = f"{policy}|{coin}|{decision}|{decimal_str(projected_drift)}"
        return {
            "fact_id": hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24],
            "drift_scorer_version": DRIFT_SCORER_VERSION,
            "policy": policy,
            "coin": coin,
            "decision": decision,
            "repair_intent": repair_intent,
            "confidence": confidence,
            "read_only": True,
            "exchange_touched": False,
            "reason": reason,
            "metadata": {
                "target_delta_notional_usd": decimal_str(target_delta),
                "follower_position_notional_usd": decimal_str(follower_position),
                "follower_open_order_delta_notional_usd": decimal_str(follower_open_orders),
                "follower_projected_notional_usd": decimal_str(follower_projected),
                "projected_drift_notional_usd": decimal_str(projected_drift),
                "position_drift_notional_usd": decimal_str(position_drift),
                "abs_projected_drift_notional_usd": decimal_str(abs(projected_drift)),
                "drift_threshold_usd": decimal_str(self.drift_threshold_usd),
                "recovery_complete": recovery["complete"],
                "missing_recovery_requirements": recovery["missing_requirements"],
            },
        }


def score_shadow_drift(
    policy_facts_path: Path,
    follower_snapshot_path: Path,
    *,
    out: Path | None = None,
    facts_out: Path | None = None,
    targets_out: Path | None = None,
    drift_threshold_usd: Decimal = DEFAULT_DRIFT_THRESHOLD_USD,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
) -> dict[str, Any]:
    if sample_size < 0:
        raise DriftInputError("sample_size must be non-negative")
    if drift_threshold_usd < 0:
        raise DriftInputError("drift threshold must be non-negative")

    follower_snapshot = read_follower_snapshot(follower_snapshot_path)
    scorer = ShadowDriftScorer(
        policy_facts_path=policy_facts_path,
        follower_snapshot_path=follower_snapshot_path,
        drift_threshold_usd=drift_threshold_usd,
        facts_out=facts_out,
        sample_size=sample_size,
    )
    for fact in iter_policy_facts(policy_facts_path):
        scorer.observe_policy_fact(fact)
    report, drift_facts = scorer.score(follower_snapshot)
    if facts_out is not None:
        facts_out.parent.mkdir(parents=True, exist_ok=True)
        with facts_out.open("w", encoding="utf-8") as handle:
            for fact in drift_facts:
                handle.write(json.dumps(fact, separators=(",", ":"), sort_keys=True))
                handle.write("\n")
    if targets_out is not None:
        write_json(targets_out, report["shadow_target_snapshot"])
    if out is not None:
        write_json(out, report)
    return report


def iter_policy_facts(path: Path) -> Iterator[dict[str, Any]]:
    if not path.exists():
        raise DriftInputError(f"policy facts file does not exist: {path}")
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                fact = json.loads(text)
            except json.JSONDecodeError as exc:
                raise DriftInputError(f"{path}:{line_no} invalid JSON: {exc.msg}") from exc
            if not isinstance(fact, dict):
                raise DriftInputError(f"{path}:{line_no} policy fact must be an object")
            yield fact


def read_follower_snapshot(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise DriftInputError(f"follower snapshot file does not exist: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DriftInputError(f"{path} invalid JSON: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise DriftInputError(f"{path} follower snapshot must be an object")
    return payload


def follower_view_for_policy(snapshot: dict[str, Any], *, policy: str) -> FollowerView:
    policy_payload = snapshot
    policies = snapshot.get("policies")
    if isinstance(policies, dict) and isinstance(policies.get(policy), dict):
        policy_payload = {**snapshot, **policies[policy]}
    positions = list_items(policy_payload.get("positions"))
    open_orders = list_items(policy_payload.get("open_orders"))
    return FollowerView(
        policy=policy,
        account_value_usd=decimal_optional(policy_payload.get("account_value_usd")),
        positions_by_coin=aggregate_positions(positions),
        open_orders_by_coin=aggregate_open_orders(open_orders),
        raw_position_count=len(positions),
        raw_open_order_count=len(open_orders),
    )


def aggregate_positions(rows: list[dict[str, Any]]) -> dict[str, Decimal]:
    result: dict[str, Decimal] = defaultdict(Decimal)
    for row in rows:
        coin = clean(row.get("coin"))
        signed = decimal_optional(row.get("signed_notional_usd"))
        if signed is None:
            notional = decimal_optional(row.get("notional_usd")) or Decimal("0")
            signed = signed_amount(notional, row.get("side") or row.get("direction"))
        result[coin] += signed
    return dict(result)


def aggregate_open_orders(rows: list[dict[str, Any]]) -> dict[str, Decimal]:
    result: dict[str, Decimal] = defaultdict(Decimal)
    for row in rows:
        coin = clean(row.get("coin"))
        notional = decimal_optional(row.get("notional_usd")) or Decimal("0")
        result[coin] += signed_amount(notional, row.get("side") or row.get("direction"))
    return dict(result)


def recovery_state(snapshot: dict[str, Any]) -> dict[str, Any]:
    raw_recovery = snapshot.get("recovery")
    recovery: dict[str, Any] = raw_recovery if isinstance(raw_recovery, dict) else {}
    requirements = {
        "source_backfill_complete": recovery.get("source_backfill_complete") is True,
        "follower_refresh_complete": recovery.get("follower_refresh_complete") is True,
        "reconcile_complete": recovery.get("reconcile_complete") is True,
    }
    missing = [key for key, value in requirements.items() if not value]
    return {
        **requirements,
        "complete": not missing,
        "missing_requirements": missing,
        "notes": clean(recovery.get("notes")),
    }


def metadata_of(fact: dict[str, Any]) -> dict[str, Any]:
    metadata = fact.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def list_items(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def signed_amount(amount: Decimal, side: Any) -> Decimal:
    label = clean(side).lower()
    if label in {"a", "ask", "sell", "short"}:
        return -amount
    if label in {"b", "bid", "buy", "long"}:
        return amount
    return amount


def signs_conflict(left: Decimal, right: Decimal) -> bool:
    return (left > 0 and right < 0) or (left < 0 and right > 0)


def target_side(signed: Decimal) -> str:
    if signed > 0:
        return "long"
    if signed < 0:
        return "short"
    return "flat"


def int_optional(value: Any) -> int | None:
    if value in (None, "", "unknown"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def decimal_optional(value: Any) -> Decimal | None:
    if value in (None, "", "unknown"):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def decimal_str(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.quantize(Decimal("0.00000001")), "f")


def decimal_map(values: dict[str, Decimal]) -> dict[str, str]:
    return {key: decimal_str(values[key]) or "0.00000000" for key in sorted(values)}


def clean(value: Any) -> str:
    if value in (None, ""):
        return "unknown"
    return str(value)


def counter_dict(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter)}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def parse_decimal_arg(value: str) -> Decimal:
    parsed = decimal_optional(value)
    if parsed is None:
        raise argparse.ArgumentTypeError(f"expected decimal value, got {value!r}")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Score follower shadow drift from read-only policy facts and a local follower snapshot."
        )
    )
    parser.add_argument("policy_facts_path", type=Path)
    parser.add_argument("follower_snapshot_path", type=Path)
    parser.add_argument("--out", type=Path, default=None, help="Write drift report JSON.")
    parser.add_argument("--facts-out", type=Path, default=None, help="Write drift facts JSONL.")
    parser.add_argument(
        "--targets-out",
        type=Path,
        default=None,
        help="Write materialized shadow target positions JSON.",
    )
    parser.add_argument(
        "--drift-threshold-usd",
        type=parse_decimal_arg,
        default=DEFAULT_DRIFT_THRESHOLD_USD,
    )
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        report = score_shadow_drift(
            args.policy_facts_path,
            args.follower_snapshot_path,
            out=args.out,
            facts_out=args.facts_out,
            targets_out=args.targets_out,
            drift_threshold_usd=args.drift_threshold_usd,
            sample_size=args.sample_size,
        )
    except DriftInputError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "report": str(args.out) if args.out is not None else None,
                "facts_out": str(args.facts_out) if args.facts_out is not None else None,
                "policy_fact_rows": report["input_counts"]["policy_fact_rows"],
                "recovery_complete": report["recovery_completion"]["complete"],
                "exchange_touched": report["exchange_touched"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    if args.out is None:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
