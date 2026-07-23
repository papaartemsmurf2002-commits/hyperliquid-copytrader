from __future__ import annotations

from collections import Counter
from dataclasses import replace
from decimal import Decimal
from time import time_ns
from typing import Any, Callable, Mapping

from .continuous_config import ADDRESS_RE, BoundContinuousPlan, BoundContinuousSlot
from .continuous_follower import catalog_position_dexes
from .exchange.hyperliquid import (
    _object_address,
    _spot_usdc_collateral,
    _validate_extra_agent,
)
from .market_catalog import CatalogRevision
from .markets import canonical_market_symbol, market_dex
from .rest_budget import authoritative_rest_weight
from .unified_account import classify_user_abstraction


InfoCallable = Callable[[dict[str, Any]], Any]
_MISSING = object()


def run_continuous_preflight(
    bound: BoundContinuousPlan,
    *,
    network: str,
    info: InfoCallable,
    observed_ms: int | None = None,
    require_flat_and_order_free: bool = False,
    require_open_orders: bool = False,
    audit_dexes: tuple[str, ...] | None = None,
    catalog: CatalogRevision | None = None,
) -> dict[str, Any]:
    """Run one injected, read-only startup audit without constructing a network client."""

    explicit_network = network.strip().lower()
    if explicit_network not in {"mainnet", "testnet"}:
        raise ValueError("network must explicitly be mainnet or testnet")
    now = time_ns() // 1_000_000 if observed_ms is None else observed_ms
    if isinstance(now, bool) or not isinstance(now, int) or now <= 0:
        raise ValueError("observed_ms must be a positive integer")

    blockers: list[str] = []
    by_type: Counter[str] = Counter()
    weight_by_type: Counter[str] = Counter()
    by_slot: dict[str, Counter[str]] = {}
    weight_by_slot: dict[str, int] = {}
    if explicit_network != bound.plan.network:
        blockers.append("explicit network does not match the continuous plan network")
    normalized_audit_dexes = (
        None
        if audit_dexes is None
        else tuple(sorted({str(dex) for dex in audit_dexes}, key=lambda item: (item != "", item)))
    )
    if require_flat_and_order_free:
        if catalog is None:
            blockers.append("bounded canary requires a pinned complete DEX catalog")
        else:
            catalog_dexes = catalog_position_dexes(catalog)
            if catalog.network.strip().lower() != explicit_network:
                blockers.append("bounded canary DEX catalog network does not match")
            if normalized_audit_dexes is not None and normalized_audit_dexes != catalog_dexes:
                blockers.append("bounded canary audit DEX set is not the pinned complete catalog")
            normalized_audit_dexes = catalog_dexes

    enabled = {slot.slot: slot for slot in bound.plan.enabled_slots}
    seen_bound: set[str] = set()
    for slot in bound.slots:
        slot_id = slot.config.slot
        if slot_id in seen_bound:
            blockers.append(f"slot {slot_id}: duplicate bound slot")
        seen_bound.add(slot_id)
        configured = enabled.get(slot_id)
        config_matches = bool(
            configured is not None
            and (
                slot.config == configured
                or (
                    slot.dynamic_market_eligibility
                    and replace(slot.config, allowed_markets=configured.allowed_markets)
                    == configured
                )
            )
        )
        if not config_matches:
            blockers.append(f"slot {slot_id}: bound slot does not match the enabled plan")
    missing = sorted(set(enabled) - seen_bound)
    if missing:
        blockers.append("enabled slots are not bound: " + ",".join(missing))

    signers = [slot.api_wallet_address.lower() for slot in bound.slots]
    if len(signers) != len(set(signers)):
        blockers.append("enabled slots require distinct API wallet signers")

    def query(slot_id: str, scope: str, payload: dict[str, Any]) -> Any:
        request_type = str(payload.get("type") or "unknown")
        weight = authoritative_rest_weight(f"info:{request_type}")
        by_type[request_type] += 1
        weight_by_type[request_type] += weight
        slot_counts = by_slot.setdefault(slot_id, Counter())
        slot_counts[request_type] += 1
        weight_by_slot[slot_id] = weight_by_slot.get(slot_id, 0) + weight
        try:
            return info(dict(payload))
        except Exception as exc:
            blockers.append(
                f"slot {slot_id}: {scope} {request_type} query failed ({type(exc).__name__})"
            )
            return _MISSING

    slot_results: list[dict[str, Any]] = []
    if explicit_network == bound.plan.network:
        master_contexts: dict[str, tuple[dict[str, Mapping[str, Any]], Any]] = {}
        masters = sorted({slot.global_account_address.lower() for slot in bound.slots})
        for index, master in enumerate(masters, start=1):
            scope = f"master-{index}"
            subaccounts = query(
                scope,
                "subaccount topology",
                {"type": "subAccounts", "user": master},
            )
            followers, topology_blockers = _subaccount_inventory(subaccounts, master=master)
            blockers.extend(f"{scope}: {item}" for item in topology_blockers)
            agents = query(
                scope,
                "API-wallet authorization inventory",
                {"type": "extraAgents", "user": master},
            )
            if agents is _MISSING or not isinstance(agents, list):
                blockers.append(f"{scope}: extraAgents response is missing or malformed")
            master_contexts[master] = (followers, agents)

        for slot in bound.slots:
            followers, agents = master_contexts.get(
                slot.global_account_address.lower(), ({}, _MISSING)
            )
            slot_result, slot_blockers = _audit_slot(
                slot,
                now=now,
                query=query,
                audit_dexes=normalized_audit_dexes,
                subaccount=followers.get(slot.config.follower_account_address.lower()),
                agents=agents,
                require_open_orders=require_open_orders or require_flat_and_order_free,
            )
            if require_open_orders and slot_result["follower_open_order_count"]:
                slot_blockers.append("continuous startup requires an order-free follower")
            if require_flat_and_order_free:
                if slot_result["follower_nonflat"]:
                    slot_blockers.append("bounded canary requires a flat follower")
                if slot_result["follower_open_order_count"]:
                    slot_blockers.append("bounded canary requires an order-free follower")
            slot_result["blockers"] = list(slot_blockers)
            slot_result["passed"] = not slot_blockers
            slot_results.append(slot_result)
            blockers.extend(f"slot {slot.config.slot}: {item}" for item in slot_blockers)

    requests_by_slot = {
        slot_id: {
            "total": sum(counts.values()),
            "calculated_weight": weight_by_slot.get(slot_id, 0),
            "by_type": dict(sorted(counts.items())),
        }
        for slot_id, counts in sorted(by_slot.items())
    }
    return {
        "version": 1,
        "network": explicit_network,
        "plan_network": bound.plan.network,
        "network_explicit": True,
        "observed_ms": now,
        "plan_sha256": bound.plan.sha256,
        "require_flat_and_order_free": bool(require_flat_and_order_free),
        "open_orders_checked": bool(require_open_orders or require_flat_and_order_free),
        "passed": not blockers and len(slot_results) == len(bound.slots),
        "blockers": blockers,
        "slots": slot_results,
        "rest_requests": {
            "total": sum(by_type.values()),
            "calculated_weight": sum(weight_by_type.values()),
            "by_type": dict(sorted(by_type.items())),
            "weight_by_type": dict(sorted(weight_by_type.items())),
            "by_slot": requests_by_slot,
        },
    }


def _audit_slot(
    slot: BoundContinuousSlot,
    *,
    now: int,
    query: Callable[[str, str, dict[str, Any]], Any],
    audit_dexes: tuple[str, ...] | None,
    subaccount: Mapping[str, Any] | None,
    agents: Any,
    require_open_orders: bool,
) -> tuple[dict[str, Any], list[str]]:
    config = slot.config
    slot_id = config.slot
    source = config.source_address.lower()
    follower = config.follower_account_address.lower()
    signer = slot.api_wallet_address.lower()
    global_account = slot.global_account_address.lower()
    expected_account_mode = slot.expected_account_mode.strip().lower()
    slot_blockers: list[str] = []

    for label, address in (
        ("source", source),
        ("follower", follower),
        ("signer", signer),
        ("global account", global_account),
    ):
        if not ADDRESS_RE.fullmatch(address):
            slot_blockers.append(f"{label} address is invalid")
    if source == follower:
        slot_blockers.append("source and follower identities must differ")
    if signer in {source, follower, global_account}:
        slot_blockers.append("API signer must differ from source, follower, and global accounts")
    if expected_account_mode not in {"standard", "unified"}:
        slot_blockers.append("expected account mode must be standard or unified")
    if expected_account_mode != "unified":
        slot_blockers.append("continuous-v1 supports unified follower accounts only")

    source_abstraction = query(
        slot_id,
        "source",
        {"type": "userAbstraction", "user": source},
    )
    source_account_mode = _account_mode(source_abstraction)
    if source_account_mode not in {"standard", "unified"}:
        slot_blockers.append("continuous-v1 source must use standard or unified mode")
    follower_abstraction = query(
        slot_id,
        "follower",
        {"type": "userAbstraction", "user": follower},
    )
    follower_account_mode = _account_mode(follower_abstraction)
    if follower_account_mode != expected_account_mode:
        slot_blockers.append("follower account mode does not match the bound Unified profile")

    principal = global_account
    signing_vault_address = follower
    if subaccount is None:
        slot_blockers.append("follower is absent from the bound master's subAccounts inventory")

    agent_valid_until: int | None = None
    signer_authorized = False
    if agents is not _MISSING:
        authorization_blockers = _validate_extra_agent(
            agents,
            signer,
            minimum_valid_until_ms=now,
        )
        slot_blockers.extend(authorization_blockers)
        signer_authorized = not authorization_blockers
        agent_valid_until = _agent_valid_until(agents, signer)

    follower_spot = subaccount.get("spotState", _MISSING) if subaccount is not None else _MISSING
    follower_collateral, follower_collateral_blockers = _collateral_summary(
        follower_spot,
        require_available=True,
    )
    slot_blockers.extend(f"follower collateral: {item}" for item in follower_collateral_blockers)

    source_dexes = _relevant_dexes(config.allowed_markets)
    follower_dexes = (
        source_dexes
        if audit_dexes is None
        else tuple(
            sorted(
                set(source_dexes) | set(audit_dexes),
                key=lambda item: (item != "", item),
            )
        )
    )
    source_positions: dict[str, Any] = {}
    source_states: dict[str, Any] = {}
    follower_positions: dict[str, Any] = {}
    follower_orders: dict[str, Any] = {}
    for dex in source_dexes:
        source_state = query(
            slot_id,
            "source",
            _dex_request("clearinghouseState", source, dex),
        )
        source_states[dex] = source_state
        source_positions[dex] = _position_summary(
            source_state,
            dex=dex,
            blockers=slot_blockers,
            label="source",
        )
    if source_account_mode == "unified":
        source_spot = query(
            slot_id,
            "source",
            {"type": "spotClearinghouseState", "user": source},
        )
        # A unified leader can legitimately have all token-0 USDC held as perp
        # collateral.  We only read the leader, so positive total sizing equity
        # is required; free collateral is not.  Followers still require
        # positive available collateral above because they submit new risk.
        source_collateral, source_collateral_blockers = _collateral_summary(
            source_spot,
            require_available=False,
        )
        source_equity_basis = "unified_spot_token0_usdc_total"
    elif source_account_mode == "standard":
        source_collateral, source_collateral_blockers = _standard_source_equity(
            source_states,
            source_positions,
        )
        source_equity_basis = "standard_sum_relevant_dex_account_value"
    else:
        source_collateral, source_collateral_blockers = (
            _empty_collateral(),
            ["source account mode is unresolved"],
        )
        source_equity_basis = "unresolved"
    slot_blockers.extend(f"source collateral: {item}" for item in source_collateral_blockers)
    for dex in follower_dexes:
        follower_state = (
            subaccount.get("clearinghouseState", _MISSING)
            if not dex and subaccount is not None
            else query(
                slot_id,
                "follower",
                _dex_request("clearinghouseState", follower, dex),
            )
        )
        follower_positions[dex] = _position_summary(
            follower_state,
            dex=dex,
            blockers=slot_blockers,
            label="follower",
        )
        if require_open_orders:
            orders = query(slot_id, "follower", _dex_request("openOrders", follower, dex))
            follower_orders[dex] = _open_order_summary(
                orders,
                dex=dex,
                blockers=slot_blockers,
            )

    follower_nonflat = any(item["count"] > 0 for item in follower_positions.values())
    follower_open_order_count = (
        sum(item["count"] for item in follower_orders.values()) if require_open_orders else None
    )
    return (
        {
            "slot": slot_id,
            "source": _redact_address(source),
            "follower": _redact_address(follower),
            "signer": _redact_address(signer),
            "identity": {
                "global_account": _redact_address(global_account),
                "expected_account_mode": expected_account_mode,
                "source_account_mode": source_account_mode,
                "source_equity_basis": source_equity_basis,
                "follower_account_mode": follower_account_mode,
                "source_role": "not_queried",
                "follower_role": "subaccount_inventory",
                "action_principal": _redact_address(principal),
                "signing_vault_address": _redact_address(signing_vault_address),
                "signer_role": "extraAgents_inventory" if signer_authorized else None,
                "signer_owner": _redact_address(global_account) if signer_authorized else None,
                "signer_authorized": signer_authorized,
                "signer_valid_until_ms": agent_valid_until,
            },
            "collateral": {
                "source": source_collateral,
                "follower": follower_collateral,
            },
            "source_dexes": list(source_dexes),
            "follower_dexes": list(follower_dexes),
            "positions": {
                "source": source_positions,
                "follower": follower_positions,
            },
            "open_orders": {"follower": follower_orders},
            "open_orders_checked": require_open_orders,
            "follower_nonflat": follower_nonflat,
            "follower_open_order_count": follower_open_order_count,
        },
        slot_blockers,
    )


def _subaccount_inventory(
    payload: Any,
    *,
    master: str,
) -> tuple[dict[str, Mapping[str, Any]], list[str]]:
    if payload is _MISSING:
        return {}, ["subAccounts response is missing"]
    if not isinstance(payload, list):
        return {}, ["subAccounts response is malformed"]
    records: dict[str, Mapping[str, Any]] = {}
    blockers: list[str] = []
    for row in payload:
        if not isinstance(row, Mapping):
            blockers.append("subAccounts contains a malformed entry")
            continue
        follower = _object_address(row, "subAccountUser")
        returned_master = _object_address(row, "master")
        if follower is None or returned_master is None:
            blockers.append("subAccounts entry has malformed follower or master identity")
            continue
        if returned_master != master:
            blockers.append("subAccounts entry belongs to a different master")
            continue
        if follower in records:
            blockers.append("subAccounts contains a duplicate follower identity")
            continue
        records[follower] = row
    return records, blockers


def _collateral_summary(
    payload: Any,
    *,
    require_available: bool,
) -> tuple[dict[str, Any], list[str]]:
    if payload is _MISSING:
        return _empty_collateral(), ["spotClearinghouseState query failed"]
    total, hold, blockers = _spot_usdc_collateral(payload)
    available: Decimal | None = None
    if total is not None and hold is not None and total >= hold:
        available = total - hold
        if total <= 0:
            blockers.append("token-0 Spot USDC total is not positive")
        if require_available and available <= 0:
            blockers.append("token-0 Spot USDC available collateral is not positive")
    return (
        {
            "token": 0,
            "coin": "USDC",
            "total": None if total is None else str(total),
            "hold": None if hold is None else str(hold),
            "available": None if available is None else str(available),
            "valid": not blockers,
        },
        blockers,
    )


def _standard_source_equity(
    states: Mapping[str, Any],
    positions: Mapping[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    blockers: list[str] = []
    by_dex: dict[str, str | None] = {}
    total = Decimal("0")
    for dex, payload in states.items():
        label = dex or "<default>"
        equity: Decimal | None = None
        if payload is _MISSING:
            blockers.append(f"DEX {label} clearinghouseState query failed")
        elif not isinstance(payload, Mapping):
            blockers.append(f"DEX {label} clearinghouseState is malformed")
        else:
            summary = payload.get("marginSummary")
            raw = summary.get("accountValue") if isinstance(summary, Mapping) else None
            try:
                equity = None if raw is None else Decimal(str(raw))
            except Exception:
                equity = None
            if equity is None or not equity.is_finite() or equity < 0:
                blockers.append(f"DEX {label} accountValue is malformed")
                equity = None
        if equity is not None:
            if positions.get(dex, {}).get("count", 0) and equity <= 0:
                blockers.append(f"DEX {label} is nonflat with no accountValue")
            total += equity
        by_dex[label] = None if equity is None else str(equity)
    if total <= 0:
        blockers.append("relevant DEX accountValue total is not positive")
    return (
        {
            "basis": "sum_relevant_dex_margin_summary_account_value",
            "total": str(total),
            "by_dex": by_dex,
            "valid": not blockers,
        },
        blockers,
    )


def _account_mode(payload: Any) -> str | None:
    if payload is _MISSING:
        return None
    mode = classify_user_abstraction(payload)
    return None if mode is None else mode.value


def _empty_collateral() -> dict[str, Any]:
    return {
        "token": 0,
        "coin": "USDC",
        "total": None,
        "hold": None,
        "available": None,
        "valid": False,
    }


def _relevant_dexes(markets: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {market_dex(market) for market in markets},
            key=lambda item: (item != "", item),
        )
    )


def _dex_request(request_type: str, user: str, dex: str) -> dict[str, Any]:
    payload: dict[str, Any] = {"type": request_type, "user": user}
    if dex:
        payload["dex"] = dex
    return payload


def _position_summary(
    payload: Any,
    *,
    dex: str,
    blockers: list[str],
    label: str,
) -> dict[str, Any]:
    if payload is _MISSING:
        return {"count": 0, "markets": []}
    if not isinstance(payload, Mapping) or not isinstance(payload.get("assetPositions"), list):
        blockers.append(f"{label} {dex or 'native'} clearinghouseState is malformed")
        return {"count": 0, "markets": []}
    markets: list[str] = []
    for row in payload["assetPositions"]:
        position = row.get("position") if isinstance(row, Mapping) else None
        if not isinstance(position, Mapping):
            blockers.append(f"{label} {dex or 'native'} position entry is malformed")
            continue
        try:
            size = Decimal(str(position.get("szi")))
        except Exception:
            blockers.append(f"{label} {dex or 'native'} position size is malformed")
            continue
        if not size.is_finite():
            blockers.append(f"{label} {dex or 'native'} position size is not finite")
            continue
        if size == 0:
            continue
        market = _market_for_dex(position.get("coin"), dex)
        if market is None:
            blockers.append(f"{label} {dex or 'native'} position market is malformed")
            continue
        markets.append(market)
    return {"count": len(markets), "markets": sorted(set(markets))}


def _open_order_summary(
    payload: Any,
    *,
    dex: str,
    blockers: list[str],
) -> dict[str, Any]:
    if payload is _MISSING:
        return {"count": 0, "markets": []}
    if not isinstance(payload, list):
        blockers.append(f"follower {dex or 'native'} openOrders is malformed")
        return {"count": 0, "markets": []}
    markets: list[str] = []
    for order in payload:
        if not isinstance(order, Mapping):
            blockers.append(f"follower {dex or 'native'} open-order entry is malformed")
            continue
        market = _market_for_dex(order.get("coin"), dex)
        if market is None:
            blockers.append(f"follower {dex or 'native'} open-order market is malformed")
            continue
        markets.append(market)
    return {"count": len(payload), "markets": sorted(set(markets))}


def _market_for_dex(raw: Any, dex: str) -> str | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    candidate = raw.strip()
    if dex and ":" not in candidate:
        candidate = f"{dex}:{candidate}"
    try:
        market = canonical_market_symbol(candidate)
    except ValueError:
        return None
    return market if market_dex(market) == dex else None


def _agent_valid_until(payload: Any, signer: str) -> int | None:
    entries = payload if isinstance(payload, list) else [payload]
    for entry in entries:
        if not isinstance(entry, Mapping) or _object_address(entry, "address") != signer:
            continue
        try:
            return int(str(entry.get("validUntil")))
        except (TypeError, ValueError):
            return None
    return None


def _redact_address(address: str | None) -> str | None:
    if address is None:
        return None
    normalized = address.lower()
    if not ADDRESS_RE.fullmatch(normalized):
        return "<invalid-address>"
    return normalized[:8] + "..." + normalized[-6:]


__all__ = ["InfoCallable", "run_continuous_preflight"]
