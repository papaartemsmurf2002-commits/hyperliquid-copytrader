from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .cloid import deterministic_cloid
from .config import MarketEligibility, RiskConfig
from .markets import canonical_market_symbol, market_dex
from .models import (
    DesiredState,
    FollowerIntent,
    IntentAction,
    IntentStatus,
    Mode,
    Position,
    now_ms,
)
from .precision import aggressive_ioc_price, quantize_size


@dataclass(frozen=True)
class AssetMeta:
    coin: str
    sz_decimals: int
    max_leverage: int | None = None


@dataclass(frozen=True)
class CopyResult:
    desired_state: DesiredState
    intents: list[FollowerIntent]
    blockers: list[str]
    sizing: dict[str, object]


class CopyEngine:
    def __init__(
        self,
        risk: RiskConfig,
        mode: Mode,
        follower_account: str = "",
        strategy_version: str = "copy-v1",
    ):
        self.risk = risk
        self.mode = mode
        self.follower_account = follower_account.lower()
        self.strategy_version = strategy_version

    def build_target_state(
        self,
        source_positions: dict[str, Position],
        *,
        asset_meta: dict[str, AssetMeta],
        mids: dict[str, Decimal],
        source_account_value: Decimal | None = None,
        follower_account_value: Decimal | None = None,
    ) -> tuple[dict[str, Position], list[str], dict[str, object]]:
        targets: dict[str, Position] = {}
        blockers: list[str] = []
        allowlist = {canonical_market_symbol(symbol) for symbol in self.risk.allowed_symbols}
        denylist = {canonical_market_symbol(symbol) for symbol in self.risk.denied_symbols}
        scale, sizing = self._effective_scale(
            source_account_value=source_account_value,
            follower_account_value=follower_account_value,
        )
        if scale is None:
            blockers.append(str(sizing["blocker"]))
            return targets, blockers, sizing

        for coin, source_position in source_positions.items():
            try:
                market = canonical_market_symbol(coin)
            except ValueError as exc:
                blockers.append(f"invalid source market {coin!r}: {exc}")
                continue
            if market in denylist:
                continue
            if (
                self.risk.market_eligibility == MarketEligibility.ALLOWLIST
                and market not in allowlist
            ):
                continue
            meta = asset_meta.get(market)
            mid = mids.get(market)
            if meta is None:
                blockers.append(f"{market} missing exchange metadata")
                continue
            if mid is None or not mid.is_finite() or mid <= 0:
                blockers.append(f"{market} missing positive mid price")
                continue
            if not source_position.size.is_finite():
                blockers.append(f"{market} source size is not finite")
                continue
            if source_position.entry_px is not None and not source_position.entry_px.is_finite():
                blockers.append(f"{market} source entry price is not finite")
                continue
            desired_leverage = source_position.leverage
            if desired_leverage is not None and desired_leverage <= 0:
                blockers.append(f"{market} source leverage {desired_leverage} is invalid")
                continue
            if desired_leverage is not None:
                desired_leverage = min(desired_leverage, self.risk.max_leverage)
            if (
                desired_leverage is not None
                and meta.max_leverage is not None
                and desired_leverage > meta.max_leverage
            ):
                blockers.append(
                    f"{market} source leverage {desired_leverage} exceeds exchange max "
                    f"{meta.max_leverage}"
                )
                continue

            # Desired state is an exact portfolio target.  Lot precision and the venue's
            # minimum notional are action-admission concerns; quantizing here permanently
            # erased proportional tracking debt on small followers.
            target_size = source_position.size * scale
            cap_price = mid
            if market_dex(market):
                cap_price = mid * (
                    Decimal("1") + self.risk.hip3_oracle_envelope_bps / Decimal("10000")
                )
            if abs(target_size) * cap_price > self.risk.max_notional_usd:
                capped_size = self.risk.max_notional_usd / cap_price
                target_size = capped_size.copy_sign(target_size)

            targets[market] = Position(
                coin=market,
                size=target_size,
                entry_px=source_position.entry_px,
                leverage=desired_leverage,
                updated_ms=now_ms(),
            )

        gross = self._gross_notional(targets, mids)
        sizing["gross_before_cap"] = gross
        sizing["gross_cap_scale"] = None
        sizing["gross_after_cap"] = gross
        if gross > self.risk.max_gross_exposure_usd:
            targets, gross, gross_scale = self._scale_targets_to_gross_cap(
                targets,
                asset_meta=asset_meta,
                mids=mids,
                gross=gross,
            )
            sizing["gross_cap_scale"] = gross_scale
            sizing["gross_after_cap"] = gross
        if gross > self.risk.max_gross_exposure_usd:
            blockers.append(
                f"target gross exposure {gross} exceeds cap {self.risk.max_gross_exposure_usd}"
            )

        targets, margin_blocker = self._apply_initial_margin_budget(
            targets,
            asset_meta=asset_meta,
            mids=mids,
            follower_account_value=follower_account_value,
            sizing=sizing,
        )
        sizing["gross_after_cap"] = self._gross_notional(targets, mids)
        if margin_blocker is not None:
            blockers.append(margin_blocker)

        return targets, blockers, sizing

    def _effective_scale(
        self,
        *,
        source_account_value: Decimal | None,
        follower_account_value: Decimal | None,
    ) -> tuple[Decimal | None, dict[str, object]]:
        base = self.risk.fixed_multiplier
        sizing: dict[str, object] = {
            "mode": "fixed_multiplier",
            "fixed_multiplier": base,
            "equity_ratio": self.risk.equity_ratio,
            "balance_sizing_enabled": self.risk.balance_sizing_enabled,
            "source_account_value": source_account_value,
            "follower_account_value": follower_account_value,
            "sizing_equity_cap_usd": self.risk.sizing_equity_cap_usd,
            "sizing_equity_usd": None,
            "raw_balance_scale": None,
            "balance_scale": None,
            "max_balance_scale": self.risk.max_balance_scale,
            "effective_scale": base,
            "detail": "fixed multiplier without account-value scaling",
        }
        if not base.is_finite() or base <= 0:
            sizing.update(
                {
                    "mode": "blocked_invalid_sizing",
                    "effective_scale": None,
                    "detail": "fixed multiplier must be finite and positive",
                    "blocker": "fixed multiplier must be finite and positive",
                }
            )
            return None, sizing
        sizing_equity_cap = self.risk.sizing_equity_cap_usd
        if sizing_equity_cap is not None and (
            not sizing_equity_cap.is_finite() or sizing_equity_cap <= 0
        ):
            detail = "sizing equity cap must be finite and positive"
            sizing.update(
                {
                    "mode": "blocked_invalid_sizing",
                    "effective_scale": None,
                    "detail": detail,
                    "blocker": detail,
                }
            )
            return None, sizing
        if self.risk.equity_ratio is not None:
            if not self.risk.equity_ratio.is_finite() or self.risk.equity_ratio <= 0:
                sizing.update(
                    {
                        "mode": "blocked_invalid_sizing",
                        "effective_scale": None,
                        "detail": "explicit equity ratio must be finite and positive",
                        "blocker": "explicit equity ratio must be finite and positive",
                    }
                )
                return None, sizing
            sizing.update(
                {
                    "mode": "explicit_equity_ratio",
                    "effective_scale": self.risk.equity_ratio,
                    "detail": "HLCT_EQUITY_RATIO overrides automatic account-value sizing",
                }
            )
            return self.risk.equity_ratio, sizing
        if not self.risk.balance_sizing_enabled:
            sizing["detail"] = "automatic account-value sizing disabled"
            return base, sizing
        balances_valid = (
            source_account_value is not None
            and follower_account_value is not None
            and source_account_value.is_finite()
            and follower_account_value.is_finite()
            and source_account_value > 0
            and follower_account_value > 0
        )
        if not balances_valid:
            if self.mode in {Mode.TESTNET, Mode.LIVE}:
                detail = (
                    "exchange mode balance sizing requires finite positive source and follower "
                    "accountValue snapshots"
                )
                sizing.update(
                    {
                        "mode": "blocked_missing_balances",
                        "effective_scale": None,
                        "detail": detail,
                        "blocker": detail,
                    }
                )
                return None, sizing
            sizing.update(
                {
                    "mode": "fixed_multiplier_waiting_for_balances",
                    "detail": "fresh source and follower accountValue snapshots are not both available",
                }
            )
            return base, sizing
        if not self.risk.max_balance_scale.is_finite() or self.risk.max_balance_scale <= 0:
            detail = "max balance scale must be finite and positive"
            sizing.update(
                {
                    "mode": "blocked_invalid_sizing",
                    "effective_scale": None,
                    "detail": detail,
                    "blocker": detail,
                }
            )
            return None, sizing
        assert source_account_value is not None
        assert follower_account_value is not None
        sizing_equity = (
            min(follower_account_value, sizing_equity_cap)
            if sizing_equity_cap is not None
            else follower_account_value
        )
        raw_balance_scale = sizing_equity / source_account_value
        balance_scale = min(raw_balance_scale, self.risk.max_balance_scale)
        effective_scale = base * balance_scale
        if not effective_scale.is_finite() or effective_scale <= 0:
            detail = "effective account-value scale must be finite and positive"
            sizing.update(
                {
                    "mode": "blocked_invalid_sizing",
                    "raw_balance_scale": raw_balance_scale,
                    "balance_scale": balance_scale,
                    "effective_scale": None,
                    "detail": detail,
                    "blocker": detail,
                }
            )
            return None, sizing
        sizing.update(
            {
                "mode": "balance_scaled",
                "raw_balance_scale": raw_balance_scale,
                "balance_scale": balance_scale,
                "sizing_equity_usd": sizing_equity,
                "effective_scale": effective_scale,
                "detail": (
                    "fixed multiplier scaled by capped sizing equity/source accountValue"
                    if sizing_equity_cap is not None
                    else "fixed multiplier scaled by follower/source accountValue"
                ),
            }
        )
        return effective_scale, sizing

    @staticmethod
    def _gross_notional(positions: dict[str, Position], mids: dict[str, Decimal]) -> Decimal:
        gross = Decimal("0")
        for coin, position in positions.items():
            if position.size == 0:
                continue
            mid = mids.get(canonical_market_symbol(coin))
            if mid is None or not mid.is_finite() or mid <= 0:
                continue
            gross += abs(position.size) * mid
        return gross

    def _apply_initial_margin_budget(
        self,
        positions: dict[str, Position],
        *,
        asset_meta: dict[str, AssetMeta],
        mids: dict[str, Decimal],
        follower_account_value: Decimal | None,
        sizing: dict[str, object],
    ) -> tuple[dict[str, Position], str | None]:
        utilization = self.risk.max_initial_margin_utilization
        sizing.update(
            {
                "max_initial_margin_utilization": utilization,
                "initial_margin_budget_status": "disabled",
                "initial_margin_budget_usd": None,
                "initial_margin_before_cap": None,
                "initial_margin_cap_scale": None,
                "initial_margin_after_cap": None,
                "initial_margin_utilization_after_cap": None,
            }
        )
        if utilization is None:
            return positions, None
        if not utilization.is_finite() or not Decimal("0") < utilization <= Decimal("1"):
            detail = "max initial margin utilization must be finite, above 0, and at or below 1"
            sizing["initial_margin_budget_status"] = "blocked_invalid_policy"
            return {}, detail
        if (
            follower_account_value is None
            or not follower_account_value.is_finite()
            or follower_account_value <= 0
        ):
            detail = (
                "initial-margin budget requires a finite positive follower accountValue snapshot"
            )
            sizing["initial_margin_budget_status"] = "blocked_invalid_equity"
            return {}, detail

        required_margin, margin_error = self._initial_margin_requirement(positions, mids)
        if margin_error is not None:
            sizing["initial_margin_budget_status"] = "blocked_invalid_leverage"
            return {}, margin_error
        assert required_margin is not None

        margin_budget = follower_account_value * utilization
        sizing.update(
            {
                "initial_margin_budget_status": "within_budget",
                "initial_margin_budget_usd": margin_budget,
                "initial_margin_before_cap": required_margin,
                "initial_margin_after_cap": required_margin,
                "initial_margin_utilization_after_cap": (required_margin / follower_account_value),
            }
        )
        if required_margin <= margin_budget:
            return positions, None

        margin_scale = margin_budget / required_margin
        scaled = self._scale_targets(
            positions,
            asset_meta=asset_meta,
            mids=mids,
            scale=margin_scale,
        )
        final_margin, margin_error = self._initial_margin_requirement(scaled, mids)
        if margin_error is not None:
            sizing["initial_margin_budget_status"] = "blocked_invalid_leverage"
            return {}, margin_error
        assert final_margin is not None
        sizing.update(
            {
                "initial_margin_budget_status": "scaled",
                "initial_margin_cap_scale": margin_scale,
                "initial_margin_after_cap": final_margin,
                "initial_margin_utilization_after_cap": final_margin / follower_account_value,
            }
        )
        if final_margin > margin_budget:
            detail = (
                f"target initial margin {final_margin} exceeds budget {margin_budget} after scaling"
            )
            sizing["initial_margin_budget_status"] = "blocked_budget_exceeded"
            return {}, detail
        return scaled, None

    @staticmethod
    def _initial_margin_requirement(
        positions: dict[str, Position], mids: dict[str, Decimal]
    ) -> tuple[Decimal | None, str | None]:
        required_margin = Decimal("0")
        for coin, position in positions.items():
            if position.size == 0:
                continue
            market = canonical_market_symbol(coin)
            leverage = position.leverage
            if leverage is None:
                return None, f"{market} target leverage is required for initial-margin budgeting"
            if isinstance(leverage, bool) or not isinstance(leverage, int) or leverage <= 0:
                return (
                    None,
                    f"{market} target leverage {leverage!r} is invalid for initial-margin budgeting",
                )
            mid = mids.get(market)
            if mid is None or not mid.is_finite() or mid <= 0:
                return None, f"{market} requires a finite positive mid for initial-margin budgeting"
            required_margin += abs(position.size) * mid / Decimal(leverage)
        if not required_margin.is_finite():
            return None, "target initial-margin requirement is not finite"
        return required_margin, None

    def _scale_targets(
        self,
        positions: dict[str, Position],
        *,
        asset_meta: dict[str, AssetMeta],
        mids: dict[str, Decimal],
        scale: Decimal,
    ) -> dict[str, Position]:
        scaled: dict[str, Position] = {}
        for coin, position in positions.items():
            market = canonical_market_symbol(coin)
            meta = asset_meta.get(market)
            mid = mids.get(market)
            target_size = position.size
            if (
                meta is not None
                and mid is not None
                and mid.is_finite()
                and mid > 0
                and target_size != 0
            ):
                target_size = target_size * scale
            scaled[market] = Position(
                coin=market,
                size=target_size,
                entry_px=position.entry_px,
                leverage=position.leverage,
                updated_ms=position.updated_ms,
            )
        return scaled

    def _scale_targets_to_gross_cap(
        self,
        positions: dict[str, Position],
        *,
        asset_meta: dict[str, AssetMeta],
        mids: dict[str, Decimal],
        gross: Decimal,
    ) -> tuple[dict[str, Position], Decimal, Decimal]:
        if gross <= 0:
            return positions, gross, Decimal("1")
        gross_scale = self.risk.max_gross_exposure_usd / gross
        scaled = self._scale_targets(
            positions,
            asset_meta=asset_meta,
            mids=mids,
            scale=gross_scale,
        )
        return scaled, self._gross_notional(scaled, mids), gross_scale

    def plan(
        self,
        *,
        source_event_key: str,
        source_positions: dict[str, Position],
        follower_positions: dict[str, Position],
        asset_meta: dict[str, AssetMeta],
        mids: dict[str, Decimal],
        source_account_value: Decimal | None = None,
        follower_account_value: Decimal | None = None,
    ) -> CopyResult:
        target_positions, blockers, sizing = self.build_target_state(
            source_positions,
            asset_meta=asset_meta,
            mids=mids,
            source_account_value=source_account_value,
            follower_account_value=follower_account_value,
        )
        state_id = deterministic_cloid(
            "desired",
            source_event_key,
            self.mode.value,
            self.follower_account,
            self._stable_target_positions(target_positions),
        )
        desired = DesiredState(
            state_id=state_id,
            source_event_key=source_event_key,
            mode=self.mode,
            positions=target_positions,
            reason=f"scaled source exposure via {sizing['mode']}",
            created_ms=now_ms(),
        )
        intents: list[FollowerIntent] = []
        all_coins = set(target_positions) | set(follower_positions)
        for coin in sorted(all_coins):
            target = target_positions.get(coin, Position(coin=coin, size=Decimal("0")))
            current = follower_positions.get(coin, Position(coin=coin, size=Decimal("0")))
            if not target.size.is_finite():
                blockers.append(f"{coin} target size is not finite")
                continue
            if not current.size.is_finite():
                blockers.append(f"{coin} follower size is not finite")
                continue
            coin_intents = self._plan_coin(
                coin=coin,
                target_size=target.size,
                current_size=current.size,
                source_event_key=source_event_key,
                asset_meta=asset_meta,
                mids=mids,
            )
            for intent in coin_intents:
                if intent.status != IntentStatus.SKIPPED:
                    continue
                if intent.reason == "missing metadata":
                    blockers.append(f"{coin} missing exchange metadata for follower delta")
                elif intent.reason == "missing mid price":
                    blockers.append(f"{coin} missing positive mid price for follower delta")
            intents.extend(coin_intents)
        return CopyResult(
            desired_state=desired,
            intents=intents,
            blockers=blockers,
            sizing=sizing,
        )

    @staticmethod
    def _stable_target_positions(positions: dict[str, Position]) -> dict[str, dict[str, object]]:
        return {
            canonical_market_symbol(coin): {
                "coin": canonical_market_symbol(position.coin),
                "size": position.size,
                "entry_px": position.entry_px,
                "leverage": position.leverage,
            }
            for coin, position in sorted(positions.items())
        }

    def _plan_coin(
        self,
        *,
        coin: str,
        target_size: Decimal,
        current_size: Decimal,
        source_event_key: str,
        asset_meta: dict[str, AssetMeta],
        mids: dict[str, Decimal],
    ) -> list[FollowerIntent]:
        if target_size == current_size:
            return []
        meta = asset_meta.get(coin)
        mid = mids.get(coin)
        if meta is None:
            return [
                self._intent(
                    action=IntentAction.NOOP,
                    coin=coin,
                    side="none",
                    size=Decimal("0"),
                    price=None,
                    reduce_only=False,
                    source_event_key=source_event_key,
                    reason="missing metadata",
                    status=IntentStatus.SKIPPED,
                )
            ]
        if mid is None or not mid.is_finite() or mid <= 0:
            return [
                self._intent(
                    action=IntentAction.NOOP,
                    coin=coin,
                    side="none",
                    size=Decimal("0"),
                    price=None,
                    reduce_only=False,
                    source_event_key=source_event_key,
                    reason="missing mid price",
                    status=IntentStatus.SKIPPED,
                )
            ]

        intents: list[FollowerIntent] = []
        if current_size != 0 and target_size != 0 and (current_size > 0) != (target_size > 0):
            close_side = "sell" if current_size > 0 else "buy"
            close_price = aggressive_ioc_price(
                mid,
                is_buy=close_side == "buy",
                slippage_bps=self.risk.close_slippage_bps,
                sz_decimals=meta.sz_decimals,
            )
            intents.append(
                self._intent(
                    action=IntentAction.CLOSE,
                    coin=coin,
                    side=close_side,
                    size=abs(current_size),
                    price=close_price,
                    reduce_only=True,
                    source_event_key=source_event_key,
                    reason=(
                        "close before source-side flip; "
                        f"reduce_only close_slippage_bps={self.risk.close_slippage_bps}"
                    ),
                )
            )
            open_side = "buy" if target_size > 0 else "sell"
            open_price = aggressive_ioc_price(
                mid,
                is_buy=open_side == "buy",
                slippage_bps=self.risk.slippage_bps,
                sz_decimals=meta.sz_decimals,
            )
            intents.append(
                self._intent(
                    action=IntentAction.OPEN,
                    coin=coin,
                    side=open_side,
                    size=abs(target_size),
                    price=open_price,
                    reduce_only=False,
                    source_event_key=source_event_key,
                    reason=f"open after source-side flip; entry_slippage_bps={self.risk.slippage_bps}",
                )
            )
            return intents

        raw_delta = target_size - current_size
        delta = quantize_size(raw_delta, meta.sz_decimals)
        if delta == 0:
            if raw_delta == 0:
                return []
            return [
                self._intent(
                    action=IntentAction.NOOP,
                    coin=coin,
                    side="none",
                    size=Decimal("0"),
                    price=None,
                    reduce_only=False,
                    source_event_key=source_event_key,
                    reason=(
                        "delta below min size/notional: "
                        f"size={abs(raw_delta)} notional={abs(raw_delta) * mid}"
                    ),
                    status=IntentStatus.SKIPPED,
                )
            ]
        notional = abs(delta) * mid
        side = "buy" if delta > 0 else "sell"
        reducing = current_size != 0 and abs(target_size) < abs(current_size)
        exact_flatten = reducing and target_size == 0 and abs(delta) == abs(current_size)
        if abs(delta) < self.risk.min_order_size and not exact_flatten:
            return [
                self._intent(
                    action=IntentAction.NOOP,
                    coin=coin,
                    side="none",
                    size=Decimal("0"),
                    price=None,
                    reduce_only=False,
                    source_event_key=source_event_key,
                    reason=f"delta below min size/notional: size={abs(delta)} notional={notional}",
                    status=IntentStatus.SKIPPED,
                )
            ]
        slippage_bps = self.risk.close_slippage_bps if reducing else self.risk.slippage_bps
        price = aggressive_ioc_price(
            mid,
            is_buy=side == "buy",
            slippage_bps=slippage_bps,
            sz_decimals=meta.sz_decimals,
        )
        action = IntentAction.REDUCE if reducing else IntentAction.OPEN
        policy_label = "reduce_only close_slippage_bps" if reducing else "entry_slippage_bps"
        return [
            self._intent(
                action=action,
                coin=coin,
                side=side,
                size=abs(delta),
                price=price,
                reduce_only=reducing,
                source_event_key=source_event_key,
                reason=f"move follower toward desired target; {policy_label}={slippage_bps}",
            )
        ]

    def _intent(
        self,
        *,
        action: IntentAction,
        coin: str,
        side: str,
        size: Decimal,
        price: Decimal | None,
        reduce_only: bool,
        source_event_key: str,
        reason: str,
        status: IntentStatus = IntentStatus.PENDING,
    ) -> FollowerIntent:
        cloid = deterministic_cloid(
            source_event_key,
            self.mode.value,
            self.follower_account,
            coin,
            action.value,
            side,
            size,
            price,
            reduce_only,
            self.strategy_version,
        )
        return FollowerIntent(
            intent_id=deterministic_cloid("intent", cloid),
            cloid=cloid,
            action=action,
            coin=coin,
            side=side,
            size=size,
            price=price,
            reduce_only=reduce_only,
            mode=self.mode,
            source_event_key=source_event_key,
            reason=reason,
            created_ms=now_ms(),
            status=status,
        )
