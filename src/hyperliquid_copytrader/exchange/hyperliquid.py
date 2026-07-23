from __future__ import annotations

import json
import urllib.error
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from decimal import Decimal
from time import sleep
from typing import Any, Callable, Iterator, Protocol

from ..cloid import deterministic_cloid, validate_cloid
from ..config import AccountMode, AppConfig, MAINNET_REST
from ..markets import canonical_market_symbol, market_dex, qualify_market_symbol
from ..models import (
    ExecutionReport,
    FollowerIntent,
    IntentAction,
    IntentStatus,
    Mode,
    OpenOrder,
    Position,
    ReconcileSnapshot,
    now_ms,
    parse_decimal,
)
from ..observer import parse_clearinghouse_positions, parse_open_orders
from ..rest_throttle import (
    apply_rest_throttle,
    call_with_rest_backoff,
    rest_throttle_enabled_for_base_url,
)
from ..signing_backend import require_release_signing_backend
from ..unified_account import (
    HyperliquidUserAbstraction,
    UnifiedAccountSnapshot,
    UnifiedAccountStateError,
    UnifiedAccountStateStream,
    classify_user_abstraction,
    non_default_dex_activity,
    normalized_abstraction_mode,
)


HISTORICAL_ORDERS_LIMIT = 2_000
SIGNED_AUTH_RESPONSE_MAX_BYTES = 1_000_000


class ExecutionAdapter(Protocol):
    def account_preflight(self) -> list[str]: ...

    def auth_probe(self, *, intent_id: str, cloid: str) -> ExecutionReport: ...

    def place_intent(self, intent: FollowerIntent) -> ExecutionReport: ...

    def place_limit_order(
        self,
        *,
        coin: str,
        side: str,
        size: Decimal,
        price: Decimal,
        cloid: str,
        reduce_only: bool = False,
        tif: str = "Gtc",
    ) -> ExecutionReport: ...

    def cancel_by_cloid(self, coin: str, cloid: str) -> ExecutionReport: ...

    def schedule_cancel(
        self, *, scheduled_time_ms: int | None, intent_id: str, cloid: str
    ) -> ExecutionReport: ...

    def order_status(self, cloid_or_oid: str | int) -> dict[str, Any]: ...

    def update_leverage(
        self,
        coin: str,
        leverage: int,
        is_cross: bool = True,
        *,
        risk_increasing: bool = True,
    ) -> ExecutionReport: ...

    def reconcile(self) -> ReconcileSnapshot: ...

    def dead_man_eligibility(self) -> dict[str, Any]: ...


class PreSendBlockedError(RuntimeError):
    """Raised by the service's last-mile gate after throttling but before signing."""


class _ThrottledSdkInfo:
    def __init__(self, wrapped: Any, *, enabled: bool):
        self._wrapped = wrapped
        self._enabled = enabled

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._wrapped, name)
        if not callable(attr):
            return attr

        def throttled(*args: Any, **kwargs: Any) -> Any:
            return call_with_rest_backoff(
                f"sdk-info:{name}",
                lambda: attr(*args, **kwargs),
                enabled=self._enabled,
            )

        return throttled


class HyperliquidExecutionAdapter:
    supports_atomic_hip3_dispatch = True

    def __init__(
        self,
        config: AppConfig,
        *,
        pre_send_check: Callable[[str, bool], Any] | None = None,
        signed_action_guard: Callable[[str], Any] | None = None,
        unified_state_provider: Callable[[], UnifiedAccountSnapshot] | None = None,
    ):
        self.config = config
        self.account = config.exchange.follower_account_address.lower()
        self.base_url = config.rest_url
        self._info: Any | None = None
        self._exchange: Any | None = None
        self._pre_send_check = pre_send_check
        self._signed_action_guard = signed_action_guard
        self._unified_state_provider = unified_state_provider
        self._unified_state_stream: UnifiedAccountStateStream | None = None
        self._last_account_context: dict[str, Any] = {
            "expected_mode": config.exchange.expected_account_mode.value,
            "detected_mode": "unknown",
            "collateral_source": "unknown",
            "account_value": None,
        }
        self._last_order_discovery: dict[str, Any] = {
            "scope": "default_perp_dex",
            "history_count": None,
            "discovered_non_default_dexes": [],
            "complete": False,
        }

    def _configured_perp_dexs(self) -> list[str]:
        """Return the SDK DEX universe needed by the configured copy allowlist."""

        dexes = {
            market_dex(canonical_market_symbol(symbol))
            for symbol in self.config.risk.allowed_symbols
        }
        dexes.discard("")
        return ["", *sorted(dexes)]

    def _all_configured_markets(self) -> bool:
        return self.config.source_dex_scope.value == "all_configured_markets"

    def set_pre_send_check(self, callback: Callable[[str, bool], Any]) -> None:
        self._pre_send_check = callback

    def set_signed_action_guard(self, callback: Callable[[str], Any]) -> None:
        self._signed_action_guard = callback

    def _check_pre_send(self, action: str, *, risk_increasing: bool) -> Any:
        if self._pre_send_check is not None:
            return self._pre_send_check(action, risk_increasing)
        return None

    @contextmanager
    def _guard_signed_action(self, action: str) -> Iterator[None]:
        if self._signed_action_guard is None:
            yield
            return
        with self._signed_action_guard(action):
            yield

    def _load_sdk(self) -> tuple[Any, Any, Any, Any]:
        try:
            from eth_account import Account
            from hyperliquid.exchange import Exchange
            from hyperliquid.info import Info
            from hyperliquid.utils.types import Cloid
        except Exception as exc:  # pragma: no cover - environment path
            raise RuntimeError(f"official Hyperliquid SDK is not importable: {exc}") from exc
        return Account, Exchange, Info, Cloid

    @property
    def info(self) -> Any:
        if self._info is None:
            _, _, Info, _ = self._load_sdk()
            raw_info = Info(
                self.base_url,
                skip_ws=True,
                perp_dexs=self._configured_perp_dexs(),
                timeout=float(self.config.ops.info_timeout_s),
            )
            self._info = _ThrottledSdkInfo(
                raw_info,
                enabled=rest_throttle_enabled_for_base_url(self.base_url),
            )
        return self._info

    @property
    def exchange(self) -> Any:
        if self._exchange is None:
            Account, Exchange, _, _ = self._load_sdk()
            wallet = Account.from_key(self.config.exchange.api_private_key)
            self._exchange = Exchange(
                wallet,
                base_url=self.base_url,
                account_address=self.account,
                vault_address=self.config.exchange.vault_address or None,
                perp_dexs=self._configured_perp_dexs(),
                timeout=float(self.config.ops.exchange_action_timeout_s),
            )
        return self._exchange

    def _configured_market_dexes(self) -> set[str]:
        return set(self._configured_perp_dexs()[1:])

    def _historical_order_dexes(self) -> set[str]:
        """Discover every DEX that can still contain an open order.

        ``historicalOrders`` is global across perp DEXes, while ``openOrders`` is DEX-scoped.
        When fewer than the documented 2,000-row limit are returned, the history is complete for
        a fresh follower and every DEX ever used can be queried authoritatively. At the limit we
        fail closed because an older still-open order could otherwise be invisible.
        """

        historical_orders = getattr(self.info, "historical_orders", None)
        if callable(historical_orders):
            payload = historical_orders(self.account)
        else:
            post = getattr(self.info, "post", None)
            if not callable(post):
                raise RuntimeError("Hyperliquid SDK Info client does not expose historicalOrders")
            payload = post("/info", {"type": "historicalOrders", "user": self.account})
        if not isinstance(payload, list):
            raise ValueError("Hyperliquid historicalOrders query returned non-list response")
        if len(payload) >= HISTORICAL_ORDERS_LIMIT:
            raise ValueError(
                "Hyperliquid historicalOrders discovery reached the 2000-row limit; "
                "global open-order DEX coverage is ambiguous"
            )
        dexes: set[str] = set()
        for index, entry in enumerate(payload):
            if not isinstance(entry, dict):
                raise ValueError(f"Hyperliquid historical order {index} must be an object")
            order = entry.get("order", entry)
            if not isinstance(order, dict):
                raise ValueError(f"Hyperliquid historical order {index}.order must be an object")
            raw_coin = order.get("coin")
            if not isinstance(raw_coin, str) or not raw_coin.strip():
                raise ValueError(f"Hyperliquid historical order {index} is missing a valid coin")
            # Spot order identities use @index and are covered by the default openOrders query.
            if raw_coin.strip().startswith("@"):
                continue
            dex = market_dex(canonical_market_symbol(raw_coin))
            if dex:
                dexes.add(dex)
        self._last_order_discovery = {
            "scope": "complete_historical_dex_discovery",
            "history_count": len(payload),
            "discovered_non_default_dexes": sorted(dexes),
            "complete": True,
        }
        return dexes

    def _combined_aggregate_state(self, aggregate: UnifiedAccountSnapshot) -> dict[str, Any]:
        """Build one default+HIP-3 position state with canonical market identities."""

        default = aggregate.default_state
        combined = dict(default)
        positions: list[dict[str, Any]] = []
        for dex, state in aggregate.clearinghouse_states.items():
            raw_positions = state.get("assetPositions")
            if not isinstance(raw_positions, list):
                raise UnifiedAccountStateError(
                    f"aggregate follower DEX {dex or '<default>'} is missing assetPositions"
                )
            for index, item in enumerate(raw_positions):
                if not isinstance(item, dict):
                    raise UnifiedAccountStateError(
                        f"aggregate follower DEX {dex or '<default>'} position {index} is malformed"
                    )
                raw_position = item.get("position", item)
                if not isinstance(raw_position, dict):
                    raise UnifiedAccountStateError(
                        f"aggregate follower DEX {dex or '<default>'} position {index} is malformed"
                    )
                raw_coin = raw_position.get("coin")
                if not isinstance(raw_coin, str) or not raw_coin.strip():
                    raise UnifiedAccountStateError(
                        f"aggregate follower DEX {dex or '<default>'} position {index} "
                        "is missing a valid coin"
                    )
                normalized = dict(item)
                normalized_position = dict(raw_position)
                normalized_position["coin"] = qualify_market_symbol(dex, raw_coin)
                if "position" in item:
                    normalized["position"] = normalized_position
                else:
                    normalized = normalized_position
                positions.append(normalized)
        combined["assetPositions"] = positions
        # Validate duplicates and every normalized position before this synthetic state is used.
        parse_clearinghouse_positions(combined)
        return combined

    def _combined_open_orders(
        self,
        default_orders: Any,
        *,
        dexes: set[str],
    ) -> list[dict[str, Any]]:
        if not isinstance(default_orders, list):
            raise ValueError("Hyperliquid open-orders query returned non-list response")
        combined = [self._qualified_order("", order) for order in default_orders]
        for dex in sorted(dexes):
            orders = self.info.open_orders(self.account, dex)
            if not isinstance(orders, list):
                raise ValueError(
                    f"Hyperliquid open-orders query for DEX {dex} returned non-list response"
                )
            combined.extend(self._qualified_order(dex, order) for order in orders)
        parse_open_orders(combined)
        return combined

    @staticmethod
    def _unconfigured_open_order_dexes(
        orders: list[dict[str, Any]], configured_dexes: set[str]
    ) -> list[str]:
        unknown: set[str] = set()
        for order in orders:
            dex = market_dex(canonical_market_symbol(order.get("coin")))
            if dex and dex not in configured_dexes:
                unknown.add(dex)
        return sorted(unknown)

    @staticmethod
    def _qualified_order(dex: str, order: Any) -> dict[str, Any]:
        if not isinstance(order, dict):
            raise ValueError(
                f"Hyperliquid open order for DEX {dex or '<default>'} is not an object"
            )
        raw_coin = order.get("coin")
        if not isinstance(raw_coin, str) or not raw_coin.strip():
            raise ValueError(
                f"Hyperliquid open order for DEX {dex or '<default>'} is missing a valid coin"
            )
        normalized = dict(order)
        normalized["coin"] = qualify_market_symbol(dex, raw_coin)
        return normalized

    def account_preflight(self) -> list[str]:
        blockers: list[str] = []
        if not self.account:
            blockers.append("follower account address missing")
            return blockers
        if not self.config.exchange.api_private_key:
            blockers.append("api private key missing")
            return blockers
        try:
            state = self.info.user_state(self.account)
            open_orders = self.info.open_orders(self.account)
            spot_state = self._spot_user_state()
            rate_limit = self._user_rate_limit()
            role = self._user_role(self.account)
            abstraction = self._user_abstraction()
            dex_abstraction = self._user_dex_abstraction()
        except Exception as exc:
            blockers.append(f"Hyperliquid account query failed: {exc}")
            return blockers
        detected_mode, mode_blockers = _resolve_user_abstraction(
            abstraction,
            expected=self.config.exchange.expected_account_mode,
        )
        blockers.extend(mode_blockers)
        if self._all_configured_markets() and detected_mode != AccountMode.UNIFIED:
            blockers.append("all_configured_markets requires a Unified follower account")
        aggregate: UnifiedAccountSnapshot | None = None
        if detected_mode == AccountMode.UNIFIED and not mode_blockers:
            try:
                aggregate = self._unified_account_snapshot()
            except Exception as exc:
                blockers.append(f"Hyperliquid unified aggregate state failed: {exc}")
        state_for_validation = state
        configured_dexes = self._configured_market_dexes()
        if aggregate is not None and self._all_configured_markets():
            try:
                active_dexes = set(non_default_dex_activity(aggregate))
                historical_order_dexes = self._historical_order_dexes()
                state_for_validation = self._combined_aggregate_state(aggregate)
                open_orders = self._combined_open_orders(
                    open_orders,
                    dexes=configured_dexes | active_dexes | historical_order_dexes,
                )
                unknown_order_dexes = self._unconfigured_open_order_dexes(
                    open_orders, configured_dexes
                )
                if unknown_order_dexes:
                    raise ValueError(
                        "Hyperliquid account has open orders on unconfigured DEXes: "
                        + ", ".join(unknown_order_dexes)
                    )
            except (UnifiedAccountStateError, ValueError) as exc:
                blockers.append(f"Hyperliquid unified aggregate market state is invalid: {exc}")
        blockers.extend(
            _validate_account_state(
                state_for_validation,
                spot_state=spot_state,
                account_mode=detected_mode,
                aggregate=aggregate,
                allowed_non_default_dexes=(
                    configured_dexes if self._all_configured_markets() else None
                ),
            )
        )
        if not isinstance(open_orders, list):
            blockers.append("Hyperliquid open-orders query returned non-list response")
        blockers.extend(_validate_user_rate_limit(rate_limit))
        blockers.extend(_validate_account_role(role, self.config.exchange.vault_address))
        principal, principal_blockers = self._action_principal(role)
        blockers.extend(principal_blockers)
        if principal is not None:
            blockers.extend(self._validate_signer_authorization(principal))
        blockers.extend(_validate_user_dex_abstraction(dex_abstraction))
        self._last_account_context = _account_context(
            expected=self.config.exchange.expected_account_mode,
            detected=detected_mode,
            state=state_for_validation,
            spot_state=spot_state,
            aggregate=aggregate,
        )
        if self._all_configured_markets() and detected_mode == AccountMode.UNIFIED:
            active_dexes = set(self._last_account_context.get("active_non_default_dexes") or [])
            self._last_account_context.update(
                {
                    "market_scope": "all_configured_markets",
                    "configured_perp_dexes": self._configured_perp_dexs(),
                    "unsupported_non_default_dexes": sorted(active_dexes - configured_dexes),
                    "open_order_discovery": dict(self._last_order_discovery),
                }
            )
        if self.config.mode.value == "live" and self.base_url != MAINNET_REST:
            blockers.append("live mode must use mainnet REST URL")
        return blockers

    def account_context(self) -> dict[str, Any]:
        return dict(self._last_account_context)

    def _user_role(self, address: str) -> Any:
        user_role = getattr(self.info, "user_role", None)
        if callable(user_role):
            return user_role(address)
        post = getattr(self.info, "post", None)
        if callable(post):
            return post("/info", {"type": "userRole", "user": address})
        raise RuntimeError("Hyperliquid SDK Info client does not expose userRole")

    def _signer_address(self) -> str:
        Account, _, _, _ = self._load_sdk()
        return str(Account.from_key(self.config.exchange.api_private_key).address).lower()

    def _vault_details(self, address: str) -> Any:
        post = getattr(self.info, "post", None)
        if not callable(post):
            raise RuntimeError("Hyperliquid SDK Info client does not expose vaultDetails")
        return post("/info", {"type": "vaultDetails", "vaultAddress": address})

    def _extra_agents(self, owner: str) -> Any:
        extra_agents = getattr(self.info, "extra_agents", None)
        if callable(extra_agents):
            return extra_agents(owner)
        post = getattr(self.info, "post", None)
        if callable(post):
            return post("/info", {"type": "extraAgents", "user": owner})
        raise RuntimeError("Hyperliquid SDK Info client does not expose extraAgents")

    def _action_principal(self, role_payload: Any) -> tuple[str | None, list[str]]:
        role = _account_role(role_payload)
        if role == "user":
            return self.account, []
        if role == "subaccount":
            master = _role_address(role_payload, "master")
            if master is None:
                return None, ["Hyperliquid subaccount userRole is missing a valid master address"]
            return master, []
        if role == "vault":
            try:
                details = self._vault_details(self.account)
            except Exception as exc:
                return None, [f"Hyperliquid vaultDetails query failed: {exc}"]
            leader = _object_address(details, "leader")
            if leader is None:
                return None, ["Hyperliquid vaultDetails is missing a valid leader address"]
            return leader, []
        return None, []

    def _validate_signer_authorization(self, principal: str) -> list[str]:
        try:
            signer = self._signer_address()
        except Exception as exc:
            return [f"Hyperliquid API signer address could not be derived: {exc}"]
        expected = self.config.exchange.api_wallet_address.strip().lower()
        if expected and signer != expected:
            return ["configured API private key does not match HLCT_API_WALLET_ADDRESS"]
        if signer == principal:
            if self.config.mode.value == "live":
                return ["live mode forbids signing with the trading-account owner private key"]
            if not self.config.exchange.allow_master_private_key:
                return [
                    "trading-account owner key is not allowed; use an approved API wallet or set "
                    "the testnet-only HLCT_ALLOW_MASTER_PRIVATE_KEY=true acknowledgement"
                ]
            return []
        try:
            signer_role = self._user_role(signer)
        except Exception as exc:
            return [f"Hyperliquid API signer userRole query failed: {exc}"]
        if _account_role(signer_role) != "agent":
            return ["API signer is not an approved agent wallet for the configured trading account"]
        owner = _role_address(signer_role, "user")
        allowed_owners = {self.account, principal}
        if owner not in allowed_owners:
            return [
                "API signer agent owner does not match the configured action account or its "
                "master/vault leader"
            ]
        try:
            agents_payload = self._extra_agents(owner)
        except Exception as exc:
            return [f"Hyperliquid extraAgents query failed: {exc}"]
        minimum_valid_until_ms = (
            now_ms()
            + self.config.ops.dead_man_cancel_ms
            + int(self.config.ops.exchange_action_timeout_s * Decimal("1000"))
        )
        return _validate_extra_agent(
            agents_payload,
            signer,
            minimum_valid_until_ms=minimum_valid_until_ms,
        )

    def _spot_user_state(self) -> Any:
        spot_user_state = getattr(self.info, "spot_user_state", None)
        if callable(spot_user_state):
            return spot_user_state(self.account)
        post = getattr(self.info, "post", None)
        if callable(post):
            return post("/info", {"type": "spotClearinghouseState", "user": self.account})
        return None

    def _user_rate_limit(self) -> Any:
        user_rate_limit = getattr(self.info, "user_rate_limit", None)
        if callable(user_rate_limit):
            return user_rate_limit(self.account)
        post = getattr(self.info, "post", None)
        if callable(post):
            return post("/info", {"type": "userRateLimit", "user": self.account})
        raise RuntimeError("Hyperliquid SDK Info client does not expose userRateLimit")

    def dead_man_eligibility(self) -> dict[str, Any]:
        """Return the public cumulative-volume gate for exchange-hosted scheduleCancel."""

        required_volume = Decimal("1000000")
        payload = self._user_rate_limit()
        if not isinstance(payload, dict):
            raise RuntimeError("Hyperliquid userRateLimit returned a non-object response")
        if str(payload.get("status") or "").lower() == "err":
            raise RuntimeError(f"Hyperliquid userRateLimit returned an error: {payload}")
        try:
            cumulative_volume = parse_decimal(payload.get("cumVlm"))
        except (ArithmeticError, TypeError, ValueError) as exc:
            raise RuntimeError(
                "Hyperliquid userRateLimit did not return parseable cumulative volume"
            ) from exc
        return {
            "eligible": cumulative_volume >= required_volume,
            "cumulative_volume_usd": cumulative_volume,
            "required_volume_usd": required_volume,
            "read_only_query": True,
            "signed_action_performed": False,
        }

    def _user_abstraction(self) -> Any:
        user_abstraction = getattr(self.info, "query_user_abstraction_state", None)
        if callable(user_abstraction):
            return user_abstraction(self.account)
        post = getattr(self.info, "post", None)
        if callable(post):
            return post("/info", {"type": "userAbstraction", "user": self.account})
        raise RuntimeError("Hyperliquid SDK Info client does not expose userAbstraction")

    def _user_dex_abstraction(self) -> Any:
        user_dex_abstraction = getattr(self.info, "query_user_dex_abstraction_state", None)
        if callable(user_dex_abstraction):
            return user_dex_abstraction(self.account)
        post = getattr(self.info, "post", None)
        if callable(post):
            return post("/info", {"type": "userDexAbstraction", "user": self.account})
        raise RuntimeError("Hyperliquid SDK Info client does not expose userDexAbstraction")

    def _unified_account_snapshot(self) -> UnifiedAccountSnapshot:
        if self._unified_state_provider is not None:
            return self._unified_state_provider()
        if self._unified_state_stream is None:
            self._unified_state_stream = UnifiedAccountStateStream(
                rest_url=self.base_url,
                account=self.account,
                timeout_s=float(self.config.ops.info_timeout_s),
                stale_after_ms=self.config.risk.stale_follower_ms,
                reconnect_attempts=self.config.ops.source_websocket_reconnect_attempts,
                reconnect_backoff_ms=self.config.ops.source_websocket_reconnect_backoff_ms,
            )
        return self._unified_state_stream.snapshot()

    @contextmanager
    def _signed_action_deadline(self) -> Iterator[int]:
        exchange = self.exchange
        expires_after_ms = now_ms() + self.config.ops.exchange_expires_after_ms
        previous = getattr(exchange, "expires_after", None)
        exchange.expires_after = expires_after_ms
        try:
            yield expires_after_ms
        finally:
            exchange.expires_after = previous

    def _expires_after_payload(self, expires_after_ms: int) -> dict[str, int]:
        return {
            "expires_after_ms": expires_after_ms,
            "expires_after_window_ms": self.config.ops.exchange_expires_after_ms,
        }

    def auth_probe(self, *, intent_id: str, cloid: str) -> ExecutionReport:
        observed = now_ms()
        expires_after_ms: int | None = None
        try:
            apply_rest_throttle(
                "exchange:noop",
                enabled=rest_throttle_enabled_for_base_url(self.base_url),
            )
            with self._guard_signed_action("auth_probe"):
                self._check_pre_send("auth_probe", risk_increasing=False)
                with self._signed_action_deadline() as expires_after_ms:
                    response = self.exchange.noop(observed)
        except PreSendBlockedError as exc:
            return ExecutionReport(
                report_id=deterministic_cloid(
                    "auth-probe-report", intent_id, cloid, "pre-send-blocked", str(exc), observed
                ),
                intent_id=intent_id,
                cloid=cloid,
                status=IntentStatus.SKIPPED,
                exchange_status="pre_send_blocked",
                exchange_ts_ms=observed,
                payload={"error": str(exc)},
            )
        except Exception as exc:
            payload: dict[str, Any] = {"error": str(exc)}
            if expires_after_ms is not None:
                payload.update(self._expires_after_payload(expires_after_ms))
            return ExecutionReport(
                report_id=deterministic_cloid(
                    "auth-probe-report", intent_id, cloid, "exception", str(exc), observed
                ),
                intent_id=intent_id,
                cloid=cloid,
                status=IntentStatus.SENT,
                exchange_status="transport_unknown",
                exchange_ts_ms=observed,
                payload=payload,
            )
        status, exchange_status = classify_auth_probe_response(response)
        return ExecutionReport(
            report_id=deterministic_cloid(
                "auth-probe-report", intent_id, cloid, response, observed
            ),
            intent_id=intent_id,
            cloid=cloid,
            status=status,
            exchange_status=exchange_status,
            exchange_ts_ms=observed,
            payload={"response": response, **self._expires_after_payload(expires_after_ms)},
        )

    def place_intent(self, intent: FollowerIntent) -> ExecutionReport:
        if intent.action == IntentAction.NOOP or intent.status == IntentStatus.SKIPPED:
            return ExecutionReport(
                report_id=deterministic_cloid("report", intent.intent_id, "skipped"),
                intent_id=intent.intent_id,
                cloid=intent.cloid,
                status=IntentStatus.SKIPPED,
                exchange_status="skipped",
                exchange_ts_ms=now_ms(),
                payload={"reason": intent.reason},
            )
        validate_cloid(intent.cloid)
        coin = canonical_market_symbol(intent.coin)
        effective_intent = intent
        order_request = {
            "coin": coin,
            "side": intent.side,
            "size": intent.size,
            "price": intent.price,
            "reduce_only": intent.reduce_only,
            "tif": "Ioc",
        }
        _, _, _, Cloid = self._load_sdk()
        expires_after_ms: int | None = None
        try:
            apply_rest_throttle(
                "exchange:order",
                enabled=rest_throttle_enabled_for_base_url(self.base_url),
            )
            with self._guard_signed_action("place_intent"):
                resolved = self._check_pre_send(
                    "place_intent",
                    risk_increasing=not intent.reduce_only,
                )
                requires_resolved_hip3 = (
                    self.config.mode in {Mode.TESTNET, Mode.LIVE}
                    and intent.action == IntentAction.OPEN
                    and not intent.reduce_only
                    and bool(market_dex(coin))
                )
                if requires_resolved_hip3 and not isinstance(resolved, FollowerIntent):
                    raise PreSendBlockedError(
                        "place_intent requires an atomically frozen final HIP-3 request"
                    )
                if isinstance(resolved, FollowerIntent):
                    immutable_original = (
                        intent.intent_id,
                        intent.cloid,
                        canonical_market_symbol(intent.coin),
                        intent.action,
                        intent.side,
                        intent.size,
                        intent.reduce_only,
                        intent.mode,
                        intent.source_event_key,
                        intent.reason,
                        intent.created_ms,
                        intent.desired_state_id,
                        intent.status,
                    )
                    immutable_resolved = (
                        resolved.intent_id,
                        resolved.cloid,
                        canonical_market_symbol(resolved.coin),
                        resolved.action,
                        resolved.side,
                        resolved.size,
                        resolved.reduce_only,
                        resolved.mode,
                        resolved.source_event_key,
                        resolved.reason,
                        resolved.created_ms,
                        resolved.desired_state_id,
                        resolved.status,
                    )
                    if immutable_resolved != immutable_original:
                        raise PreSendBlockedError(
                            "place_intent resolver changed immutable order identity"
                        )
                    effective_intent = resolved
                    order_request = {
                        "coin": coin,
                        "side": effective_intent.side,
                        "size": effective_intent.size,
                        "price": effective_intent.price,
                        "reduce_only": effective_intent.reduce_only,
                        "tif": "Ioc",
                    }
                with self._signed_action_deadline() as expires_after_ms:
                    response = self.exchange.order(
                        coin,
                        is_buy=effective_intent.side == "buy",
                        sz=float(effective_intent.size),
                        limit_px=float(effective_intent.price or Decimal("0")),
                        order_type={"limit": {"tif": "Ioc"}},
                        reduce_only=effective_intent.reduce_only,
                        cloid=Cloid.from_str(intent.cloid),
                    )
        except PreSendBlockedError as exc:
            return ExecutionReport(
                report_id=deterministic_cloid(
                    "report", intent.intent_id, "pre-send-blocked", str(exc)
                ),
                intent_id=intent.intent_id,
                cloid=intent.cloid,
                status=IntentStatus.SKIPPED,
                exchange_status="pre_send_blocked",
                exchange_ts_ms=now_ms(),
                payload={"error": str(exc), "order_request": order_request},
            )
        except Exception as exc:
            payload: dict[str, Any] = {"error": str(exc), "order_request": order_request}
            if expires_after_ms is not None:
                payload.update(self._expires_after_payload(expires_after_ms))
            return ExecutionReport(
                report_id=deterministic_cloid("report", intent.intent_id, "exception", str(exc)),
                intent_id=intent.intent_id,
                cloid=intent.cloid,
                status=IntentStatus.SENT,
                exchange_status="transport_unknown",
                exchange_ts_ms=now_ms(),
                payload=payload,
            )
        status, exchange_status, filled_size = classify_action_response(
            response,
            expected_size=effective_intent.size,
            action_type="order",
        )
        payload = {
            "response": response,
            "expected_size": effective_intent.size,
            "order_request": order_request,
            **self._expires_after_payload(expires_after_ms),
        }
        if filled_size is not None:
            payload["filled_size"] = filled_size
        return ExecutionReport(
            report_id=deterministic_cloid("report", intent.intent_id, response),
            intent_id=intent.intent_id,
            cloid=intent.cloid,
            status=status,
            exchange_status=exchange_status,
            exchange_ts_ms=now_ms(),
            payload=payload,
        )

    def place_limit_order(
        self,
        *,
        coin: str,
        side: str,
        size: Decimal,
        price: Decimal,
        cloid: str,
        reduce_only: bool = False,
        tif: str = "Gtc",
    ) -> ExecutionReport:
        validate_cloid(cloid)
        coin = canonical_market_symbol(coin)
        order_request = {
            "coin": coin,
            "side": side,
            "size": size,
            "price": price,
            "reduce_only": reduce_only,
            "tif": tif,
        }
        _, _, _, Cloid = self._load_sdk()
        expires_after_ms: int | None = None
        try:
            apply_rest_throttle(
                "exchange:limit_order",
                enabled=rest_throttle_enabled_for_base_url(self.base_url),
            )
            with self._guard_signed_action(f"place_limit_order:{tif}"):
                self._check_pre_send(
                    f"place_limit_order:{tif}",
                    risk_increasing=not reduce_only,
                )
                with self._signed_action_deadline() as expires_after_ms:
                    response = self.exchange.order(
                        coin,
                        is_buy=side == "buy",
                        sz=float(size),
                        limit_px=float(price),
                        order_type={"limit": {"tif": tif}},
                        reduce_only=reduce_only,
                        cloid=Cloid.from_str(cloid),
                    )
        except PreSendBlockedError as exc:
            return ExecutionReport(
                report_id=deterministic_cloid("limit-report", cloid, "pre-send-blocked", str(exc)),
                intent_id="limit:" + cloid,
                cloid=cloid,
                status=IntentStatus.SKIPPED,
                exchange_status="pre_send_blocked",
                exchange_ts_ms=now_ms(),
                payload={"error": str(exc), "order_request": order_request},
            )
        except Exception as exc:
            payload: dict[str, Any] = {"error": str(exc), "order_request": order_request}
            if expires_after_ms is not None:
                payload.update(self._expires_after_payload(expires_after_ms))
            return ExecutionReport(
                report_id=deterministic_cloid("limit-report", cloid, "exception", str(exc)),
                intent_id="limit:" + cloid,
                cloid=cloid,
                status=IntentStatus.SENT,
                exchange_status="transport_unknown",
                exchange_ts_ms=now_ms(),
                payload=payload,
            )
        status, exchange_status, filled_size = classify_action_response(
            response,
            expected_size=size,
            action_type="order",
        )
        payload = {
            "response": response,
            "tif": tif,
            "expected_size": size,
            "order_request": order_request,
            **self._expires_after_payload(expires_after_ms),
        }
        if filled_size is not None:
            payload["filled_size"] = filled_size
        return ExecutionReport(
            report_id=deterministic_cloid("limit-report", cloid, response),
            intent_id="limit:" + cloid,
            cloid=cloid,
            status=status,
            exchange_status=exchange_status,
            exchange_ts_ms=now_ms(),
            payload=payload,
        )

    def cancel_by_cloid(self, coin: str, cloid: str) -> ExecutionReport:
        validate_cloid(cloid)
        coin = canonical_market_symbol(coin)
        _, _, _, Cloid = self._load_sdk()
        expires_after_ms: int | None = None
        try:
            apply_rest_throttle(
                "exchange:cancel_by_cloid",
                enabled=rest_throttle_enabled_for_base_url(self.base_url),
            )
            with self._guard_signed_action("cancel_by_cloid"):
                self._check_pre_send("cancel_by_cloid", risk_increasing=False)
                with self._signed_action_deadline() as expires_after_ms:
                    response = self.exchange.cancel_by_cloid(coin, Cloid.from_str(cloid))
        except PreSendBlockedError as exc:
            return ExecutionReport(
                report_id=deterministic_cloid("cancel-report", cloid, "pre-send-blocked", str(exc)),
                intent_id="cancel:" + cloid,
                cloid=cloid,
                status=IntentStatus.SKIPPED,
                exchange_status="pre_send_blocked",
                exchange_ts_ms=now_ms(),
                payload={"error": str(exc)},
            )
        except Exception as exc:
            payload: dict[str, Any] = {"error": str(exc)}
            if expires_after_ms is not None:
                payload.update(self._expires_after_payload(expires_after_ms))
            return ExecutionReport(
                report_id=deterministic_cloid("cancel-report", cloid, "exception", str(exc)),
                intent_id="cancel:" + cloid,
                cloid=cloid,
                status=IntentStatus.SENT,
                exchange_status="transport_unknown",
                exchange_ts_ms=now_ms(),
                payload=payload,
            )
        status, exchange_status, _ = classify_action_response(response, action_type="cancel")
        return ExecutionReport(
            report_id=deterministic_cloid("cancel-report", cloid, response),
            intent_id="cancel:" + cloid,
            cloid=cloid,
            status=status,
            exchange_status=exchange_status,
            exchange_ts_ms=now_ms(),
            payload={"response": response, **self._expires_after_payload(expires_after_ms)},
        )

    def schedule_cancel(
        self, *, scheduled_time_ms: int | None, intent_id: str, cloid: str
    ) -> ExecutionReport:
        validate_cloid(cloid)
        observed = now_ms()
        expires_after_ms: int | None = None
        try:
            apply_rest_throttle(
                "exchange:schedule_cancel",
                enabled=rest_throttle_enabled_for_base_url(self.base_url),
            )
            with self._guard_signed_action("schedule_cancel"):
                self._check_pre_send("schedule_cancel", risk_increasing=False)
                with self._signed_action_deadline() as expires_after_ms:
                    response = self.exchange.schedule_cancel(scheduled_time_ms)
        except PreSendBlockedError as exc:
            return ExecutionReport(
                report_id=deterministic_cloid(
                    "dead-man-report",
                    intent_id,
                    cloid,
                    "pre-send-blocked",
                    str(exc),
                    observed,
                ),
                intent_id=intent_id,
                cloid=cloid,
                status=IntentStatus.SKIPPED,
                exchange_status="pre_send_blocked",
                exchange_ts_ms=observed,
                payload={"scheduled_time_ms": scheduled_time_ms, "error": str(exc)},
            )
        except Exception as exc:
            payload: dict[str, Any] = {
                "scheduled_time_ms": scheduled_time_ms,
                "error": str(exc),
            }
            if expires_after_ms is not None:
                payload.update(self._expires_after_payload(expires_after_ms))
            return ExecutionReport(
                report_id=deterministic_cloid(
                    "dead-man-report", intent_id, cloid, "exception", str(exc), observed
                ),
                intent_id=intent_id,
                cloid=cloid,
                status=IntentStatus.SENT,
                exchange_status="transport_unknown",
                exchange_ts_ms=observed,
                payload=payload,
            )
        status, exchange_status = classify_schedule_cancel_response(response, scheduled_time_ms)
        return ExecutionReport(
            report_id=deterministic_cloid("dead-man-report", intent_id, cloid, response, observed),
            intent_id=intent_id,
            cloid=cloid,
            status=status,
            exchange_status=exchange_status,
            exchange_ts_ms=observed,
            payload={
                "scheduled_time_ms": scheduled_time_ms,
                "response": response,
                **self._expires_after_payload(expires_after_ms),
            },
        )

    def order_status(self, cloid_or_oid: str | int) -> dict[str, Any]:
        if isinstance(cloid_or_oid, int):
            return self.info.query_order_by_oid(self.account, cloid_or_oid)
        value = str(cloid_or_oid)
        if value.isdecimal():
            return self.info.query_order_by_oid(self.account, int(value))
        validate_cloid(value)
        _, _, _, Cloid = self._load_sdk()
        query_by_cloid = getattr(self.info, "query_order_by_cloid", None)
        if callable(query_by_cloid):
            return query_by_cloid(self.account, Cloid.from_str(value))
        return self.info.query_order_by_oid(self.account, value)

    def update_leverage(
        self,
        coin: str,
        leverage: int,
        is_cross: bool = True,
        *,
        risk_increasing: bool = True,
    ) -> ExecutionReport:
        observed = now_ms()
        coin = canonical_market_symbol(coin)
        cloid = deterministic_cloid("leverage", self.account, coin, leverage, is_cross)
        expires_after_ms: int | None = None
        try:
            apply_rest_throttle(
                "exchange:update_leverage",
                enabled=rest_throttle_enabled_for_base_url(self.base_url),
            )
            with self._guard_signed_action("update_leverage"):
                self._check_pre_send("update_leverage", risk_increasing=risk_increasing)
                with self._signed_action_deadline() as expires_after_ms:
                    response = self.exchange.update_leverage(int(leverage), coin, is_cross=is_cross)
        except PreSendBlockedError as exc:
            return ExecutionReport(
                report_id=deterministic_cloid(
                    "leverage-report", cloid, "pre-send-blocked", str(exc), observed
                ),
                intent_id=f"leverage:{coin}:{leverage}",
                cloid=cloid,
                status=IntentStatus.SKIPPED,
                exchange_status="pre_send_blocked",
                exchange_ts_ms=observed,
                payload={
                    "coin": coin,
                    "leverage": leverage,
                    "is_cross": is_cross,
                    "error": str(exc),
                },
            )
        except Exception as exc:
            payload: dict[str, Any] = {
                "coin": coin,
                "leverage": leverage,
                "is_cross": is_cross,
                "error": str(exc),
            }
            if expires_after_ms is not None:
                payload.update(self._expires_after_payload(expires_after_ms))
            return ExecutionReport(
                report_id=deterministic_cloid(
                    "leverage-report", cloid, "exception", str(exc), observed
                ),
                intent_id=f"leverage:{coin}:{leverage}",
                cloid=cloid,
                status=IntentStatus.SENT,
                exchange_status="transport_unknown",
                exchange_ts_ms=observed,
                payload=payload,
            )
        status, exchange_status = classify_leverage_response(response)
        return ExecutionReport(
            report_id=deterministic_cloid("leverage-report", cloid, response, observed),
            intent_id=f"leverage:{coin}:{leverage}",
            cloid=cloid,
            status=status,
            exchange_status=exchange_status,
            exchange_ts_ms=observed,
            payload={
                "coin": coin,
                "leverage": leverage,
                "is_cross": is_cross,
                "response": response,
                **self._expires_after_payload(expires_after_ms),
            },
        )

    def reconcile(self) -> ReconcileSnapshot:
        requested_ms = now_ms()
        initial_state = self.info.user_state(self.account)
        orders = self.info.open_orders(self.account)
        # Preserve strict exchange-truth errors before any account-mode interpretation.
        parse_clearinghouse_positions(initial_state, requested_ms)
        parse_open_orders(orders, requested_ms)
        abstraction = self._user_abstraction()
        detected_mode, blockers = _resolve_user_abstraction(
            abstraction,
            expected=self.config.exchange.expected_account_mode,
        )
        if self._all_configured_markets() and detected_mode != AccountMode.UNIFIED:
            blockers.append("all_configured_markets requires a Unified follower account")
        blockers.extend(_validate_user_dex_abstraction(self._user_dex_abstraction()))
        spot_state: Any = None
        aggregate: UnifiedAccountSnapshot | None = None
        configured_dexes = self._configured_market_dexes()
        if detected_mode == AccountMode.UNIFIED and not blockers:
            spot_state = self._spot_user_state()
            aggregate = self._unified_account_snapshot()
            if self._all_configured_markets():
                try:
                    active_dexes = set(non_default_dex_activity(aggregate))
                    historical_order_dexes = self._historical_order_dexes()
                    state = self._combined_aggregate_state(aggregate)
                    orders = self._combined_open_orders(
                        orders,
                        dexes=configured_dexes | active_dexes | historical_order_dexes,
                    )
                    unknown_order_dexes = self._unconfigured_open_order_dexes(
                        orders, configured_dexes
                    )
                    if unknown_order_dexes:
                        raise ValueError(
                            "Hyperliquid account has open orders on unconfigured DEXes: "
                            + ", ".join(unknown_order_dexes)
                        )
                except (UnifiedAccountStateError, ValueError) as exc:
                    blockers.append(f"Hyperliquid unified aggregate market state is invalid: {exc}")
                    state = aggregate.default_state
            else:
                state = aggregate.default_state
        else:
            state = initial_state
        blockers.extend(
            _validate_account_state(
                state,
                spot_state=spot_state,
                account_mode=detected_mode,
                aggregate=aggregate,
                allowed_non_default_dexes=(
                    configured_dexes if self._all_configured_markets() else None
                ),
            )
        )
        if not isinstance(orders, list):
            blockers.append("Hyperliquid open-orders query returned non-list response")
        if blockers:
            raise RuntimeError("; ".join(blockers))
        assert detected_mode is not None
        # Reconciliation freshness is when all required REST and aggregate checks completed. The
        # aggregate's nested exchange mutation time remains in payload for audit but must not make a
        # flat, unchanged account look stale.
        observed = now_ms()
        context = _account_context(
            expected=self.config.exchange.expected_account_mode,
            detected=detected_mode,
            state=state,
            spot_state=spot_state,
            aggregate=aggregate,
        )
        self._last_account_context = context
        if self._all_configured_markets() and detected_mode == AccountMode.UNIFIED:
            active_dexes = set(self._last_account_context.get("active_non_default_dexes") or [])
            self._last_account_context.update(
                {
                    "market_scope": "all_configured_markets",
                    "configured_perp_dexes": self._configured_perp_dexs(),
                    "unsupported_non_default_dexes": sorted(active_dexes - configured_dexes),
                    "open_order_discovery": dict(self._last_order_discovery),
                }
            )
            context = dict(self._last_account_context)
        payload: dict[str, Any] = {
            "account_mode": detected_mode.value,
            "account_value": context.get("account_value"),
            "account_context": context,
            "clearinghouseState": state,
            "openOrders": orders,
        }
        if detected_mode == AccountMode.UNIFIED:
            payload["spotClearinghouseState"] = spot_state
            payload["unified_aggregate"] = {
                "observed_ms": aggregate.observed_ms if aggregate is not None else None,
                "received_ms": aggregate.received_ms if aggregate is not None else None,
                "dex_count": aggregate.dex_count if aggregate is not None else 0,
                "active_non_default_dexes": (
                    non_default_dex_activity(aggregate) if aggregate is not None else []
                ),
                "market_scope": (
                    "all_configured_markets"
                    if self._all_configured_markets()
                    else "default_perp_dex"
                ),
                "configured_perp_dexes": self._configured_perp_dexs(),
                "unsupported_non_default_dexes": sorted(
                    set(non_default_dex_activity(aggregate) if aggregate is not None else [])
                    - configured_dexes
                ),
                "open_order_discovery": dict(self._last_order_discovery),
            }
        return ReconcileSnapshot(
            snapshot_id=deterministic_cloid(
                "follower-reconcile", self.account, detected_mode.value, observed, state, orders
            ),
            account=self.account,
            positions=parse_clearinghouse_positions(state, observed),
            open_orders=parse_open_orders(orders, observed),
            observed_ms=observed,
            source=(
                "hyperliquid-info-unified"
                if detected_mode == AccountMode.UNIFIED
                else "hyperliquid-info"
            ),
            payload=payload,
        )


def classify_action_response(
    response: Any,
    *,
    expected_size: Decimal | None = None,
    action_type: str | None = None,
) -> tuple[IntentStatus, str, Decimal | None]:
    response_type = _action_response_type(response)
    effective_type = (action_type or response_type or "").lower()
    statuses = _action_statuses(response)

    if _top_level_status_is_error(response):
        return IntentStatus.REJECTED, "rejected", _total_filled_size(statuses)

    if statuses:
        if _statuses_have_error(statuses):
            return IntentStatus.REJECTED, "rejected", _total_filled_size(statuses)
        if effective_type in {"cancel", "cancelbycloid", "cancel_by_cloid"}:
            if _statuses_have_success(statuses):
                return IntentStatus.CANCELED, "canceled", None
            return IntentStatus.ACKED, "ambiguous_cancel_response", None

        filled_size = _total_filled_size(statuses)
        if filled_size is not None:
            if expected_size is not None:
                if filled_size <= 0:
                    return IntentStatus.ACKED, "no_fill", filled_size
                if filled_size < expected_size:
                    return IntentStatus.ACKED, "partial_fill", filled_size
                if filled_size > expected_size:
                    return IntentStatus.ACKED, "overfill", filled_size
            return IntentStatus.FILLED, "filled", filled_size
        if _statuses_have_key(statuses, "resting"):
            return IntentStatus.ACKED, "resting", None
        if _statuses_have_success(statuses):
            return IntentStatus.ACKED, "acked", None
        if effective_type == "order":
            return IntentStatus.ACKED, "ambiguous_order_response", None

    filled_size = _total_filled_size(response)
    if expected_size is not None and filled_size is not None:
        if filled_size <= 0:
            return IntentStatus.ACKED, "no_fill", filled_size
        if filled_size < expected_size:
            return IntentStatus.ACKED, "partial_fill", filled_size
        if filled_size > expected_size:
            return IntentStatus.ACKED, "overfill", filled_size
    text = str(response).lower()
    if "error" in text or "rejected" in text:
        return IntentStatus.REJECTED, "rejected", filled_size
    if effective_type in {"order", "cancel", "cancelbycloid", "cancel_by_cloid"}:
        return IntentStatus.ACKED, f"ambiguous_{effective_type}_response", filled_size
    return IntentStatus.ACKED, "acked", filled_size


def classify_auth_probe_response(response: Any) -> tuple[IntentStatus, str]:
    if _top_level_status_is_error(response):
        return IntentStatus.REJECTED, "rejected"
    if _status_has_key_or_value(response, "error", "rejected"):
        return IntentStatus.REJECTED, "rejected"
    if isinstance(response, dict) and str(response.get("status", "")).lower() == "ok":
        return IntentStatus.ACKED, "auth_probe_ok"
    text = str(response).lower()
    if text.strip() == "ok" or "'status': 'ok'" in text or '"status": "ok"' in text:
        return IntentStatus.ACKED, "auth_probe_ok"
    return IntentStatus.REJECTED, "ambiguous_auth_probe_response"


def bounded_signed_noop_auth_probe(
    *,
    private_key: str,
    expected_api_wallet: str,
    vault_address: str | None,
    base_url: str = MAINNET_REST,
    timeout_s: float = 15.0,
    expires_window_ms: int = 10_000,
) -> dict[str, Any]:
    """Send exactly one bounded signed ``noop`` without constructing SDK ``Info``.

    ``Exchange`` eagerly fetches spot and perpetual metadata even though ``noop`` does
    not need it.  The prelaunch authentication gate must be one signed, no-order
    request, so this helper uses the official signer directly and returns only
    redacted classification evidence.
    """

    if base_url.rstrip("/") != MAINNET_REST.rstrip("/"):
        raise ValueError("prelaunch signed authentication is mainnet-only")
    if not private_key.strip() or not expected_api_wallet.strip():
        raise ValueError("signed authentication credential identity is incomplete")
    if not 0 < expires_window_ms <= 30_000:
        raise ValueError("signed authentication expiry window is outside the safety bound")
    if not 0 < timeout_s <= 30:
        raise ValueError("signed authentication timeout is outside the safety bound")

    from eth_account import Account
    from hyperliquid.utils.signing import sign_l1_action

    require_release_signing_backend()
    wallet = Account.from_key(private_key.strip())
    wallet_backend = getattr(getattr(wallet, "_key_obj", None), "backend", None)
    require_release_signing_backend(backend=wallet_backend)
    derived_wallet = str(wallet.address).lower()
    if derived_wallet != expected_api_wallet.strip().lower():
        raise ValueError("private key does not derive the preview-bound API wallet")

    observed_ms = now_ms()
    expires_after_ms = observed_ms + expires_window_ms
    action = {"type": "noop"}
    normalized_vault = vault_address.strip().lower() if vault_address else None
    signature = sign_l1_action(
        wallet,
        action,
        normalized_vault,
        observed_ms,
        expires_after_ms,
        True,
    )
    request_payload = {
        "action": action,
        "nonce": observed_ms,
        "signature": signature,
        "vaultAddress": normalized_vault,
        "expiresAfter": expires_after_ms,
    }
    apply_rest_throttle(
        "exchange:noop",
        enabled=rest_throttle_enabled_for_base_url(base_url),
    )
    request = urllib.request.Request(
        base_url.rstrip("/") + "/exchange",
        data=json.dumps(request_payload, separators=(",", ":")).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_s) as response:  # nosec B310
        raw = response.read(SIGNED_AUTH_RESPONSE_MAX_BYTES + 1)
    if not raw or len(raw) > SIGNED_AUTH_RESPONSE_MAX_BYTES:
        raise RuntimeError("signed authentication response is empty or exceeds its size bound")
    response_payload = json.loads(raw)
    status, exchange_status = classify_auth_probe_response(response_payload)
    return {
        "passed": status is IntentStatus.ACKED and exchange_status == "auth_probe_ok",
        "status": status.value,
        "exchange_status": exchange_status,
        "observed_ms": observed_ms,
        "expires_after_ms": expires_after_ms,
    }


def classify_schedule_cancel_response(
    response: Any, scheduled_time_ms: int | None
) -> tuple[IntentStatus, str]:
    if _top_level_status_is_error(response):
        return IntentStatus.REJECTED, "rejected"
    if _status_has_key_or_value(response, "error", "rejected"):
        return IntentStatus.REJECTED, "rejected"
    exchange_status = "dead_man_cleared" if scheduled_time_ms is None else "dead_man_scheduled"
    if isinstance(response, dict) and str(response.get("status", "")).lower() == "ok":
        return IntentStatus.ACKED, exchange_status
    text = str(response).lower()
    if text.strip() == "ok" or "'status': 'ok'" in text or '"status": "ok"' in text:
        return IntentStatus.ACKED, exchange_status
    return IntentStatus.REJECTED, "ambiguous_dead_man_response"


def _validate_account_state(
    state: Any,
    *,
    spot_state: Any = None,
    account_mode: AccountMode | None = AccountMode.STANDARD,
    aggregate: UnifiedAccountSnapshot | None = None,
    allowed_non_default_dexes: set[str] | None = None,
) -> list[str]:
    blockers: list[str] = []
    if not isinstance(state, dict):
        return ["Hyperliquid account query returned non-object state"]
    if not state:
        return [
            "Hyperliquid account state is empty; verify HLCT_FOLLOWER_ACCOUNT_ADDRESS is the "
            "master/subaccount/vault address, not an API wallet"
        ]
    asset_positions = state.get("assetPositions")
    if not isinstance(asset_positions, list):
        blockers.append("Hyperliquid account state missing assetPositions list")

    margin_summary = _margin_summary(state)
    if margin_summary is None:
        blockers.append("Hyperliquid account state missing marginSummary accountValue")
        return blockers

    try:
        account_value = parse_decimal(margin_summary["accountValue"])
    except Exception as exc:
        blockers.append(f"Hyperliquid accountValue is not parseable: {exc}")
        return blockers
    if account_mode == AccountMode.UNIFIED:
        spot_total, _spot_hold, spot_blockers = _spot_usdc_collateral(spot_state)
        blockers.extend(spot_blockers)
        if aggregate is None:
            blockers.append("Hyperliquid unified account is missing aggregate all-DEX truth")
        else:
            try:
                active_dexes = non_default_dex_activity(aggregate)
            except UnifiedAccountStateError as exc:
                blockers.append(f"Hyperliquid unified aggregate state is invalid: {exc}")
            else:
                if active_dexes:
                    if allowed_non_default_dexes is None:
                        blockers.append(
                            "Hyperliquid unified account has unsupported non-default DEX activity: "
                            + ", ".join(active_dexes)
                        )
                    else:
                        unknown = sorted(set(active_dexes) - allowed_non_default_dexes)
                        if unknown:
                            blockers.append(
                                "Hyperliquid unified account has active unconfigured DEXes: "
                                + ", ".join(unknown)
                            )
        if spot_total is not None and spot_total <= 0:
            blockers.append(
                "Hyperliquid unified account requires positive Spot USDC collateral before "
                "exchange mode"
            )
        return blockers
    if account_value <= 0:
        detail = "Hyperliquid follower accountValue must be positive before exchange mode"
        spot_usdc, _spot_hold, _spot_blockers = _spot_usdc_collateral(spot_state)
        if spot_usdc is not None and spot_usdc > 0:
            detail += (
                f"; Spot USDC balance is {spot_usdc}, but the configured standard Perps "
                "accountValue is zero. Transfer USDC to Perps or configure unified mode only "
                "after confirming userAbstraction."
            )
        blockers.append(detail)
    return blockers


def _spot_usdc_collateral(
    spot_state: Any,
) -> tuple[Decimal | None, Decimal | None, list[str]]:
    blockers: list[str] = []
    if not isinstance(spot_state, dict):
        return None, None, ["Hyperliquid spot state must be an object for unified accounts"]
    balances = spot_state.get("balances")
    if not isinstance(balances, list):
        return None, None, ["Hyperliquid spot state is missing balances list"]
    matches: list[dict[str, Any]] = []
    for balance in balances:
        if not isinstance(balance, dict):
            blockers.append("Hyperliquid spot balance entry must be an object")
            continue
        if str(balance.get("coin", "")).upper() != "USDC":
            continue
        matches.append(balance)
    if len(matches) != 1:
        blockers.append("Hyperliquid unified account requires exactly one Spot USDC balance entry")
        return None, None, blockers
    balance = matches[0]
    token = balance.get("token")
    if token is None:
        blockers.append("Hyperliquid Spot USDC balance is missing collateral token index 0")
    else:
        try:
            if int(str(token)) != 0:
                blockers.append("Hyperliquid Spot USDC balance must use collateral token index 0")
        except (TypeError, ValueError):
            blockers.append("Hyperliquid Spot USDC token index is invalid")
    try:
        total = parse_decimal(balance.get("total"))
        hold = parse_decimal(balance.get("hold"))
    except (ArithmeticError, TypeError, ValueError) as exc:
        blockers.append(f"Hyperliquid Spot USDC collateral is not parseable: {exc}")
        return None, None, blockers
    if total < 0:
        blockers.append("Hyperliquid Spot USDC total cannot be negative")
    if hold < 0:
        blockers.append("Hyperliquid Spot USDC hold cannot be negative")
    if hold > total:
        blockers.append("Hyperliquid Spot USDC hold cannot exceed total")
    return total, hold, blockers


def _account_context(
    *,
    expected: AccountMode,
    detected: AccountMode | None,
    state: Any,
    spot_state: Any,
    aggregate: UnifiedAccountSnapshot | None,
) -> dict[str, Any]:
    account_value: Decimal | None = None
    spot_total: Decimal | None = None
    spot_hold: Decimal | None = None
    collateral_source = "unknown"
    if detected == AccountMode.UNIFIED:
        spot_total, spot_hold, _ = _spot_usdc_collateral(spot_state)
        account_value = spot_total
        collateral_source = "spot_usdc_unified"
    elif detected == AccountMode.STANDARD and isinstance(state, dict):
        summary = _margin_summary(state)
        if summary is not None:
            try:
                account_value = parse_decimal(summary.get("accountValue"))
            except (ArithmeticError, TypeError, ValueError):
                account_value = None
        collateral_source = "perp_margin_summary"
    active_non_default: list[str] = []
    if aggregate is not None:
        try:
            active_non_default = non_default_dex_activity(aggregate)
        except UnifiedAccountStateError:
            active_non_default = ["<invalid>"]
    return {
        "expected_mode": expected.value,
        "detected_mode": detected.value if detected is not None else "unknown",
        "collateral_source": collateral_source,
        "account_value": account_value,
        "spot_usdc_total": spot_total,
        "spot_usdc_hold": spot_hold,
        "aggregate_observed_ms": aggregate.observed_ms if aggregate is not None else None,
        "aggregate_dex_count": aggregate.dex_count if aggregate is not None else 0,
        "active_non_default_dexes": active_non_default,
    }


def _margin_summary(state: dict[str, Any]) -> dict[str, Any] | None:
    for key in ("marginSummary", "crossMarginSummary"):
        summary = state.get(key)
        if isinstance(summary, dict) and summary.get("accountValue") is not None:
            return summary
    return None


def _validate_user_rate_limit(rate_limit: Any) -> list[str]:
    if isinstance(rate_limit, dict):
        if _status_has_key_or_value(rate_limit, "error", "rate limit"):
            return [f"Hyperliquid userRateLimit diagnostic returned error: {rate_limit}"]
        return []
    if rate_limit is None:
        return ["Hyperliquid userRateLimit diagnostic returned empty response"]
    return ["Hyperliquid userRateLimit diagnostic returned non-object response"]


def _validate_account_role(role_payload: Any, vault_address: str) -> list[str]:
    role = _account_role(role_payload)
    if role is None:
        return [f"Hyperliquid userRole diagnostic returned unrecognized response: {role_payload}"]
    if role == "missing":
        return ["Hyperliquid userRole is missing; verify follower account address"]
    if role == "agent":
        return [
            "Hyperliquid userRole is agent/API wallet; configure the master/subaccount/vault "
            "trading account as HLCT_FOLLOWER_ACCOUNT_ADDRESS"
        ]
    if role in {"vault", "subaccount"}:
        if not vault_address:
            return [
                f"Hyperliquid userRole is {role}; set HLCT_VAULT_ADDRESS to the same address "
                "as HLCT_FOLLOWER_ACCOUNT_ADDRESS for signed actions"
            ]
        return []
    if role == "user":
        if vault_address:
            return [
                "HLCT_VAULT_ADDRESS is set but Hyperliquid userRole is user; remove the vault "
                "address or configure the actual vault/subaccount account"
            ]
        return []
    return [f"Hyperliquid userRole {role!r} is unsupported by this copytrader"]


def _account_role(value: Any) -> str | None:
    if isinstance(value, str):
        raw_role = value
    elif isinstance(value, dict) and value.get("role") is not None:
        raw_role = str(value["role"])
    else:
        return None
    return raw_role.strip().replace("_", "").replace("-", "").lower()


def _normalized_address(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    address = value.strip().lower()
    if len(address) != 42 or not address.startswith("0x"):
        return None
    try:
        int(address[2:], 16)
    except ValueError:
        return None
    return address


def _object_address(payload: Any, key: str) -> str | None:
    if not isinstance(payload, dict):
        return None
    return _normalized_address(payload.get(key))


def _role_address(payload: Any, key: str) -> str | None:
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
        return None
    return _normalized_address(payload["data"].get(key))


def _validate_extra_agent(
    payload: Any,
    signer: str,
    *,
    minimum_valid_until_ms: int,
) -> list[str]:
    entries = payload if isinstance(payload, list) else [payload]
    for entry in entries:
        if not isinstance(entry, dict) or _object_address(entry, "address") != signer:
            continue
        valid_until = entry.get("validUntil")
        try:
            valid_until_ms = int(str(valid_until))
        except (TypeError, ValueError):
            return ["approved API wallet has an invalid validUntil timestamp"]
        if valid_until_ms <= minimum_valid_until_ms:
            return [
                "approved API wallet registration is expired or expires before the configured "
                "signed-action safety window ends"
            ]
        return []
    return ["API signer is not listed in the trading-account owner's active extraAgents"]


def _resolve_user_abstraction(
    value: Any,
    *,
    expected: AccountMode,
) -> tuple[AccountMode | None, list[str]]:
    mode = classify_user_abstraction(value)
    if mode is None:
        return None, [f"Hyperliquid userAbstraction returned unrecognized response: {value}"]
    if mode == HyperliquidUserAbstraction.STANDARD:
        detected = AccountMode.STANDARD
    elif mode == HyperliquidUserAbstraction.UNIFIED:
        detected = AccountMode.UNIFIED
    elif mode in {
        HyperliquidUserAbstraction.PORTFOLIO_MARGIN,
        HyperliquidUserAbstraction.DEX_ABSTRACTION,
    }:
        return None, [
            "Hyperliquid account abstraction mode "
            f"{mode.value} is unsupported; only standard and unified accounts are allowed"
        ]
    else:
        return None, [
            f"Hyperliquid account abstraction mode {mode.value!r} is unsupported by this copytrader"
        ]
    if expected != AccountMode.AUTO and expected != detected:
        return detected, [
            f"Hyperliquid account mode mismatch: configured {expected.value}, detected "
            f"{detected.value}"
        ]
    return detected, []


def _validate_user_dex_abstraction(value: Any) -> list[str]:
    if value is None:
        return []
    mode = normalized_abstraction_mode(value)
    if mode is None:
        return [f"Hyperliquid userDexAbstraction returned unrecognized response: {value}"]
    if mode in {"false", "disabled", "default"}:
        return []
    if mode in {"true", "enabled", "dexabstraction"}:
        return [
            "Hyperliquid DEX abstraction is enabled; this copytrader requires it disabled for "
            "both standard and unified accounts"
        ]
    return [f"Hyperliquid DEX abstraction state {mode!r} is unsupported by this copytrader"]


def classify_leverage_response(response: Any) -> tuple[IntentStatus, str]:
    if _top_level_status_is_error(response):
        return IntentStatus.REJECTED, "rejected"
    text = str(response).lower()
    if "error" in text or "rejected" in text:
        return IntentStatus.REJECTED, "rejected"
    if isinstance(response, dict):
        status = str(response.get("status", "")).lower()
        body = response.get("response")
        if status == "ok" and isinstance(body, dict):
            response_type = str(body.get("type", "")).lower()
            data = body.get("data")
            data_status = str(data.get("status", "")).lower() if isinstance(data, dict) else ""
            if response_type == "updateleverage" and data_status in {"success", "ok"}:
                return IntentStatus.ACKED, "leverage_updated"
            if response_type == "default":
                return IntentStatus.ACKED, "leverage_updated"
    return IntentStatus.REJECTED, "ambiguous_leverage_response"


def _action_response_type(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    response = value.get("response")
    if isinstance(response, dict) and response.get("type") is not None:
        return str(response["type"])
    return None


def _action_statuses(value: Any) -> list[Any]:
    if not isinstance(value, dict):
        return []
    response = value.get("response")
    if not isinstance(response, dict):
        return []
    data = response.get("data")
    if isinstance(data, dict):
        statuses = data.get("statuses")
        if isinstance(statuses, list):
            return statuses
        status = data.get("status")
        if status is not None:
            return [status]
    return []


def _statuses_have_error(statuses: list[Any]) -> bool:
    return any(_status_has_key_or_value(status, "error", "rejected") for status in statuses)


def _statuses_have_success(statuses: list[Any]) -> bool:
    return any(_status_has_key_or_value(status, "success") for status in statuses)


def _statuses_have_key(statuses: list[Any], key: str) -> bool:
    lowered_key = key.lower()
    return any(
        isinstance(status, dict) and lowered_key in {str(k).lower() for k in status}
        for status in statuses
    )


def _status_has_key_or_value(value: Any, *needles: str) -> bool:
    lowered_needles = tuple(needle.lower() for needle in needles)
    if isinstance(value, dict):
        for key, item in value.items():
            if any(needle in str(key).lower() for needle in lowered_needles):
                return True
            if _status_has_key_or_value(item, *lowered_needles):
                return True
        return False
    if isinstance(value, list):
        return any(_status_has_key_or_value(item, *lowered_needles) for item in value)
    return any(needle in str(value).lower() for needle in lowered_needles)


def _top_level_status_is_error(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    status = value.get("status")
    if not isinstance(status, str):
        return False
    return status.strip().lower() in {"err", "error", "failed", "rejected"}


def _total_filled_size(value: Any) -> Decimal | None:
    sizes = _filled_sizes(value)
    if not sizes:
        return None
    return sum(sizes, Decimal("0"))


def _filled_sizes(value: Any) -> list[Decimal]:
    if isinstance(value, dict):
        sizes: list[Decimal] = []
        if "filled" in value:
            filled = value["filled"]
            if isinstance(filled, dict):
                for key in ("totalSz", "sz", "size"):
                    if filled.get(key) is not None:
                        return [parse_decimal(filled[key])]
            return []
        for item in value.values():
            sizes.extend(_filled_sizes(item))
        return sizes
    if isinstance(value, list):
        sizes = []
        for item in value:
            sizes.extend(_filled_sizes(item))
        return sizes
    return []


def classify_order_status(payload: Any) -> tuple[IntentStatus | None, str]:
    """Classify Hyperliquid orderStatus-like payloads without assuming one schema shape."""

    statuses = [value.lower() for value in _status_strings(payload)]
    if not statuses:
        return None, "unknown"
    if any("rejected" in status for status in statuses):
        return IntentStatus.REJECTED, _first_matching(statuses, "rejected")
    if any(_is_canceled_order_status(status) for status in statuses):
        return IntentStatus.CANCELED, _first_canceled_order_status(statuses)
    if any("filled" in status for status in statuses):
        return IntentStatus.FILLED, _first_matching(statuses, "filled")
    if any(status in {"open", "resting", "triggered", "active"} for status in statuses):
        return IntentStatus.ACKED, _first_matching(
            statuses, "open", "resting", "triggered", "active"
        )
    if any(status in {"unknown", "missing", "notfound", "not_found"} for status in statuses):
        return None, _first_matching(statuses, "unknown", "missing", "not")
    return None, statuses[0]


def _is_canceled_order_status(status: str) -> bool:
    return "canceled" in status or "cancelled" in status or status == "scheduledcancel"


def _first_canceled_order_status(statuses: list[str]) -> str:
    for status in statuses:
        if _is_canceled_order_status(status):
            return status
    return statuses[0] if statuses else ""


def _status_strings(value: Any) -> list[str]:
    return _collect_status_strings(value, direct_string=True)


def _collect_status_strings(value: Any, *, direct_string: bool) -> list[str]:
    if isinstance(value, dict):
        found: list[str] = []
        for key, item in value.items():
            is_status_field = str(key).lower() in {
                "status",
                "statuses",
                "orderstatus",
                "state",
            }
            if is_status_field and isinstance(item, str):
                found.append(item)
            elif isinstance(item, (dict, list)):
                found.extend(_collect_status_strings(item, direct_string=is_status_field))
        return found
    if isinstance(value, list):
        found = []
        for item in value:
            found.extend(_collect_status_strings(item, direct_string=direct_string))
        return found
    if direct_string and isinstance(value, str):
        return [value]
    return []


def _first_matching(statuses: list[str], *needles: str) -> str:
    for status in statuses:
        if any(needle in status for needle in needles):
            return status
    return statuses[0] if statuses else "unknown"


@dataclass
class FakeExecutionAdapter:
    account: str = "0xf000000000000000000000000000000000000000"
    preflight_blockers: list[str] = field(default_factory=list)
    positions: dict[str, Position] = field(default_factory=dict)
    open_orders: list[OpenOrder] = field(default_factory=list)
    reports: list[ExecutionReport] = field(default_factory=list)
    status_by_cloid: dict[str, dict[str, Any]] = field(default_factory=dict)
    leverage_updates: list[tuple[str, int, bool]] = field(default_factory=list)
    configured_leverage: dict[str, int] = field(default_factory=dict)
    forced_status: IntentStatus | None = None
    leverage_status: IntentStatus | None = None
    auth_probe_status: IntentStatus | None = None
    auth_probe_reports: list[ExecutionReport] = field(default_factory=list)
    schedule_cancel_status: IntentStatus | None = None
    schedule_cancel_reports: list[ExecutionReport] = field(default_factory=list)
    scheduled_cancel_times: list[int | None] = field(default_factory=list)
    delay_s: float = 0.0
    leverage_delay_s: float = 0.0
    auth_probe_delay_s: float = 0.0
    account_value: Decimal = Decimal("1000")
    cumulative_volume: Decimal = Decimal("1000000")

    def account_preflight(self) -> list[str]:
        return list(self.preflight_blockers)

    def dead_man_eligibility(self) -> dict[str, Any]:
        required_volume = Decimal("1000000")
        return {
            "eligible": self.cumulative_volume >= required_volume,
            "cumulative_volume_usd": self.cumulative_volume,
            "required_volume_usd": required_volume,
            "read_only_query": True,
            "signed_action_performed": False,
        }

    def auth_probe(self, *, intent_id: str, cloid: str) -> ExecutionReport:
        if self.auth_probe_delay_s:
            sleep(self.auth_probe_delay_s)
        status = self.auth_probe_status or IntentStatus.ACKED
        exchange_status = "auth_probe_ok" if status == IntentStatus.ACKED else status.value
        payload: dict[str, Any] = {"account": self.account}
        if status == IntentStatus.REJECTED:
            payload["error"] = "auth probe rejected"
        report = ExecutionReport(
            report_id=deterministic_cloid("fake-auth-probe", intent_id, cloid, len(self.reports)),
            intent_id=intent_id,
            cloid=cloid,
            status=status,
            exchange_status=exchange_status,
            exchange_ts_ms=now_ms(),
            payload=payload,
        )
        self.auth_probe_reports.append(report)
        return report

    def place_intent(self, intent: FollowerIntent) -> ExecutionReport:
        if self.delay_s:
            sleep(self.delay_s)
        status = self.forced_status
        if status is None:
            status = (
                IntentStatus.ACKED
                if intent.status != IntentStatus.SKIPPED
                else IntentStatus.SKIPPED
            )
        report = ExecutionReport(
            report_id=deterministic_cloid("fake-report", intent.intent_id, len(self.reports)),
            intent_id=intent.intent_id,
            cloid=intent.cloid,
            status=status,
            exchange_status=status.value,
            exchange_ts_ms=now_ms(),
            payload={"intent": intent},
        )
        self.reports.append(report)
        if intent.status != IntentStatus.SKIPPED and status != IntentStatus.REJECTED:
            self.status_by_cloid[intent.cloid] = {
                "status": "filled" if status == IntentStatus.FILLED else "open",
                "order": {"cloid": intent.cloid},
            }
        if status == IntentStatus.FILLED:
            current = self.positions.get(
                intent.coin,
                Position(
                    coin=intent.coin,
                    size=Decimal("0"),
                    leverage=self.configured_leverage.get(intent.coin),
                ),
            )
            signed_delta = intent.size if intent.side == "buy" else -intent.size
            next_size = current.size + signed_delta
            if intent.reduce_only and current.size != 0 and next_size * current.size < 0:
                next_size = Decimal("0")
            if next_size == 0:
                self.positions.pop(intent.coin, None)
            else:
                self.positions[intent.coin] = Position(
                    coin=intent.coin,
                    size=next_size,
                    entry_px=intent.price or current.entry_px,
                    leverage=self.configured_leverage.get(intent.coin, current.leverage),
                    updated_ms=now_ms(),
                )
        return report

    def schedule_cancel(
        self, *, scheduled_time_ms: int | None, intent_id: str, cloid: str
    ) -> ExecutionReport:
        status = self.schedule_cancel_status or IntentStatus.ACKED
        exchange_status = (
            "dead_man_cleared"
            if status == IntentStatus.ACKED and scheduled_time_ms is None
            else "dead_man_scheduled"
            if status == IntentStatus.ACKED
            else status.value
        )
        payload: dict[str, Any] = {"scheduled_time_ms": scheduled_time_ms}
        if status == IntentStatus.REJECTED:
            payload["error"] = "dead-man schedule rejected"
        report = ExecutionReport(
            report_id=deterministic_cloid(
                "fake-dead-man", intent_id, cloid, len(self.schedule_cancel_reports)
            ),
            intent_id=intent_id,
            cloid=cloid,
            status=status,
            exchange_status=exchange_status,
            exchange_ts_ms=now_ms(),
            payload=payload,
        )
        self.scheduled_cancel_times.append(scheduled_time_ms)
        self.schedule_cancel_reports.append(report)
        return report

    def place_limit_order(
        self,
        *,
        coin: str,
        side: str,
        size: Decimal,
        price: Decimal,
        cloid: str,
        reduce_only: bool = False,
        tif: str = "Gtc",
    ) -> ExecutionReport:
        if self.delay_s:
            sleep(self.delay_s)
        status = self.forced_status or IntentStatus.ACKED
        report = ExecutionReport(
            report_id=deterministic_cloid("fake-limit", cloid, len(self.reports)),
            intent_id="limit:" + cloid,
            cloid=cloid,
            status=status,
            exchange_status=status.value,
            exchange_ts_ms=now_ms(),
            payload={"coin": coin, "side": side, "size": size, "price": price, "tif": tif},
        )
        if status != IntentStatus.REJECTED:
            self.open_orders.append(
                OpenOrder(
                    coin=coin,
                    side=side,
                    size=size,
                    price=price,
                    cloid=cloid,
                    reduce_only=reduce_only,
                    updated_ms=now_ms(),
                )
            )
            self.status_by_cloid[cloid] = {"status": "open"}
        self.reports.append(report)
        return report

    def cancel_by_cloid(self, coin: str, cloid: str) -> ExecutionReport:
        self.status_by_cloid[cloid] = {"status": "canceled"}
        self.open_orders = [order for order in self.open_orders if order.cloid != cloid]
        report = ExecutionReport(
            report_id=deterministic_cloid("fake-cancel", cloid, len(self.reports)),
            intent_id="cancel:" + cloid,
            cloid=cloid,
            status=IntentStatus.CANCELED,
            exchange_status="canceled",
            exchange_ts_ms=now_ms(),
            payload={"coin": coin, "cloid": cloid},
        )
        self.reports.append(report)
        return report

    def order_status(self, cloid_or_oid: str | int) -> dict[str, Any]:
        return self.status_by_cloid.get(str(cloid_or_oid), {"status": "unknown"})

    def update_leverage(
        self,
        coin: str,
        leverage: int,
        is_cross: bool = True,
        *,
        risk_increasing: bool = True,
    ) -> ExecutionReport:
        if self.leverage_delay_s:
            sleep(self.leverage_delay_s)
        status = self.leverage_status or IntentStatus.ACKED
        cloid = deterministic_cloid("fake-leverage", self.account, coin, leverage, is_cross)
        exchange_status = "leverage_updated" if status == IntentStatus.ACKED else status.value
        payload: dict[str, Any] = {"coin": coin, "leverage": leverage, "is_cross": is_cross}
        if status == IntentStatus.REJECTED:
            payload["error"] = "leverage update rejected"
        report = ExecutionReport(
            report_id=deterministic_cloid("fake-leverage-report", cloid, len(self.reports)),
            intent_id=f"leverage:{coin}:{leverage}",
            cloid=cloid,
            status=status,
            exchange_status=exchange_status,
            exchange_ts_ms=now_ms(),
            payload=payload,
        )
        self.reports.append(report)
        if status == IntentStatus.ACKED:
            self.leverage_updates.append((coin, leverage, is_cross))
            self.configured_leverage[coin] = leverage
            if coin in self.positions:
                self.positions[coin] = replace(
                    self.positions[coin],
                    leverage=leverage,
                    updated_ms=now_ms(),
                )
        return report

    def reconcile(self) -> ReconcileSnapshot:
        observed = now_ms()
        return ReconcileSnapshot(
            snapshot_id=deterministic_cloid(
                "fake-reconcile", observed, self.positions, self.open_orders
            ),
            account=self.account,
            positions=dict(self.positions),
            open_orders=list(self.open_orders),
            observed_ms=observed,
            source="fake",
            payload={
                "account_mode": "standard",
                "account_value": self.account_value,
                "account_context": {
                    "expected_mode": "auto",
                    "detected_mode": "standard",
                    "collateral_source": "perp_margin_summary",
                    "account_value": self.account_value,
                    "spot_usdc_total": None,
                    "spot_usdc_hold": None,
                    "aggregate_observed_ms": None,
                    "aggregate_dex_count": 0,
                    "active_non_default_dexes": [],
                },
                "clearinghouseState": {
                    "marginSummary": {"accountValue": self.account_value},
                    "crossMarginSummary": {"accountValue": self.account_value},
                },
            },
        )
