from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from time import monotonic_ns, time_ns
from typing import Any, Mapping

from .action_journal import ActionJournal, ActionRecord, ActionState
from .cloid import deterministic_cloid
from .desired_engine import NextAction
from .reconciliation import classify_order_status
from .ws_actions import PostOutcome, PostResult, WsPostMux, build_ioc_action, sign_ioc_action


@dataclass(frozen=True, slots=True)
class ExecutionAttempt:
    record: ActionRecord
    result: PostResult
    received_to_send_ms: Decimal | None
    send_to_response_ms: Decimal | None
    execution_context: Mapping[str, Any] = field(default_factory=dict)


class ContinuousSignerLane:
    """One serialized signer/nonce lane with no REST or orchestration policy."""

    def __init__(
        self,
        *,
        follower_account: str,
        api_wallet_address: str,
        key_file: Path,
        vault_address: str | None,
        is_mainnet: bool,
        journal: ActionJournal,
        expires_window_ms: int = 5_000,
    ) -> None:
        if expires_window_ms <= 0:
            raise ValueError("expires window must be positive")
        from eth_account import Account

        private_key = key_file.read_text(encoding="utf-8").strip()
        try:
            wallet = Account.from_key(private_key)
        finally:
            private_key = ""
        derived = str(wallet.address).lower()
        if derived != api_wallet_address.lower():
            raise ValueError("API-wallet key does not match the bound credential profile")
        self._initialize(
            follower_account=follower_account,
            api_wallet_address=derived,
            vault_address=vault_address,
            is_mainnet=is_mainnet,
            journal=journal,
            expires_window_ms=expires_window_ms,
        )
        self._wallet = wallet

    @classmethod
    def monitor_only(
        cls,
        *,
        follower_account: str,
        api_wallet_address: str,
        vault_address: str | None,
        is_mainnet: bool,
        journal: ActionJournal,
    ) -> ContinuousSignerLane:
        """Build a read-only lane without opening or parsing private-key files."""

        lane = cls.__new__(cls)
        lane._initialize(
            follower_account=follower_account,
            api_wallet_address=api_wallet_address,
            vault_address=vault_address,
            is_mainnet=is_mainnet,
            journal=journal,
            expires_window_ms=5_000,
        )
        lane._wallet = None
        return lane

    def _initialize(
        self,
        *,
        follower_account: str,
        api_wallet_address: str,
        vault_address: str | None,
        is_mainnet: bool,
        journal: ActionJournal,
        expires_window_ms: int,
    ) -> None:
        self.follower_account = follower_account.lower()
        self.api_wallet_address = api_wallet_address.lower()
        self.vault_address = None if vault_address is None else vault_address.lower()
        self.is_mainnet = is_mainnet
        self.journal = journal
        self.expires_window_ms = expires_window_ms
        self._lock = asyncio.Lock()

    @property
    def signing_enabled(self) -> bool:
        return self._wallet is not None

    def recover_provably_unsent(self) -> tuple[ActionRecord, ...]:
        recovered: list[ActionRecord] = []
        for record in self.journal.recovery_actions(
            follower_account=self.follower_account,
            api_wallet=self.api_wallet_address,
        ):
            if record.state is ActionState.PREPARED:
                recovered.append(self.journal.mark_not_sent(record.cloid))
        return tuple(recovered)

    def unresolved_signed_remaining(self) -> dict[str, Decimal]:
        result: dict[str, Decimal] = {}
        for record in self.journal.recovery_actions(
            follower_account=self.follower_account,
            api_wallet=self.api_wallet_address,
        ):
            if record.state is ActionState.PREPARED:
                continue
            action = json.loads(record.action_json)
            order = action["orders"][0]
            signed_remaining = record.remaining_size if bool(order["b"]) else -record.remaining_size
            result[record.market] = result.get(record.market, Decimal("0")) + signed_remaining
        return result

    async def execute_ioc(
        self,
        *,
        action: NextAction,
        asset_id: int,
        limit_px: Decimal,
        mux: WsPostMux,
        required_epoch: int,
        received_mono_ns: int | None = None,
        on_prepared: Callable[[ActionRecord], None] | None = None,
    ) -> ExecutionAttempt:
        if self._wallet is None:
            raise RuntimeError("monitor-only signer lane cannot execute an action")
        async with self._lock:
            unresolved = self.journal.recovery_actions(
                follower_account=self.follower_account,
                api_wallet=self.api_wallet_address,
            )
            if unresolved:
                states = ", ".join(f"{row.cloid}:{row.state.value}" for row in unresolved)
                raise RuntimeError(f"signer lane has unresolved durable actions: {states}")
            attempt_no = self.journal.next_attempt_no(
                follower_account=self.follower_account,
                api_wallet=self.api_wallet_address,
                desired_id=action.desired_id,
                market=action.market,
            )
            cloid = deterministic_cloid(
                "continuous-v1",
                self.follower_account,
                action.market,
                action.desired_id,
                attempt_no,
                action.side,
                action.size,
                limit_px,
                action.reduce_only,
            )
            ioc = build_ioc_action(
                asset_id=asset_id,
                is_buy=action.side == "buy",
                size=action.size,
                limit_px=limit_px,
                reduce_only=action.reduce_only,
                cloid=cloid,
            )
            now_ms = time_ns() // 1_000_000
            nonce = self.journal.reserve_nonce(
                follower_account=self.follower_account,
                api_wallet=self.api_wallet_address,
                wall_ms=now_ms,
            )
            signed = sign_ioc_action(
                ioc,
                wallet=self._wallet,
                nonce=nonce,
                expires_after_ms=now_ms + self.expires_window_ms,
                is_mainnet=self.is_mainnet,
                vault_address=self.vault_address,
            )
            prepared = self.journal.prepare_action(
                follower_account=self.follower_account,
                api_wallet=self.api_wallet_address,
                desired_id=action.desired_id,
                market=action.market,
                attempt_no=attempt_no,
                cloid=cloid,
                nonce=nonce,
                requested_size=action.size,
                action_json=_exact_json(ioc.action),
                signed_payload_json=_exact_json(signed.payload),
                expires_after_ms=signed.expires_after_ms,
                request_id=f"pending:{cloid}",
                created_ms=now_ms,
            )
            if on_prepared is not None:
                on_prepared(prepared)
            sent_mono_ns: int | None = None

            async def before_send(request_id: int) -> None:
                nonlocal sent_mono_ns
                self.journal.mark_send_attempted(cloid, request_id=request_id)
                sent_mono_ns = monotonic_ns()

            result = await mux.post_action(
                signed,
                required_epoch=required_epoch,
                before_send=before_send,
            )
            response_mono_ns = monotonic_ns()
            record = self._record_result(cloid, result)
            baseline = received_mono_ns
            return ExecutionAttempt(
                record=record,
                result=result,
                received_to_send_ms=(
                    None
                    if baseline is None or sent_mono_ns is None
                    else _duration_ms(sent_mono_ns - baseline)
                ),
                send_to_response_ms=(
                    None if sent_mono_ns is None else _duration_ms(response_mono_ns - sent_mono_ns)
                ),
            )

    async def resolve_by_cloid(
        self,
        cloid: str,
        *,
        mux: WsPostMux,
        required_epoch: int,
    ) -> ActionRecord:
        async with self._lock:
            record = self.journal.get_owned_action(
                cloid,
                follower_account=self.follower_account,
                api_wallet=self.api_wallet_address,
            )
            if record is None:
                raise ValueError("cannot resolve a CLOID not owned by this signer lane")
            if record.terminal:
                return record
            result = await mux.post_info(
                {"type": "orderStatus", "user": self.follower_account, "oid": record.cloid},
                required_epoch=required_epoch,
            )
            if result.outcome is not PostOutcome.INFO:
                return self.journal.mark_unknown(record.cloid, detail=result.reason)
            payload = result.response
            if isinstance(payload, Mapping) and payload.get("type") == "info":
                payload = payload.get("payload")
            resolution = classify_order_status(payload, expected_size=record.requested_size)
            if not resolution.resolved:
                return self.journal.mark_unknown(record.cloid, detail=resolution.state)
            state: ActionState | None
            if resolution.state in {"cancelled", "expired"}:
                state = (
                    ActionState.PARTIALLY_FILLED
                    if resolution.cumulative_filled_abs > 0
                    else ActionState.CANCELED
                )
            else:
                state = {
                    "filled": ActionState.FILLED,
                    "partially_filled": ActionState.PARTIALLY_FILLED,
                    "rejected": ActionState.REJECTED,
                    "resting": ActionState.RESTING,
                }.get(resolution.state)
            if state is None:
                return self.journal.mark_unknown(record.cloid, detail=resolution.state)
            return self.journal.record_outcome(
                record.cloid,
                state=state,
                cumulative_filled_size=resolution.cumulative_filled_abs,
                detail=f"ws_order_status:{resolution.state}",
            )

    def _record_result(self, cloid: str, result: PostResult) -> ActionRecord:
        current = self.journal.get_action(cloid)
        if current is None:
            raise RuntimeError("prepared action disappeared from its journal")
        if result.outcome is PostOutcome.NOT_SENT:
            if current.state is not ActionState.PREPARED:
                return self.journal.mark_unknown(cloid, detail="transport_not_sent_after_boundary")
            return self.journal.mark_not_sent(cloid)
        mapping = {
            PostOutcome.UNKNOWN: ActionState.UNKNOWN,
            PostOutcome.REJECTED: ActionState.REJECTED,
            PostOutcome.FILLED: ActionState.FILLED,
            PostOutcome.PARTIALLY_FILLED: ActionState.PARTIALLY_FILLED,
            PostOutcome.CANCELLED: ActionState.CANCELED,
        }
        state = mapping.get(result.outcome)
        if state is None:
            return self.journal.mark_unknown(cloid, detail=f"unexpected:{result.outcome.value}")
        return self.journal.record_outcome(
            cloid,
            state=state,
            cumulative_filled_size=result.filled_size,
            detail=result.reason,
        )


def _exact_json(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value), separators=(",", ":"), sort_keys=True)


def _duration_ms(duration_ns: int) -> Decimal:
    return Decimal(max(0, duration_ns)) / Decimal("1000000")
