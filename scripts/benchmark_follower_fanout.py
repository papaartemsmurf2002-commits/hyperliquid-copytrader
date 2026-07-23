from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
import time
from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from eth_account import Account

from hyperliquid_copytrader.action_journal import ActionJournal
from hyperliquid_copytrader.continuous_executor import ContinuousSignerLane, ExecutionAttempt
from hyperliquid_copytrader.desired_engine import NextAction
from hyperliquid_copytrader.ws_actions import PostOutcome, PostResult, WsPostMux


def _percentile(values: Sequence[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _summary(values: Sequence[float]) -> dict[str, float | int | None]:
    return {
        "samples": len(values),
        "median_ms": _round(_percentile(values, 0.50)),
        "p95_ms": _round(_percentile(values, 0.95)) if len(values) >= 20 else None,
        "p99_ms": _round(_percentile(values, 0.99)) if len(values) >= 100 else None,
        "maximum_ms": round(max(values), 3) if values else None,
    }


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, 3)


class _BenchmarkMux:
    """A non-network WS boundary: writes serialize, acknowledgements do not."""

    def __init__(self, *, slow_lane: str, slow_ack_ms: float, reject_lane: str) -> None:
        self.slow_lane = slow_lane
        self.slow_ack_s = slow_ack_ms / 1_000
        self.reject_lane = reject_lane
        self._write_lock = asyncio.Lock()
        self._request_id = 0

    async def post_action(
        self,
        signed: Any,
        *,
        before_send: Any,
        **_kwargs: Any,
    ) -> PostResult:
        task = asyncio.current_task()
        lane = task.get_name() if task is not None else "unknown"
        async with self._write_lock:
            self._request_id += 1
            request_id = self._request_id
            await before_send(request_id)
            await asyncio.sleep(0)
        await asyncio.sleep(self.slow_ack_s if lane == self.slow_lane else 0)
        if lane == self.reject_lane:
            return PostResult(
                request_id,
                PostOutcome.REJECTED,
                {},
                "benchmark_explicit_reject",
                Decimal("0"),
            )
        return PostResult(
            request_id,
            PostOutcome.FILLED,
            {},
            "benchmark_fill",
            signed.ioc.expected_size,
        )


def _lane(root: Path, index: int) -> tuple[ContinuousSignerLane, ActionJournal]:
    wallet = Account.create(f"hlct-fanout-{index}")
    key_path = root / f"slot{index}.key"
    key_path.write_text(wallet.key.hex(), encoding="utf-8")
    journal = ActionJournal(root / f"slot{index}.sqlite3")
    follower = f"0x{index + 10_000:040x}"
    lane = ContinuousSignerLane(
        follower_account=follower,
        api_wallet_address=str(wallet.address).lower(),
        key_file=key_path,
        vault_address=follower,
        is_mainnet=False,
        journal=journal,
    )
    return lane, journal


async def _run(*, rounds: int, followers: int, slow_ack_ms: float) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="hlct-fanout-") as raw_root:
        root = Path(raw_root)
        built = [_lane(root, index) for index in range(1, followers + 1)]
        lanes = [item[0] for item in built]
        journals = [item[1] for item in built]
        mux = _BenchmarkMux(
            slow_lane=f"slot{followers}",
            slow_ack_ms=slow_ack_ms,
            reject_lane=f"slot{max(1, followers - 1)}",
        )
        received_to_send: list[float] = []
        send_to_response: list[float] = []
        ordinary_send_to_response: list[float] = []
        outcomes: list[PostOutcome] = []
        started = time.perf_counter_ns()
        try:
            for round_index in range(rounds):
                received_ns = time.monotonic_ns()
                action = NextAction(
                    desired_id=f"leader-event-{round_index}",
                    market="BTC",
                    side="buy" if round_index % 2 == 0 else "sell",
                    size=Decimal("0.001"),
                    reduce_only=False,
                    reason="synthetic common leader event",
                )

                async def execute(index: int) -> ExecutionAttempt:
                    return await lanes[index].execute_ioc(
                        action=action,
                        asset_id=0,
                        limit_px=Decimal("100000"),
                        mux=cast(WsPostMux, mux),
                        required_epoch=1,
                        received_mono_ns=received_ns,
                    )

                attempts = await asyncio.gather(
                    *(
                        asyncio.create_task(execute(index), name=f"slot{index + 1}")
                        for index in range(followers)
                    )
                )
                for index, attempt in enumerate(attempts, start=1):
                    outcomes.append(attempt.result.outcome)
                    if attempt.received_to_send_ms is not None:
                        received_to_send.append(float(attempt.received_to_send_ms))
                    if attempt.send_to_response_ms is not None:
                        value = float(attempt.send_to_response_ms)
                        send_to_response.append(value)
                        if index != followers:
                            ordinary_send_to_response.append(value)
        finally:
            for journal in journals:
                journal.close()
        duration_s = (time.perf_counter_ns() - started) / 1_000_000_000
    total = rounds * followers
    return {
        "version": 1,
        "workload": {
            "common_leader_events": rounds,
            "independent_follower_lanes": followers,
            "total_actions": total,
            "slow_lane_ack_ms": slow_ack_ms,
            "one_explicit_reject_per_event": True,
            "network_or_exchange_included": False,
        },
        "method": (
            "Real per-follower SQLite journals, nonce allocation, CLOID creation, signing and "
            "serialized socket-write boundary; synthetic acknowledgements and no exchange calls."
        ),
        "leader_event_to_socket_write": _summary(received_to_send),
        "socket_write_to_synthetic_ack": _summary(send_to_response),
        "non_slow_follower_ack": _summary(ordinary_send_to_response),
        "throughput_actions_per_second": round(total / duration_s, 2),
        "duration_s": round(duration_s, 3),
        "filled": sum(item is PostOutcome.FILLED for item in outcomes),
        "explicit_rejections": sum(item is PostOutcome.REJECTED for item in outcomes),
        "unknown_or_nonterminal": sum(
            item not in {PostOutcome.FILLED, PostOutcome.REJECTED} for item in outcomes
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark one synthetic leader event fanning out to independent signer lanes."
    )
    parser.add_argument("--rounds", type=int, default=30)
    parser.add_argument("--followers", type=int, default=10)
    parser.add_argument("--slow-ack-ms", type=float, default=50.0)
    args = parser.parse_args()
    if not 1 <= args.rounds <= 1_000:
        parser.error("--rounds must be between 1 and 1000")
    if not 2 <= args.followers <= 10:
        parser.error("--followers must be between 2 and 10")
    if not 0 <= args.slow_ack_ms <= 10_000:
        parser.error("--slow-ack-ms must be between 0 and 10000")
    print(
        json.dumps(
            asyncio.run(
                _run(
                    rounds=args.rounds,
                    followers=args.followers,
                    slow_ack_ms=args.slow_ack_ms,
                )
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
