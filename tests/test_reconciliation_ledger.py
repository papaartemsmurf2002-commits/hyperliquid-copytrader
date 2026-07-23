from __future__ import annotations

from decimal import Decimal
from time import monotonic, time_ns
from typing import Any

import pytest

from hyperliquid_copytrader.account_state import AccountStateBook
from hyperliquid_copytrader.reconciliation import (
    BudgetedInfoClient,
    Reconciler,
    RestRetrySession,
    classify_order_status,
)
from hyperliquid_copytrader.rest_budget import RestGrant, RestPriority


FOLLOWER = "0x" + "2" * 40


@pytest.mark.parametrize(
    ("exchange_status", "expected_state"),
    [
        ("marginCanceled", "cancelled"),
        ("vaultWithdrawalCanceled", "cancelled"),
        ("openInterestCapCanceled", "cancelled"),
        ("selfTradeCanceled", "cancelled"),
        ("reduceOnlyCanceled", "cancelled"),
        ("siblingFilledCanceled", "cancelled"),
        ("delistedCanceled", "cancelled"),
        ("liquidatedCanceled", "cancelled"),
        ("scheduledCancel", "cancelled"),
        ("tickRejected", "rejected"),
        ("minTradeNtlRejected", "rejected"),
        ("perpMarginRejected", "rejected"),
        ("reduceOnlyRejected", "rejected"),
        ("badAloPxRejected", "rejected"),
        ("iocCancelRejected", "rejected"),
        ("badTriggerPxRejected", "rejected"),
        ("marketOrderNoLiquidityRejected", "rejected"),
        ("positionIncreaseAtOpenInterestCapRejected", "rejected"),
        ("positionFlipAtOpenInterestCapRejected", "rejected"),
        ("tooAggressiveAtOpenInterestCapRejected", "rejected"),
        ("openInterestIncreaseRejected", "rejected"),
        ("insufficientSpotBalanceRejected", "rejected"),
        ("oracleRejected", "rejected"),
        ("perpMaxPositionRejected", "rejected"),
    ],
)
def test_fast_order_status_classifier_covers_documented_terminal_variants(
    exchange_status: str,
    expected_state: str,
) -> None:
    payload = {
        "status": "order",
        "order": {
            "order": {"oid": 17, "origSz": "2.5", "sz": "1.0"},
            "status": exchange_status,
        },
    }

    result = classify_order_status(payload, expected_size=Decimal("2.5"))

    assert result.resolved is True
    assert result.terminal is True
    assert result.state == expected_state
    assert result.cumulative_filled_abs == Decimal("1.5")
    assert result.oid == 17


def test_order_status_overfill_is_a_protocol_contradiction() -> None:
    result = classify_order_status(
        {
            "status": "order",
            "order": {
                "order": {"oid": 17, "origSz": "2.5", "sz": "0"},
                "status": "filled",
                "filledSz": "3",
            },
        },
        expected_size=Decimal("2.5"),
    )

    assert result.resolved is False
    assert result.terminal is False
    assert result.state == "reported_fill_exceeds_submitted_size"
    assert result.cumulative_filled_abs == Decimal("3")


class _InfoClient:
    def __init__(self, ledger: list[dict[str, Any]], observed_ms: int) -> None:
        self.ledger = ledger
        self.observed_ms = observed_ms
        self.calls: list[dict[str, Any]] = []

    def info(
        self,
        payload: dict[str, Any],
        *,
        priority: Any = None,
        retry_session: Any = None,
    ) -> Any:
        del priority, retry_session
        self.calls.append(dict(payload))
        request_type = payload["type"]
        state = {
            "assetPositions": [],
            "marginSummary": {
                "accountValue": "1000",
                "totalMarginUsed": "0",
            },
            "time": self.observed_ms,
        }
        if request_type == "openOrders":
            return []
        if request_type == "userNonFundingLedgerUpdates":
            return list(self.ledger)
        if request_type == "spotClearinghouseState":
            return {
                "balances": [{"coin": "USDC", "token": 0, "total": "1000", "hold": "0"}],
                "time": self.observed_ms,
            }
        if request_type == "clearinghouseState":
            return state
        raise AssertionError(f"unexpected info request {request_type}")


@pytest.mark.asyncio
async def test_two_account_ten_dex_two_observations_preserve_phase_under_denials() -> None:
    class Grants:
        def __init__(self) -> None:
            self.attempts = 0
            self.successes = 0
            self.denied_for_success: set[int] = set()

        def request_grant(self, **payload: object) -> RestGrant:
            self.attempts += 1
            priority = RestPriority(int(str(payload["priority"])))
            endpoint = str(payload["endpoint"])
            logical_request = self.successes + 1
            if logical_request % 7 == 0 and logical_request not in self.denied_for_success:
                self.denied_for_success.add(logical_request)
                return RestGrant(
                    grant_id=f"denied-{self.attempts}",
                    granted=False,
                    generation="generation-terminal-phase",
                    coordinator_epoch=1,
                    sender="fleet-runtime",
                    sender_epoch=1,
                    message_id=self.attempts,
                    priority=priority,
                    endpoint=endpoint,
                    weight=2,
                    pool="reserve",
                    granted_wall_ms=time_ns() // 1_000_000,
                    retry_after_ms=1,
                    reason="rolling_weight_budget_exhausted",
                )
            self.successes += 1
            return RestGrant(
                grant_id=f"granted-{self.attempts}",
                granted=True,
                generation="generation-terminal-phase",
                coordinator_epoch=1,
                sender="fleet-runtime",
                sender_epoch=1,
                message_id=self.attempts,
                priority=priority,
                endpoint=endpoint,
                weight=2,
                pool="reserve",
                granted_wall_ms=time_ns() // 1_000_000,
                retry_after_ms=0,
                reason="granted",
            )

    grants = Grants()
    raw_calls: list[dict[str, Any]] = []
    client = BudgetedInfoClient(
        base_url="https://api.hyperliquid.xyz",
        timeout_s=1,
        grants=grants,  # type: ignore[arg-type]
        budget_waiter=lambda _wait: None,
    )

    def raw_info(payload: dict[str, Any]) -> Any:
        raw_calls.append(dict(payload))
        if payload["type"] == "clearinghouseState":
            return {
                "assetPositions": [],
                "marginSummary": {"accountValue": "1000", "totalMarginUsed": "0"},
                "time": time_ns() // 1_000_000,
            }
        if payload["type"] == "openOrders":
            return []
        if payload["type"] == "spotClearinghouseState":
            return {
                "balances": [{"coin": "USDC", "token": 0, "total": "1000", "hold": "0"}],
                "time": time_ns() // 1_000_000,
            }
        raise AssertionError(f"unexpected terminal request {payload}")

    client.raw.info = raw_info
    dexes = ("", *(f"dex-{index}" for index in range(1, 10)))
    reconciler = Reconciler(
        client=client,
        state_book=AccountStateBook(),
        freshness_ms=60_000,
        all_dexes=dexes,
    )
    followers = ("0x" + "1" * 40, "0x" + "2" * 40)
    sessions: list[RestRetrySession] = []
    for observation in range(2):
        session = RestRetrySession(wait_deadline_mono=monotonic() + 30)
        sessions.append(session)
        for follower in followers:
            result = await reconciler.refresh_follower(
                follower,
                trigger=f"terminal-{observation}",
                full_audit=True,
                catalog_revision="catalog-1",
                priority_override=RestPriority.AMBIGUITY_CONTAINMENT,
                retry_session=session,
                publish=False,
            )
            assert result.revision.confidence == "full_audit"

    assert len(raw_calls) == 84
    assert sum(call["type"] == "clearinghouseState" for call in raw_calls) == 40
    assert sum(call["type"] == "openOrders" for call in raw_calls) == 40
    assert sum(call["type"] == "spotClearinghouseState" for call in raw_calls) == 4
    assert grants.successes == 84
    assert grants.attempts == 96
    assert len(grants.denied_for_success) == 12
    assert all(session.first_response_wall_ms > 0 for session in sessions)


def _ledger_row(observed_ms: int, nonce: int) -> dict[str, Any]:
    return {
        "time": observed_ms + nonce,
        "hash": f"0x{nonce:064x}",
        "delta": {"type": "deposit", "usdc": str(nonce)},
    }


def _reconciler(client: _InfoClient, state_book: AccountStateBook) -> Reconciler:
    return Reconciler(
        client=client,  # type: ignore[arg-type]
        state_book=state_book,
        freshness_ms=5_000,
        all_dexes=("",),
        monitor_nonfunding_ledger=True,
    )


@pytest.mark.asyncio
async def test_ledger_baseline_is_durable_and_uncommitted_detection_repeats() -> None:
    observed_ms = time_ns() // 1_000_000
    first = _ledger_row(observed_ms, 1)
    client = _InfoClient([first], observed_ms)
    state_book = AccountStateBook()
    reconciler = _reconciler(client, state_book)

    baseline = await reconciler.refresh_follower(
        FOLLOWER,
        trigger="startup",
        full_audit=True,
        catalog_revision="catalog-1",
        publish=False,
    )
    assert baseline.external_activity == ()
    assert baseline.revision.nonfunding_ledger_checkpoint["seen_identities"]
    state_book.publish_follower(baseline.revision)

    second = _ledger_row(observed_ms, 2)
    client.ledger.append(second)
    detected = await reconciler.refresh_follower(
        FOLLOWER,
        trigger="periodic_full",
        full_audit=True,
        catalog_revision="catalog-1",
        publish=False,
    )
    assert detected.external_activity == (second,)

    retry = await reconciler.refresh_follower(
        FOLLOWER,
        trigger="retry_after_failed_actor_commit",
        full_audit=True,
        catalog_revision="catalog-1",
        publish=False,
    )
    assert retry.external_activity == (second,)


@pytest.mark.asyncio
async def test_monitored_ledger_cannot_publish_before_incident_classification() -> None:
    observed_ms = time_ns() // 1_000_000
    reconciler = _reconciler(_InfoClient([], observed_ms), AccountStateBook())

    with pytest.raises(ValueError, match="classified before publication"):
        await reconciler.refresh_follower(
            FOLLOWER,
            trigger="unsafe",
            full_audit=True,
            catalog_revision="catalog-1",
            publish=True,
        )
