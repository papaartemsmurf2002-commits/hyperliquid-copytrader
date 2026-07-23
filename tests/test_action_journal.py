from __future__ import annotations

import sqlite3
from decimal import Decimal

import pytest

from hyperliquid_copytrader.action_journal import ActionJournal, ActionState
from hyperliquid_copytrader.cloid import deterministic_cloid


FOLLOWER = "0xFollower"
API_WALLET = "0xAgent"


def _prepare(
    journal: ActionJournal,
    *,
    desired_id: str = "desired-1",
    market: str = "BTC",
    requested_size: str = "1",
    wall_ms: int = 1_000,
    suffix: str = "action",
) -> tuple[str, int]:
    attempt_no = journal.next_attempt_no(
        follower_account=FOLLOWER,
        api_wallet=API_WALLET,
        desired_id=desired_id,
        market=market,
    )
    nonce = journal.reserve_nonce(
        follower_account=FOLLOWER,
        api_wallet=API_WALLET,
        wall_ms=wall_ms,
    )
    cloid = deterministic_cloid("continuous-v1", desired_id, market, attempt_no, suffix)
    journal.prepare_action(
        follower_account=FOLLOWER,
        api_wallet=API_WALLET,
        desired_id=desired_id,
        market=market,
        attempt_no=attempt_no,
        cloid=cloid,
        nonce=nonce,
        requested_size=requested_size,
        action_json=f'{{"type":"order","nonce":{nonce}}}',
        signed_payload_json=f'{{"signature":"sig-{suffix}","nonce":{nonce}}}',
        expires_after_ms=wall_ms + 10_000,
        request_id=f"request-{suffix}",
        created_ms=wall_ms,
    )
    return cloid, nonce


def test_nonce_lane_is_monotonic_across_clock_rollback_and_reopen(tmp_path):
    path = tmp_path / "continuous-v1.sqlite3"
    first = ActionJournal(path)
    second = ActionJournal(path)
    try:
        assert (
            first.reserve_nonce(
                follower_account=FOLLOWER,
                api_wallet=API_WALLET,
                wall_ms=10_000,
            )
            == 10_000
        )
        assert (
            second.reserve_nonce(
                follower_account=FOLLOWER,
                api_wallet=API_WALLET,
                wall_ms=9_000,
            )
            == 10_001
        )
        assert (
            first.reserve_nonce(
                follower_account=FOLLOWER,
                api_wallet=API_WALLET,
                wall_ms=9_000,
            )
            == 10_002
        )
        assert (
            first.reserve_nonce(
                follower_account="0xOtherFollower",
                api_wallet=API_WALLET,
                wall_ms=9_000,
            )
            == 9_000
        )
    finally:
        first.close()
        second.close()

    reopened = ActionJournal(path)
    try:
        assert (
            reopened.last_nonce(
                follower_account=FOLLOWER,
                api_wallet=API_WALLET,
            )
            == 10_002
        )
        assert (
            reopened.reserve_nonce(
                follower_account=FOLLOWER,
                api_wallet=API_WALLET,
                wall_ms=8_000,
            )
            == 10_003
        )
    finally:
        reopened.close()


def test_prepare_commits_exact_payload_in_wal_before_send(tmp_path):
    path = tmp_path / "continuous-v1.sqlite3"
    exact_action = ' { "type" : "order", "orders" : [] }\n'
    exact_payload = '\n{ "method" : "post", "id" : 7, "request" : { } } '
    with ActionJournal(path) as journal:
        nonce = journal.reserve_nonce(
            follower_account=FOLLOWER,
            api_wallet=API_WALLET,
            wall_ms=2_000,
        )
        cloid = deterministic_cloid("continuous-v1", "exact", 1)
        prepared = journal.prepare_action(
            follower_account=FOLLOWER,
            api_wallet=API_WALLET,
            desired_id="desired-exact",
            market="xyz:XYZ100",
            attempt_no=1,
            cloid=cloid,
            nonce=nonce,
            requested_size="2.500",
            action_json=exact_action,
            signed_payload_json=exact_payload,
            expires_after_ms=12_000,
            request_id="7",
            created_ms=2_000,
        )

        assert prepared.state is ActionState.PREPARED
        assert prepared.provably_not_sent
        assert prepared.action_json == exact_action
        assert prepared.signed_payload_json == exact_payload
        assert prepared.requested_size == Decimal("2.500")

    connection = sqlite3.connect(path)
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        row = connection.execute(
            "SELECT state, action_json, signed_payload_json FROM actions WHERE cloid=?",
            (cloid,),
        ).fetchone()
        assert row == (ActionState.PREPARED.value, exact_action, exact_payload)
    finally:
        connection.close()


def test_send_boundary_prevents_blind_resend_and_drives_recovery(tmp_path):
    path = tmp_path / "continuous-v1.sqlite3"
    with ActionJournal(path) as journal:
        prepared_cloid, _ = _prepare(journal, suffix="prepared")
        attempted_cloid, _ = _prepare(
            journal,
            desired_id="desired-2",
            suffix="attempted",
            wall_ms=2_000,
        )
        not_sent_cloid, _ = _prepare(
            journal,
            desired_id="desired-3",
            suffix="not-sent",
            wall_ms=3_000,
        )

        attempted = journal.mark_send_attempted(
            attempted_cloid,
            request_id=42,
            observed_ms=2_100,
        )
        assert attempted.state is ActionState.SEND_ATTEMPTED
        assert attempted.request_id == "42"
        assert attempted.ambiguous
        with pytest.raises(ValueError, match="only from PREPARED"):
            journal.mark_send_attempted(attempted_cloid, observed_ms=2_101)

        not_sent = journal.mark_not_sent(not_sent_cloid, observed_ms=3_100)
        assert not_sent.state is ActionState.NOT_SENT
        assert not_sent.terminal

        assert [item.cloid for item in journal.recovery_actions()] == [
            prepared_cloid,
            attempted_cloid,
        ]

    with ActionJournal(path) as reopened:
        recovered = reopened.recovery_actions(
            follower_account=FOLLOWER,
            api_wallet=API_WALLET,
        )
        assert [item.state for item in recovered] == [
            ActionState.PREPARED,
            ActionState.SEND_ATTEMPTED,
        ]
        unknown = reopened.mark_unknown(
            attempted_cloid,
            detail="socket closed after send",
            observed_ms=4_000,
        )
        assert unknown.state is ActionState.UNKNOWN
        assert unknown.ambiguous
        rejected = reopened.record_outcome(
            attempted_cloid,
            state=ActionState.REJECTED,
            detail="venue rejected",
            observed_ms=4_100,
        )
        assert rejected.terminal
        assert [item.cloid for item in reopened.recovery_actions()] == [prepared_cloid]


def test_durable_attempts_and_cloid_ownership_are_exact(tmp_path):
    path = tmp_path / "continuous-v1.sqlite3"
    with ActionJournal(path) as journal:
        first_cloid, _ = _prepare(journal, suffix="attempt-1")
        journal.mark_not_sent(first_cloid, observed_ms=1_100)

    with ActionJournal(path) as reopened:
        assert (
            reopened.next_attempt_no(
                follower_account=FOLLOWER,
                api_wallet=API_WALLET,
                desired_id="desired-1",
                market="BTC",
            )
            == 2
        )
        second_cloid, _ = _prepare(
            reopened,
            suffix="attempt-2",
            wall_ms=2_000,
        )
        second = reopened.get_owned_action(
            second_cloid,
            follower_account=FOLLOWER.upper(),
            api_wallet=API_WALLET.upper(),
        )
        assert second is not None
        assert second.attempt_no == 2
        assert (
            reopened.get_owned_action(
                second_cloid,
                follower_account="0xDifferent",
                api_wallet=API_WALLET,
            )
            is None
        )
        assert not reopened.owns_cloid(
            second_cloid,
            follower_account=FOLLOWER,
            api_wallet="0xDifferentAgent",
        )

        skipped_nonce = reopened.reserve_nonce(
            follower_account=FOLLOWER,
            api_wallet=API_WALLET,
            wall_ms=3_000,
        )
        with pytest.raises(ValueError, match="next durable attempt"):
            reopened.prepare_action(
                follower_account=FOLLOWER,
                api_wallet=API_WALLET,
                desired_id="desired-1",
                market="BTC",
                attempt_no=4,
                cloid=deterministic_cloid("continuous-v1", "attempt-4"),
                nonce=skipped_nonce,
                requested_size="1",
                action_json="{}",
                signed_payload_json="{}",
                expires_after_ms=13_000,
                request_id="request-4",
                created_ms=3_000,
            )


def test_stale_nonce_reservation_cannot_be_prepared_after_lane_advances(tmp_path):
    with ActionJournal(tmp_path / "continuous-v1.sqlite3") as journal:
        stale_nonce = journal.reserve_nonce(
            follower_account=FOLLOWER,
            api_wallet=API_WALLET,
            wall_ms=1_000,
        )
        journal.reserve_nonce(
            follower_account=FOLLOWER,
            api_wallet=API_WALLET,
            wall_ms=1_000,
        )
        with pytest.raises(ValueError, match="latest durable reservation"):
            journal.prepare_action(
                follower_account=FOLLOWER,
                api_wallet=API_WALLET,
                desired_id="desired-stale",
                market="BTC",
                attempt_no=1,
                cloid=deterministic_cloid("continuous-v1", "stale"),
                nonce=stale_nonce,
                requested_size="1",
                action_json="{}",
                signed_payload_json="{}",
                expires_after_ms=11_000,
                request_id="request-stale",
                created_ms=1_000,
            )


def test_ioc_partial_fill_is_terminal_and_cumulative_replays_are_idempotent(tmp_path):
    with ActionJournal(tmp_path / "continuous-v1.sqlite3") as journal:
        cloid, _ = _prepare(journal, requested_size="1.0")
        journal.mark_send_attempted(cloid, observed_ms=1_100)

        first_fill = journal.record_cumulative_fill(cloid, "0.4", observed_ms=1_200)
        assert first_fill.state is ActionState.SEND_ATTEMPTED
        assert not first_fill.terminal
        replay = journal.record_cumulative_fill(cloid, Decimal("0.400"), observed_ms=1_300)
        assert replay == first_fill

        partial = journal.record_outcome(
            cloid,
            state=ActionState.PARTIALLY_FILLED,
            cumulative_filled_size="0.4",
            observed_ms=1_400,
        )
        assert partial.terminal
        assert partial.remaining_size == Decimal("0.6")
        assert journal.recovery_actions() == ()

        later_fill = journal.record_cumulative_fill(cloid, "0.7", observed_ms=1_500)
        assert later_fill.state is ActionState.PARTIALLY_FILLED
        assert later_fill.cumulative_filled_size == Decimal("0.7")
        full = journal.record_cumulative_fill(cloid, "1.0", observed_ms=1_600)
        assert full.state is ActionState.FILLED
        assert full.remaining_size == 0

        with pytest.raises(ValueError, match="cannot decrease"):
            journal.record_cumulative_fill(cloid, "0.9", observed_ms=1_700)
        with pytest.raises(ValueError, match="cannot exceed"):
            journal.record_cumulative_fill(cloid, "1.1", observed_ms=1_700)


def test_invalid_outcomes_cannot_invent_or_hide_fills(tmp_path):
    with ActionJournal(tmp_path / "continuous-v1.sqlite3") as journal:
        cloid, _ = _prepare(journal, requested_size="1")
        with pytest.raises(ValueError, match="invalid action transition"):
            journal.record_outcome(cloid, state=ActionState.REJECTED, observed_ms=1_050)
        with pytest.raises(ValueError, match="provably unsent"):
            journal.record_cumulative_fill(cloid, "0.1", observed_ms=1_050)

        journal.mark_send_attempted(cloid, observed_ms=1_100)
        with pytest.raises(ValueError, match="FILLED requires"):
            journal.record_outcome(
                cloid,
                state=ActionState.FILLED,
                cumulative_filled_size="0.9",
                observed_ms=1_200,
            )
        with pytest.raises(ValueError, match="must be PARTIALLY_FILLED"):
            journal.record_outcome(
                cloid,
                state=ActionState.CANCELED,
                cumulative_filled_size="0.1",
                observed_ms=1_200,
            )

        canceled = journal.record_outcome(
            cloid,
            state=ActionState.CANCELED,
            observed_ms=1_300,
        )
        assert canceled.terminal
        corrected = journal.record_cumulative_fill(cloid, "0.1", observed_ms=1_400)
        assert corrected.state is ActionState.PARTIALLY_FILLED
        assert corrected.cumulative_filled_size == Decimal("0.1")
