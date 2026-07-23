from __future__ import annotations

from dataclasses import asdict, dataclass

from .models import SafeModeReason


@dataclass(frozen=True)
class IncidentGuidance:
    reason: SafeModeReason
    severity: str
    blocks_new_risk: bool
    automatic_retry: bool
    required_action: str
    resume_gate: str


_GUIDANCE: dict[SafeModeReason, IncidentGuidance] = {
    SafeModeReason.NONE: IncidentGuidance(
        reason=SafeModeReason.NONE,
        severity="normal",
        blocks_new_risk=False,
        automatic_retry=False,
        required_action="Monitor source and follower state.",
        resume_gate="No safe-mode clearance required.",
    ),
    SafeModeReason.ACCOUNT_NOT_CONFIGURED: IncidentGuidance(
        reason=SafeModeReason.ACCOUNT_NOT_CONFIGURED,
        severity="configuration",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Configure follower account credentials and rerun preflight.",
        resume_gate="Preflight must pass with the intended follower account.",
    ),
    SafeModeReason.CONFIG_INVALID: IncidentGuidance(
        reason=SafeModeReason.CONFIG_INVALID,
        severity="configuration",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Fix the rejected configuration value before running controls again.",
        resume_gate="Preflight must pass after the config change.",
    ),
    SafeModeReason.PREFLIGHT_FAILED: IncidentGuidance(
        reason=SafeModeReason.PREFLIGHT_FAILED,
        severity="configuration",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Resolve every preflight blocker; do not bypass the failed gate.",
        resume_gate="Preflight must pass.",
    ),
    SafeModeReason.LIVE_BLOCKED: IncidentGuidance(
        reason=SafeModeReason.LIVE_BLOCKED,
        severity="critical",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Keep live disabled until explicit live flags, credentials, risk caps, and preflight are reviewed.",
        resume_gate="All live gates and preflight must pass.",
    ),
    SafeModeReason.TESTNET_BLOCKED: IncidentGuidance(
        reason=SafeModeReason.TESTNET_BLOCKED,
        severity="configuration",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Resolve testnet exchange-mode preflight blockers; no extra testnet enable flag is required.",
        resume_gate="Testnet preflight must pass with credentials, risk caps, local gates, and auth probe.",
    ),
    SafeModeReason.STARTUP_RECONCILE: IncidentGuidance(
        reason=SafeModeReason.STARTUP_RECONCILE,
        severity="critical",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Compare journal baseline to exchange truth and repair or settle any mismatch.",
        resume_gate="Manual reconcile must prove follower positions and open orders match the journal.",
    ),
    SafeModeReason.DUPLICATE_EVENT: IncidentGuidance(
        reason=SafeModeReason.DUPLICATE_EVENT,
        severity="diagnostic",
        blocks_new_risk=False,
        automatic_retry=False,
        required_action="No action for a deduped event unless duplicates continue with missing state.",
        resume_gate="No safe-mode clearance required for dedupe-only handling.",
    ),
    SafeModeReason.OUT_OF_ORDER_EVENT: IncidentGuidance(
        reason=SafeModeReason.OUT_OF_ORDER_EVENT,
        severity="critical",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Stop trusting incremental source ordering and reconcile source state from REST.",
        resume_gate="Fresh source reconcile and follower reconcile must be accepted.",
    ),
    SafeModeReason.MISSED_EVENT_GAP: IncidentGuidance(
        reason=SafeModeReason.MISSED_EVENT_GAP,
        severity="critical",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Backfill fills/orders from REST and verify no source events were missed.",
        resume_gate="REST backfill plus follower reconcile must explain the gap.",
    ),
    SafeModeReason.WEBSOCKET_DISCONNECT: IncidentGuidance(
        reason=SafeModeReason.WEBSOCKET_DISCONNECT,
        severity="degraded",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Reconnect the source websocket and reconcile source state from REST.",
        resume_gate="Fresh source data and follower reconcile are required before resume.",
    ),
    SafeModeReason.REST_LAG: IncidentGuidance(
        reason=SafeModeReason.REST_LAG,
        severity="degraded",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Wait for REST/info responses to recover; check network and Hyperliquid status.",
        resume_gate="A fresh REST reconcile must pass.",
    ),
    SafeModeReason.RESTART_MID_FILL: IncidentGuidance(
        reason=SafeModeReason.RESTART_MID_FILL,
        severity="critical",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Settle pending intents by querying order status before creating new risk.",
        resume_gate="No pending intents may remain and follower reconcile must match.",
    ),
    SafeModeReason.PARTIAL_FILL: IncidentGuidance(
        reason=SafeModeReason.PARTIAL_FILL,
        severity="critical",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Reconcile the actual filled size and decide whether to close, reduce, or accept residual exposure manually.",
        resume_gate="Follower exposure and open orders must match the journal after settlement.",
    ),
    SafeModeReason.CANCEL_REJECT: IncidentGuidance(
        reason=SafeModeReason.CANCEL_REJECT,
        severity="critical",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Query order status by cloid; do not assume the order was canceled.",
        resume_gate="Order status must be terminal or the open order must be journaled and reconciled.",
    ),
    SafeModeReason.ORDER_TIMEOUT: IncidentGuidance(
        reason=SafeModeReason.ORDER_TIMEOUT,
        severity="critical",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Do not retry blindly; query exchange order status and reconcile follower state.",
        resume_gate="The timed-out action must be classified terminal or reconciled as open exposure.",
    ),
    SafeModeReason.RAPID_FLIP: IncidentGuidance(
        reason=SafeModeReason.RAPID_FLIP,
        severity="critical",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Let the source settle, then reconcile source and follower state before copying again.",
        resume_gate="The latest source position must be fresh and unambiguous.",
    ),
    SafeModeReason.UNSUPPORTED_SYMBOL: IncidentGuidance(
        reason=SafeModeReason.UNSUPPORTED_SYMBOL,
        severity="configuration",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Review the symbol allowlist and exchange metadata; do not estimate unsupported markets.",
        resume_gate="The symbol must be supported or excluded with a fresh desired-state baseline.",
    ),
    SafeModeReason.RATE_LIMIT: IncidentGuidance(
        reason=SafeModeReason.RATE_LIMIT,
        severity="degraded",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Wait for the local/exchange rate window to cool down and inspect recent reports for loops.",
        resume_gate="Rate counters must cool down and preflight/reconcile must pass.",
    ),
    SafeModeReason.PRECISION_ERROR: IncidentGuidance(
        reason=SafeModeReason.PRECISION_ERROR,
        severity="configuration",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Refresh market metadata and inspect price/size quantization for the rejected coin.",
        resume_gate="A corrected normalized order must pass preflight and guard checks.",
    ),
    SafeModeReason.MARGIN_ERROR: IncidentGuidance(
        reason=SafeModeReason.MARGIN_ERROR,
        severity="critical",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Inspect follower equity, leverage, and open exposure; reduce risk caps if needed.",
        resume_gate="Follower account state must reconcile within configured risk caps.",
    ),
    SafeModeReason.CLOCK_SKEW: IncidentGuidance(
        reason=SafeModeReason.CLOCK_SKEW,
        severity="critical",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Fix local clock/nonce health before sending signed actions.",
        resume_gate="Signed auth probe and preflight must pass after clock correction.",
    ),
    SafeModeReason.STALE_SOURCE: IncidentGuidance(
        reason=SafeModeReason.STALE_SOURCE,
        severity="degraded",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Refresh source observation via websocket or REST reconcile before planning.",
        resume_gate="Source event age must be within the configured threshold.",
    ),
    SafeModeReason.STALE_FOLLOWER: IncidentGuidance(
        reason=SafeModeReason.STALE_FOLLOWER,
        severity="degraded",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Refresh follower reconcile; do not trade on stale account truth.",
        resume_gate="Follower snapshot age must be within the configured threshold.",
    ),
    SafeModeReason.MANUAL_INTERVENTION: IncidentGuidance(
        reason=SafeModeReason.MANUAL_INTERVENTION,
        severity="critical",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Inspect exchange-side positions and open orders; close or journal any untracked state.",
        resume_gate="Manual reconcile must show no untracked exposure or unknown open orders.",
    ),
    SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE: IncidentGuidance(
        reason=SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE,
        severity="critical",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Treat the exchange result as unknown; query order/account state before any retry.",
        resume_gate="Exchange truth must classify the ambiguous action as terminal or intentionally open.",
    ),
    SafeModeReason.OPERATOR_KILL_SWITCH: IncidentGuidance(
        reason=SafeModeReason.OPERATOR_KILL_SWITCH,
        severity="operator",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Leave the kill-switch file in place until the incident is reviewed.",
        resume_gate="Remove the kill switch only after manual reconcile and operator approval.",
    ),
    SafeModeReason.RISK_LIMIT: IncidentGuidance(
        reason=SafeModeReason.RISK_LIMIT,
        severity="critical",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Reduce multiplier, allowlist, leverage, or notional caps before new risk.",
        resume_gate="A new plan must fit every configured risk cap.",
    ),
    SafeModeReason.DUPLICATE_INTENT: IncidentGuidance(
        reason=SafeModeReason.DUPLICATE_INTENT,
        severity="critical",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Inspect intent journal and cloids for replay before sending anything else.",
        resume_gate="Journal uniqueness and follower exchange truth must be verified.",
    ),
    SafeModeReason.CIRCUIT_BREAKER: IncidentGuidance(
        reason=SafeModeReason.CIRCUIT_BREAKER,
        severity="critical",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Investigate consecutive exchange failures; do not resume until the root cause is clear.",
        resume_gate="Failure cooldown, preflight, and manual reconcile must pass.",
    ),
    SafeModeReason.CONCURRENT_INSTANCE: IncidentGuidance(
        reason=SafeModeReason.CONCURRENT_INSTANCE,
        severity="critical",
        blocks_new_risk=True,
        automatic_retry=False,
        required_action="Stop the duplicate process or wait for a stale lease to expire.",
        resume_gate="Only one exchange actor may hold the runtime lease.",
    ),
}


def incident_guidance(reason: SafeModeReason, *, enabled: bool) -> dict[str, object]:
    effective_reason = reason if enabled else SafeModeReason.NONE
    guidance = _GUIDANCE[effective_reason]
    return {
        **asdict(guidance),
        "reason": guidance.reason.value,
    }


def assert_guidance_complete() -> None:
    missing = set(SafeModeReason) - set(_GUIDANCE)
    if missing:
        missing_values = ", ".join(sorted(reason.value for reason in missing))
        raise AssertionError(f"missing incident guidance for: {missing_values}")
