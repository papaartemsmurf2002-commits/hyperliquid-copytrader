from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from .cloid import deterministic_cloid
from .models import ExecutionReport, FollowerIntent, IntentStatus, Position, now_ms


@dataclass
class PaperAccount:
    positions: dict[str, Position] = field(default_factory=dict)
    reports: list[ExecutionReport] = field(default_factory=list)

    def apply(self, intent: FollowerIntent) -> ExecutionReport:
        if intent.status == IntentStatus.SKIPPED or intent.size == 0:
            status = IntentStatus.SKIPPED
        else:
            signed_delta = intent.size if intent.side == "buy" else -intent.size
            current = self.positions.get(intent.coin, Position(coin=intent.coin, size=Decimal("0")))
            next_size = current.size + signed_delta
            if intent.reduce_only and abs(next_size) > abs(current.size):
                status = IntentStatus.REJECTED
            else:
                status = IntentStatus.FILLED
                if next_size == 0:
                    self.positions.pop(intent.coin, None)
                else:
                    self.positions[intent.coin] = Position(
                        coin=intent.coin,
                        size=next_size,
                        entry_px=intent.price or current.entry_px,
                        leverage=current.leverage,
                        updated_ms=now_ms(),
                    )
        report = ExecutionReport(
            report_id=deterministic_cloid(
                "paper-report", intent.intent_id, len(self.reports), status.value
            ),
            intent_id=intent.intent_id,
            cloid=intent.cloid,
            status=status,
            exchange_status="paper_" + status.value,
            exchange_ts_ms=now_ms(),
            payload={"positions": self.positions},
        )
        self.reports.append(report)
        return report
