from __future__ import annotations

from decimal import Decimal

from hyperliquid_copytrader.copy_engine import AssetMeta, CopyEngine
from hyperliquid_copytrader.models import Mode, Position
from hyperliquid_copytrader.paper import PaperAccount


def test_replay_source_snapshots_into_paper_account(base_config):
    engine = CopyEngine(base_config.risk, Mode.PAPER, follower_account="0xf0")
    paper = PaperAccount()
    meta = {"BTC": AssetMeta("BTC", 5)}
    mids = {"BTC": Decimal("50000")}
    snapshots = [
        {"BTC": Position("BTC", Decimal("1"), leverage=2)},
        {"BTC": Position("BTC", Decimal("2"), leverage=2)},
        {"BTC": Position("BTC", Decimal("0"), leverage=2)},
    ]
    for idx, source_positions in enumerate(snapshots):
        result = engine.plan(
            source_event_key=f"replay-{idx}",
            source_positions=source_positions,
            follower_positions=paper.positions,
            asset_meta=meta,
            mids=mids,
        )
        for intent in result.intents:
            paper.apply(intent)
    assert paper.positions == {}
