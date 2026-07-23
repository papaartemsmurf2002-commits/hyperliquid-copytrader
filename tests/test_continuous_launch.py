from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from hyperliquid_copytrader import continuous_launch
from hyperliquid_copytrader.continuous_launch import (
    ARM_ACKNOWLEDGEMENT,
    FLEET_UPDATE_ACKNOWLEDGEMENT,
    LEADER_UPDATE_ACKNOWLEDGEMENT,
    STOP_ACKNOWLEDGEMENT,
    ContinuousLaunchController,
    ContinuousLaunchPaths,
)
from hyperliquid_copytrader.windows_runtime import WindowsProcessIdentity


def _plan(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "network": "mainnet",
                "runtime_id": "continuous-ui-test",
                "startup_baseline_only": True,
                "max_combined_gross_usd": "15",
                "slots": [
                    {
                        "slot": "acc1",
                        "source_address": "0x" + "1" * 40,
                        "follower_account_address": "0x" + "2" * 40,
                        "credential_profile_id": "acc1",
                        "multiplier": "0.01",
                        "max_order_notional_usd": "12",
                        "max_gross_exposure_usd": "15",
                        "max_open_positions": 1,
                        "max_leverage": 1,
                        "action_limit_per_minute": 6,
                        "allowed_markets": ["BTC"],
                        "enabled": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_controller_spawns_only_continuous_runner_and_requests_graceful_stop(
    monkeypatch, tmp_path: Path
) -> None:
    repo, plan = tmp_path / "repo", tmp_path / "continuous-plan.json"
    runner = repo / "scripts" / "run_continuous_fleet.py"
    runner.parent.mkdir(parents=True)
    runner.write_text("# runner\n", encoding="utf-8")
    _plan(plan)
    paths = ContinuousLaunchPaths.build(
        repo_root=repo,
        plan=plan,
        state_root=tmp_path / "state",
    )
    controller = ContinuousLaunchController(paths)
    spawned: list[list[str]] = []
    identity = WindowsProcessIdentity(4321, 99, "python.exe")

    def spawn(argv, **_kwargs):  # type: ignore[no-untyped-def]
        spawned.append(list(argv))
        return SimpleNamespace(pid=identity.pid, terminate=lambda: None)

    monkeypatch.setattr(continuous_launch, "spawn_hidden_detached", spawn)
    monkeypatch.setattr(continuous_launch, "inspect_process_identity", lambda _pid: identity)
    monkeypatch.setattr(
        continuous_launch, "exact_process_is_alive", lambda value: value == identity
    )

    preview = controller.preview()
    started = controller.start(acknowledgement=ARM_ACKNOWLEDGEMENT)
    runner_status_path = Path(str(started["state_dir"])) / "status.json"
    runner_status_path.write_text(
        json.dumps(
            {
                "status": "running",
                "execution_enabled": True,
                "updated_ms": continuous_launch.time_ns() // 1_000_000,
                "slots": {
                    "acc1": {
                        "state": "RUNNING",
                        "reason": "desired and follower state agree",
                        "data_stale": False,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    running = controller.status()
    stopping = controller.stop(acknowledgement=STOP_ACKNOWLEDGEMENT)

    assert preview["launchable"] is True
    assert started["online"] is True
    assert started["execution_enabled"] is False
    assert running["status"] == "running"
    assert running["execution_enabled"] is True
    assert running["healthy"] is True
    assert running["ready_slots"] == 1
    assert stopping["status"] == "stopping"
    argv = spawned[0]
    assert str(runner) in argv
    assert "run_slot_supervisor.py" not in " ".join(argv)
    assert "guardian" not in " ".join(argv).lower()
    assert "--duration" not in argv
    assert "--enable-rest-recovery-fallback" in argv
    assert argv[argv.index("--arm") + 1] == ARM_ACKNOWLEDGEMENT
    assert argv[argv.index("--operator-rearm") + 1] == ARM_ACKNOWLEDGEMENT
    engine_state_dir = Path(argv[argv.index("--engine-state-dir") + 1])
    assert engine_state_dir == Path(str(started["engine_state_dir"]))
    assert engine_state_dir.parts[-3:] == (
        "engines",
        "mainnet",
        "continuous-ui-test",
    )
    assert engine_state_dir != Path(str(started["state_dir"]))
    stop_file = Path(str(stopping["state_dir"])) / "stop.requested.json"
    assert json.loads(stop_file.read_text(encoding="utf-8"))["reason"] == "operator_stop"

    monkeypatch.setattr(continuous_launch, "exact_process_is_alive", lambda _value: False)
    runner_status_path.write_text(
        json.dumps({"status": "stopped", "execution_enabled": True}), encoding="utf-8"
    )
    stopped = controller.status()
    assert stopped["status"] == "stopped"
    assert stopped["execution_enabled"] is False
    runner_status_path.write_text(json.dumps({"status": "error"}), encoding="utf-8")
    assert controller.status()["status"] == "error"


def test_explicit_engine_state_directory_is_used_by_ui_preview(tmp_path: Path) -> None:
    repo, plan = tmp_path / "repo", tmp_path / "continuous-plan.json"
    runner = repo / "scripts" / "run_continuous_fleet.py"
    runner.parent.mkdir(parents=True)
    runner.write_text("# runner\n", encoding="utf-8")
    _plan(plan)
    durable = tmp_path / "latest-engine"
    controller = ContinuousLaunchController(
        ContinuousLaunchPaths.build(
            repo_root=repo,
            plan=plan,
            state_root=tmp_path / "state",
            engine_state_dir=durable,
        )
    )

    preview = controller.preview()

    assert preview["launchable"] is True
    assert preview["engine_state_dir"] == str(durable.resolve())


def test_preview_reports_missing_plan_without_starting_any_process(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runner = repo / "scripts" / "run_continuous_fleet.py"
    runner.parent.mkdir(parents=True)
    runner.write_text("# runner\n", encoding="utf-8")
    controller = ContinuousLaunchController(
        ContinuousLaunchPaths.build(
            repo_root=repo,
            plan=tmp_path / "missing.json",
            state_root=tmp_path / "state",
        )
    )

    preview = controller.preview()

    assert preview["launchable"] is False
    assert "unreadable" in preview["blockers"][0]


def test_offline_leader_update_keeps_signer_and_starts_fresh_engine_identity(
    tmp_path: Path,
) -> None:
    repo, plan = tmp_path / "repo", tmp_path / "continuous-plan.json"
    runner = repo / "scripts" / "run_continuous_fleet.py"
    runner.parent.mkdir(parents=True)
    runner.write_text("# runner\n", encoding="utf-8")
    _plan(plan)
    profile_dir = repo / ".secrets" / "operator-profiles" / "acc1"
    profile_dir.mkdir(parents=True)
    key_path = profile_dir / "api-wallet.key"
    key_path.write_text("not-opened-by-public-update\n", encoding="utf-8")
    profile_path = profile_dir / "profile.json"
    profile_path.write_text(
        json.dumps(
            {
                "profile_version": 3,
                "profile_id": "acc1",
                "profile_label": "acc1",
                "network": "mainnet",
                "source_wallet": "0x" + "1" * 40,
                "global_account_address": "0x" + "3" * 40,
                "subaccount_name": "acc1",
                "follower_account_address": "0x" + "2" * 40,
                "api_wallet_address": "0x" + "4" * 40,
                "expected_account_mode": "unified",
                "coin": "BTC",
                "eligibility": "all_active_markets",
                "denied_symbols": [],
                "api_private_key_file": str(key_path.resolve()),
            }
        ),
        encoding="utf-8",
    )
    controller = ContinuousLaunchController(
        ContinuousLaunchPaths.build(
            repo_root=repo,
            plan=plan,
            state_root=tmp_path / "state",
        )
    )
    replacement = "0x" + "5" * 40

    preview = controller.update_leaders(
        leaders={"acc1": replacement},
        acknowledgement=LEADER_UPDATE_ACKNOWLEDGEMENT,
    )

    saved_plan = json.loads(plan.read_text(encoding="utf-8"))
    saved_profile = json.loads(profile_path.read_text(encoding="utf-8"))
    assert preview["launchable"] is True
    assert saved_plan["slots"][0]["source_address"] == replacement
    assert saved_profile["source_wallet"] == replacement
    assert saved_plan["runtime_id"].startswith("fleet-")
    assert saved_plan["runtime_id"] != "continuous-ui-test"
    assert key_path.read_text(encoding="utf-8") == "not-opened-by-public-update\n"


def test_offline_fleet_update_adds_profile_markets_and_limits_without_opening_keys(
    tmp_path: Path,
) -> None:
    repo, plan = tmp_path / "repo", tmp_path / "continuous-plan.json"
    runner = repo / "scripts" / "run_continuous_fleet.py"
    runner.parent.mkdir(parents=True)
    runner.write_text("# runner\n", encoding="utf-8")
    _plan(plan)

    def profile(profile_id: str, source: str, follower: str, owner: str, wallet: str) -> Path:
        profile_dir = repo / ".secrets" / "operator-profiles" / profile_id
        profile_dir.mkdir(parents=True)
        key_path = profile_dir / "api-wallet.key"
        key_path.write_text(f"sealed-{profile_id}\n", encoding="utf-8")
        (profile_dir / "profile.json").write_text(
            json.dumps(
                {
                    "profile_version": 3,
                    "profile_id": profile_id,
                    "profile_label": profile_id,
                    "network": "mainnet",
                    "source_wallet": source,
                    "global_account_address": owner,
                    "subaccount_name": profile_id,
                    "follower_account_address": follower,
                    "api_wallet_address": wallet,
                    "expected_account_mode": "unified",
                    "coin": "BTC",
                    "eligibility": "all_active_markets",
                    "denied_symbols": [],
                    "api_private_key_file": str(key_path.resolve()),
                }
            ),
            encoding="utf-8",
        )
        return key_path

    key1 = profile("acc1", "0x" + "1" * 40, "0x" + "2" * 40, "0x" + "3" * 40, "0x" + "4" * 40)
    key2 = profile("acc2", "0x" + "5" * 40, "0x" + "6" * 40, "0x" + "7" * 40, "0x" + "8" * 40)
    slots = json.loads(plan.read_text(encoding="utf-8"))["slots"]
    slots.append(
        {
            **slots[0],
            "slot": "acc2",
            "source_address": "0x" + "5" * 40,
            "follower_account_address": "0x" + "6" * 40,
            "credential_profile_id": "acc2",
            "allowed_markets": ["ETH", "xyz:NVDA"],
        }
    )
    controller = ContinuousLaunchController(
        ContinuousLaunchPaths.build(repo_root=repo, plan=plan, state_root=tmp_path / "state")
    )

    preview = controller.update_fleet(
        slots=slots,
        max_combined_gross_usd="30",
        acknowledgement=FLEET_UPDATE_ACKNOWLEDGEMENT,
    )

    saved = json.loads(plan.read_text(encoding="utf-8"))
    assert preview["launchable"] is True
    assert [slot["slot"] for slot in saved["slots"]] == ["acc1", "acc2"]
    assert all("allowed_markets" not in slot for slot in saved["slots"])
    assert saved["runtime_id"].startswith("fleet-")
    assert key1.read_text(encoding="utf-8") == "sealed-acc1\n"
    assert key2.read_text(encoding="utf-8") == "sealed-acc2\n"
