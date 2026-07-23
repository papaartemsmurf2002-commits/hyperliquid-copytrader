from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import hyperliquid_copytrader.cli as cli_module
from hyperliquid_copytrader.cli import app
from hyperliquid_copytrader.models import Mode
from hyperliquid_copytrader.preflight import PreflightReport


SOURCE_WALLET = "0xcf7c4feb434751146a48b895e96caeb15838f92c"


def _set_cli_env(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("HLCT_SOURCE_WALLET", SOURCE_WALLET)
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "cli.sqlite3"))
    monkeypatch.delenv("HLCT_API_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("HLCT_API_PRIVATE_KEY_FILE", raising=False)


def test_cli_help_lists_operator_commands():
    result = CliRunner().invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "preflight" in result.output
    assert "readiness" in result.output
    assert "run" not in result.output
    assert "follow-source" not in result.output
    assert "settle-pending" not in result.output
    assert "testnet-smoke" not in result.output
    assert "testnet-active-smoke" not in result.output
    assert "console" not in result.output
    assert "containment-watchdog" not in result.output
    assert "mainnet-canary" not in result.output
    assert "measure-context-feed" not in result.output
    assert "benchmark-fast-execution" not in result.output


def test_load_env_explicitly_blocks_hostile_dotenv_when_disabled(monkeypatch, tmp_path):
    import dotenv

    hostile_values = {
        "HLCT_MODE": "live",
        "HLCT_LIVE_COPY_ENABLE": "true",
        "HLCT_ALLOWED_SYMBOLS": "DOGE",
        "HLCT_MAX_NOTIONAL_USD": "999999",
        "HLCT_API_PRIVATE_KEY": "0x" + "ab" * 32,
    }
    hostile_path = tmp_path / ".env"
    hostile_path.write_text(
        "\n".join(f"{key}={value}" for key, value in hostile_values.items()) + "\n",
        encoding="utf-8",
    )
    for key in hostile_values:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("PYTHON_DOTENV_DISABLED", "1")

    calls: list[object] = []
    real_load_dotenv = dotenv.load_dotenv

    def load_hostile_dotenv(*_args, **_kwargs):
        calls.append(hostile_path)
        return real_load_dotenv(dotenv_path=hostile_path, override=False)

    monkeypatch.setattr(dotenv, "load_dotenv", load_hostile_dotenv)

    cli_module._load_env()

    assert calls == []
    assert all(key not in os.environ for key in hostile_values)


def test_browser_ui_scrubs_legacy_signer_environment(monkeypatch) -> None:
    monkeypatch.setenv("HLCT_API_PRIVATE_KEY", "0x" + "ab" * 32)
    monkeypatch.setenv("HLCT_API_PRIVATE_KEY_FILE", r"C:\secrets\legacy.key")

    cli_module._scrub_browser_ui_signer_environment()

    assert "HLCT_API_PRIVATE_KEY" not in os.environ
    assert "HLCT_API_PRIVATE_KEY_FILE" not in os.environ


def test_browser_ui_dotenv_loader_never_populates_signer_keys(monkeypatch, tmp_path) -> None:
    import dotenv

    path = tmp_path / ".env"
    path.write_text(
        "HLCT_GUI_TOKEN=local-controller-token\n"
        "HLCT_API_PRIVATE_KEY=0x" + "ab" * 32 + "\n"
        "HLCT_API_PRIVATE_KEY_FILE=C:\\\\secrets\\\\legacy.key\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(dotenv, "find_dotenv", lambda: str(path))
    monkeypatch.delenv("PYTHON_DOTENV_DISABLED", raising=False)
    monkeypatch.delenv("HLCT_GUI_TOKEN", raising=False)
    monkeypatch.delenv("HLCT_API_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("HLCT_API_PRIVATE_KEY_FILE", raising=False)

    cli_module._load_env(
        excluded_keys=cli_module._BROWSER_UI_EXCLUDED_ENV_KEYS,
        dotenv_path=path,
    )

    assert os.environ["HLCT_GUI_TOKEN"] == "local-controller-token"
    assert "HLCT_API_PRIVATE_KEY" not in os.environ
    assert "HLCT_API_PRIVATE_KEY_FILE" not in os.environ


def test_browser_ui_dotenv_never_supplies_external_throttle_attestation(
    monkeypatch,
    tmp_path,
) -> None:
    path = tmp_path / ".env"
    path.write_text(
        "HLCT_GUI_TOKEN=local-controller-token\n"
        "HLCT_EXTERNAL_THROTTLE_ATTESTATION=CURRENT CONNECTION IS EXTERNALLY THROTTLED\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("PYTHON_DOTENV_DISABLED", raising=False)
    monkeypatch.delenv("HLCT_GUI_TOKEN", raising=False)
    monkeypatch.delenv("HLCT_EXTERNAL_THROTTLE_ATTESTATION", raising=False)

    cli_module._load_env(
        excluded_keys=cli_module._BROWSER_UI_DOTENV_EXCLUDED_ENV_KEYS,
        dotenv_path=path,
    )

    assert os.environ["HLCT_GUI_TOKEN"] == "local-controller-token"
    assert "HLCT_EXTERNAL_THROTTLE_ATTESTATION" not in os.environ


def test_fleet_repo_root_is_explicit_marker_validated_and_checkout_only(tmp_path) -> None:
    checkout = Path(cli_module.__file__).resolve().parents[2]
    assert cli_module._resolve_repo_root(checkout) == checkout

    with pytest.raises(ValueError, match="complete repository checkout"):
        cli_module._resolve_repo_root(tmp_path)

    for name in (
        "pyproject.toml",
        "src/hyperliquid_copytrader/__init__.py",
        "docs/ARCHITECTURE.md",
        "requirements/ci-lock.txt",
    ):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("marker\n", encoding="utf-8")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "tests").mkdir()
    with pytest.raises(ValueError, match="checkout-only"):
        cli_module._resolve_repo_root(tmp_path)


def test_cli_preflight_shadow_outputs_json(monkeypatch, tmp_path):
    _set_cli_env(monkeypatch, tmp_path)

    result = CliRunner().invoke(app, ["preflight", "--mode", "shadow"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["mode"] == "shadow"
    assert payload["passed"] is True
    assert payload["blockers"] == []


def test_cli_readiness_shadow_outputs_blocker_aware_snapshot(monkeypatch, tmp_path):
    _set_cli_env(monkeypatch, tmp_path)

    result = CliRunner().invoke(app, ["readiness", "--mode", "shadow"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["mode"] == "shadow"
    assert payload["readiness_label"] in {"shadow_operational", "shadow_blocked"}
    assert any(check["name"] == "local_preflight_passed" for check in payload["checks"])


def test_cli_verify_refreshes_exchange_truth_after_signed_preflight(monkeypatch):
    calls: list[str] = []

    class FakeStore:
        def count(self, _table: str) -> int:
            return 0

    class FakeService:
        store = FakeStore()

        def __init__(self, _config) -> None:
            pass

        def preflight(self) -> PreflightReport:
            calls.append("preflight")
            return PreflightReport(mode=Mode.TESTNET, passed=True)

        def refresh_readiness_truth(self) -> dict[str, object]:
            calls.append("refresh")
            return {"passed": True}

        def dashboard(self, *, security_cached: bool) -> dict[str, object]:
            assert security_cached is False
            calls.append("dashboard")
            return {
                "security": {"passed": True},
                "ops": {
                    "kill_switch_active": False,
                    "kill_switch_path": "KILL_SWITCH",
                    "pending_intent_count": 0,
                },
                "runtime": {},
            }

    monkeypatch.setattr(cli_module, "load_config", lambda _mode: SimpleNamespace(mode=Mode.TESTNET))
    monkeypatch.setattr(cli_module, "CopyTraderService", FakeService)
    monkeypatch.setattr(cli_module, "readiness_snapshot", lambda _dashboard: {"ready": True})

    result = CliRunner().invoke(app, ["verify", "--mode", "testnet"])

    assert result.exit_code == 0
    assert calls == ["preflight", "refresh", "dashboard"]
    assert json.loads(result.output)["truth_refresh"] == {"passed": True}


def test_operator_command_wrappers_use_one_service_boundary(monkeypatch) -> None:
    outputs: list[dict[str, object]] = []

    class FakeService:
        def __init__(self, _config) -> None:
            self.heartbeats: list[dict[str, object]] = []

        def record_runner_heartbeat(self, **payload):
            self.heartbeats.append(payload)
            return {}

        def source_follow_startup_sync_status(self):
            return {"ready": True}

        def readiness(self):
            return {"command": "readiness"}

        def refresh_readiness_truth(self):
            return {"command": "refresh"}

        async def observe_source_websocket(self, *, stop_after_messages):
            return {"command": "observe", "messages": stop_after_messages}

        def backfill_source_fills(self, **payload):
            return {"command": "backfill", **payload}

        def manual_reconcile(self):
            return {"command": "reconcile"}

    config = SimpleNamespace(mode=Mode.SHADOW)
    monkeypatch.setattr(cli_module, "_load_env", lambda **_kwargs: None)
    monkeypatch.setattr(cli_module, "load_config", lambda _mode: config)
    monkeypatch.setattr(cli_module, "CopyTraderService", FakeService)
    monkeypatch.setattr(cli_module, "_print_json", outputs.append)

    cli_module.readiness("shadow")
    cli_module.refresh_readiness_truth("shadow")
    cli_module.observe_source("shadow", messages=2)
    cli_module.backfill_source_fills("shadow", start_time_ms=10, end_time_ms=20)
    cli_module.reconcile("shadow")

    assert [payload["command"] for payload in outputs] == [
        "readiness",
        "refresh",
        "observe",
        "backfill",
        "reconcile",
    ]


def test_serve_constructs_continuous_app_without_starting_network(monkeypatch, tmp_path) -> None:
    import hyperliquid_copytrader.continuous_launch as continuous_launch
    import hyperliquid_copytrader.web.app as web_app_module
    import uvicorn

    config = SimpleNamespace(
        mode=Mode.SHADOW,
        host="127.0.0.1",
        port=8123,
        ops=SimpleNamespace(gui_token=""),
        db_path=tmp_path / "source.sqlite3",
    )
    created: list[dict[str, object]] = []
    served: list[dict[str, object]] = []

    class FakePaths:
        @staticmethod
        def build(*, repo_root, plan, engine_state_dir=None):
            assert engine_state_dir == tmp_path / "latest-engine"
            return FakePaths()

    class FakeController:
        def __init__(self, paths) -> None:
            self.paths = paths

    def fake_replace(value, **changes):
        return SimpleNamespace(**{**vars(value), **changes})

    def fake_create_app(**payload):
        created.append(payload)
        return SimpleNamespace(name="app")

    def fake_uvicorn_run(app, **payload):
        served.append({"app": app, **payload})

    monkeypatch.setattr(cli_module, "_load_env", lambda **_kwargs: None)
    monkeypatch.setattr(cli_module, "_scrub_browser_ui_signer_environment", lambda: None)
    monkeypatch.setattr(cli_module, "load_config", lambda _mode: config)
    monkeypatch.setattr(cli_module, "replace", fake_replace)
    monkeypatch.setattr(
        cli_module,
        "CopyTraderService",
        lambda selected, **payload: SimpleNamespace(config=selected, options=payload),
    )
    monkeypatch.setattr(continuous_launch, "ContinuousLaunchPaths", FakePaths)
    monkeypatch.setattr(continuous_launch, "ContinuousLaunchController", FakeController)
    monkeypatch.setattr(web_app_module, "create_app", fake_create_app)
    monkeypatch.setattr(uvicorn, "run", fake_uvicorn_run)

    checkout_root = Path(cli_module.__file__).resolve().parents[2]
    cli_module.serve(
        "shadow",
        repo_root=checkout_root,
        continuous_plan=tmp_path / "continuous-plan.json",
        continuous_engine_state_dir=tmp_path / "latest-engine",
    )
    assert len(created) == 1
    assert getattr(created[0]["service"], "options") == {"execution_enabled": False}
    assert isinstance(created[0]["continuous_launch_controller"], FakeController)
    assert "fleet_launch_controller" not in created[0]
    assert created[0]["credential_root"] == checkout_root
    assert served[0]["workers"] == 1
    assert served[0]["reload"] is False
