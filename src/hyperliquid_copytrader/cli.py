from __future__ import annotations

import asyncio
import json
import os
from dataclasses import replace
from pathlib import Path
from typing import Optional

import typer

from .config import load_config
from .models import Mode, to_jsonable
from .network_evidence import EXTERNAL_THROTTLE_ATTESTATION_ENV
from .ops import readiness_snapshot
from .service import CopyTraderService


app = typer.Typer(help="Hyperliquid copy-trader operator CLI.")


_BROWSER_UI_EXCLUDED_ENV_KEYS = frozenset({"HLCT_API_PRIVATE_KEY", "HLCT_API_PRIVATE_KEY_FILE"})
_BROWSER_UI_DOTENV_EXCLUDED_ENV_KEYS = _BROWSER_UI_EXCLUDED_ENV_KEYS | {
    EXTERNAL_THROTTLE_ATTESTATION_ENV
}


def _load_env(
    *,
    excluded_keys: frozenset[str] = frozenset(),
    dotenv_path: Path | None = None,
) -> None:
    # Do not rely on python-dotenv's own disable switch here. Older versions allowed by
    # the project dependency range do not honor it, and validation children must never
    # let a workspace .env file repopulate policy or credential variables deliberately
    # stripped by their controller.
    dotenv_disabled = os.getenv("PYTHON_DOTENV_DISABLED", "").strip().lower()
    if dotenv_disabled in {"1", "true", "yes", "on"}:
        return
    try:
        from dotenv import dotenv_values, find_dotenv, load_dotenv
    except Exception:
        return
    if not excluded_keys:
        if dotenv_path is None:
            load_dotenv()
        else:
            load_dotenv(dotenv_path=dotenv_path)
        return
    selected_path: str | Path = dotenv_path if dotenv_path is not None else find_dotenv()
    if not selected_path or not Path(selected_path).is_file():
        return
    for key, value in dotenv_values(selected_path).items():
        if key not in excluded_keys and value is not None:
            os.environ.setdefault(key, value)


def _scrub_browser_ui_signer_environment() -> None:
    """Keep the browser UI process credential-free even when .env has signer keys."""

    for key in _BROWSER_UI_EXCLUDED_ENV_KEYS:
        os.environ.pop(key, None)


def _resolve_repo_root(repo_root: Path | None) -> Path:
    """Bind the browser controller and its children to one source checkout."""

    root = (Path.cwd() if repo_root is None else repo_root).resolve()
    required_files = (
        root / "pyproject.toml",
        root / "src" / "hyperliquid_copytrader" / "__init__.py",
        root / "docs" / "ARCHITECTURE.md",
        root / "requirements" / "ci-lock.txt",
    )
    required_dirs = (root / "scripts", root / "tests")
    missing = [path.relative_to(root).as_posix() for path in required_files if not path.is_file()]
    missing.extend(
        f"{path.relative_to(root).as_posix()}/" for path in required_dirs if not path.is_dir()
    )
    if missing:
        raise ValueError(
            f"continuous serve requires a complete repository checkout: {sorted(missing)}"
        )
    package_root = (root / "src" / "hyperliquid_copytrader").resolve()
    imported_package_file = Path(__file__).resolve()
    if not imported_package_file.is_relative_to(package_root):
        raise ValueError(
            "continuous serve is checkout-only: the running controller package is not from "
            f"{package_root}"
        )
    return root


def _print_json(payload: object) -> None:
    typer.echo(json.dumps(to_jsonable(payload), indent=2, sort_keys=True))


@app.command()
def preflight(
    mode: Optional[str] = typer.Option(None, help="Override mode for this check."),
) -> None:
    _load_env()
    config = load_config(mode)
    service = CopyTraderService(config)
    _print_json(service.preflight())


@app.command()
def verify(
    mode: Optional[str] = typer.Option(None, help="Override mode for this verification."),
) -> None:
    _load_env()
    config = load_config(mode)
    service = CopyTraderService(config)
    preflight_report = service.preflight()
    truth_refresh = None
    if config.mode in {Mode.TESTNET, Mode.LIVE} and preflight_report.passed:
        truth_refresh = service.refresh_readiness_truth()
    dashboard = service.dashboard(security_cached=False)
    payload = {
        "mode": config.mode.value,
        "preflight": preflight_report,
        "truth_refresh": truth_refresh,
        "readiness": readiness_snapshot(dashboard),
        "security": dashboard["security"],
        "kill_switch_active": dashboard["ops"]["kill_switch_active"],
        "kill_switch_path": dashboard["ops"]["kill_switch_path"],
        "pending_intent_count": dashboard["ops"]["pending_intent_count"],
        "runtime": dashboard["runtime"],
        "ops": dashboard["ops"],
        "journals": {
            "source_events": service.store.count("source_events"),
            "desired_states": service.store.count("desired_states"),
            "follower_intents": service.store.count("follower_intents"),
            "execution_reports": service.store.count("execution_reports"),
            "safe_mode_transitions": service.store.count("safe_mode_transitions"),
        },
    }
    _print_json(payload)


@app.command()
def readiness(
    mode: Optional[str] = typer.Option(None, help="Override mode for this readiness check."),
) -> None:
    _load_env()
    service = CopyTraderService(load_config(mode))
    _print_json(service.readiness())


@app.command("refresh-readiness-truth")
def refresh_readiness_truth(
    mode: Optional[str] = typer.Option(None, help="Override mode for this read-only refresh."),
) -> None:
    """Refresh source/follower snapshots without planning or placing an order."""
    _load_env()
    service = CopyTraderService(load_config(mode))
    _print_json(service.refresh_readiness_truth())


@app.command("observe-source")
def observe_source(
    mode: Optional[str] = typer.Option(None, help="Override mode for websocket observation."),
    messages: Optional[int] = typer.Option(
        None,
        help="Stop after this many non-heartbeat source websocket messages.",
    ),
) -> None:
    _load_env()
    service = CopyTraderService(load_config(mode))
    _print_json(asyncio.run(service.observe_source_websocket(stop_after_messages=messages)))


@app.command("backfill-source-fills")
def backfill_source_fills(
    mode: Optional[str] = typer.Option(
        None, help="Override mode for this source fill/TWAP backfill."
    ),
    start_time_ms: Optional[int] = typer.Option(None, help="Inclusive start time in epoch ms."),
    end_time_ms: Optional[int] = typer.Option(None, help="Inclusive end time in epoch ms."),
) -> None:
    _load_env()
    service = CopyTraderService(load_config(mode))
    _print_json(
        service.backfill_source_fills(
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
    )


@app.command()
def reconcile(
    mode: Optional[str] = typer.Option(None, help="Override mode for this reconcile."),
) -> None:
    _load_env()
    service = CopyTraderService(load_config(mode))
    _print_json(service.manual_reconcile())


@app.command()
def serve(
    mode: Optional[str] = typer.Option(None, help="Override mode for the GUI."),
    repo_root: Optional[Path] = typer.Option(
        None,
        "--repo-root",
        help="Exact source checkout used for the continuous plan, UI state, and child process.",
    ),
    continuous_plan: Optional[Path] = typer.Option(
        None,
        "--continuous-plan",
        help="Explicit continuous-runtime plan exposed to the local start/stop panel.",
    ),
    continuous_engine_state_dir: Optional[Path] = typer.Option(
        None,
        "--continuous-engine-state-dir",
        help="Explicit durable engine state used by the continuous start button.",
    ),
) -> None:
    # The UI remains credential-free. The continuous child loads only its
    # profile-bound key files after the operator explicitly arms it.
    _scrub_browser_ui_signer_environment()
    try:
        resolved_repo_root = _resolve_repo_root(repo_root)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--repo-root") from exc
    _load_env(
        excluded_keys=_BROWSER_UI_DOTENV_EXCLUDED_ENV_KEYS,
        dotenv_path=resolved_repo_root / ".env",
    )
    if mode:
        os.environ["HLCT_MODE"] = mode
    config = load_config(mode)
    import ipaddress

    try:
        literal_host = ipaddress.ip_address(config.host)
    except ValueError as exc:
        raise typer.BadParameter(
            "continuous-capable browser UI requires a numeric loopback bind",
            param_hint="--mode",
        ) from exc
    if not literal_host.is_loopback:
        raise typer.BadParameter(
            "continuous-capable browser UI requires a numeric loopback bind",
            param_hint="--mode",
        )
    from .continuous_launch import ContinuousLaunchController, ContinuousLaunchPaths
    from .web.app import create_app

    local = Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local")))
    ui_state_dir = local / "HyperliquidCopytrader" / "ui"
    ui_state_dir.mkdir(parents=True, exist_ok=True)
    config = replace(config, db_path=ui_state_dir / "ui-control.sqlite3")
    service = CopyTraderService(config, execution_enabled=False)
    continuous_controller = None
    if continuous_plan is not None:
        launch_paths = ContinuousLaunchPaths.build(
            repo_root=resolved_repo_root,
            plan=continuous_plan,
            engine_state_dir=continuous_engine_state_dir,
        )
        continuous_controller = ContinuousLaunchController(launch_paths)
    web_app = create_app(
        service=service,
        credential_root=resolved_repo_root,
        continuous_launch_controller=continuous_controller,
    )
    import uvicorn

    uvicorn.run(
        web_app,
        host=config.host,
        port=config.port,
        reload=False,
        workers=1,
    )


if __name__ == "__main__":
    app()
