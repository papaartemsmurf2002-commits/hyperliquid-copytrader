from __future__ import annotations

import json
import os
import secrets
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from time import time_ns
from typing import Any, Mapping

from .continuous_config import bind_continuous_plan, load_continuous_plan
from .credential_setup import CredentialSetupError, FleetCredentialProfileRegistry
from .windows_runtime import (
    WindowsProcessIdentity,
    atomic_json_write,
    exact_process_is_alive,
    inspect_process_identity,
    process_identity_payload,
    spawn_hidden_detached,
)


ARM_ACKNOWLEDGEMENT = "LIVE_CONTINUOUS"
STOP_ACKNOWLEDGEMENT = "STOP_CONTINUOUS"
LEADER_UPDATE_ACKNOWLEDGEMENT = "UPDATE_LEADERS"
FLEET_UPDATE_ACKNOWLEDGEMENT = "UPDATE_FLEET"


@dataclass(frozen=True, slots=True)
class ContinuousLaunchPaths:
    repo_root: Path
    plan: Path
    state_root: Path
    runner: Path
    engine_state_dir: Path | None = None

    @classmethod
    def build(
        cls,
        *,
        repo_root: Path | str,
        plan: Path | str,
        state_root: Path | str | None = None,
        engine_state_dir: Path | str | None = None,
    ) -> ContinuousLaunchPaths:
        repo = Path(repo_root).resolve()
        local = Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local")))
        return cls(
            repo_root=repo,
            plan=Path(plan).resolve(),
            state_root=Path(
                state_root or local / "HyperliquidCopytrader" / "runtime" / "continuous-ui"
            ).resolve(),
            runner=(repo / "scripts" / "run_continuous_fleet.py").resolve(),
            engine_state_dir=(
                None if engine_state_dir is None else Path(engine_state_dir).resolve()
            ),
        )


class ContinuousLaunchController:
    """Small browser-to-runner process boundary; trading policy stays in the runner."""

    def __init__(self, paths: ContinuousLaunchPaths) -> None:
        self.paths = paths
        self.paths.state_root.mkdir(parents=True, exist_ok=True)
        self._record_path = self.paths.state_root / "controller.json"
        self._lock = threading.RLock()

    def preview(self) -> dict[str, Any]:
        blockers: list[str] = []
        plan_payload: dict[str, Any] | None = None
        engine_state_dir: Path | None = None
        try:
            plan = load_continuous_plan(self.paths.plan)
            engine_state_dir = self.paths.engine_state_dir or self._engine_state_dir(
                network=plan.network,
                runtime_id=plan.runtime_id,
            )
            plan_payload = {
                "path": str(plan.path),
                "sha256": plan.sha256,
                "runtime_id": plan.runtime_id,
                "network": plan.network,
                "max_combined_gross_usd": str(plan.max_combined_gross_usd),
                "slots": [
                    {
                        "slot": slot.slot,
                        "source": slot.source_address,
                        "follower": slot.follower_account_address,
                        "credential_profile_id": slot.credential_profile_id,
                        "multiplier": str(slot.multiplier),
                        "max_order_notional_usd": str(slot.max_order_notional_usd),
                        "max_gross_exposure_usd": str(slot.max_gross_exposure_usd),
                        "max_open_positions": slot.max_open_positions,
                        "max_leverage": slot.max_leverage,
                        "action_limit_per_minute": slot.action_limit_per_minute,
                        "market_policy": "credential_profile",
                        "enabled": slot.enabled,
                    }
                    for slot in plan.slots
                ],
            }
        except ValueError as exc:
            blockers.append(str(exc))
        if not self.paths.runner.is_file():
            blockers.append(f"continuous runner is missing: {self.paths.runner}")
        current = self.status()
        if current.get("online") is True:
            blockers.append("continuous runner is already active")
        return {
            "launchable": not blockers,
            "blockers": blockers,
            "acknowledgement": ARM_ACKNOWLEDGEMENT,
            "runner": str(self.paths.runner),
            "recovery_rest_enabled": True,
            "engine_state_dir": str(engine_state_dir) if engine_state_dir is not None else None,
            "plan": plan_payload,
            "current": current,
        }

    def start(self, *, acknowledgement: str) -> dict[str, Any]:
        if acknowledgement != ARM_ACKNOWLEDGEMENT:
            raise ValueError(f"acknowledgement must equal {ARM_ACKNOWLEDGEMENT}")
        with self._lock:
            current = self.status()
            if current.get("online") is True:
                return current
            preview = self.preview()
            if preview["launchable"] is not True:
                raise RuntimeError(
                    "continuous launch is blocked: " + "; ".join(preview["blockers"])
                )
            generation = f"continuous-{time_ns() // 1_000_000}-{secrets.token_hex(4)}"
            state_dir = (self.paths.state_root / generation).resolve()
            state_dir.mkdir(parents=True, exist_ok=False)
            engine_state_dir = Path(str(preview["engine_state_dir"])).resolve()
            engine_state_dir.mkdir(parents=True, exist_ok=True)
            stop_file = state_dir / "stop.requested.json"
            stdout_path, stderr_path = state_dir / "stdout.log", state_dir / "stderr.log"
            argv = [
                sys.executable,
                str(self.paths.runner),
                "--repo-root",
                str(self.paths.repo_root),
                "--plan",
                str(self.paths.plan),
                "--state-dir",
                str(state_dir),
                "--engine-state-dir",
                str(engine_state_dir),
                "--stop-file",
                str(stop_file),
                "--enable-rest-recovery-fallback",
                "--arm",
                ARM_ACKNOWLEDGEMENT,
                "--operator-rearm",
                ARM_ACKNOWLEDGEMENT,
            ]
            process = None
            try:
                with (
                    stdout_path.open("ab", buffering=0) as stdout,
                    stderr_path.open("ab", buffering=0) as stderr,
                ):
                    process = spawn_hidden_detached(
                        argv,
                        cwd=self.paths.repo_root,
                        stdout=stdout,
                        stderr=stderr,
                        env={**os.environ, "PYTHONUNBUFFERED": "1"},
                    )
                identity = inspect_process_identity(process.pid)
                record = {
                    "version": 1,
                    "generation": generation,
                    "created_ms": time_ns() // 1_000_000,
                    "plan": str(self.paths.plan),
                    "state_dir": str(state_dir),
                    "engine_state_dir": str(engine_state_dir),
                    "stop_file": str(stop_file),
                    "stdout": str(stdout_path),
                    "stderr": str(stderr_path),
                    "process": process_identity_payload(identity),
                }
                atomic_json_write(self._record_path, record)
            except Exception:
                if process is not None:
                    process.terminate()
                raise
            return self.status()

    def update_leaders(
        self,
        *,
        leaders: Mapping[str, Any],
        acknowledgement: str,
    ) -> dict[str, Any]:
        if acknowledgement != LEADER_UPDATE_ACKNOWLEDGEMENT:
            raise ValueError(f"acknowledgement must equal {LEADER_UPDATE_ACKNOWLEDGEMENT}")
        with self._lock:
            if self.status().get("online") is True:
                raise RuntimeError("leaders cannot change while the continuous runner is active")
            plan = load_continuous_plan(self.paths.plan)
            known_slots = {slot.slot: slot for slot in plan.slots}
            normalized: dict[str, str] = {}
            for raw_slot, raw_address in leaders.items():
                slot_id = str(raw_slot or "").strip().lower()
                if slot_id not in known_slots:
                    raise ValueError(f"unknown continuous slot: {slot_id or '<empty>'}")
                normalized[slot_id] = str(raw_address or "").strip().lower()
            if not normalized:
                raise ValueError("at least one leader address is required")

            raw = _read_json(self.paths.plan, maximum_bytes=1_048_576)
            if raw is None or not isinstance(raw.get("slots"), list):
                raise ValueError("continuous plan is unreadable")
            updated_slots: list[dict[str, Any]] = []
            profile_sources: dict[str, str] = {}
            old_profile_sources: dict[str, str] = {}
            for item in raw["slots"]:
                if not isinstance(item, Mapping):
                    raise ValueError("continuous plan contains a malformed slot")
                slot_payload = {
                    key: value for key, value in item.items() if key != "allowed_markets"
                }
                slot_id = str(slot_payload.get("slot") or "").lower()
                if slot_id in normalized:
                    slot_payload["source_address"] = normalized[slot_id]
                    profile_id = str(slot_payload.get("credential_profile_id") or "").lower()
                    profile_sources[profile_id] = normalized[slot_id]
                    old_profile_sources[profile_id] = known_slots[slot_id].source_address
                updated_slots.append(slot_payload)
            raw["slots"] = updated_slots
            # Leader attribution is durable identity. Give the edited plan a
            # fresh engine state instead of mixing two leaders in one journal.
            raw["runtime_id"] = f"fleet-{time_ns() // 1_000_000}"

            candidate = self.paths.state_root / f"leader-update-{secrets.token_hex(6)}.json"
            registry = FleetCredentialProfileRegistry(self.paths.repo_root)
            try:
                atomic_json_write(candidate, raw)
                load_continuous_plan(candidate)
                registry.replace_sources(profile_sources)
                try:
                    os.replace(candidate, self.paths.plan)
                except Exception:
                    registry.replace_sources(old_profile_sources)
                    raise
            except CredentialSetupError as exc:
                raise ValueError(str(exc)) from exc
            finally:
                try:
                    candidate.unlink()
                except FileNotFoundError:
                    pass
            return self.preview()

    def update_fleet(
        self,
        *,
        slots: list[Mapping[str, Any]],
        max_combined_gross_usd: Any,
        acknowledgement: str,
    ) -> dict[str, Any]:
        """Replace the offline fleet plan using existing isolated signer profiles."""

        if acknowledgement != FLEET_UPDATE_ACKNOWLEDGEMENT:
            raise ValueError(f"acknowledgement must equal {FLEET_UPDATE_ACKNOWLEDGEMENT}")
        with self._lock:
            if self.status().get("online") is True:
                raise RuntimeError("fleet configuration cannot change while the runner is active")
            if not isinstance(slots, list):
                raise ValueError("fleet slots must be a list")
            raw = _read_json(self.paths.plan, maximum_bytes=1_048_576)
            if raw is None:
                raise ValueError("continuous plan is unreadable")
            raw["slots"] = [
                {key: value for key, value in slot.items() if key != "allowed_markets"}
                for slot in slots
            ]
            raw["max_combined_gross_usd"] = str(max_combined_gross_usd)
            raw["runtime_id"] = f"fleet-{time_ns() // 1_000_000}"

            candidate = self.paths.state_root / f"fleet-update-{secrets.token_hex(6)}.json"
            registry = FleetCredentialProfileRegistry(self.paths.repo_root)
            records, invalid = registry._records_with_health(verify_secrets=False)
            if invalid:
                raise ValueError(
                    "invalid credential profiles must be repaired first: "
                    + ", ".join(sorted(invalid))
                )
            existing_sources = {
                str(record["profile_id"]): str(record["source_wallet"]).lower()
                for record in records
            }
            profile_sources: dict[str, str] = {}
            for slot in raw["slots"]:
                profile_id = str(slot.get("credential_profile_id") or "").strip().lower()
                source = str(slot.get("source_address") or "").strip().lower()
                profile_sources[profile_id] = source
            old_sources = {
                profile_id: existing_sources[profile_id]
                for profile_id in profile_sources
                if profile_id in existing_sources
            }
            profiles_changed = False
            try:
                atomic_json_write(candidate, raw)
                candidate_plan = load_continuous_plan(candidate)
                registry.replace_sources(profile_sources)
                profiles_changed = True
                bind_continuous_plan(
                    candidate_plan,
                    repo_root=self.paths.repo_root,
                    verify_secrets=False,
                )
                os.replace(candidate, self.paths.plan)
            except CredentialSetupError as exc:
                if profiles_changed and old_sources:
                    registry.replace_sources(old_sources)
                raise ValueError(str(exc)) from exc
            except Exception:
                if profiles_changed and old_sources:
                    registry.replace_sources(old_sources)
                raise
            finally:
                try:
                    candidate.unlink()
                except FileNotFoundError:
                    pass
            return self.preview()

    def stop(self, *, acknowledgement: str) -> dict[str, Any]:
        if acknowledgement != STOP_ACKNOWLEDGEMENT:
            raise ValueError(f"acknowledgement must equal {STOP_ACKNOWLEDGEMENT}")
        with self._lock:
            record = self._record()
            if record is None:
                return self.status()
            identity = _identity(record.get("process"))
            if identity is None or not exact_process_is_alive(identity):
                return self.status()
            stop_file = Path(str(record["stop_file"])).resolve()
            atomic_json_write(
                stop_file,
                {
                    "version": 1,
                    "generation": record.get("generation"),
                    "requested_ms": time_ns() // 1_000_000,
                    "reason": "operator_stop",
                },
            )
            return self.status()

    def status(self) -> dict[str, Any]:
        record = self._record()
        if record is None:
            return {
                "status": "idle",
                "online": False,
                "execution_enabled": False,
                "plan": str(self.paths.plan),
            }
        identity = _identity(record.get("process"))
        online = identity is not None and exact_process_is_alive(identity)
        state_dir = Path(str(record.get("state_dir") or "")).resolve()
        runner_status = _read_json(state_dir / "status.json") or {}
        stop_requested = Path(str(record.get("stop_file") or "")).is_file()
        raw_status = str(runner_status.get("status") or "").strip().lower()
        if online:
            status = "stopping" if stop_requested else raw_status or "starting"
        else:
            status = raw_status if raw_status in {"stopped", "error"} else "exited"
        updated_ms = _optional_positive_int(runner_status.get("updated_ms"))
        status_age_ms = None if updated_ms is None else max(0, time_ns() // 1_000_000 - updated_ms)
        slots = runner_status.get("slots")
        slot_rows = slots if isinstance(slots, Mapping) else {}
        ready_slots = sum(
            1
            for value in slot_rows.values()
            if isinstance(value, Mapping)
            and value.get("state") == "RUNNING"
            and value.get("data_stale") is not True
        )
        stale = bool(
            online and raw_status == "running" and (status_age_ms is None or status_age_ms > 5_000)
        )
        healthy = bool(
            online
            and not stale
            and raw_status == "running"
            and slot_rows
            and ready_slots == len(slot_rows)
        )
        return {
            "status": status,
            "online": online,
            "healthy": healthy,
            "stale": stale,
            "status_age_ms": status_age_ms,
            "ready_slots": ready_slots,
            "reported_slots": len(slot_rows),
            # A live PID is not proof that the runner parsed the arm token and
            # constructed an execution-enabled engine. Only runner-owned status
            # may assert that capability.
            "execution_enabled": online and runner_status.get("execution_enabled") is True,
            "generation": record.get("generation"),
            "plan": record.get("plan"),
            "state_dir": str(state_dir),
            "engine_state_dir": record.get("engine_state_dir"),
            "stop_requested": stop_requested,
            "process": record.get("process"),
            "runner": runner_status,
            "stdout": record.get("stdout"),
            "stderr": record.get("stderr"),
        }

    def _record(self) -> dict[str, Any] | None:
        return _read_json(self._record_path)

    def _engine_state_dir(
        self,
        *,
        network: str,
        runtime_id: str,
    ) -> Path:
        # Both path components are validated by load_continuous_plan. A stable
        # runtime identity must retain old UNKNOWN/SEND_ATTEMPTED actions and
        # attribution across plan edits; the runtime's persisted identity check
        # blocks incompatible revisions instead of silently forking truth.
        return (self.paths.state_root / "engines" / network / runtime_id).resolve()


def _identity(payload: Any) -> WindowsProcessIdentity | None:
    if not isinstance(payload, Mapping):
        return None
    try:
        return WindowsProcessIdentity(
            pid=int(payload["pid"]),
            creation_filetime=int(payload["creation_filetime"]),
            image_path=str(payload["image_path"]),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _read_json(path: Path, *, maximum_bytes: int = 65_536) -> dict[str, Any] | None:
    try:
        raw = path.read_bytes()
        if len(raw) > maximum_bytes:
            return None
        payload = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return dict(payload) if isinstance(payload, Mapping) else None


def _optional_positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None
