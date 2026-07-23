from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from hyperliquid_copytrader.continuous_runner import ARM_TOKEN, run_continuous_fleet


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the WS-first continuous fleet (monitor-only unless explicitly armed)."
    )
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument(
        "--engine-state-dir",
        type=Path,
        required=True,
        help="stable durable engine state reused across generation launches",
    )
    parser.add_argument("--duration", type=float)
    parser.add_argument("--stop-file", type=Path)
    parser.add_argument(
        "--arm",
        default="",
        help=f"exactly {ARM_TOKEN} enables IOC execution; empty is monitor-only",
    )
    parser.add_argument(
        "--operator-rearm",
        default="",
        help=(f"exactly {ARM_TOKEN} clears a completed fail-close latch after flat preflight"),
    )
    parser.add_argument(
        "--enable-rest-recovery-fallback",
        action="store_true",
        help="allow bounded HTTP fill repair only after a WS recovery failure",
    )
    args = parser.parse_args()
    try:
        result = asyncio.run(
            run_continuous_fleet(
                repo_root=args.repo_root,
                plan_path=args.plan,
                state_dir=args.state_dir,
                engine_state_dir=args.engine_state_dir,
                arm=args.arm,
                operator_rearm=args.operator_rearm,
                duration_s=args.duration,
                stop_file=args.stop_file,
                enable_rest_recovery_fallback=args.enable_rest_recovery_fallback,
            )
        )
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception as exc:
        print(
            json.dumps(
                {"status": "error", "error_type": type(exc).__name__, "error": str(exc)},
                sort_keys=True,
            )
        )
        raise SystemExit(2) from None
    print(
        json.dumps(
            {
                "status": result.status,
                "armed": result.armed,
                "status_path": str(result.status_path),
                "metrics_path": str(result.metrics_path),
                "engine_state_dir": str(result.engine_state_dir),
                "startup_http_requests": result.startup_http_requests,
                "startup_http_weight": result.startup_http_weight,
                "metrics_dropped": result.metrics_dropped,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
