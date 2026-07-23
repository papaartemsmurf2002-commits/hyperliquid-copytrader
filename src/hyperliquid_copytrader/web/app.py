from __future__ import annotations

import asyncio
import base64
import ipaddress
import json
import secrets
from collections.abc import Callable
from contextlib import asynccontextmanager
from decimal import Decimal, DecimalException
from hmac import compare_digest
from pathlib import Path
from threading import Lock
from time import monotonic
from urllib.parse import urlsplit

from fastapi import FastAPI, Form, HTTPException, Request, status as http_status
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from ..address_analysis import AddressAnalysisService, valid_address
from ..config import MAINNET_REST, TESTNET_REST, load_config
from ..continuous_launch import ContinuousLaunchController
from ..credential_setup import (
    CredentialProfileStore,
    CredentialSetupError,
    FleetCredentialProfileRegistry,
    SubaccountResolver,
)
from ..leaderboard import LeaderboardService
from ..market_catalog import PublicMarketInfoClient, resolve_public_market_universe
from ..markets import MarketIdentityError
from ..models import Mode, SafeModeReason
from ..observer import HyperliquidInfoClient
from ..preflight import is_loopback_host
from ..run_history import RunHistoryService
from ..runtime import SlidingWindowRateLimiter
from ..service import CopyTraderService


TEMPLATE_DIR = Path(__file__).with_name("templates")
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))
SECURITY_HEADERS = {
    "Cache-Control": "no-store",
    "Cross-Origin-Opener-Policy": "same-origin",
    "Cross-Origin-Resource-Policy": "same-origin",
    "Permissions-Policy": "camera=(), geolocation=(), microphone=(), payment=(), usb=()",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
}


def _content_security_policy(nonce: str) -> str:
    return (
        "default-src 'self'; "
        f"script-src 'self' 'nonce-{nonce}'; "
        f"style-src 'self' 'nonce-{nonce}'; "
        "img-src 'self' data:; "
        "connect-src 'self'; "
        "object-src 'none'; "
        "base-uri 'self'; "
        "form-action 'self'; "
        "frame-ancestors 'none'"
    )


def _origin_matches_request(request: Request, origin: str) -> bool:
    parsed = urlsplit(origin)
    if not parsed.scheme or not parsed.netloc:
        return False
    request_host = request.headers.get("host", request.url.netloc).lower()
    return (
        parsed.scheme.lower() == request.url.scheme.lower()
        and parsed.netloc.lower() == request_host
    )


def _client_is_local(request: Request) -> bool:
    host = (request.client.host if request.client else "").lower()
    return host in {"127.0.0.1", "::1", "localhost", "testclient"}


def _request_uses_trusted_loopback_authority(request: Request) -> bool:
    raw_host = request.headers.get("host", "").strip()
    parsed_host = urlsplit(f"//{raw_host}").hostname
    host = (parsed_host or "").lower()
    if host in {"127.0.0.1", "::1", "localhost"}:
        return _client_is_local(request)
    client_host = (request.client.host if request.client else "").lower()
    return client_host == "testclient" and host == "testserver"


def _assert_analytics_rate(app: FastAPI, *, force_refresh: bool) -> None:
    for limiter, detail in (
        (app.state.analytics_query_rate_limiter, "analytics query rate limit hit"),
        *(
            ((app.state.analytics_refresh_rate_limiter, "analytics refresh rate limit hit"),)
            if force_refresh
            else ()
        ),
    ):
        decision = limiter.check()
        if not decision.ok:
            raise HTTPException(http_status.HTTP_429_TOO_MANY_REQUESTS, detail)
        limiter.record()


def _assert_settings_allowed(request: Request, control_name: str) -> None:
    def deny(status_code: int, detail: str) -> None:
        _audit_control(request, control_name, "denied", detail)
        raise HTTPException(status_code, detail)

    if not _request_uses_trusted_loopback_authority(request):
        deny(http_status.HTTP_403_FORBIDDEN, "credential setup requires literal loopback access")
    origin = request.headers.get("origin")
    referer = request.headers.get("referer")
    if not origin and not referer:
        deny(
            http_status.HTTP_403_FORBIDDEN, "credential setup requires same-origin browser evidence"
        )
    if origin and not _origin_matches_request(request, origin):
        deny(http_status.HTTP_403_FORBIDDEN, "cross-origin credential request blocked")
    if not origin and referer and not _origin_matches_request(request, referer):
        deny(http_status.HTTP_403_FORBIDDEN, "cross-origin credential request blocked")
    limiter = request.app.state.settings_rate_limiter
    decision = limiter.check()
    if not decision.ok:
        deny(http_status.HTTP_429_TOO_MANY_REQUESTS, "credential setup rate limit hit")
    limiter.record()


def _literal_loopback(value: str) -> bool:
    try:
        return ipaddress.ip_address(value).is_loopback
    except ValueError:
        return False


def _assert_launch_request(
    request: Request,
    *,
    controller_attribute: str,
    label: str,
    require_origin: bool,
) -> None:
    controller = getattr(request.app.state, controller_attribute, None)
    if controller is None:
        raise HTTPException(
            http_status.HTTP_503_SERVICE_UNAVAILABLE,
            f"{label} controller is not configured",
        )
    raw_host = request.headers.get("host", "").strip()
    host = urlsplit(f"//{raw_host}").hostname or ""
    peer = request.client.host if request.client else ""
    if not _literal_loopback(host) or not _literal_loopback(peer):
        raise HTTPException(
            http_status.HTTP_421_MISDIRECTED_REQUEST,
            f"{label} requires numeric literal-loopback Host and peer",
        )
    token = request.app.state.service.config.ops.gui_token
    if token and not getattr(request.state, "operator_authenticated", False):
        raise HTTPException(http_status.HTTP_401_UNAUTHORIZED, "operator token required")
    if require_origin:
        origin = request.headers.get("origin", "")
        if not origin or not _origin_matches_request(request, origin):
            raise HTTPException(
                http_status.HTTP_403_FORBIDDEN,
                f"{label} mutation requires exact same-origin browser evidence",
            )


def _exact_payload(payload: dict, keys: set[str]) -> None:
    if set(payload) != keys:
        raise HTTPException(
            http_status.HTTP_422_UNPROCESSABLE_CONTENT,
            "request JSON fields do not match the fleet route contract",
        )


async def _bounded_json_object(request: Request, *, max_bytes: int = 4_096) -> dict:
    body = await request.body()
    if len(body) > max_bytes:
        raise HTTPException(http_status.HTTP_413_CONTENT_TOO_LARGE, "request is too large")
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HTTPException(
            http_status.HTTP_422_UNPROCESSABLE_CONTENT, "request must be valid JSON"
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(
            http_status.HTTP_422_UNPROCESSABLE_CONTENT, "request must be a JSON object"
        )
    return payload


def _control_audit_payload(request: Request) -> dict:
    service = request.app.state.service
    return {
        "client": request.client.host if request.client else "",
        "method": request.method,
        "path": request.url.path,
        "origin": request.headers.get("origin", ""),
        "referer_present": bool(request.headers.get("referer")),
        "user_agent": request.headers.get("user-agent", "")[:200],
        "token_configured": bool(service.config.ops.gui_token),
        "token_supplied": bool(
            request.headers.get("x-operator-token", "")
            or getattr(request.state, "operator_authenticated", False)
        ),
    }


def _request_has_operator_auth(request: Request, configured_token: str) -> bool:
    header_token = request.headers.get("x-operator-token", "")
    if header_token and compare_digest(header_token, configured_token):
        return True
    scheme, _, encoded = request.headers.get("authorization", "").partition(" ")
    if scheme.lower() != "basic" or not encoded:
        return False
    try:
        decoded = base64.b64decode(encoded, validate=True).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return False
    username, separator, password = decoded.partition(":")
    return bool(separator and username == "operator" and compare_digest(password, configured_token))


def _audit_control(
    request: Request,
    control_name: str,
    status: str,
    detail: str,
) -> None:
    request.app.state.service.store.append_control_audit(
        control=control_name,
        status=status,
        detail=detail[:300],
        payload=_control_audit_payload(request),
    )


def _assert_control_allowed(request: Request, control_name: str) -> None:
    service = request.app.state.service

    def deny(status_code: int, detail: str) -> None:
        _audit_control(request, control_name, "denied", detail)
        raise HTTPException(status_code, detail)

    if not request.app.state.control_authority:
        deny(
            http_status.HTTP_409_CONFLICT,
            "dashboard is monitor-only; start the integrated console to use controls",
        )
    configured_token = service.config.ops.gui_token
    if not configured_token and not _request_uses_trusted_loopback_authority(request):
        deny(
            http_status.HTTP_403_FORBIDDEN,
            "unauthenticated controls require a literal loopback Host",
        )
    origin = request.headers.get("origin")
    referer = request.headers.get("referer")
    if origin and not _origin_matches_request(request, origin):
        deny(http_status.HTTP_403_FORBIDDEN, "cross-origin control request blocked")
    if not origin and referer and not _origin_matches_request(request, referer):
        deny(http_status.HTTP_403_FORBIDDEN, "cross-origin control request blocked")
    if not origin and not referer and not _client_is_local(request):
        deny(http_status.HTTP_403_FORBIDDEN, "control request missing same-origin evidence")
    if configured_token:
        if not getattr(request.state, "operator_authenticated", False):
            deny(http_status.HTTP_403_FORBIDDEN, "operator token required")
    if control_name != "pause":
        limiter = request.app.state.control_rate_limiter
        decision = limiter.check()
        if not decision.ok:
            deny(http_status.HTTP_429_TOO_MANY_REQUESTS, "dashboard control rate limit hit")
        limiter.record()


def _control_failure_reason(service: CopyTraderService, exc: Exception) -> SafeModeReason:
    detail = str(exc).lower()
    local_validation = (
        isinstance(exc, (ValueError, DecimalException))
        or "only runs in testnet mode" in detail
        or "missing from testnet mids or metadata" in detail
        or "would exceed max notional cap" in detail
    )
    if local_validation or service.config.mode not in {Mode.TESTNET, Mode.LIVE}:
        return SafeModeReason.CONFIG_INVALID
    return SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE


def _blocked_control_result(result: object) -> str | None:
    if not isinstance(result, dict):
        return None
    safe_mode = result.get("safe_mode")
    safe_mode_enabled = isinstance(safe_mode, dict) and safe_mode.get("enabled") is True
    explicit_failure = result.get("passed") is False or result.get("ok") is False
    try:
        pending_after = int(result.get("pending_after") or 0)
    except (TypeError, ValueError):
        pending_after = 1
    incomplete_settlement = (
        any(bool(result.get(field)) for field in ("ambiguous", "errors", "still_open"))
        or pending_after > 0
    )
    if not (safe_mode_enabled or explicit_failure or incomplete_settlement):
        return None
    detail = ""
    if isinstance(safe_mode, dict):
        detail = str(safe_mode.get("detail") or safe_mode.get("reason") or "")
    if not detail:
        detail = str(result.get("detail") or result.get("status") or "blocked outcome")
    return detail[:220]


def _run_control(
    request: Request, control_name: str, action: Callable[[], object]
) -> RedirectResponse:
    service = request.app.state.service
    audit_status = "success"
    audit_detail = "control accepted"
    try:
        if control_name == "pause":
            result = action()
        else:
            with request.app.state.service_operation_lock:
                result = action()
        blocked_detail = _blocked_control_result(result)
        if control_name != "pause" and blocked_detail:
            audit_status = "blocked"
            audit_detail = f"control returned a blocked outcome: {blocked_detail}"
    except HTTPException:
        raise
    except Exception as exc:
        reason = _control_failure_reason(service, exc)
        detail = f"GUI control {control_name} failed: {exc}"
        audit_status = "failed"
        audit_detail = detail
        if reason == SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE:
            service.shield.exchange_error(detail)
        else:
            service.safe_mode.trip(reason, detail)
    _audit_control(request, control_name, audit_status, audit_detail)
    service.invalidate_security_audit_cache()
    return RedirectResponse("/", status_code=303)


def create_app(
    *,
    service: CopyTraderService | None = None,
    run_history_service: RunHistoryService | None = None,
    control_authority: bool = False,
    start_worker: bool = False,
    worker_interval_s: float = 5.0,
    credential_root: Path | None = None,
    subaccount_resolver: SubaccountResolver | None = None,
    market_catalog_client_factory: Callable[[str], PublicMarketInfoClient] | None = None,
    continuous_launch_controller: ContinuousLaunchController | None = None,
) -> FastAPI:
    if worker_interval_s <= 0:
        raise ValueError("worker_interval_s must be positive")
    config = service.config if service is not None else load_config()
    if not is_loopback_host(config.host):
        raise ValueError(
            "direct non-loopback GUI binding is unsupported; use a trusted TLS reverse proxy"
        )
    if start_worker and not control_authority:
        raise ValueError("integrated worker requires control authority")
    if control_authority and config.mode == Mode.LIVE:
        raise ValueError("integrated GUI controls are disabled for live/mainnet mode")
    service = service or CopyTraderService(config, execution_enabled=control_authority)
    service_operation_lock = Lock()
    stop_worker = asyncio.Event()

    async def polling_worker() -> None:
        ttl_ms = max(60_000, int(worker_interval_s * 3_000))
        service.record_runner_heartbeat(
            status="starting",
            detail="integrated polling worker starting",
            ttl_ms=ttl_ms,
        )
        try:
            while not stop_worker.is_set():
                service.record_runner_heartbeat(
                    status="running",
                    detail="copy cycle in progress",
                    ttl_ms=ttl_ms,
                )
                try:

                    def run_cycle() -> object:
                        with service_operation_lock:
                            return service.run_once()

                    await asyncio.to_thread(run_cycle)
                except Exception as exc:
                    detail = f"integrated polling worker failed: {exc}"
                    service.safe_mode.trip(SafeModeReason.CONFIG_INVALID, detail)
                    service.record_runner_heartbeat(
                        status="error",
                        detail=detail,
                        ttl_ms=ttl_ms,
                        cycle_completed=True,
                    )
                else:
                    service.record_runner_heartbeat(
                        status="idle",
                        detail="last copy cycle completed",
                        ttl_ms=ttl_ms,
                        cycle_completed=True,
                    )
                try:
                    await asyncio.wait_for(stop_worker.wait(), timeout=worker_interval_s)
                except TimeoutError:
                    continue
        finally:
            service.record_runner_heartbeat(
                status="stopped",
                detail="integrated polling worker stopped",
                ttl_ms=0,
            )

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        task = asyncio.create_task(polling_worker()) if start_worker else None
        try:
            yield
        finally:
            if task is not None:
                stop_worker.set()
                await task

    app = FastAPI(
        title="Hyperliquid Copytrader",
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.service = service
    app.state.control_authority = control_authority
    app.state.integrated_worker = start_worker
    app.state.service_operation_lock = service_operation_lock
    app.state.continuous_launch_controller = continuous_launch_controller
    app.state.leaderboard = LeaderboardService(config.leaderboard)
    app.state.address_analysis = AddressAnalysisService(config.address_analytics)
    app.state.run_history = run_history_service or RunHistoryService()
    resolved_credential_root = credential_root or Path.cwd()
    app.state.credential_store = CredentialProfileStore(resolved_credential_root)
    app.state.credential_registry = FleetCredentialProfileRegistry(
        resolved_credential_root,
        legacy_store=app.state.credential_store,
    )
    app.state.subaccount_resolver = subaccount_resolver or SubaccountResolver()
    app.state.market_catalog_client_factory = market_catalog_client_factory or (
        lambda base_url: HyperliquidInfoClient(base_url, timeout_s=12.0)
    )
    app.state.market_catalog_cache = {}
    app.state.market_catalog_lock = Lock()
    app.state.control_rate_limiter = SlidingWindowRateLimiter(
        max_events=config.ops.dashboard_control_max_per_minute
    )
    app.state.analytics_query_rate_limiter = SlidingWindowRateLimiter(max_events=20)
    app.state.analytics_refresh_rate_limiter = SlidingWindowRateLimiter(max_events=2)
    app.state.settings_rate_limiter = SlidingWindowRateLimiter(max_events=5)

    def dashboard_payload(*, include_recent: bool = True) -> dict:
        payload = app.state.service.dashboard(include_recent=include_recent)
        payload["runner"] = {
            **payload.get("runner", {}),
            "control_authority": app.state.control_authority,
            "integrated_worker": app.state.integrated_worker,
            "continuous_launch_capable": app.state.continuous_launch_controller is not None,
        }
        return payload

    @app.middleware("http")
    async def add_security_headers(request: Request, call_next: Callable):
        csp_nonce = secrets.token_urlsafe(18)
        request.state.csp_nonce = csp_nonce
        configured_token = app.state.service.config.ops.gui_token
        authenticated = not configured_token or _request_has_operator_auth(
            request, configured_token
        )
        request.state.operator_authenticated = authenticated
        if not configured_token and not _request_uses_trusted_loopback_authority(request):
            response = PlainTextResponse(
                "tokenless GUI access requires a literal loopback Host",
                status_code=421,
            )
        elif configured_token and not authenticated:
            response = PlainTextResponse(
                "operator authentication required",
                status_code=http_status.HTTP_401_UNAUTHORIZED,
                headers={"WWW-Authenticate": 'Basic realm="Hyperliquid Copytrader"'},
            )
        else:
            response = await call_next(request)
        for header, value in SECURITY_HEADERS.items():
            response.headers.setdefault(header, value)
        response.headers.setdefault("Content-Security-Policy", _content_security_policy(csp_nonce))
        return response

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "continuous_launch_capable": app.state.continuous_launch_controller is not None,
                "csp_nonce": request.state.csp_nonce,
            },
        )

    @app.get("/analytics", response_class=HTMLResponse)
    def analytics(request: Request) -> HTMLResponse:
        dashboard = dashboard_payload()
        dashboard["runner"] = {
            **dashboard.get("runner", {}),
            # The restored backup dashboard is deliberately an observation and
            # analysis surface. Continuous fleet lifecycle ownership remains on /.
            "control_authority": False,
            "fleet_launch_capable": False,
        }
        return templates.TemplateResponse(
            request,
            "analytics.html",
            {
                "dashboard": dashboard,
                "csp_nonce": request.state.csp_nonce,
            },
        )

    @app.get("/favicon.ico", include_in_schema=False)
    def favicon() -> Response:
        return Response(status_code=http_status.HTTP_204_NO_CONTENT)

    @app.get("/api/status")
    def status(include_recent: bool = True) -> dict:
        return dashboard_payload(include_recent=include_recent)

    @app.post("/api/continuous/preview")
    async def continuous_preview(request: Request) -> dict:
        _assert_launch_request(
            request,
            controller_attribute="continuous_launch_controller",
            label="continuous launch",
            require_origin=True,
        )
        payload = await _bounded_json_object(request)
        _exact_payload(payload, set())
        return await asyncio.to_thread(app.state.continuous_launch_controller.preview)

    @app.post("/api/continuous/start")
    async def continuous_start(request: Request) -> dict:
        _assert_launch_request(
            request,
            controller_attribute="continuous_launch_controller",
            label="continuous launch",
            require_origin=True,
        )
        payload = await _bounded_json_object(request)
        _exact_payload(payload, {"acknowledgement"})
        try:
            return await asyncio.to_thread(
                app.state.continuous_launch_controller.start,
                acknowledgement=str(payload["acknowledgement"]),
            )
        except ValueError as exc:
            raise HTTPException(http_status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(http_status.HTTP_409_CONFLICT, str(exc)) from exc

    @app.post("/api/continuous/stop")
    async def continuous_stop(request: Request) -> dict:
        _assert_launch_request(
            request,
            controller_attribute="continuous_launch_controller",
            label="continuous launch",
            require_origin=True,
        )
        payload = await _bounded_json_object(request)
        _exact_payload(payload, {"acknowledgement"})
        try:
            return await asyncio.to_thread(
                app.state.continuous_launch_controller.stop,
                acknowledgement=str(payload["acknowledgement"]),
            )
        except ValueError as exc:
            raise HTTPException(http_status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc

    @app.post("/api/continuous/leaders")
    async def continuous_leaders(request: Request) -> dict:
        _assert_launch_request(
            request,
            controller_attribute="continuous_launch_controller",
            label="continuous launch",
            require_origin=True,
        )
        payload = await _bounded_json_object(request)
        _exact_payload(payload, {"leaders", "acknowledgement"})
        leaders = payload.get("leaders")
        if not isinstance(leaders, dict):
            raise HTTPException(
                http_status.HTTP_422_UNPROCESSABLE_CONTENT,
                "leaders must be a slot-to-address object",
            )
        try:
            return await asyncio.to_thread(
                app.state.continuous_launch_controller.update_leaders,
                leaders=leaders,
                acknowledgement=str(payload["acknowledgement"]),
            )
        except ValueError as exc:
            raise HTTPException(http_status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(http_status.HTTP_409_CONFLICT, str(exc)) from exc

    @app.post("/api/continuous/plan")
    async def continuous_plan(request: Request) -> dict:
        _assert_launch_request(
            request,
            controller_attribute="continuous_launch_controller",
            label="continuous launch",
            require_origin=True,
        )
        payload = await _bounded_json_object(request, max_bytes=262_144)
        _exact_payload(
            payload,
            {"slots", "max_combined_gross_usd", "acknowledgement"},
        )
        slots = payload.get("slots")
        if not isinstance(slots, list) or not all(isinstance(slot, dict) for slot in slots):
            raise HTTPException(
                http_status.HTTP_422_UNPROCESSABLE_CONTENT,
                "slots must be a list of slot objects",
            )
        try:
            return await asyncio.to_thread(
                app.state.continuous_launch_controller.update_fleet,
                slots=slots,
                max_combined_gross_usd=payload.get("max_combined_gross_usd"),
                acknowledgement=str(payload["acknowledgement"]),
            )
        except ValueError as exc:
            raise HTTPException(http_status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(http_status.HTTP_409_CONFLICT, str(exc)) from exc

    @app.get("/api/continuous/status")
    async def continuous_status(request: Request) -> dict:
        _assert_launch_request(
            request,
            controller_attribute="continuous_launch_controller",
            label="continuous launch",
            require_origin=False,
        )
        return await asyncio.to_thread(app.state.continuous_launch_controller.status)

    @app.get("/api/readiness")
    def readiness() -> dict:
        return app.state.service.readiness()

    def continuous_runner_may_be_online() -> bool:
        """Fail closed when analytics cannot prove the trading runner is idle."""

        controller = app.state.continuous_launch_controller
        if controller is None:
            return False
        try:
            return bool(controller.status().get("online"))
        except Exception:
            # A broken control/status boundary must not allow an expensive analytics
            # refresh to compete with execution and reconciliation traffic.
            return True

    @app.get("/api/leaderboard")
    def leaderboard(
        force_refresh: bool = False,
        limit: int | None = None,
        min_volume_usd: Decimal | None = None,
        min_account_value_usd: Decimal | None = None,
    ) -> dict:
        _assert_analytics_rate(app, force_refresh=force_refresh)
        if limit is not None and not 1 <= limit <= 100:
            raise HTTPException(422, "limit must be between 1 and 100")
        for name, value in (
            ("min_volume_usd", min_volume_usd),
            ("min_account_value_usd", min_account_value_usd),
        ):
            if value is not None and (not value.is_finite() or value < 0):
                raise HTTPException(422, f"{name} must be a finite non-negative number")
        if continuous_runner_may_be_online():
            return app.state.leaderboard.local_snapshot(
                limit=limit,
                min_volume_usd=min_volume_usd,
                min_account_value_usd=min_account_value_usd,
            )
        return app.state.leaderboard.snapshot(
            force_refresh=force_refresh,
            limit=limit,
            min_volume_usd=min_volume_usd,
            min_account_value_usd=min_account_value_usd,
        )

    @app.get("/api/address-analysis")
    def address_analysis(
        address: str,
        force_refresh: bool = False,
        window_days: int | None = None,
    ) -> dict:
        _assert_analytics_rate(app, force_refresh=force_refresh)
        if window_days is not None and not 1 <= window_days <= 365:
            raise HTTPException(422, "window_days must be between 1 and 365")
        normalized = address.strip().lower()
        if not valid_address(normalized):
            raise HTTPException(422, "invalid address")
        if continuous_runner_may_be_online():
            return app.state.address_analysis.local_snapshot(
                normalized,
                window_days=window_days,
            )
        return app.state.address_analysis.analyze(
            normalized,
            force_refresh=force_refresh,
            window_days=window_days,
        )

    @app.get("/api/runs")
    def run_history() -> dict:
        return app.state.run_history.snapshot()

    @app.get("/api/credentials")
    def credential_status() -> dict:
        return app.state.credential_store.public_status(active_config=app.state.service.config)

    @app.get("/api/credential-profiles")
    def credential_profile_status() -> dict:
        return app.state.credential_registry.public_status(active_config=app.state.service.config)

    @app.get("/api/market-universe")
    def market_universe(network: str = "mainnet") -> dict:
        """Return a short-lived, unsigned preview of the complete active perp catalog."""

        normalized = str(network or "").strip().lower()
        if normalized not in {"mainnet", "testnet"}:
            raise HTTPException(
                http_status.HTTP_422_UNPROCESSABLE_CONTENT,
                "network must be mainnet or testnet",
            )
        runner_online = continuous_runner_may_be_online()
        cache_age_seconds: float | None = None
        with app.state.market_catalog_lock:
            cached = app.state.market_catalog_cache.get(normalized)
            if cached is not None:
                cache_age_seconds = max(0.0, monotonic() - cached[0])
            if cached is not None and (
                (cache_age_seconds is not None and cache_age_seconds <= 300) or runner_online
            ):
                manifest = cached[1]
            elif runner_online:
                raise HTTPException(
                    http_status.HTTP_503_SERVICE_UNAVAILABLE,
                    "market catalog preview is cache-only while the continuous runner is online",
                )
            else:
                base_url = MAINNET_REST if normalized == "mainnet" else TESTNET_REST
                try:
                    manifest = resolve_public_market_universe(
                        app.state.market_catalog_client_factory(base_url),
                        network=normalized,
                    )
                except (
                    MarketIdentityError,
                    OSError,
                    RuntimeError,
                    ValueError,
                ) as exc:
                    raise HTTPException(
                        http_status.HTTP_502_BAD_GATEWAY,
                        f"public market catalog is unavailable: {exc}",
                    ) from exc
                app.state.market_catalog_cache[normalized] = (monotonic(), manifest)
                cache_age_seconds = 0.0
        payload = manifest.to_payload()
        return {
            **payload,
            "active_market_count": len(manifest.symbols),
            "dex_count": len(manifest.dexes),
            "cache_ttl_seconds": 300,
            "cache_age_seconds": cache_age_seconds,
            "cache_stale": bool(cache_age_seconds is not None and cache_age_seconds > 300),
            "external_refresh_blocked_while_runner_online": runner_online,
            "read_only_query": True,
            "signed_action_performed": False,
            "launch_catalog_is_refreshed_separately": True,
        }

    @app.post("/api/credential-profiles")
    async def save_credential_profile(request: Request) -> dict:
        _assert_settings_allowed(request, "fleet credential setup")
        payload = await _bounded_json_object(request, max_bytes=16_384)
        try:
            app.state.credential_registry.validate(payload)
            app.state.subaccount_resolver.assert_selection(payload)
            result = app.state.credential_registry.save(
                payload,
                active_config=app.state.service.config,
            )
        except RuntimeError as exc:
            raise HTTPException(http_status.HTTP_409_CONFLICT, str(exc)) from exc
        except CredentialSetupError as exc:
            _audit_control(request, "fleet credential setup", "denied", str(exc))
            raise HTTPException(http_status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
        except OSError as exc:
            _audit_control(
                request,
                "fleet credential setup",
                "failed",
                "local profile-vault write failed",
            )
            raise HTTPException(
                http_status.HTTP_500_INTERNAL_SERVER_ERROR,
                "Local profile-vault storage failed; no exchange action was attempted.",
            ) from exc
        app.state.service.invalidate_security_audit_cache()
        _audit_control(
            request,
            "fleet credential setup",
            "success",
            f"saved isolated credential profile {payload.get('profile_id', '')!s}",
        )
        return result

    @app.post("/api/credential-profiles/delete")
    async def delete_credential_profile(request: Request) -> dict:
        _assert_settings_allowed(request, "fleet credential delete")
        payload = await _bounded_json_object(request)
        try:
            result = app.state.credential_registry.delete(
                profile_id=str(payload.get("profile_id") or ""),
                confirmation=str(payload.get("confirmation") or ""),
                active_config=app.state.service.config,
            )
        except RuntimeError as exc:
            raise HTTPException(http_status.HTTP_409_CONFLICT, str(exc)) from exc
        except CredentialSetupError as exc:
            _audit_control(request, "fleet credential delete", "denied", str(exc))
            raise HTTPException(http_status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
        except OSError as exc:
            _audit_control(
                request,
                "fleet credential delete",
                "failed",
                "local profile-vault delete failed",
            )
            raise HTTPException(
                http_status.HTTP_500_INTERNAL_SERVER_ERROR,
                "Local profile deletion failed.",
            ) from exc
        _audit_control(
            request,
            "fleet credential delete",
            "success",
            f"deleted isolated credential profile {payload.get('profile_id', '')!s}",
        )
        return result

    @app.post("/api/credential-profiles/market-policy")
    async def update_credential_profile_market_policy(request: Request) -> dict:
        _assert_settings_allowed(request, "fleet credential market update")
        if continuous_runner_may_be_online():
            detail = "stop the continuous runner before changing a profile market denylist"
            _audit_control(request, "fleet credential market update", "denied", detail)
            raise HTTPException(http_status.HTTP_409_CONFLICT, detail)
        payload = await _bounded_json_object(request, max_bytes=16_384)
        _exact_payload(payload, {"profile_id", "denied_symbols", "confirmation"})
        try:
            result = app.state.credential_registry.update_market_policy(
                profile_id=str(payload.get("profile_id") or ""),
                denied_symbols=payload.get("denied_symbols"),
                confirmation=str(payload.get("confirmation") or ""),
                active_config=app.state.service.config,
            )
        except RuntimeError as exc:
            raise HTTPException(http_status.HTTP_409_CONFLICT, str(exc)) from exc
        except CredentialSetupError as exc:
            _audit_control(request, "fleet credential market update", "denied", str(exc))
            raise HTTPException(http_status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
        except OSError as exc:
            _audit_control(
                request,
                "fleet credential market update",
                "failed",
                "local profile-vault metadata write failed",
            )
            raise HTTPException(
                http_status.HTTP_500_INTERNAL_SERVER_ERROR,
                "Local profile market update failed; no exchange action was attempted.",
            ) from exc
        app.state.service.invalidate_security_audit_cache()
        _audit_control(
            request,
            "fleet credential market update",
            "success",
            f"updated saved market denylist for {payload.get('profile_id', '')!s}",
        )
        return result

    @app.post("/api/credential-profiles/import-legacy")
    async def import_legacy_credential_profile(request: Request) -> dict:
        _assert_settings_allowed(request, "fleet credential legacy import")
        payload = await _bounded_json_object(request)
        try:
            result = app.state.credential_registry.import_legacy(
                profile_id=str(payload.get("profile_id") or ""),
                confirmation=str(payload.get("confirmation") or ""),
                active_config=app.state.service.config,
            )
        except RuntimeError as exc:
            raise HTTPException(http_status.HTTP_409_CONFLICT, str(exc)) from exc
        except CredentialSetupError as exc:
            _audit_control(request, "fleet credential legacy import", "denied", str(exc))
            raise HTTPException(http_status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
        except OSError as exc:
            raise HTTPException(
                http_status.HTTP_500_INTERNAL_SERVER_ERROR,
                "Legacy profile import failed locally.",
            ) from exc
        _audit_control(
            request,
            "fleet credential legacy import",
            "success",
            "copied legacy credential into an isolated vault profile",
        )
        return result

    @app.post("/api/credential-profiles/select-legacy")
    async def select_legacy_credential_profile(request: Request) -> dict:
        _assert_settings_allowed(request, "fleet credential legacy selection")
        payload = await _bounded_json_object(request)
        try:
            result = app.state.credential_registry.select_legacy(
                profile_id=str(payload.get("profile_id") or ""),
                confirmation=str(payload.get("confirmation") or ""),
                active_config=app.state.service.config,
            )
        except RuntimeError as exc:
            raise HTTPException(http_status.HTTP_409_CONFLICT, str(exc)) from exc
        except CredentialSetupError as exc:
            _audit_control(request, "fleet credential legacy selection", "denied", str(exc))
            raise HTTPException(http_status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
        except OSError as exc:
            raise HTTPException(
                http_status.HTTP_500_INTERNAL_SERVER_ERROR,
                "Legacy profile selection failed locally.",
            ) from exc
        app.state.service.invalidate_security_audit_cache()
        _audit_control(
            request,
            "fleet credential legacy selection",
            "success",
            "explicitly selected a restart-loaded legacy canary profile",
        )
        return result

    @app.post("/api/credentials/subaccounts")
    async def resolve_credential_subaccounts(request: Request) -> dict:
        _assert_settings_allowed(request, "credential subaccount lookup")
        payload = await _bounded_json_object(request)
        try:
            result = app.state.subaccount_resolver.resolve(
                network=str(payload.get("network") or ""),
                global_account_address=str(payload.get("global_account_address") or ""),
            )
        except RuntimeError as exc:
            raise HTTPException(http_status.HTTP_409_CONFLICT, str(exc)) from exc
        except CredentialSetupError as exc:
            _audit_control(request, "credential subaccount lookup", "denied", str(exc))
            raise HTTPException(http_status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
        except Exception as exc:
            _audit_control(
                request,
                "credential subaccount lookup",
                "failed",
                "read-only Hyperliquid lookup failed",
            )
            raise HTTPException(
                http_status.HTTP_502_BAD_GATEWAY,
                "Read-only Hyperliquid subaccount lookup failed; no profile was changed.",
            ) from exc
        _audit_control(
            request,
            "credential subaccount lookup",
            "success",
            f"resolved {result['subaccount_count']} public subaccounts without signing",
        )
        return result

    @app.post("/api/credentials/subaccount-details")
    async def resolve_credential_subaccount_details(request: Request) -> dict:
        _assert_settings_allowed(request, "credential subaccount details")
        payload = await _bounded_json_object(request)
        try:
            result = app.state.subaccount_resolver.details(
                network=str(payload.get("network") or ""),
                follower_account_address=str(payload.get("follower_account_address") or ""),
            )
        except RuntimeError as exc:
            raise HTTPException(http_status.HTTP_409_CONFLICT, str(exc)) from exc
        except CredentialSetupError as exc:
            _audit_control(request, "credential subaccount details", "denied", str(exc))
            raise HTTPException(http_status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
        except Exception as exc:
            _audit_control(
                request,
                "credential subaccount details",
                "failed",
                "read-only selected-subaccount inspection failed",
            )
            raise HTTPException(
                http_status.HTTP_502_BAD_GATEWAY,
                "Read-only selected-subaccount inspection failed; no profile was changed.",
            ) from exc
        _audit_control(
            request,
            "credential subaccount details",
            "success",
            "detected selected-subaccount account mode and collateral without signing",
        )
        return result

    @app.post("/api/credentials")
    async def save_credentials(request: Request) -> dict:
        _assert_settings_allowed(request, "credential setup")
        payload = await _bounded_json_object(request)
        try:
            app.state.credential_store.validate(payload)
            app.state.subaccount_resolver.assert_selection(payload)
            result = app.state.credential_store.save(
                payload, active_config=app.state.service.config
            )
        except RuntimeError as exc:
            raise HTTPException(http_status.HTTP_409_CONFLICT, str(exc)) from exc
        except CredentialSetupError as exc:
            _audit_control(request, "credential setup", "denied", str(exc))
            raise HTTPException(http_status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
        except Exception as exc:
            _audit_control(
                request,
                "credential setup",
                "failed",
                "read-only subaccount ownership verification failed",
            )
            raise HTTPException(
                http_status.HTTP_502_BAD_GATEWAY,
                "Read-only subaccount ownership verification failed; nothing was saved.",
            ) from exc
        except OSError as exc:
            _audit_control(request, "credential setup", "failed", "local secret write failed")
            raise HTTPException(
                http_status.HTTP_500_INTERNAL_SERVER_ERROR,
                "Local credential storage failed; no exchange action was attempted.",
            ) from exc
        app.state.service.invalidate_security_audit_cache()
        _audit_control(
            request,
            "credential setup",
            "success",
            "validated dedicated API wallet and saved local restart-required profile",
        )
        return result

    @app.post("/api/credentials/clear")
    async def clear_credentials(request: Request) -> dict:
        _assert_settings_allowed(request, "credential clear")
        payload = await _bounded_json_object(request)
        if payload.get("confirmation") != "FORGET_LOCAL_CREDENTIALS":
            _audit_control(request, "credential clear", "denied", "confirmation did not match")
            raise HTTPException(
                http_status.HTTP_422_UNPROCESSABLE_CONTENT,
                "Type FORGET_LOCAL_CREDENTIALS to remove the local profile.",
            )
        try:
            result = app.state.credential_store.clear(active_config=app.state.service.config)
        except RuntimeError as exc:
            raise HTTPException(http_status.HTTP_409_CONFLICT, str(exc)) from exc
        except OSError as exc:
            _audit_control(request, "credential clear", "failed", "local secret removal failed")
            raise HTTPException(
                http_status.HTTP_500_INTERNAL_SERVER_ERROR,
                "Local credential removal failed.",
            ) from exc
        app.state.service.invalidate_security_audit_cache()
        _audit_control(request, "credential clear", "success", "local credential profile removed")
        return result

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics() -> str:
        return app.state.service.metrics_text()

    @app.post("/controls/pause")
    def pause(
        request: Request,
        reason: str = Form("operator pause"),
    ) -> RedirectResponse:
        _assert_control_allowed(request, "pause")
        return _run_control(request, "pause", lambda: app.state.service.pause(reason))

    @app.post("/controls/resume")
    def resume(
        request: Request,
        detail: str = Form("operator resume"),
    ) -> RedirectResponse:
        _assert_control_allowed(request, "resume")
        return _run_control(request, "resume", lambda: app.state.service.resume(detail))

    @app.post("/controls/reconcile")
    def reconcile(request: Request) -> RedirectResponse:
        _assert_control_allowed(request, "reconcile")
        return _run_control(request, "reconcile", app.state.service.manual_reconcile)

    @app.post("/controls/settle-pending")
    def settle_pending(request: Request) -> RedirectResponse:
        _assert_control_allowed(request, "settle-pending")
        return _run_control(request, "settle-pending", app.state.service.settle_pending_intents)

    @app.post("/controls/run-once")
    def run_once(request: Request) -> RedirectResponse:
        _assert_control_allowed(request, "run-once")
        return _run_control(request, "run-once", app.state.service.run_once)

    @app.post("/controls/testnet-smoke")
    def testnet_smoke(
        request: Request,
        coin: str = Form("BTC"),
        size: str = Form("0.0001"),
    ) -> RedirectResponse:
        _assert_control_allowed(request, "testnet-smoke")
        return _run_control(
            request,
            "testnet-smoke",
            lambda: app.state.service.testnet_smoke(coin=coin, size=Decimal(size)),
        )

    @app.post("/controls/testnet-active-smoke")
    def testnet_active_smoke(
        request: Request,
        coin: str = Form("BTC"),
        size: str = Form("0.0001"),
    ) -> RedirectResponse:
        _assert_control_allowed(request, "testnet-active-smoke")
        return _run_control(
            request,
            "testnet-active-smoke",
            lambda: app.state.service.testnet_active_smoke(coin=coin, size=Decimal(size)),
        )

    return app
