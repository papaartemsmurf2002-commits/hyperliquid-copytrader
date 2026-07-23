from __future__ import annotations

import asyncio
import errno
import ipaddress
import selectors
import socket
import ssl
import sys
import time
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from hashlib import sha256
from typing import Any
from urllib.parse import urlsplit


IPV4_FALLBACK_DELAY_S = 0.25
DEFAULT_OPEN_TIMEOUT_S = 10.0


@dataclass(frozen=True, slots=True)
class WebSocketEndpoint:
    uri: str
    host: str
    port: int
    proxy_configured: bool
    ipv6_addresses: tuple[str, ...]
    ipv4_addresses: tuple[str, ...]

    @property
    def ipv6_available(self) -> bool:
        return bool(self.ipv6_addresses)

    def payload(self) -> dict[str, Any]:
        return {
            "policy": "ipv6_preferred_happy_eyeballs_v1",
            "ipv4_fallback_delay_ms": round(IPV4_FALLBACK_DELAY_S * 1_000),
            "target_host": self.host,
            "target_port": self.port,
            "proxy_configured": self.proxy_configured,
            "ipv6_available": self.ipv6_available,
            "ipv6_addresses": list(self.ipv6_addresses),
            "ipv4_addresses": list(self.ipv4_addresses),
        }


def _target(uri: str) -> tuple[str, int]:
    parsed = urlsplit(uri)
    if parsed.scheme not in {"ws", "wss"} or not parsed.hostname:
        raise ValueError("websocket URI must use ws:// or wss:// with a host")
    return parsed.hostname, parsed.port or (443 if parsed.scheme == "wss" else 80)


def _resolved_proxy(uri: str, *, proxy: Any = True) -> str | None:
    if proxy is None or proxy is False:
        return None
    if isinstance(proxy, str):
        return proxy.strip() or None
    if proxy is not True:
        raise TypeError("websocket proxy must be True, None, or an explicit proxy URI")
    # Use the exact resolver used by the pinned websockets client.  Besides
    # matching its scheme precedence, this applies the host:port bypass test
    # and prevents a check/connect configuration race when we pass the result
    # explicitly to the connector.
    from websockets.proxy import get_proxy
    from websockets.uri import parse_uri

    return get_proxy(parse_uri(uri))


def _proxy_configured(uri: str, *, proxy: Any = True) -> bool:
    return _resolved_proxy(uri, proxy=proxy) is not None


def _address_text(family: int, sockaddr: tuple[Any, ...]) -> str:
    text = str(sockaddr[0])
    if family == socket.AF_INET6:
        return text.split("%", 1)[0]
    return text


def _deduplicated_candidates(
    rows: Sequence[tuple[Any, Any, int, str, tuple[Any, ...]]],
) -> list[tuple[int, tuple[Any, ...]]]:
    result: list[tuple[int, tuple[Any, ...]]] = []
    seen: set[tuple[int, str]] = set()
    for family, socket_type, protocol, _canonical_name, sockaddr in rows:
        if (
            family not in {socket.AF_INET6, socket.AF_INET}
            or socket_type != socket.SOCK_STREAM
            or protocol not in {0, socket.IPPROTO_TCP}
        ):
            continue
        key = (family, _address_text(family, sockaddr))
        if key in seen:
            continue
        seen.add(key)
        result.append((family, sockaddr))
    return result


def _ipv6_first_interleave(
    candidates: list[tuple[int, tuple[Any, ...]]],
) -> list[tuple[int, tuple[Any, ...]]]:
    ipv6 = [candidate for candidate in candidates if candidate[0] == socket.AF_INET6]
    ipv4 = [candidate for candidate in candidates if candidate[0] == socket.AF_INET]
    ordered: list[tuple[int, tuple[Any, ...]]] = []
    for index in range(max(len(ipv6), len(ipv4))):
        if index < len(ipv6):
            ordered.append(ipv6[index])
        if index < len(ipv4):
            ordered.append(ipv4[index])
    return ordered


def websocket_endpoint(uri: str, *, proxy: Any = True) -> WebSocketEndpoint:
    host, port = _target(uri)
    rows = socket.getaddrinfo(
        host,
        port,
        family=socket.AF_UNSPEC,
        type=socket.SOCK_STREAM,
        proto=socket.IPPROTO_TCP,
    )
    candidates = _deduplicated_candidates(rows)
    ipv6 = tuple(
        _address_text(family, sockaddr)
        for family, sockaddr in candidates
        if family == socket.AF_INET6
    )
    ipv4 = tuple(
        _address_text(family, sockaddr)
        for family, sockaddr in candidates
        if family == socket.AF_INET
    )
    return WebSocketEndpoint(
        uri=uri,
        host=host,
        port=port,
        proxy_configured=_proxy_configured(uri, proxy=proxy),
        ipv6_addresses=ipv6,
        ipv4_addresses=ipv4,
    )


async def _async_candidates(uri: str) -> list[tuple[int, tuple[Any, ...]]]:
    host, port = _target(uri)
    loop = asyncio.get_running_loop()
    rows = await loop.getaddrinfo(
        host,
        port,
        family=socket.AF_UNSPEC,
        type=socket.SOCK_STREAM,
        proto=socket.IPPROTO_TCP,
    )
    return _deduplicated_candidates(rows)


async def _connected_socket(
    family: int,
    sockaddr: tuple[Any, ...],
    *,
    delay_s: float,
) -> socket.socket:
    if delay_s > 0:
        await asyncio.sleep(delay_s)
    candidate = socket.socket(family, socket.SOCK_STREAM, socket.IPPROTO_TCP)
    candidate.setblocking(False)
    candidate.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    try:
        await asyncio.get_running_loop().sock_connect(candidate, sockaddr)
    except BaseException:
        candidate.close()
        raise
    return candidate


async def _ipv6_preferred_socket(
    uri: str,
    *,
    timeout_s: float | None,
    families: tuple[int, ...] = (socket.AF_INET6, socket.AF_INET),
) -> socket.socket:
    candidates = [
        candidate
        for candidate in _ipv6_first_interleave(await _async_candidates(uri))
        if candidate[0] in families
    ]
    if not candidates:
        raise OSError("websocket target did not resolve to an IPv4 or IPv6 TCP address")
    tasks = {
        asyncio.create_task(
            _connected_socket(
                family,
                sockaddr,
                delay_s=index * IPV4_FALLBACK_DELAY_S,
            )
        )
        for index, (family, sockaddr) in enumerate(candidates)
    }

    async def first_success() -> socket.socket:
        failures: list[BaseException] = []
        pending = set(tasks)
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                try:
                    return task.result()
                except BaseException as exc:
                    failures.append(exc)
        detail = "; ".join(str(item) for item in failures if str(item))
        raise OSError(
            "all IPv6-preferred websocket TCP candidates failed" + (f": {detail}" if detail else "")
        )

    try:
        if timeout_s is None:
            winner = await first_success()
        else:
            winner = await asyncio.wait_for(first_success(), timeout=timeout_s)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    for task in tasks:
        if task.done() and not task.cancelled():
            try:
                other = task.result()
            except BaseException:
                continue
            if other is not winner:
                other.close()
    return winner


def _ipv6_preferred_socket_sync(
    uri: str,
    *,
    timeout_s: float | None,
    families: tuple[int, ...] = (socket.AF_INET6, socket.AF_INET),
) -> socket.socket:
    """Synchronous Happy Eyeballs dial for the thread-owned unified stream."""

    host, port = _target(uri)
    candidates = [
        candidate
        for candidate in _ipv6_first_interleave(
            _deduplicated_candidates(
                socket.getaddrinfo(
                    host,
                    port,
                    family=socket.AF_UNSPEC,
                    type=socket.SOCK_STREAM,
                    proto=socket.IPPROTO_TCP,
                )
            )
        )
        if candidate[0] in families
    ]
    if not candidates:
        raise OSError("websocket target did not resolve to an IPv4 or IPv6 TCP address")
    started = time.monotonic()
    deadline = None if timeout_s is None else started + timeout_s
    next_candidate = 0
    pending: dict[socket.socket, None] = {}
    in_progress = {
        0,
        errno.EINPROGRESS,
        errno.EWOULDBLOCK,
        errno.EALREADY,
        10035,  # WSAEWOULDBLOCK
        10036,  # WSAEINPROGRESS
        10037,  # WSAEALREADY
    }
    failures: list[OSError] = []

    def close_pending(*, except_socket: socket.socket | None = None) -> None:
        for candidate in tuple(pending):
            if candidate is not except_socket:
                candidate.close()
            pending.pop(candidate, None)

    with selectors.DefaultSelector() as selector:
        while True:
            now = time.monotonic()
            while next_candidate < len(candidates) and (
                next_candidate == 0 or now - started >= next_candidate * IPV4_FALLBACK_DELAY_S
            ):
                family, sockaddr = candidates[next_candidate]
                next_candidate += 1
                candidate = socket.socket(family, socket.SOCK_STREAM, socket.IPPROTO_TCP)
                candidate.setblocking(False)
                candidate.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                error = candidate.connect_ex(sockaddr)
                if error == 0:
                    close_pending()
                    candidate.setblocking(True)
                    return candidate
                if error not in in_progress:
                    failures.append(OSError(error, f"connect failed for {sockaddr[0]}"))
                    candidate.close()
                    continue
                pending[candidate] = None
                selector.register(candidate, selectors.EVENT_WRITE)

            if deadline is not None and now >= deadline:
                close_pending()
                raise TimeoutError("IPv6-preferred websocket TCP connection timed out")
            if not pending and next_candidate >= len(candidates):
                detail = "; ".join(str(item) for item in failures if str(item))
                raise OSError(
                    "all IPv6-preferred websocket TCP candidates failed"
                    + (f": {detail}" if detail else "")
                )

            next_launch = (
                started + next_candidate * IPV4_FALLBACK_DELAY_S
                if next_candidate < len(candidates)
                else None
            )
            waits = [0.1]
            if deadline is not None:
                waits.append(max(0.0, deadline - now))
            if next_launch is not None:
                waits.append(max(0.0, next_launch - now))
            if not pending:
                # Windows' SelectSelector rejects an empty descriptor set. A
                # synchronously failed IPv6 candidate must still reach the
                # delayed IPv4 fallback instead of escaping with WSAEINVAL.
                wait_s = min(waits)
                if wait_s > 0:
                    time.sleep(wait_s)
                continue
            for key, _events in selector.select(min(waits)):
                ready_socket = key.fileobj
                if not isinstance(ready_socket, socket.socket):
                    continue
                selector.unregister(ready_socket)
                pending.pop(ready_socket, None)
                error = ready_socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                if error:
                    failures.append(OSError(error, "websocket TCP connect failed"))
                    ready_socket.close()
                    continue
                close_pending(except_socket=ready_socket)
                ready_socket.setblocking(True)
                return ready_socket


def websocket_transport_snapshot(connection: Any, uri: str) -> dict[str, Any]:
    resolved_proxy = _resolved_proxy(uri)
    proxy_configured = resolved_proxy is not None
    if proxy_configured:
        host, port = _target(uri)
        endpoint = WebSocketEndpoint(
            uri=uri,
            host=host,
            port=port,
            proxy_configured=True,
            ipv6_addresses=(),
            ipv4_addresses=(),
        )
    else:
        endpoint = websocket_endpoint(uri)
    remote = getattr(connection, "remote_address", None)
    local = getattr(connection, "local_address", None)
    remote_host = str(remote[0]) if isinstance(remote, tuple) and remote else ""
    local_host = str(local[0]) if isinstance(local, tuple) and local else ""
    selected_family = "proxy_managed" if proxy_configured else "unknown"
    if not proxy_configured:
        try:
            selected_family = "ipv6" if ipaddress.ip_address(remote_host).version == 6 else "ipv4"
        except ValueError:
            pass
    payload = endpoint.payload()
    if proxy_configured:
        parsed_proxy = urlsplit(str(resolved_proxy))
        proxy_identity = (
            f"{parsed_proxy.scheme.casefold()}://"
            f"{str(parsed_proxy.hostname or '').casefold()}:"
            f"{parsed_proxy.port or 0}"
        )
        payload.update(
            {
                "origin_resolution": "proxy_managed",
                "ipv6_available": None,
                "proxy_identity_sha256": sha256(proxy_identity.encode("utf-8")).hexdigest(),
            }
        )
    else:
        payload["origin_resolution"] = "local_dns"
        payload["ipv6_unavailable_reason"] = "" if endpoint.ipv6_available else "no_aaaa_records"
    payload.update(
        {
            "selected_family": selected_family,
            "remote_address": remote_host,
            "local_address": local_host,
            "ipv4_fallback_used": not proxy_configured
            and endpoint.ipv6_available
            and selected_family == "ipv4",
            "ipv6_selected_when_available": None
            if proxy_configured or not endpoint.ipv6_available
            else selected_family == "ipv6",
        }
    )
    return payload


@contextmanager
def connect_websocket_sync_ipv6_preferred(
    uri: str,
    **kwargs: Any,
) -> Iterator[Any]:
    from websockets.sync.client import connect

    proxy = kwargs.get("proxy", True)
    resolved_proxy = _resolved_proxy(uri, proxy=proxy)
    connection_kwargs = dict(kwargs)
    connection_kwargs["proxy"] = resolved_proxy
    if resolved_proxy is not None:
        with connect(uri, **connection_kwargs) as connection:
            yield connection
        return
    raw_timeout = kwargs.get("open_timeout", DEFAULT_OPEN_TIMEOUT_S)
    timeout_s = None if raw_timeout is None else float(raw_timeout)
    if timeout_s is not None and timeout_s <= 0:
        raise TimeoutError("websocket open timeout must be positive")
    started = time.monotonic()

    def remaining_timeout() -> float | None:
        if timeout_s is None:
            return None
        remaining = timeout_s - (time.monotonic() - started)
        if remaining <= 0:
            raise TimeoutError("websocket open timeout elapsed during IPv6-preferred connect")
        return remaining

    def open_direct(
        families: tuple[int, ...],
    ) -> tuple[Any, Any, socket.socket]:
        preconnected = _ipv6_preferred_socket_sync(
            uri,
            timeout_s=remaining_timeout(),
            families=families,
        )
        direct_kwargs = dict(connection_kwargs)
        direct_kwargs["sock"] = preconnected
        direct_kwargs["open_timeout"] = remaining_timeout()
        try:
            # websockets.sync.client.connect performs the TCP/TLS/WebSocket
            # opening work before it returns.  Keep that call inside the
            # family-tagging guard so a transport-level IPv6 opening failure
            # can make the single bounded IPv4 retry below.
            connector = connect(uri, **direct_kwargs)
            connection = connector.__enter__()
        except BaseException as exc:
            if isinstance(exc, OSError):
                setattr(exc, "_hlct_preconnected_family", preconnected.family)
            preconnected.close()
            raise
        return connector, connection, preconnected

    try:
        connector, connection, preconnected = open_direct((socket.AF_INET6, socket.AF_INET))
    except OSError as exc:
        if getattr(exc, "_hlct_preconnected_family", None) == socket.AF_INET6 and not isinstance(
            exc, ssl.SSLError
        ):
            connector, connection, preconnected = open_direct((socket.AF_INET,))
        else:
            raise
    try:
        yield connection
    except BaseException:
        exc_info = sys.exc_info()
        connector.__exit__(*exc_info)
        raise
    else:
        connector.__exit__(None, None, None)


@asynccontextmanager
async def connect_websocket_ipv6_preferred(
    uri: str,
    **kwargs: Any,
) -> AsyncIterator[Any]:
    """Connect IPv6 first and race IPv4 after 250 ms when the endpoint is direct.

    TLS SNI and the HTTP Host header still come from ``uri`` because the selected
    numeric socket is supplied through websockets' documented ``sock`` hook.
    Configured proxies retain ownership of destination resolution; in that case
    address-family selection is delegated unchanged to the pinned websockets
    proxy implementation.
    """

    import websockets

    proxy = kwargs.get("proxy", True)
    resolved_proxy = _resolved_proxy(uri, proxy=proxy)
    connection_kwargs = dict(kwargs)
    connection_kwargs["proxy"] = resolved_proxy
    if resolved_proxy is not None:
        connector = websockets.connect(uri, **connection_kwargs)
        connection = await connector.__aenter__()
    else:
        raw_timeout = connection_kwargs.get("open_timeout", DEFAULT_OPEN_TIMEOUT_S)
        timeout_s = None if raw_timeout is None else float(raw_timeout)
        if timeout_s is not None and timeout_s <= 0:
            raise TimeoutError("websocket open timeout must be positive")
        started = asyncio.get_running_loop().time()

        def remaining_timeout() -> float | None:
            if timeout_s is None:
                return None
            remaining = timeout_s - (asyncio.get_running_loop().time() - started)
            if remaining <= 0:
                raise TimeoutError("websocket open timeout elapsed during IPv6-preferred connect")
            return remaining

        async def open_direct(
            families: tuple[int, ...],
        ) -> tuple[Any, Any, socket.socket]:
            preconnected = await _ipv6_preferred_socket(
                uri,
                timeout_s=remaining_timeout(),
                families=families,
            )
            direct_kwargs = dict(connection_kwargs)
            direct_kwargs["sock"] = preconnected
            direct_kwargs["open_timeout"] = remaining_timeout()
            connector = websockets.connect(uri, **direct_kwargs)
            try:
                connection = await connector.__aenter__()
            except BaseException as exc:
                if isinstance(exc, OSError):
                    setattr(exc, "_hlct_preconnected_family", preconnected.family)
                preconnected.close()
                raise
            return connector, connection, preconnected

        try:
            connector, connection, preconnected = await open_direct(
                (socket.AF_INET6, socket.AF_INET)
            )
        except OSError as exc:
            if getattr(
                exc, "_hlct_preconnected_family", None
            ) == socket.AF_INET6 and not isinstance(exc, ssl.SSLError):
                connector, connection, preconnected = await open_direct((socket.AF_INET,))
            else:
                raise
    try:
        yield connection
    except BaseException:
        exc_info = sys.exc_info()
        await connector.__aexit__(*exc_info)
        raise
    else:
        await connector.__aexit__(None, None, None)
