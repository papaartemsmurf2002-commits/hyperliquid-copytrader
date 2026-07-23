from __future__ import annotations

import asyncio
import socket
import threading
from typing import Any

import pytest

from hyperliquid_copytrader import websocket_transport


def _row(family: int, address: str) -> tuple[int, int, int, str, tuple[Any, ...]]:
    sockaddr: tuple[Any, ...]
    if family == socket.AF_INET6:
        sockaddr = (address, 443, 0, 0)
    else:
        sockaddr = (address, 443)
    return family, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", sockaddr


def test_endpoint_reports_ipv6_capability_and_preserves_ipv4_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [
            _row(socket.AF_INET, "192.0.2.1"),
            _row(socket.AF_INET6, "2001:db8::1"),
        ],
    )
    monkeypatch.setattr(websocket_transport, "_resolved_proxy", lambda *_args, **_kwargs: None)
    endpoint = websocket_transport.websocket_endpoint("wss://api.example/ws")

    assert endpoint.ipv6_available is True
    assert endpoint.ipv6_addresses == ("2001:db8::1",)
    assert endpoint.ipv4_addresses == ("192.0.2.1",)
    assert endpoint.payload()["policy"] == "ipv6_preferred_happy_eyeballs_v1"
    assert endpoint.payload()["ipv4_fallback_delay_ms"] == 250


def test_ipv6_first_interleave_starts_with_ipv6() -> None:
    candidates = [
        (socket.AF_INET, ("192.0.2.1", 443)),
        (socket.AF_INET, ("192.0.2.2", 443)),
        (socket.AF_INET6, ("2001:db8::1", 443, 0, 0)),
        (socket.AF_INET6, ("2001:db8::2", 443, 0, 0)),
    ]

    assert websocket_transport._ipv6_first_interleave(candidates) == [
        (socket.AF_INET6, ("2001:db8::1", 443, 0, 0)),
        (socket.AF_INET, ("192.0.2.1", 443)),
        (socket.AF_INET6, ("2001:db8::2", 443, 0, 0)),
        (socket.AF_INET, ("192.0.2.2", 443)),
    ]


def test_ipv4_is_used_after_ipv6_connection_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeSocket:
        def __init__(self, family: int) -> None:
            self.family = family
            self.closed = False

        def close(self) -> None:
            self.closed = True

    async def candidates(_uri: str) -> list[tuple[int, tuple[Any, ...]]]:
        return [
            (socket.AF_INET6, ("2001:db8::1", 443, 0, 0)),
            (socket.AF_INET, ("192.0.2.1", 443)),
        ]

    attempts: list[int] = []

    async def connect(
        family: int,
        _sockaddr: tuple[Any, ...],
        *,
        delay_s: float,
    ) -> Any:
        await asyncio.sleep(delay_s)
        attempts.append(family)
        if family == socket.AF_INET6:
            raise OSError("IPv6 route unavailable")
        return FakeSocket(family)

    monkeypatch.setattr(websocket_transport, "IPV4_FALLBACK_DELAY_S", 0.001)
    monkeypatch.setattr(websocket_transport, "_async_candidates", candidates)
    monkeypatch.setattr(websocket_transport, "_connected_socket", connect)

    selected = asyncio.run(
        websocket_transport._ipv6_preferred_socket("wss://api.example/ws", timeout_s=1)
    )

    assert selected.family == socket.AF_INET
    assert attempts == [socket.AF_INET6, socket.AF_INET]


def test_sync_immediate_ipv6_failure_reaches_ipv4_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    server.settimeout(3)
    port = int(server.getsockname()[1])
    accepted: list[bool] = []

    def accept_once() -> None:
        connection, _address = server.accept()
        accepted.append(True)
        connection.close()

    thread = threading.Thread(target=accept_once, daemon=True)
    thread.start()
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [
            _row(socket.AF_INET6, "2001:db8::1")[:-1] + (("2001:db8::1", port, 0, 0),),
            _row(socket.AF_INET, "127.0.0.1")[:-1] + (("127.0.0.1", port),),
        ],
    )
    try:
        selected = websocket_transport._ipv6_preferred_socket_sync(
            "wss://api.example/ws",
            timeout_s=2,
        )
        assert selected.family == socket.AF_INET
        selected.close()
        thread.join(timeout=2)
        assert accepted == [True]
    finally:
        server.close()


def test_sync_websocket_open_failure_on_ipv6_retries_ipv4(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeRawSocket:
        def __init__(self, family: int) -> None:
            self.family = family
            self.closed = False

        def close(self) -> None:
            self.closed = True

    class FakeConnection:
        remote_address = ("192.0.2.1", 443)
        local_address = ("192.0.2.2", 50000)

        def __enter__(self) -> FakeConnection:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    selected_families: list[tuple[int, ...]] = []
    sockets: list[FakeRawSocket] = []

    def preferred(
        _uri: str,
        *,
        timeout_s: float | None,
        families: tuple[int, ...],
    ) -> Any:
        assert timeout_s is None or timeout_s > 0
        selected_families.append(families)
        family = socket.AF_INET6 if socket.AF_INET6 in families else socket.AF_INET
        raw = FakeRawSocket(family)
        sockets.append(raw)
        return raw

    def connect(_uri: str, **kwargs: Any) -> FakeConnection:
        if kwargs["sock"].family == socket.AF_INET6:
            raise ConnectionResetError("IPv6 TLS transport reset")
        return FakeConnection()

    import websockets.sync.client

    monkeypatch.setattr(websocket_transport, "_resolved_proxy", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(websocket_transport, "_ipv6_preferred_socket_sync", preferred)
    monkeypatch.setattr(websockets.sync.client, "connect", connect)

    with websocket_transport.connect_websocket_sync_ipv6_preferred(
        "wss://api.example/ws",
        open_timeout=7,
    ) as connection:
        assert connection.remote_address[0] == "192.0.2.1"

    assert selected_families == [
        (socket.AF_INET6, socket.AF_INET),
        (socket.AF_INET,),
    ]
    assert sockets[0].closed is True


def test_connector_supplies_preconnected_socket_without_changing_uri(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeRawSocket:
        family = socket.AF_INET6

        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    class FakeConnection:
        remote_address = ("2001:db8::1", 443, 0, 0)
        local_address = ("2001:db8::2", 50000, 0, 0)

    class FakeConnector:
        async def __aenter__(self) -> FakeConnection:
            return FakeConnection()

        async def __aexit__(self, *_args: object) -> None:
            return None

    raw = FakeRawSocket()
    captured: dict[str, Any] = {}

    async def preferred(
        _uri: str,
        *,
        timeout_s: float,
        families: tuple[int, ...],
    ) -> Any:
        captured["timeout_s"] = timeout_s
        captured["families"] = families
        return raw

    def connect(uri: str, **kwargs: Any) -> FakeConnector:
        captured["uri"] = uri
        captured["kwargs"] = kwargs
        return FakeConnector()

    import websockets

    monkeypatch.setattr(websocket_transport, "_resolved_proxy", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(websocket_transport, "_ipv6_preferred_socket", preferred)
    monkeypatch.setattr(websockets, "connect", connect)

    async def exercise() -> None:
        async with websocket_transport.connect_websocket_ipv6_preferred(
            "wss://api.example/ws",
            open_timeout=7,
            ping_interval=None,
        ) as connection:
            assert isinstance(connection, FakeConnection)

    asyncio.run(exercise())

    assert captured["uri"] == "wss://api.example/ws"
    assert captured["timeout_s"] == pytest.approx(7, abs=0.01)
    assert captured["families"] == (socket.AF_INET6, socket.AF_INET)
    assert captured["kwargs"]["sock"] is raw
    assert captured["kwargs"]["proxy"] is None
    assert "host" not in captured["kwargs"]
    assert "server_hostname" not in captured["kwargs"]


def test_ipv6_websocket_open_transport_failure_retries_ipv4(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeRawSocket:
        def __init__(self, family: int) -> None:
            self.family = family
            self.closed = False

        def close(self) -> None:
            self.closed = True

    class FakeConnection:
        remote_address = ("192.0.2.1", 443)
        local_address = ("192.0.2.2", 50000)

    class FakeConnector:
        def __init__(self, *, fail: bool) -> None:
            self.fail = fail

        async def __aenter__(self) -> FakeConnection:
            if self.fail:
                raise ConnectionResetError("IPv6 TLS transport reset")
            return FakeConnection()

        async def __aexit__(self, *_args: object) -> None:
            return None

    selected_families: list[tuple[int, ...]] = []
    sockets: list[FakeRawSocket] = []

    async def preferred(
        _uri: str,
        *,
        timeout_s: float | None,
        families: tuple[int, ...],
    ) -> Any:
        assert timeout_s is None or timeout_s > 0
        selected_families.append(families)
        family = socket.AF_INET6 if socket.AF_INET6 in families else socket.AF_INET
        raw = FakeRawSocket(family)
        sockets.append(raw)
        return raw

    def connect(_uri: str, **kwargs: Any) -> FakeConnector:
        return FakeConnector(fail=kwargs["sock"].family == socket.AF_INET6)

    import websockets

    monkeypatch.setattr(websocket_transport, "_resolved_proxy", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(websocket_transport, "_ipv6_preferred_socket", preferred)
    monkeypatch.setattr(websockets, "connect", connect)

    async def exercise() -> None:
        async with websocket_transport.connect_websocket_ipv6_preferred(
            "wss://api.example/ws",
            open_timeout=7,
        ) as connection:
            assert connection.remote_address[0] == "192.0.2.1"

    asyncio.run(exercise())

    assert selected_families == [
        (socket.AF_INET6, socket.AF_INET),
        (socket.AF_INET,),
    ]
    assert sockets[0].closed is True


def test_transport_snapshot_explicitly_reports_ipv4_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    endpoint = websocket_transport.WebSocketEndpoint(
        uri="wss://api.example/ws",
        host="api.example",
        port=443,
        proxy_configured=False,
        ipv6_addresses=("2001:db8::1",),
        ipv4_addresses=("192.0.2.1",),
    )
    monkeypatch.setattr(websocket_transport, "_resolved_proxy", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(websocket_transport, "websocket_endpoint", lambda *_args: endpoint)

    connection = type(
        "Connection",
        (),
        {"remote_address": ("192.0.2.1", 443), "local_address": ("192.0.2.2", 50000)},
    )()
    payload = websocket_transport.websocket_transport_snapshot(
        connection,
        "wss://api.example/ws",
    )

    assert payload["selected_family"] == "ipv4"
    assert payload["ipv4_fallback_used"] is True
    assert payload["ipv6_selected_when_available"] is False


def test_proxy_snapshot_delegates_origin_resolution_without_local_dns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        websocket_transport,
        "_resolved_proxy",
        lambda *_args, **_kwargs: "socks5h://127.0.0.1:1080",
    )
    monkeypatch.setattr(
        websocket_transport,
        "websocket_endpoint",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("proxy-owned origin must not be resolved locally")
        ),
    )
    connection = type(
        "Connection",
        (),
        {"remote_address": ("127.0.0.1", 1080), "local_address": ("127.0.0.1", 50000)},
    )()

    payload = websocket_transport.websocket_transport_snapshot(
        connection,
        "wss://api.example/ws",
    )

    assert payload["origin_resolution"] == "proxy_managed"
    assert payload["selected_family"] == "proxy_managed"
    assert payload["ipv6_available"] is None
    assert payload["ipv6_selected_when_available"] is None
