import asyncio
import threading

import pytest


def _mcp_available() -> bool:
    """Check if MCP dependencies are installed."""
    try:
        import mcp  # noqa: F401

        return True
    except ImportError:
        return False


MCP_AVAILABLE = _mcp_available()
MCP_SKIP_REASON = "MCP dependencies not installed"


class FakeLoop:
    def create_task(self, coro) -> None:
        coro.close()

    def run_forever(self) -> None:
        return None

    def is_running(self) -> bool:
        return False

    def is_closed(self) -> bool:
        return False


class FakeThread:
    def __init__(self, target, daemon: bool = False):
        self._target = target

    def start(self) -> None:
        self._target()

    def join(self) -> None:
        return None


class FakeEvent:
    def __init__(self):
        self._flag = False

    def set(self) -> None:
        self._flag = True

    def is_set(self) -> bool:
        return self._flag

    def wait(self, timeout: float | None = None) -> bool:
        return self._flag


def test_stdio_connect_times_out_when_session_not_ready(monkeypatch):
    if not MCP_AVAILABLE:
        pytest.skip(MCP_SKIP_REASON)

    from framework.runner.mcp_client import MCPClient, MCPServerConfig

    monkeypatch.setattr(asyncio, "new_event_loop", lambda: FakeLoop())
    monkeypatch.setattr(asyncio, "set_event_loop", lambda loop: None)
    monkeypatch.setattr(threading, "Thread", FakeThread)
    monkeypatch.setattr(threading, "Event", FakeEvent)

    client = MCPClient(
        MCPServerConfig(
            name="test",
            transport="stdio",
            command="echo",
        )
    )

    with pytest.raises(
        RuntimeError, match="Timed out waiting for MCP stdio session to initialize"
    ):
        client._connect_stdio()
import asyncio
import threading

import pytest


def _mcp_available() -> bool:
    """Check if MCP dependencies are installed."""
    try:
        import mcp  # noqa: F401

        return True
    except ImportError:
        return False


MCP_AVAILABLE = _mcp_available()
MCP_SKIP_REASON = "MCP dependencies not installed"


class FakeLoop:
    def create_task(self, coro) -> None:
        coro.close()

    def run_forever(self) -> None:
        return None

    def is_running(self) -> bool:
        return False

    def is_closed(self) -> bool:
        return False


class FakeThread:
    def __init__(self, target, daemon: bool = False):
        self._target = target

    def start(self) -> None:
        self._target()

    def join(self) -> None:
        return None


class FakeEvent:
    def __init__(self):
        self._flag = False

    def set(self) -> None:
        self._flag = True

    def is_set(self) -> bool:
        return self._flag

    def wait(self, timeout: float | None = None) -> bool:
        return self._flag


def test_stdio_connect_times_out_when_session_not_ready(monkeypatch):
    if not MCP_AVAILABLE:
        pytest.skip(MCP_SKIP_REASON)

    from framework.runner.mcp_client import MCPClient, MCPServerConfig

    monkeypatch.setattr(asyncio, "new_event_loop", lambda: FakeLoop())
    monkeypatch.setattr(asyncio, "set_event_loop", lambda loop: None)
    monkeypatch.setattr(threading, "Thread", FakeThread)
    monkeypatch.setattr(threading, "Event", FakeEvent)

    client = MCPClient(
        MCPServerConfig(
            name="test",
            transport="stdio",
            command="echo",
        )
    )

    with pytest.raises(
        RuntimeError, match="Timed out waiting for MCP stdio session to initialize"
    ):
        client._connect_stdio()
