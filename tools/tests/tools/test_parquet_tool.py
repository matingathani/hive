"""Tests for parquet_tool - Read and query Parquet files."""

import importlib.util
from pathlib import Path
from unittest.mock import patch

import pytest
from fastmcp import FastMCP

from aden_tools.tools.parquet_tool.parquet_tool import register_tools

duckdb_available = importlib.util.find_spec("duckdb") is not None

TEST_WORKSPACE_ID = "test-workspace"
TEST_AGENT_ID = "test-agent"
TEST_SESSION_ID = "test-session"


@pytest.fixture
def parquet_tools(mcp: FastMCP, tmp_path: Path):
    with patch("aden_tools.tools.file_system_toolkits.security.WORKSPACES_DIR", str(tmp_path)):
        register_tools(mcp)
        yield {
            "parquet_read": mcp._tool_manager._tools["parquet_read"].fn,
            "parquet_write": mcp._tool_manager._tools["parquet_write"].fn,
            "parquet_info": mcp._tool_manager._tools["parquet_info"].fn,
        }


@pytest.fixture
def session_dir(tmp_path: Path) -> Path:
    session_path = tmp_path / TEST_WORKSPACE_ID / TEST_AGENT_ID / TEST_SESSION_ID
    session_path.mkdir(parents=True, exist_ok=True)
    return session_path


@pytest.fixture
def basic_parquet(session_dir: Path):
    if not duckdb_available:
        pytest.skip("duckdb not installed")

    import duckdb

    parquet_file = session_dir / "basic.parquet"
    conn = duckdb.connect()
    conn.execute(
        "CREATE TABLE data AS SELECT * FROM (VALUES (1, 'Alice', 30), (2, 'Bob', 25),"
        " (3, 'Charlie', 35)) AS t(id, name, age)"
    )
    conn.execute("COPY data TO ? (FORMAT PARQUET)", [str(parquet_file)])
    return parquet_file


def test_parquet_read_basic(parquet_tools, basic_parquet, tmp_path):
    if not duckdb_available:
        pytest.skip("duckdb not installed")

    with patch("aden_tools.tools.file_system_toolkits.security.WORKSPACES_DIR", str(tmp_path)):
        result = parquet_tools["parquet_read"](
            path="basic.parquet",
            workspace_id=TEST_WORKSPACE_ID,
            agent_id=TEST_AGENT_ID,
            session_id=TEST_SESSION_ID,
        )

    assert result["success"] is True
    assert result["column_count"] == 3
    assert result["row_count"] == 3
    assert result["total_rows"] == 3


def test_parquet_read_with_filters(parquet_tools, basic_parquet, tmp_path):
    if not duckdb_available:
        pytest.skip("duckdb not installed")

    with patch("aden_tools.tools.file_system_toolkits.security.WORKSPACES_DIR", str(tmp_path)):
        result = parquet_tools["parquet_read"](
            path="basic.parquet",
            workspace_id=TEST_WORKSPACE_ID,
            agent_id=TEST_AGENT_ID,
            session_id=TEST_SESSION_ID,
            filters=[{"column": "age", "op": ">", "value": 30}],
        )

    assert result["success"] is True
    assert result["row_count"] == 1
    assert result["rows"][0]["name"] == "Charlie"


def test_parquet_read_group_by(parquet_tools, basic_parquet, tmp_path):
    if not duckdb_available:
        pytest.skip("duckdb not installed")

    with patch("aden_tools.tools.file_system_toolkits.security.WORKSPACES_DIR", str(tmp_path)):
        result = parquet_tools["parquet_read"](
            path="basic.parquet",
            workspace_id=TEST_WORKSPACE_ID,
            agent_id=TEST_AGENT_ID,
            session_id=TEST_SESSION_ID,
            group_by=["age"],
            aggregates=[{"column": "id", "op": "count", "alias": "count_id"}],
            order_by=[{"column": "age", "direction": "desc"}],
        )

    assert result["success"] is True
    assert result["rows"][0]["count_id"] == 1


def test_parquet_info(parquet_tools, basic_parquet, tmp_path):
    if not duckdb_available:
        pytest.skip("duckdb not installed")

    with patch("aden_tools.tools.file_system_toolkits.security.WORKSPACES_DIR", str(tmp_path)):
        result = parquet_tools["parquet_info"](
            path="basic.parquet",
            workspace_id=TEST_WORKSPACE_ID,
            agent_id=TEST_AGENT_ID,
            session_id=TEST_SESSION_ID,
        )

    assert result["success"] is True
    assert result["column_count"] == 3
    assert result["total_rows"] == 3


def test_parquet_write_and_read(parquet_tools, session_dir, tmp_path):
    if not duckdb_available:
        pytest.skip("duckdb not installed")

    with patch("aden_tools.tools.file_system_toolkits.security.WORKSPACES_DIR", str(tmp_path)):
        write_result = parquet_tools["parquet_write"](
            path="written.parquet",
            workspace_id=TEST_WORKSPACE_ID,
            agent_id=TEST_AGENT_ID,
            session_id=TEST_SESSION_ID,
            columns=["id", "name"],
            rows=[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        )

        assert write_result["success"] is True

        read_result = parquet_tools["parquet_read"](
            path="written.parquet",
            workspace_id=TEST_WORKSPACE_ID,
            agent_id=TEST_AGENT_ID,
            session_id=TEST_SESSION_ID,
        )

    assert read_result["success"] is True
    assert read_result["row_count"] == 2


def test_parquet_extension_required(parquet_tools, session_dir, tmp_path):
    with patch("aden_tools.tools.file_system_toolkits.security.WORKSPACES_DIR", str(tmp_path)):
        result = parquet_tools["parquet_read"](
            path="data.txt",
            workspace_id=TEST_WORKSPACE_ID,
            agent_id=TEST_AGENT_ID,
            session_id=TEST_SESSION_ID,
        )

    assert "error" in result
    assert ".parquet" in result["error"]
