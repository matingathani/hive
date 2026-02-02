"""Parquet Tool - Read and query Parquet files."""

from __future__ import annotations

import os
import re
from typing import Any

from fastmcp import FastMCP

from ..file_system_toolkits.security import get_secure_path

ALLOWED_OPERATORS = {"=", "!=", ">", "<", ">=", "<=", "in", "like"}
ALLOWED_AGGREGATES = {"count", "sum", "avg", "min", "max"}
ALLOWED_ORDER = {"asc", "desc"}
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str) -> bool:
    return bool(IDENTIFIER_RE.match(name))


def _error_invalid_identifier(name: str) -> dict:
    return {"error": f"Invalid column name: {name}"}


def _require_duckdb() -> tuple[object | None, dict | None]:
    try:
        import duckdb

        return duckdb, None
    except ImportError:
        return None, {"error": "duckdb is required for parquet tools"}


def _build_query(
    table_expr: str,
    selected_columns: list[str] | None,
    filters: list[dict] | None,
    group_by: list[str] | None,
    order_by: list[dict] | None,
    limit: int | None,
    offset: int,
    aggregates: list[dict] | None,
) -> tuple[str, list[Any]] | tuple[None, dict]:
    params: list[Any] = []

    if selected_columns:
        for col in selected_columns:
            if not _validate_identifier(col):
                return None, _error_invalid_identifier(col)

    if group_by:
        for col in group_by:
            if not _validate_identifier(col):
                return None, _error_invalid_identifier(col)

    select_parts: list[str] = []
    if group_by:
        select_parts.extend(group_by)
        if aggregates:
            for agg in aggregates:
                column = agg.get("column")
                func = str(agg.get("op", "")).lower()
                alias = agg.get("alias")
                if not column or not _validate_identifier(column):
                    return None, _error_invalid_identifier(str(column))
                if func not in ALLOWED_AGGREGATES:
                    return None, {"error": f"Unsupported aggregate: {func}"}
                expr = f"{func}({column})"
                if alias:
                    if not _validate_identifier(alias):
                        return None, _error_invalid_identifier(str(alias))
                    expr = f"{expr} AS {alias}"
                select_parts.append(expr)
        else:
            select_parts = group_by
    elif selected_columns:
        select_parts = selected_columns
    else:
        select_parts = ["*"]

    where_parts: list[str] = []
    if filters:
        for condition in filters:
            column = condition.get("column")
            op = str(condition.get("op", "")).lower()
            value = condition.get("value")
            if not column or not _validate_identifier(column):
                return None, _error_invalid_identifier(str(column))
            if op not in ALLOWED_OPERATORS:
                return None, {"error": f"Unsupported operator: {op}"}
            if op == "in":
                if not isinstance(value, list) or not value:
                    return None, {"error": "IN operator requires a non-empty list value"}
                placeholders = ", ".join(["?"] * len(value))
                where_parts.append(f"{column} IN ({placeholders})")
                params.extend(value)
            else:
                where_parts.append(f"{column} {op} ?")
                params.append(value)

    order_parts: list[str] = []
    if order_by:
        for entry in order_by:
            column = entry.get("column")
            direction = str(entry.get("direction", "asc")).lower()
            if not column or not _validate_identifier(column):
                return None, _error_invalid_identifier(str(column))
            if direction not in ALLOWED_ORDER:
                return None, {"error": f"Unsupported order direction: {direction}"}
            order_parts.append(f"{column} {direction.upper()}")

    query = f"SELECT {', '.join(select_parts)} FROM {table_expr}"

    if where_parts:
        query += " WHERE " + " AND ".join(where_parts)
    if group_by:
        query += " GROUP BY " + ", ".join(group_by)
    if order_parts:
        query += " ORDER BY " + ", ".join(order_parts)
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    if offset:
        query += " OFFSET ?"
        params.append(offset)

    return query, params


def register_tools(mcp: FastMCP) -> None:
    """Register parquet tools with the MCP server."""

    @mcp.tool()
    def parquet_read(
        path: str,
        workspace_id: str,
        agent_id: str,
        session_id: str,
        limit: int | None = None,
        offset: int = 0,
        selected_columns: list[str] | None = None,
        filters: list[dict] | None = None,
        group_by: list[str] | None = None,
        order_by: list[dict] | None = None,
        aggregates: list[dict] | None = None,
    ) -> dict:
        """
        Read a Parquet file and return its contents.

        Supports filters, group by, and ordering via structured parameters.
        """
        if offset < 0 or (limit is not None and limit < 0):
            return {"error": "offset and limit must be non-negative"}

        if not path.lower().endswith(".parquet"):
            return {"error": "File must have .parquet extension"}

        duckdb, error = _require_duckdb()
        if error:
            return error

        try:
            secure_path = get_secure_path(path, workspace_id, agent_id, session_id)
            if not os.path.exists(secure_path):
                return {"error": f"File not found: {path}"}

            table_expr = "read_parquet(?)"
            query, params_or_error = _build_query(
                table_expr=table_expr,
                selected_columns=selected_columns,
                filters=filters,
                group_by=group_by,
                order_by=order_by,
                limit=limit,
                offset=offset,
                aggregates=aggregates,
            )
            if not query:
                return params_or_error

            params = [secure_path, *params_or_error]

            conn = duckdb.connect()
            result = conn.execute(query, params).fetchdf()
            rows = result.to_dict(orient="records")
            columns = list(result.columns)

            total_rows = conn.execute("SELECT COUNT(*) FROM read_parquet(?)", [secure_path]).fetchone()[0]

            return {
                "success": True,
                "path": path,
                "columns": columns,
                "column_count": len(columns),
                "rows": rows,
                "row_count": len(rows),
                "total_rows": total_rows,
                "offset": offset,
                "limit": limit,
            }
        except Exception as exc:
            return {"error": f"Failed to read parquet: {str(exc)}"}

    @mcp.tool()
    def parquet_write(
        path: str,
        workspace_id: str,
        agent_id: str,
        session_id: str,
        columns: list[str],
        rows: list[dict],
    ) -> dict:
        """Write data to a Parquet file."""
        if not path.lower().endswith(".parquet"):
            return {"error": "File must have .parquet extension"}

        if not columns:
            return {"error": "columns cannot be empty"}

        duckdb, error = _require_duckdb()
        if error:
            return error

        try:
            import pandas as pd

            secure_path = get_secure_path(path, workspace_id, agent_id, session_id)
            parent_dir = os.path.dirname(secure_path)
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)

            df = pd.DataFrame(rows, columns=columns)
            conn = duckdb.connect()
            conn.register("data_df", df)
            conn.execute("COPY data_df TO ? (FORMAT PARQUET)", [secure_path])

            return {
                "success": True,
                "path": path,
                "columns": columns,
                "column_count": len(columns),
                "rows_written": len(df),
            }
        except Exception as exc:
            return {"error": f"Failed to write parquet: {str(exc)}"}

    @mcp.tool()
    def parquet_info(
        path: str,
        workspace_id: str,
        agent_id: str,
        session_id: str,
    ) -> dict:
        """Return metadata about a Parquet file."""
        if not path.lower().endswith(".parquet"):
            return {"error": "File must have .parquet extension"}

        duckdb, error = _require_duckdb()
        if error:
            return error

        try:
            secure_path = get_secure_path(path, workspace_id, agent_id, session_id)
            if not os.path.exists(secure_path):
                return {"error": f"File not found: {path}"}

            conn = duckdb.connect()
            describe = conn.execute("DESCRIBE SELECT * FROM read_parquet(?)", [secure_path]).fetchall()
            columns = [row[0] for row in describe]
            total_rows = conn.execute("SELECT COUNT(*) FROM read_parquet(?)", [secure_path]).fetchone()[0]

            return {
                "success": True,
                "path": path,
                "columns": columns,
                "column_count": len(columns),
                "total_rows": total_rows,
                "file_size": os.path.getsize(secure_path),
            }
        except Exception as exc:
            return {"error": f"Failed to read parquet info: {str(exc)}"}
