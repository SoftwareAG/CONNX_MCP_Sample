import asyncio
import hashlib
import logging
import os
from typing import Any, Dict, List, Optional

import pyodbc
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# Load .env from current working directory (if present).
# Host-provided environment variables still override .env values.
load_dotenv()

# Required configuration (no unsafe defaults)
CONNX_DSN = os.getenv("CONNX_DSN")
CONNX_USER = os.getenv("CONNX_USER")
CONNX_PASS = os.getenv("CONNX_PASS")

# Optional security controls
CONNX_ALLOW_WRITES = os.getenv("CONNX_ALLOW_WRITES", "false").strip().lower() == "true"

# Setup logging (log to stderr to avoid interfering with MCP stdout)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# MCP Server Initialization
mcp = FastMCP("connx-database-server")


def _assert_config() -> None:
    missing = [k for k in ("CONNX_DSN", "CONNX_USER", "CONNX_PASS") if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required config values: {', '.join(missing)}")


def _sql_fingerprint(sql: str) -> str:
    """Short stable fingerprint for logs without leaking SQL text."""
    digest = hashlib.sha256(sql.encode("utf-8", errors="ignore")).hexdigest()
    return digest[:12]


def _is_single_statement(sql: str) -> bool:
    """
    Basic single-statement check.
    - Reject semicolons to avoid multi-statement batches.
    - Strip whitespace.
    """
    s = (sql or "").strip()
    return bool(s) and (";" not in s)


def _is_select_only(sql: str) -> bool:
    """
    Enforce SELECT-only for query tool.
    ANSI SQL-92 doesn't include WITH; keep it simple for safety.
    If you need WITH/CTEs later, expand this carefully.
    """
    s = (sql or "").lstrip().lower()
    return s.startswith("select")


def get_connx_connection():
    """Establish a connection to CONNX via pyodbc."""
    _assert_config()
    conn_str = f"DSN={CONNX_DSN};UID={CONNX_USER};PWD={CONNX_PASS}"
    try:
        conn = pyodbc.connect(conn_str)
        logger.info("Successfully connected to CONNX")
        return conn
    except pyodbc.Error as e:
        logger.error("Connection failed: %s", e)
        raise ValueError(f"Failed to connect to CONNX: {str(e)}")


async def execute_query_async(query: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
    """Asynchronous execution of SELECT queries via CONNX."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, execute_query, query, params)


def execute_query(query: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
    """Execute SELECT query and return results as list of dicts."""
    conn = get_connx_connection()
    fp = _sql_fingerprint(query)
    try:
        cursor = conn.cursor()
        cursor.execute(query, params or [])
        if cursor.description is None:
            # A SELECT should provide a description; if not, treat as an error.
            raise ValueError("Query did not return a result set (cursor.description is None).")

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
        logger.info("Query OK fp=%s rows=%d", fp, len(results))
        return results
    except (pyodbc.Error, ValueError) as e:
        logger.error("Query failed fp=%s err=%s", fp, e)
        raise ValueError(f"Query execution failed: {str(e)}")
    finally:
        conn.close()


async def execute_update_async(query: str, params: Optional[List[Any]] = None) -> int:
    """Asynchronous execution of UPDATE/INSERT/DELETE."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, execute_update, query, params)


def execute_update(query: str, params: Optional[List[Any]] = None) -> int:
    """Execute non-SELECT query and return affected rows."""
    conn = get_connx_connection()
    fp = _sql_fingerprint(query)
    try:
        cursor = conn.cursor()
        cursor.execute(query, params or [])
        affected = cursor.rowcount
        conn.commit()
        logger.info("Update OK fp=%s affected=%s", fp, affected)
        return int(affected) if affected is not None else 0
    except pyodbc.Error as e:
        conn.rollback()
        logger.error("Update failed fp=%s err=%s", fp, e)
        raise ValueError(f"Update execution failed: {str(e)}")
    finally:
        conn.close()


# MCP Tools
@mcp.tool()
async def query_connx(query: str) -> Dict[str, Any]:
    """
    Query data from CONNX-connected databases using SQL.

    Security:
    - Enforces single-statement SELECT-only.
    - Use parameterized queries for values (preferred via purpose-built tools).
    """
    if not _is_single_statement(query):
        return {"error": "Only a single SQL statement is allowed (no semicolons)."}
    if not _is_select_only(query):
        return {"error": "Only SELECT statements are allowed for query_connx."}

    try:
        results = await execute_query_async(query)
        return {"results": results, "count": len(results)}
    except ValueError as e:
        return {"error": str(e)}


@mcp.tool()
async def update_connx(operation: str, query: str) -> Dict[str, Any]:
    """
    Perform update operations via CONNX.

    Security:
    - Writes are disabled unless CONNX_ALLOW_WRITES=true.
    - Enforces single-statement execution.
    """
    if not CONNX_ALLOW_WRITES:
        return {"error": "Writes are disabled. Set CONNX_ALLOW_WRITES=true to enable update operations."}

    if operation.lower() not in ["insert", "update", "delete"]:
        return {"error": "Invalid operation. Must be 'insert', 'update', or 'delete'."}

    if not _is_single_statement(query):
        return {"error": "Only a single SQL statement is allowed (no semicolons)."}

    try:
        affected = await execute_update_async(query)
        return {"affected_rows": affected, "message": f"{operation.capitalize()} completed successfully."}
    except ValueError as e:
        return {"error": str(e)}


# MCP Resources
@mcp.resource("schema://schema")
async def get_schema() -> Dict[str, Any]:
    query = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS"
    try:
        results = await execute_query_async(query)
        return {"schemas": results}
    except ValueError as e:
        return {"error": str(e)}


@mcp.resource("schema://schema/{table_name}")
async def get_schema_for_table(table_name: str) -> Dict[str, Any]:
    query = (
        "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_NAME = ?"
    )
    try:
        results = await execute_query_async(query, params=[table_name])
        return {"schemas": results}
    except ValueError as e:
        return {"error": str(e)}


# Optional helper: map full state names to 2-letter codes (extend as needed)
STATE_NAME_TO_CODE = {
    "virginia": "VA",
    "california": "CA",
    "texas": "TX",
    "new york": "NY",
    "florida": "FL",
    # add more as you want
}


def _normalize_state(state: str) -> str:
    s = (state or "").strip()
    if not s:
        return s
    return STATE_NAME_TO_CODE.get(s.lower(), s)


@mcp.tool()
async def find_customers(state: str, city: Optional[str] = None, max_rows: int = 100) -> Dict[str, Any]:
    """
    Find customers by state and optional city.

    Notes:
    - VSAM/CONNX string columns are often fixed-width CHAR and right-space padded.
      Use RTRIM() for consistent comparisons and clean output.
    - ANSI SQL-92: no LIMIT/TOP; we apply max_rows in Python after fetch.
    """
    state_code = _normalize_state(state)

    sql = """
        SELECT
            RTRIM(CUSTOMERID)       AS CUSTOMERID,
            RTRIM(CUSTOMERNAME)     AS CUSTOMERNAME,
            RTRIM(CUSTOMERADDRESS)  AS CUSTOMERADDRESS,
            RTRIM(CUSTOMERCITY)     AS CUSTOMERCITY,
            RTRIM(CUSTOMERSTATE)    AS CUSTOMERSTATE,
            RTRIM(CUSTOMERZIP)      AS CUSTOMERZIP,
            RTRIM(CUSTOMERCOUNTRY)  AS CUSTOMERCOUNTRY,
            RTRIM(CUSTOMERPHONE)    AS CUSTOMERPHONE
        FROM daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM
        WHERE UPPER(RTRIM(CUSTOMERSTATE)) = UPPER(?)
    """
    params: List[Any] = [state_code]

    if city and city.strip():
        sql += " AND UPPER(RTRIM(CUSTOMERCITY)) = UPPER(?)"
        params.append(city.strip())

    sql += " ORDER BY RTRIM(CUSTOMERNAME)"

    try:
        results = await execute_query_async(sql, params=params)

        truncated = False
        if max_rows and max_rows > 0 and len(results) > max_rows:
            results = results[:max_rows]
            truncated = True

        return {"results": results, "count": len(results), "truncated": truncated}
    except ValueError as e:
        return {"error": str(e)}


# Main Entry Point
if __name__ == "__main__":
    # FastMCP.run() manages its own event loop via anyio.run()
    mcp.run(transport="stdio")