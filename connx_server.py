import asyncio
import logging
import re
from typing import Any, Dict, List, Optional

import pyodbc
from mcp.server.fastmcp import FastMCP  # Assuming FastMCP is installed via mcp[cli] or separately

from dotenv import load_dotenv
import os

load_dotenv()  # loads .env from current working directory

CONNX_DSN = os.getenv("CONNX_DSN", "Share_2025")
CONNX_USER = os.getenv("CONNX_USER", "sag")
CONNX_PASS = os.getenv("CONNX_PASS", "sag")

# Setup logging (log to stderr to avoid interfering with MCP stdout)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MCP Server Initialization
mcp = FastMCP("connx-database-server")

def _assert_config():
    missing = [k for k in ("CONNX_DSN", "CONNX_USER", "CONNX_PASS") if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required config values: {', '.join(missing)}")

_assert_config()

def get_connx_connection():
    """Establish a connection to CONNX via pyodbc."""
    conn_str = f"DSN={CONNX_DSN};UID={CONNX_USER};PWD={CONNX_PASS}"
    try:
        conn = pyodbc.connect(conn_str)
        logger.info("Successfully connected to CONNX")
        return conn
    except pyodbc.Error as e:
        logger.error(f"Connection failed: {e}")
        raise ValueError(f"Failed to connect to CONNX: {str(e)}")


def sanitize_input(input_str: str) -> str:
    """Basic sanitization to prevent SQL injection (use with parameterized queries)."""
    # Remove common injection patterns, including full comments
    return re.sub(r'(--.*|;|/\*.*\*/|DROP|ALTER|EXEC|UNION|SELECT\s+.*\s+FROM\s+INFORMATION_SCHEMA)', '', input_str,
                  flags=re.IGNORECASE)


async def execute_query_async(query: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
    """Asynchronous execution of SELECT queries via CONNX."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, execute_query, query, params)


def execute_query(query: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
    """Execute SELECT query and return results as list of dicts."""
    conn = get_connx_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sanitize_input(query), params or [])
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        logger.info(f"Query executed successfully: {query}")
        return results
    except pyodbc.Error as e:
        logger.error(f"Query failed: {e}")
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
    try:
        cursor = conn.cursor()
        cursor.execute(sanitize_input(query), params or [])
        affected = cursor.rowcount
        conn.commit()
        logger.info(f"Update executed successfully: {query}, affected rows: {affected}")
        return affected
    except pyodbc.Error as e:
        conn.rollback()
        logger.error(f"Update failed: {e}")
        raise ValueError(f"Update execution failed: {str(e)}")
    finally:
        conn.close()


# MCP Tools
@mcp.tool()
async def query_connx(query: str) -> Dict[str, Any]:
    """
    Query data from CONNX-connected databases using SQL.

    Args:
        query: SQL SELECT statement (e.g., 'SELECT * FROM Sales WHERE Region = ?')

    Returns:
        Dict with 'results' (list of dicts) and 'count'.
    """
    try:
        results = await execute_query_async(query)
        return {"results": results, "count": len(results)}
    except ValueError as e:
        return {"error": str(e)}


@mcp.tool()
async def update_connx(operation: str, query: str) -> Dict[str, Any]:
    """
    Perform update operations via CONNX.

    Args:
        operation: 'insert', 'update', or 'delete'
        query: Full SQL statement for the operation

    Returns:
        Dict with 'affected_rows' or 'error'.
    """
    if operation.lower() not in ['insert', 'update', 'delete']:
        return {"error": "Invalid operation. Must be 'insert', 'update', or 'delete'."}
    try:
        affected = await execute_update_async(query)
        return {"affected_rows": affected, "message": f"{operation.capitalize()} completed successfully."}
    except ValueError as e:
        return {"error": str(e)}


# 1) Base schema resource (no params)
@mcp.resource("schema://schema")
async def get_schema() -> Dict[str, Any]:
    query = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS"
    try:
        results = await execute_query_async(query)
        return {"schemas": results}
    except ValueError as e:
        return {"error": str(e)}


# 2) Parameterized schema resource (path param)
@mcp.resource("schema://schema/{table_name}")
async def get_schema_for_table(table_name: str) -> Dict[str, Any]:
    query = (
        "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_NAME = '{sanitize_input(table_name)}'"
    )
    try:
        results = await execute_query_async(query)
        return {"schemas": results}
    except ValueError as e:
        return {"error": str(e)}
# Main Entry Point
async def main():
    # Run the server (use 'stdio' for local, 'http' for remote)
    await mcp.run(transport='stdio')  # Or 'http' with port configuration


if __name__ == "__main__":
    asyncio.run(main())