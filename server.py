# server.py
import json
import logging
import sys
from typing import Any, Dict

from db_connx import ConnxDatabase, ConnxSafetyError, ConnxConfigError

# ---------- Logging ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger("mcp_connx")


# ---------- Initialize DB ----------

try:
    db = ConnxDatabase("config.json")
    logger.info("Initialized ConnxDatabase with DSN=%s", db.dsn)
except ConnxConfigError as e:
    logger.error("Configuration error: %s", e)
    # In a real MCP server you might signal this to the client
    db = None


# ---------- Tool implementations ----------

def tool_connx_query_safe(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Tool: connx_query_safe
    params:
      - sql: str
      - max_rows: int (optional)
    """
    if db is None:
        raise RuntimeError("CONNX database is not initialized.")

    sql = params.get("sql")
    if not sql or not isinstance(sql, str):
        raise ValueError("Parameter 'sql' must be a non-empty string.")

    max_rows = params.get("max_rows", 500)
    if not isinstance(max_rows, int) or max_rows <= 0:
        max_rows = 500

    logger.info("Executing safe CONNX query (max_rows=%s)", max_rows)
    result = db.query(sql, max_rows=max_rows)
    return {
        "ok": True,
        "data": result,
    }


def tool_connx_list_tables(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Tool: connx_list_tables
    params:
      - schema: str (optional)
    """
    if db is None:
        raise RuntimeError("CONNX database is not initialized.")

    schema = params.get("schema")
    logger.info("Listing tables for schema=%s", schema)
    tables = db.list_tables(schema=schema)
    return {
        "ok": True,
        "tables": tables,
    }


def tool_connx_describe_table(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Tool: connx_describe_table
    params:
      - table_name: str (required)
    """
    if db is None:
        raise RuntimeError("CONNX database is not initialized.")

    table_name = params.get("table_name")
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Parameter 'table_name' is required and must be a string.")

    logger.info("Describing table %s", table_name)
    cols = db.describe_table(table_name)
    return {
        "ok": True,
        "columns": cols,
    }


# Map method names to tool implementations
TOOLS: Dict[str, Any] = {
    "connx_query_safe": tool_connx_query_safe,
    "connx_list_tables": tool_connx_list_tables,
    "connx_describe_table": tool_connx_describe_table,
}


# ---------- JSON-RPC / MCP-style loop ----------

def handle_request(request: Dict[str, Any]) -> Dict[str, Any]:
    """
    Minimal JSON-RPC 2.0-style handler. MCP sits on top of JSON-RPC,
    so your real MCP server will have more structure (initialize, prompts, tools list, etc.).

    Expected:
    {
      "jsonrpc": "2.0",
      "id": 1,
      "method": "tools/connx_query_safe",
      "params": { ... }
    }
    """
    jsonrpc = request.get("jsonrpc")
    if jsonrpc != "2.0":
        raise ValueError("Only JSON-RPC 2.0 is supported in this skeleton.")

    method = request.get("method")
    req_id = request.get("id")
    params = request.get("params") or {}

    if not method or not isinstance(method, str):
        raise ValueError("Missing 'method'.")

    # Simple convention: methods starting with "tools/" map to TOOLS dict
    if method.startswith("tools/"):
        tool_name = method.split("/", 1)[1]
        tool_func = TOOLS.get(tool_name)
        if not tool_func:
            raise ValueError(f"Unknown tool: {tool_name}")

        try:
            result = tool_func(params)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": result,
            }
        except ConnxSafetyError as se:
            logger.warning("Safety error: %s", se)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32001,
                    "message": "Safety error",
                    "data": str(se),
                },
            }
        except Exception as e:  # noqa: BLE001
            logger.exception("Tool error")
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32000,
                    "message": "Tool execution error",
                    "data": str(e),
                },
            }

    # You could also add MCP "initialize", "tools/list", etc. here.
    raise ValueError(f"Unknown method: {method}")


def main() -> None:
    """
    Simple stdio loop: read JSON per line, respond JSON per line.
    MCP clients generally spawn the server as a subprocess and
    communicate via stdin/stdout in a similar way.
    """
    logger.info("Starting MCP-style CONNX server on stdio")

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            request = json.loads(line)
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON: %s", e)
            continue

        try:
            response = handle_request(request)
        except Exception as e:  # noqa: BLE001
            logger.exception("Request handling error")
            response = {
                "jsonrpc": "2.0",
                "id": request.get("id"),
                "error": {
                    "code": -32603,
                    "message": "Internal error",
                    "data": str(e),
                },
            }

        # Write JSON one-line per response, flush for interactive use
        sys.stdout.write(json.dumps(response) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()