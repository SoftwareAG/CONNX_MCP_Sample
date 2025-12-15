# mcp_connx_server.py
import json 
import logging
import sys
from typing import Any, Dict

from connx_core import ConnxCore, ConnxConfigError, ConnxSafetyError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("mcp_connx")

try:
    core = ConnxCore("config.json")
    logger.info("Initialized ConnxCore with DSN=%s", core.dsn)
except ConnxConfigError as e:
    logger.error("Configuration error: %s", e)
    core = None


def tool_connx_list_tables(params: Dict[str, Any]) -> Dict[str, Any]:
    if core is None:
        raise RuntimeError("CONNX core not initialized.")
    schema = params.get("schema")
    tables = core.list_tables(schema=schema)
    return {"ok": True, "tables": tables}


def tool_connx_describe_table(params: Dict[str, Any]) -> Dict[str, Any]:
    if core is None:
        raise RuntimeError("CONNX core not initialized.")
    table_name = params.get("table_name")
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Parameter 'table_name' is required.")
    cols = core.describe_table(table_name)
    return {"ok": True, "columns": cols}


def tool_connx_query_safe(params: Dict[str, Any]) -> Dict[str, Any]:
    if core is None:
        raise RuntimeError("CONNX core not initialized.")
    sql = params.get("sql")
    if not sql or not isinstance(sql, str):
        raise ValueError("Parameter 'sql' is required and must be a string.")
    max_rows = params.get("max_rows")
    result = core.safe_query(sql, max_rows=max_rows)
    return {"ok": True, "data": result}


TOOLS: Dict[str, Any] = {
    "connx_list_tables": tool_connx_list_tables,
    "connx_describe_table": tool_connx_describe_table,
    "connx_query_safe": tool_connx_query_safe,
}


def handle_request(request: Dict[str, Any]) -> Dict[str, Any]:
    """
    Minimal JSON-RPC 2.0 handler for tools/* methods.
    """
    rid = request.get("id")
    method = request.get("method")
    params = request.get("params") or {}

    if method and method.startswith("tools/"):
        tool_name = method.split("/", 1)[1]
        func = TOOLS.get(tool_name)
        if not func:
            return {
                "jsonrpc": "2.0",
                "id": rid,
                "error": {
                    "code": -32601,
                    "message": f"Unknown tool '{tool_name}'"
                }
            }

        try:
            result = func(params)
            return {"jsonrpc": "2.0", "id": rid, "result": result}
        except ConnxSafetyError as se:
            logger.warning("Safety error: %s", se)
            return {
                "jsonrpc": "2.0",
                "id": rid,
                "error": {
                    "code": -32001,
                    "message": "Safety error",
                    "data": str(se)
                }
            }
        except Exception as e:  # noqa: BLE001
            logger.exception("Tool execution error")
            return {
                "jsonrpc": "2.0",
                "id": rid,
                "error": {
                    "code": -32000,
                    "message": "Tool execution error",
                    "data": str(e)
                }
            }

    return {
        "jsonrpc": "2.0",
        "id": rid,
        "error": {
            "code": -32601,
            "message": f"Unknown method '{method}'"
        }
    }


def main() -> None:
    logger.info("Starting MCP-style CONNX server on stdin/stdout...")
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON: %s", e)
            continue

        resp = handle_request(req)
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()