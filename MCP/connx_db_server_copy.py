import json
import logging
import sys
import pyodbc
from mcp.server.fastmcp import FastMCP
import re

logging.basicConfig(level=logging.INFO, stream=sys.stderr)
mcp = FastMCP("connx_db")

CONNX_DSN = "Share_2025"
CONNX_CONN_STR = f"DSN={CONNX_DSN};UID=sag;PWD=sag"


@mcp.tool()
async def query_database(sql_query: str) -> str:
    """
    Execute an ANSI SQL-92 query via CONNX and return JSON.
    For SELECT: {ok, type:'select', columns:[...], rows:[[...]], truncated, max_rows}
    For non-SELECT: {ok, type:'non_select', rows_affected}
    """
    conn = None
    cursor = None
    bad = re.search(r"(?i)\b(top|limit|offset|fetch|qualify)\b", sql_query)
    if bad:
        return json.dumps({
            "ok": False,
            "error": f"Non-ANSI SQL-92 keyword detected: {bad.group(1)}. Please remove it."
        })

    try:
        conn = pyodbc.connect(CONNX_CONN_STR)
        cursor = conn.cursor()
        cursor.execute(sql_query)

        # Non-SELECT statements return no cursor.description
        if cursor.description is None:
            try:
                conn.commit()
            except Exception:
                pass
            return json.dumps({"ok": True, "type": "non_select", "rows_affected": cursor.rowcount})

        columns = [desc[0] for desc in cursor.description] if cursor.description else []

        max_rows = 200
        rows = cursor.fetchmany(max_rows)

        rows_out = []
        for r in rows:
            rows_out.append([None if v is None else str(v) for v in r])

        truncated = bool(cursor.fetchmany(1))

        payload = {
            "ok": True,
            "type": "select",
            "columns": columns,
            "rows": rows_out,
            "row_count_returned": len(rows_out),
            "truncated": truncated,
            "max_rows": max_rows,
        }
        return json.dumps(payload)

    except Exception as e:
        logging.exception("query_database error")
        return json.dumps({"ok": False, "error": str(e)})

    finally:
        try:
            if cursor is not None:
                cursor.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


@mcp.tool()
async def list_tables(schema: str = None, table_name_like: str = None) -> str:
    """
    List available tables using ODBC metadata.
    Returns JSON: {ok, type:'tables', count, tables:[{table_cat, table_schem, table_name, table_type, remarks}]}
    """
    conn = None
    cursor = None
    try:
        conn = pyodbc.connect(CONNX_CONN_STR)
        cursor = conn.cursor()

        rows = cursor.tables(tableType="TABLE").fetchall()

        out = []
        for r in rows:
            out.append({
                "table_cat": getattr(r, "table_cat", None),
                "table_schem": getattr(r, "table_schem", None),
                "table_name": getattr(r, "table_name", None),
                "table_type": getattr(r, "table_type", None),
                "remarks": getattr(r, "remarks", None),
            })

        if schema:
            s = schema.strip().lower()
            out = [t for t in out if (t.get("table_schem") or "").lower() == s]

        if table_name_like:
            pat = table_name_like.strip().lower()
            out = [t for t in out if pat in (t.get("table_name") or "").lower()]

        return json.dumps({"ok": True, "type": "tables", "count": len(out), "tables": out})

    except Exception as e:
        logging.exception("list_tables error")
        return json.dumps({"ok": False, "error": str(e)})

    finally:
        try:
            if cursor is not None:
                cursor.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


@mcp.tool()
async def describe_table(qualified_table: str) -> str:
    """
    Describe columns using ODBC metadata.
    qualified_table can be:
      schema.table  OR  catalog.schema.table  OR  just table
    Returns JSON: {ok, type:'describe', qualified_table, count, columns:[...]}
    """
    conn = None
    cursor = None
    try:
        conn = pyodbc.connect(CONNX_CONN_STR)
        cursor = conn.cursor()

        parts = [p.strip() for p in qualified_table.split(".") if p.strip()]
        catalog = schema = table = None

        if len(parts) == 1:
            table = parts[0]
        elif len(parts) == 2:
            schema, table = parts
        else:
            catalog, schema, table = parts[-3], parts[-2], parts[-1]

        cols = cursor.columns(table=table, schema=schema, catalog=catalog).fetchall()

        out = []
        for c in cols:
            out.append({
                "column_name": getattr(c, "column_name", None),
                "type_name": getattr(c, "type_name", None),
                "column_size": getattr(c, "column_size", None),
                "decimal_digits": getattr(c, "decimal_digits", None),
                "nullable": getattr(c, "nullable", None),
                "remarks": getattr(c, "remarks", None),
            })

        return json.dumps({
            "ok": True,
            "type": "describe",
            "qualified_table": qualified_table,
            "count": len(out),
            "columns": out
        })

    except Exception as e:
        logging.exception("describe_table error")
        return json.dumps({"ok": False, "error": str(e)})

    finally:
        try:
            if cursor is not None:
                cursor.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()