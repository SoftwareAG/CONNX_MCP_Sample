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

# Result limits
def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        value = int(os.getenv(name, str(default)))
        if value < minimum:
            return default
        return value
    except (TypeError, ValueError):
        return default


MAX_RESULT_ROWS = _env_int("CONNX_MAX_ROWS", default=1000, minimum=1)

# Setup logging (log to stderr to avoid interfering with MCP stdout)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# MCP Server Initialization
mcp = FastMCP("connx-database-server")

# -------------------------------
# Natural-language entity aliases
# -------------------------------

ENTITY_ALIASES = {
    "customers": {
        "aliases": ["customer", "customers", "client", "clients", "accounts", "buyers", "companies"],
        "table": "daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM",
        "description": "VSAM-backed customer master file accessed via CONNX"
    },
    "orders": {
        "aliases": ["order", "orders", "purchases", "transactions", "sales"],
        "table": "daea_Mainframe_VSAM.dbo.ORDERS_VSAM",
        "description": "Customer order transactions stored in VSAM"
    },
    "products": {
        "aliases": ["product", "products", "items", "inventory", "goods"],
        "table": "daea_Mainframe_VSAM.dbo.PRODUCTS_VSAM",
        "description": "Product master file stored in VSAM"
    }
}


def resolve_entity(name: str) -> Optional[str]:
    """
    Resolve a natural-language entity name to a canonical table.
    """
    if not name:
        return None

    n = name.strip().lower()

    for entity in ENTITY_ALIASES.values():
        if n in entity["aliases"]:
            return entity["table"]

    return None

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


def _first_keyword(sql: str) -> str:
    """Return the first keyword/token of the SQL (lowercased)."""
    return (sql or "").lstrip().split(" ", 1)[0].lower()


def _effective_limit(requested: Optional[int]) -> int:
    """Clamp requested row limit to the configured maximum."""
    if requested and requested > 0:
        return min(requested, MAX_RESULT_ROWS)
    return MAX_RESULT_ROWS


def get_connx_connection():
    """Establish a connection to CONNX via pyodbc."""
    _assert_config()

    timeout = int(os.getenv("CONNX_TIMEOUT", "30"))
    conn_str = f"DSN={CONNX_DSN};UID={CONNX_USER};PWD={CONNX_PASS}"
    try:
        conn = pyodbc.connect(conn_str, timeout=timeout)
        logger.info("Successfully connected to CONNX")
        return conn
    except pyodbc.Error as e:
        logger.error("Connection failed: %s", e)
        raise ValueError(f"Failed to connect to CONNX: {str(e)}")


async def execute_query_async(
    query: str,
    params: Optional[List[Any]] = None,
    max_rows: Optional[int] = None
) -> List[Dict[str, Any]]:
    """Asynchronous execution of SELECT queries via CONNX."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, execute_query, query, params, max_rows)


def execute_query(
    query: str,
    params: Optional[List[Any]] = None,
    max_rows: Optional[int] = None
) -> List[Dict[str, Any]]:
    """Execute SELECT query and return results as list of dicts."""
    conn = get_connx_connection()
    fp = _sql_fingerprint(query)
    limit = max_rows if max_rows and max_rows > 0 else MAX_RESULT_ROWS
    try:
        cursor = conn.cursor()
        # cursor.timeout = int(os.getenv("CONNX_TIMEOUT", "30"))
        cursor.execute(query, params or [])
        if cursor.description is None:
            # A SELECT should provide a description; if not, treat as an error.
            raise ValueError("Query did not return a result set (cursor.description is None).")

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchmany(limit + 1) if limit else cursor.fetchall()
        truncated = len(rows) > limit if limit else False
        if truncated:
            rows = rows[:limit]
        results = [dict(zip(columns, row)) for row in rows]
        logger.info("Query OK fp=%s rows=%d", fp, len(results))
        if truncated:
            logger.info("Query truncated fp=%s limit=%d", fp, limit)
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
        results = await execute_query_async(query, max_rows=MAX_RESULT_ROWS)
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

    op = operation.strip().lower()
    if op not in ["insert", "update", "delete"]:
        return {"error": "Invalid operation. Must be 'insert', 'update', or 'delete'."}

    if _first_keyword(query) != op:
        return {"error": f"SQL must start with {op.upper()} for this operation."}

    if not _is_single_statement(query):
        return {"error": "Only a single SQL statement is allowed (no semicolons)."}

    try:
        affected = await execute_update_async(query)
        return {"affected_rows": affected, "message": f"{operation.capitalize()} completed successfully."}
    except ValueError as e:
        return {"error": str(e)}

@mcp.tool()
async def count_customers() -> Dict[str, Any]:
    """
    Return the total number of customers.
    Uses the canonical customers table.
    """
    sql = """
        SELECT COUNT(*) AS TOTAL_CUSTOMERS
        FROM daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM
    """
    try:
        rows = await execute_query_async(sql)
        return {
            "total_customers": rows[0]["TOTAL_CUSTOMERS"]
        }
    except ValueError as e:
        return {"error": str(e)}

# MCP Resources
@mcp.resource("schema://schema")
async def get_schema() -> Dict[str, Any]:
    query = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS"
    try:
        results = await execute_query_async(query, max_rows=MAX_RESULT_ROWS)
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
        results = await execute_query_async(query, params=[table_name], max_rows=MAX_RESULT_ROWS)
        return {"schemas": results}
    except ValueError as e:
        return {"error": str(e)}

@mcp.resource("schema://domain/customers")
async def customers_domain_metadata() -> Dict[str, Any]:
    """
    Canonical metadata describing where 'customers' data lives.
    Used by MCP clients to avoid guessing table names.
    """
    return {
        "entity": "customers",
        "description": "Customer master data from mainframe VSAM via CONNX",
        "primary_table": "daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM",
        "primary_key": "CUSTOMERID",
        "common_queries": {
            "count_all": (
                "SELECT COUNT(*) AS TOTAL_CUSTOMERS "
                "FROM daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM"
            ),
            "by_state": (
                "SELECT * FROM daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM "
                "WHERE CUSTOMERSTATE = ?"
            )
        },
        "columns": {
            "CUSTOMERID": "Customer identifier",
            "CUSTOMERNAME": "Customer name",
            "CUSTOMERSTATE": "2-letter US state code",
            "CUSTOMERCITY": "City",
            "CUSTOMERZIP": "Postal code"
        }
    }

@mcp.resource("domain://datasets")
async def datasets() -> Dict[str, Any]:
    return {
        "datasets": [
            {
                "logical_name": "customers",
                "table": "daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM",
                "primary_key": "CUSTOMERID",
                "columns": [
                    "CUSTOMERID","CUSTOMERNAME","CUSTOMERADDRESS","CUSTOMERCITY",
                    "CUSTOMERSTATE","CUSTOMERZIP","CUSTOMERCOUNTRY","CUSTOMERPHONE"
                ],
                "notes": "VSAM fields are fixed-width CHAR; use RTRIM() for comparisons/output."
            }
        ]
    }
# Optional helper: map full state names to 2-letter codes (extend as needed)
STATE_NAME_TO_CODE = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY"
}

def _normalize_state(state: str) -> str:
    s = (state or "").strip()
    if not s:
        return s
    return STATE_NAME_TO_CODE.get(s.lower(), s)

@mcp.tool()
async def customers_by_state() -> Dict[str, Any]:
    sql = """
        SELECT
            RTRIM(CUSTOMERSTATE) AS STATE,
            COUNT(*) AS CUSTOMER_COUNT
        FROM daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM
        GROUP BY RTRIM(CUSTOMERSTATE)
        ORDER BY CUSTOMER_COUNT DESC
    """
    rows = await execute_query_async(sql)
    return {"states": rows}

@mcp.tool()
async def customer_cities() -> Dict[str, Any]:
    sql = """
        SELECT DISTINCT RTRIM(CUSTOMERCITY) AS CITY
        FROM daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM
        ORDER BY CITY
    """
    rows = await execute_query_async(sql)
    return {"cities": rows}

@mcp.tool()
async def customers_missing_phone() -> Dict[str, Any]:
    sql = """
        SELECT
            RTRIM(CUSTOMERID) AS CUSTOMERID,
            RTRIM(CUSTOMERNAME) AS CUSTOMERNAME
        FROM daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM
        WHERE RTRIM(CUSTOMERPHONE) = ''
    """
    rows = await execute_query_async(sql)
    return {"results": rows, "count": len(rows)}

@mcp.tool()
async def get_customer(customer_id: str) -> Dict[str, Any]:
    sql = """
        SELECT
            RTRIM(CUSTOMERID) AS CUSTOMERID,
            RTRIM(CUSTOMERNAME) AS CUSTOMERNAME,
            RTRIM(CUSTOMERADDRESS) AS CUSTOMERADDRESS,
            RTRIM(CUSTOMERCITY) AS CUSTOMERCITY,
            RTRIM(CUSTOMERSTATE) AS CUSTOMERSTATE,
            RTRIM(CUSTOMERZIP) AS CUSTOMERZIP,
            RTRIM(CUSTOMERPHONE) AS CUSTOMERPHONE
        FROM daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM
        WHERE RTRIM(CUSTOMERID) = ?
    """
    rows = await execute_query_async(sql, params=[customer_id])
    return {"customer": rows[0] if rows else None}

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
        limit = _effective_limit(max_rows)
        fetch_limit = min(limit + 1, MAX_RESULT_ROWS + 1)
        results = await execute_query_async(sql, params=params, max_rows=fetch_limit)

        truncated = len(results) > limit
        if truncated:
            results = results[:limit]

        return {"results": results, "count": len(results), "truncated": truncated}
    except ValueError as e:
        return {"error": str(e)}

@mcp.tool()
async def describe_entities() -> Dict[str, Any]:
    """
    Describe known business entities and their underlying data sources.
    """
    entities = []
    for name, info in ENTITY_ALIASES.items():
        entities.append({
            "entity": name,
            "aliases": info["aliases"],
            "table": info["table"],
            "description": info["description"]
        })

    return {"entities": entities}

@mcp.tool()
async def count_entities(entity: str) -> Dict[str, Any]:
    """
    Count rows for a known business entity (e.g., customers, clients).
    """
    table = resolve_entity(entity)

    if not table:
        return {"error": f"Unknown entity: {entity}"}

    sql = f"SELECT COUNT(*) AS TOTAL_COUNT FROM {table}"
    rows = await execute_query_async(sql)

    return {
        "entity": entity,
        "table": table,
        "total": rows[0]["TOTAL_COUNT"]
    }

@mcp.resource("semantic://entities")
async def get_semantic_entities() -> Dict[str, Any]:
    return {
        "entities": [
            {
                "entity": "customers",
                "aliases": ["customer", "customers", "client", "clients", "accounts", "buyers", "companies"],
                "table": "daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM",
                "primary_key": "CUSTOMERID",
                "description": "Customer master records stored in a VSAM file",
            },
            {
                "entity": "orders",
                "aliases": ["order", "orders", "purchases", "transactions", "sales"],
                "table": "daea_Mainframe_VSAM.dbo.ORDERS_VSAM",
                "primary_key": "ORDERID",
                "foreign_keys": {
                    "CUSTOMERID": "customers.CUSTOMERID",
                    "PRODUCTID": "products.PRODUCTID",
                },
                "relationships": {
                    "customers": "orders.CUSTOMERID -> customers.CUSTOMERID",
                    "products": "orders.PRODUCTID -> products.PRODUCTID",
                },
                "description": "Customer order transactions stored in VSAM",
            },
            {
                "entity": "products",
                "aliases": ["product", "products", "items", "inventory", "goods"],
                "table": "daea_Mainframe_VSAM.dbo.PRODUCTS_VSAM",
                "primary_key": "PRODUCTID",
                "description": "Product master file stored in VSAM",
            },
        ]
    }

@mcp.tool()
async def customer_orders_for_product(
    customer_id: str,
    product_name: str,
    max_rows: int = 50
) -> Dict[str, Any]:
    """
    Get detailed order information for a specific customer and product.

    Args:
        customer_id: Customer identifier
        product_name: Name of the product
        max_rows: Maximum number of orders to return (default: 50)

    Returns order details including dates, quantities, etc.
    """
    sql = """
        SELECT
            o.ORDERID,
            o.ORDERDATE,
            o.PRODUCTQUANTITY,
            RTRIM(p.PRODUCTNAME) AS PRODUCTNAME,
            RTRIM(c.CUSTOMERNAME) AS CUSTOMERNAME
        FROM daea_Mainframe_VSAM.dbo.ORDERS_VSAM o
        INNER JOIN daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM c 
            ON RTRIM(c.CUSTOMERID) = RTRIM(o.CUSTOMERID)
        INNER JOIN daea_Mainframe_VSAM.dbo.PRODUCTS_VSAM p 
            ON o.PRODUCTID = p.PRODUCTID
        WHERE RTRIM(c.CUSTOMERID) = ?
          AND UPPER(RTRIM(p.PRODUCTNAME)) = UPPER(?)
        ORDER BY o.ORDERDATE DESC
    """

    try:
        limit = _effective_limit(max_rows)
        results = await execute_query_async(
            sql, 
            params=[customer_id.strip(), product_name.strip()], 
            max_rows=limit
        )

        return {
            "customer_id": customer_id,
            "product_name": product_name,
            "orders": results,
            "count": len(results)
        }
    except ValueError as e:
        return {"error": str(e)}

# Main Entry Point
if __name__ == "__main__": # pragma: no cover
    # FastMCP.run() manages its own event loop via anyio.run()
    mcp.run(transport="stdio")
