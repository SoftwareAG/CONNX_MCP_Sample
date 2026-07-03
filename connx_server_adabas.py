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

# Required configuration for the Adabas-focused server.
CONNX_DSN_ADABAS = os.getenv("CONNX_DSN_ADABAS")
CONNX_USER = os.getenv("CONNX_USER")
CONNX_PASS = os.getenv("CONNX_PASS")

EMPLOYEES_TABLE = "DAEA.dbo.EMPLOYEES"
VEHICLES_TABLE = "DAEA.dbo.VEHICLES"

ENTITY_ALIASES = {
    "employees": {
        "aliases": ["employee", "employees", "personnel", "staff", "workers"],
        "table": EMPLOYEES_TABLE,
        "description": "Adabas employee master records accessed via CONNX",
    },
    "vehicles": {
        "aliases": ["vehicle", "vehicles", "cars", "fleet", "auto", "autos"],
        "table": VEHICLES_TABLE,
        "description": "Adabas vehicle records linked to employees via PERSONNEL_ID",
    },
}


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        value = int(os.getenv(name, str(default)))
        if value < minimum:
            return default
        return value
    except (TypeError, ValueError):
        return default


MAX_RESULT_ROWS = _env_int("CONNX_MAX_ROWS", default=1000, minimum=1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

mcp = FastMCP("connx-adabas-server")


def _assert_config() -> None:
    missing = [k for k in ("CONNX_DSN_ADABAS", "CONNX_USER", "CONNX_PASS") if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing required config values: {', '.join(missing)}")


def _sql_fingerprint(sql: str) -> str:
    digest = hashlib.sha256(sql.encode("utf-8", errors="ignore")).hexdigest()
    return digest[:12]


def _is_single_statement(sql: str) -> bool:
    s = (sql or "").strip()
    return bool(s) and (";" not in s)


def _is_select_only(sql: str) -> bool:
    s = (sql or "").lstrip().lower()
    return s.startswith("select")


def resolve_entity(name: str) -> Optional[str]:
    if not name:
        return None

    n = name.strip().lower()
    for entity in ENTITY_ALIASES.values():
        if n in entity["aliases"]:
            return entity["table"]

    return None


def _effective_limit(requested: Optional[int]) -> int:
    if requested and requested > 0:
        return min(requested, MAX_RESULT_ROWS)
    return MAX_RESULT_ROWS


def get_connx_connection():
    """Establish a connection to CONNX for the Adabas DSN."""
    _assert_config()

    timeout = int(os.getenv("CONNX_TIMEOUT", "30"))
    conn_str = f"DSN={CONNX_DSN_ADABAS};UID={CONNX_USER};PWD={CONNX_PASS}"
    try:
        conn = pyodbc.connect(conn_str, timeout=timeout)
        logger.info("Successfully connected to CONNX Adabas DSN")
        return conn
    except pyodbc.Error as e:
        logger.error("Adabas connection failed: %s", e)
        raise ValueError(f"Failed to connect to CONNX Adabas DSN: {str(e)}")


async def execute_query_async(
    query: str,
    params: Optional[List[Any]] = None,
    max_rows: Optional[int] = None,
) -> List[Dict[str, Any]]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, execute_query, query, params, max_rows)


def execute_query(
    query: str,
    params: Optional[List[Any]] = None,
    max_rows: Optional[int] = None,
) -> List[Dict[str, Any]]:
    conn = get_connx_connection()
    fp = _sql_fingerprint(query)
    limit = max_rows if max_rows and max_rows > 0 else MAX_RESULT_ROWS
    try:
        cursor = conn.cursor()
        cursor.execute(query, params or [])
        if cursor.description is None:
            raise ValueError("Query did not return a result set (cursor.description is None).")

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchmany(limit + 1) if limit else cursor.fetchall()
        if limit and len(rows) > limit:
            rows = rows[:limit]
            logger.info("Query truncated fp=%s limit=%d", fp, limit)

        results = [dict(zip(columns, row)) for row in rows]
        logger.info("Query OK fp=%s rows=%d", fp, len(results))
        return results
    except (pyodbc.Error, ValueError) as e:
        logger.error("Query failed fp=%s err=%s", fp, e)
        raise ValueError(f"Query execution failed: {str(e)}")
    finally:
        conn.close()


@mcp.tool()
async def query_connx(query: str) -> Dict[str, Any]:
    """
    Query data from the CONNX-connected Adabas DSN using SQL.

    Security:
    - Enforces single-statement SELECT-only.
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
async def describe_server() -> Dict[str, Any]:
    """Return a small metadata payload to help MCP clients route Adabas questions."""
    return {
        "server": "connx-adabas-server",
        "backend": "Adabas",
        "dsn_env_var": "CONNX_DSN_ADABAS",
        "mode": "read-only",
        "available_capabilities": [
            "query_connx",
            "count_employees",
            "count_vehicles",
            "get_employee",
            "get_vehicles_for_employee",
            "find_employees_by_city",
            "employees_with_vehicles",
            "vehicles_by_department",
            "leased_vehicles_by_department",
            "vehicles_by_country",
            "vehicle_summary_by_make",
            "describe_entities",
            "count_entities",
            "schema://schema",
            "schema://schema/{table_name}",
            "schema://domain/employees",
            "schema://domain/vehicles",
            "semantic://entities",
            "domain://datasets",
        ],
    }


@mcp.tool()
async def count_employees() -> Dict[str, Any]:
    sql = f"SELECT COUNT(*) AS TOTAL_EMPLOYEES FROM {EMPLOYEES_TABLE}"
    try:
        rows = await execute_query_async(sql)
        return {"total_employees": rows[0]["TOTAL_EMPLOYEES"]}
    except ValueError as e:
        return {"error": str(e)}


@mcp.tool()
async def count_vehicles() -> Dict[str, Any]:
    sql = f"SELECT COUNT(*) AS TOTAL_VEHICLES FROM {VEHICLES_TABLE}"
    try:
        rows = await execute_query_async(sql)
        return {"total_vehicles": rows[0]["TOTAL_VEHICLES"]}
    except ValueError as e:
        return {"error": str(e)}


@mcp.tool()
async def get_employee(personnel_id: str) -> Dict[str, Any]:
    sql = f"""
        SELECT
            PERSONNEL_ID,
            ISN_EMPLOYEES,
            FIRST_NAME,
            NAME,
            MIDDLE_NAME,
            MAR_STAT,
            SEX,
            BIRTH,
            CITY,
            POST_CODE,
            COUNTRY,
            AREA_CODE,
            PHONE,
            DEPT,
            JOB_TITLE,
            LEAVE_DUE,
            LEAVE_TAKEN,
            LEAVE_LEFT,
            DEPARTMENT,
            DEPT_PERSON
        FROM {EMPLOYEES_TABLE}
        WHERE PERSONNEL_ID = ?
    """
    try:
        rows = await execute_query_async(sql, params=[personnel_id.strip()], max_rows=1)
        return {"employee": rows[0] if rows else None}
    except ValueError as e:
        return {"error": str(e)}


@mcp.tool()
async def get_vehicles_for_employee(personnel_id: str, max_rows: int = 25) -> Dict[str, Any]:
    sql = f"""
        SELECT
            v.ISN_VEHICLES,
            v.REG_NUM,
            v.CHASSIS_NUM,
            v.PERSONNEL_ID,
            v.MAKE,
            v.MODEL,
            v.COLOUR,
            v.YEAR,
            v.CLASS,
            v.LEASE_PUR,
            v.DATE_ACQ,
            v.CURR_CODE,
            v.MODEL_YEAR_MAKE
        FROM {VEHICLES_TABLE} v
        WHERE v.PERSONNEL_ID = ?
        ORDER BY v.REG_NUM
    """
    try:
        limit = _effective_limit(max_rows)
        results = await execute_query_async(sql, params=[personnel_id.strip()], max_rows=limit)
        return {
            "personnel_id": personnel_id,
            "vehicles": results,
            "count": len(results),
        }
    except ValueError as e:
        return {"error": str(e)}


@mcp.tool()
async def find_employees_by_city(city: str, max_rows: int = 100) -> Dict[str, Any]:
    sql = f"""
        SELECT
            PERSONNEL_ID,
            ISN_EMPLOYEES,
            FIRST_NAME,
            NAME,
            CITY,
            COUNTRY,
            JOB_TITLE,
            DEPARTMENT
        FROM {EMPLOYEES_TABLE}
        WHERE UPPER(CITY) = UPPER(?)
        ORDER BY NAME, FIRST_NAME
    """
    try:
        limit = _effective_limit(max_rows)
        results = await execute_query_async(sql, params=[city.strip()], max_rows=limit)
        return {"results": results, "count": len(results)}
    except ValueError as e:
        return {"error": str(e)}


@mcp.tool()
async def employees_with_vehicles(max_rows: int = 100) -> Dict[str, Any]:
    sql = f"""
        SELECT
            e.PERSONNEL_ID,
            e.ISN_EMPLOYEES,
            e.NAME,
            e.FIRST_NAME,
            v.REG_NUM,
            v.MAKE,
            v.MODEL,
            v.COLOUR
        FROM {EMPLOYEES_TABLE} e
        INNER JOIN {VEHICLES_TABLE} v
            ON e.PERSONNEL_ID = v.PERSONNEL_ID
        ORDER BY e.NAME, e.FIRST_NAME, v.REG_NUM
    """
    try:
        limit = _effective_limit(max_rows)
        results = await execute_query_async(sql, max_rows=limit)
        return {"results": results, "count": len(results)}
    except ValueError as e:
        return {"error": str(e)}


@mcp.tool()
async def vehicles_by_department() -> Dict[str, Any]:
    sql = f"""
        SELECT
            e.DEPARTMENT,
            COUNT(*) AS VEHICLE_COUNT
        FROM {EMPLOYEES_TABLE} e
        INNER JOIN {VEHICLES_TABLE} v
            ON e.PERSONNEL_ID = v.PERSONNEL_ID
        GROUP BY e.DEPARTMENT
        ORDER BY VEHICLE_COUNT DESC, e.DEPARTMENT
    """
    try:
        rows = await execute_query_async(sql)
        return {"departments": rows}
    except ValueError as e:
        return {"error": str(e)}


@mcp.tool()
async def leased_vehicles_by_department() -> Dict[str, Any]:
    sql = f"""
        SELECT
            e.DEPARTMENT,
            COUNT(*) AS LEASED_VEHICLE_COUNT
        FROM {EMPLOYEES_TABLE} e
        INNER JOIN {VEHICLES_TABLE} v
            ON e.PERSONNEL_ID = v.PERSONNEL_ID
        WHERE UPPER(v.LEASE_PUR) = 'LEASE'
        GROUP BY e.DEPARTMENT
        ORDER BY LEASED_VEHICLE_COUNT DESC, e.DEPARTMENT
    """
    try:
        rows = await execute_query_async(sql)
        return {"departments": rows}
    except ValueError as e:
        return {"error": str(e)}


@mcp.tool()
async def vehicles_by_country() -> Dict[str, Any]:
    sql = f"""
        SELECT
            e.COUNTRY,
            COUNT(*) AS VEHICLE_COUNT
        FROM {EMPLOYEES_TABLE} e
        INNER JOIN {VEHICLES_TABLE} v
            ON e.PERSONNEL_ID = v.PERSONNEL_ID
        GROUP BY e.COUNTRY
        ORDER BY VEHICLE_COUNT DESC, e.COUNTRY
    """
    try:
        rows = await execute_query_async(sql)
        return {"countries": rows}
    except ValueError as e:
        return {"error": str(e)}


@mcp.tool()
async def vehicle_summary_by_make() -> Dict[str, Any]:
    sql = f"""
        SELECT
            MAKE,
            COUNT(*) AS VEHICLE_COUNT
        FROM {VEHICLES_TABLE}
        GROUP BY MAKE
        ORDER BY VEHICLE_COUNT DESC, MAKE
    """
    try:
        rows = await execute_query_async(sql)
        return {"makes": rows}
    except ValueError as e:
        return {"error": str(e)}


@mcp.tool()
async def describe_entities() -> Dict[str, Any]:
    entities = []
    for name, info in ENTITY_ALIASES.items():
        entities.append(
            {
                "entity": name,
                "aliases": info["aliases"],
                "table": info["table"],
                "description": info["description"],
            }
        )
    return {"entities": entities}


@mcp.tool()
async def count_entities(entity: str) -> Dict[str, Any]:
    table = resolve_entity(entity)
    if not table:
        return {"error": f"Unknown entity: {entity}"}

    sql = f"SELECT COUNT(*) AS TOTAL_COUNT FROM {table}"
    rows = await execute_query_async(sql)
    return {"entity": entity, "table": table, "total": rows[0]["TOTAL_COUNT"]}


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


@mcp.resource("schema://domain/employees")
async def employees_domain_metadata() -> Dict[str, Any]:
    return {
        "entity": "employees",
        "description": "Employee master data from Adabas via CONNX",
        "primary_table": EMPLOYEES_TABLE,
        "primary_key": "PERSONNEL_ID",
        "common_queries": {
            "count_all": f"SELECT COUNT(*) AS TOTAL_EMPLOYEES FROM {EMPLOYEES_TABLE}",
            "by_city": (
                f"SELECT PERSONNEL_ID, NAME, FIRST_NAME, CITY "
                f"FROM {EMPLOYEES_TABLE} WHERE UPPER(CITY) = UPPER(?)"
            ),
        },
        "columns": {
            "PERSONNEL_ID": "Employee identifier used to join to vehicles",
            "ISN_EMPLOYEES": "Adabas internal sequence number",
            "FIRST_NAME": "Employee first name",
            "NAME": "Employee last name",
            "CITY": "City",
            "DEPARTMENT": "Department name",
            "JOB_TITLE": "Job title",
        },
    }


@mcp.resource("schema://domain/vehicles")
async def vehicles_domain_metadata() -> Dict[str, Any]:
    return {
        "entity": "vehicles",
        "description": "Vehicle assignments from Adabas via CONNX",
        "primary_table": VEHICLES_TABLE,
        "join_key": "PERSONNEL_ID",
        "common_queries": {
            "count_all": f"SELECT COUNT(*) AS TOTAL_VEHICLES FROM {VEHICLES_TABLE}",
            "for_employee": (
                f"SELECT REG_NUM, MAKE, MODEL, COLOUR "
                f"FROM {VEHICLES_TABLE} WHERE PERSONNEL_ID = ?"
            ),
        },
        "columns": {
            "REG_NUM": "Vehicle registration number",
            "CHASSIS_NUM": "Vehicle chassis number",
            "PERSONNEL_ID": "Employee identifier linked to EMPLOYEES",
            "MAKE": "Vehicle manufacturer",
            "MODEL": "Vehicle model",
            "COLOUR": "Vehicle color",
        },
    }


@mcp.resource("semantic://entities")
async def get_semantic_entities() -> Dict[str, Any]:
    return {
        "entities": [
            {
                "entity": "employees",
                "aliases": ENTITY_ALIASES["employees"]["aliases"],
                "table": EMPLOYEES_TABLE,
                "primary_key": "PERSONNEL_ID",
                "description": "Employee master data stored in Adabas",
            },
            {
                "entity": "vehicles",
                "aliases": ENTITY_ALIASES["vehicles"]["aliases"],
                "table": VEHICLES_TABLE,
                "primary_key": "REG_NUM",
                "foreign_keys": {"PERSONNEL_ID": "employees.PERSONNEL_ID"},
                "relationships": {
                    "employees": "vehicles.PERSONNEL_ID -> employees.PERSONNEL_ID",
                },
                "description": "Vehicle records assigned to employees in Adabas",
            },
        ]
    }


@mcp.resource("domain://datasets")
async def datasets() -> Dict[str, Any]:
    return {
        "backend": "Adabas",
        "datasets": [
            {
                "logical_name": "employees",
                "table": EMPLOYEES_TABLE,
                "primary_key": "PERSONNEL_ID",
                "columns": [
                    "ISN_EMPLOYEES",
                    "PERSONNEL_ID",
                    "FIRST_NAME",
                    "NAME",
                    "CITY",
                    "COUNTRY",
                    "DEPARTMENT",
                    "JOB_TITLE",
                ],
            },
            {
                "logical_name": "vehicles",
                "table": VEHICLES_TABLE,
                "primary_key": "REG_NUM",
                "join_key": "PERSONNEL_ID",
                "columns": [
                    "ISN_VEHICLES",
                    "REG_NUM",
                    "PERSONNEL_ID",
                    "MAKE",
                    "MODEL",
                    "COLOUR",
                    "YEAR",
                    "CLASS",
                ],
            },
        ],
        "notes": [
            "Employees and vehicles are linked by PERSONNEL_ID.",
            "The Adabas demo server remains read-only and focused on these two entities.",
        ],
    }


if __name__ == "__main__":  # pragma: no cover
    mcp.run(transport="stdio")
