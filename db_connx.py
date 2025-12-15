# db_connx.py
import json
import os
import re
from typing import Any, Dict, List, Tuple

import pyodbc
from dotenv import load_dotenv

load_dotenv()


class ConnxConfigError(Exception):
    pass


class ConnxSafetyError(Exception):
    pass


class ConnxDatabase:
    """
    Thin wrapper around a CONNX ODBC DSN with safety checks.
    """

    def __init__(self, config_path: str = "config.json") -> None:
        if not os.path.exists(config_path):
            raise ConnxConfigError(f"Config file not found: {config_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)

        connx_cfg = cfg.get("connx") or {}
        safety_cfg = cfg.get("safety") or {}

        self.dsn: str = connx_cfg.get("dsn") or os.getenv("CONNX_DSN", "")
        self.user: str = connx_cfg.get("user") or os.getenv("CONNX_USER", "")
        self.password: str = connx_cfg.get("password") or os.getenv("CONNX_PASSWORD", "")
        self.default_database: str = connx_cfg.get("default_database", "")
        self.schema_prefix: str = connx_cfg.get("schema_prefix", "")

        if not self.dsn:
            raise ConnxConfigError("CONNX DSN is not configured (config.json or env).")

        self.allow_ddl: bool = bool(safety_cfg.get("allow_ddl", False))
        self.allow_dml: bool = bool(safety_cfg.get("allow_dml", False))

        self._forbidden_ddl = re.compile(
            r"\b(CREATE|ALTER|DROP|TRUNCATE|RENAME)\b",
            re.IGNORECASE,
        )
        self._forbidden_dml = re.compile(
            r"\b(INSERT|UPDATE|DELETE|MERGE)\b",
            re.IGNORECASE,
        )

    # ---------- Connection helpers ----------

    def _connection_string(self) -> str:
        parts = [f"DSN={self.dsn}"]
        if self.user:
            parts.append(f"UID={self.user}")
        if self.password:
            parts.append(f"PWD={self.password}")
        if self.default_database:
            parts.append(f"DATABASE={self.default_database}")
        return ";".join(parts)

    def connect(self) -> pyodbc.Connection:
        conn_str = self._connection_string()
        return pyodbc.connect(conn_str)

    # ---------- Safety checks ----------

    def _check_sql_safety(self, sql: str) -> None:
        """
        Very simple heuristic safety layer:
        - If DDL not allowed, block DDL keywords.
        - If DML not allowed, block DML keywords.
        """

        # Strip comments to reduce false negatives
        stripped = re.sub(r"--.*?$|/\*.*?\*/", "", sql, flags=re.MULTILINE | re.DOTALL)

        if not self.allow_ddl and self._forbidden_ddl.search(stripped):
            raise ConnxSafetyError(
                "DDL statements are not allowed by configuration (CREATE, ALTER, DROP, ...)."
            )

        if not self.allow_dml and self._forbidden_dml.search(stripped):
            raise ConnxSafetyError(
                "DML statements are not allowed by configuration (INSERT, UPDATE, DELETE, MERGE)."
            )

    # ---------- Public query methods ----------

    def query(self, sql: str, max_rows: int = 500) -> Dict[str, Any]:
        """
        Run a SELECT (or safe SQL) and return rows as a list of dicts.
        """

        self._check_sql_safety(sql)

        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)

            columns = [col[0] for col in cursor.description] if cursor.description else []
            rows: List[Tuple[Any, ...]] = []

            for _ in range(max_rows):
                row = cursor.fetchone()
                if not row:
                    break
                rows.append(row)

        data = [dict(zip(columns, row)) for row in rows]
        return {
            "columns": columns,
            "rows": data,
            "row_count": len(data),
        }

    def list_tables(self, schema: str | None = None) -> List[Dict[str, Any]]:
        """
        Return a list of tables/views accessible via CONNX.
        Uses cursor.tables() which is supported by many ODBC drivers.
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            # tableType filter: 'TABLE', 'VIEW', etc.
            cursor.tables()
            result: List[Dict[str, Any]] = []
            for row in cursor:
                table_schema = getattr(row, "table_schem", None)
                if schema and table_schema and table_schema.lower() != schema.lower():
                    continue
                result.append(
                    {
                        "table_cat": getattr(row, "table_cat", None),
                        "table_schem": table_schema,
                        "table_name": getattr(row, "table_name", None),
                        "table_type": getattr(row, "table_type", None),
                        "remarks": getattr(row, "remarks", None),
                    }
                )
        return result

    def describe_table(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Use cursor.columns() to describe a table (column name/type/size).
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.columns(table=table_name)
            cols: List[Dict[str, Any]] = []
            for row in cursor:
                cols.append(
                    {
                        "table_name": getattr(row, "table_name", None),
                        "column_name": getattr(row, "column_name", None),
                        "type_name": getattr(row, "type_name", None),
                        "column_size": getattr(row, "column_size", None),
                        "nullable": getattr(row, "nullable", None),
                    }
                )
        return cols