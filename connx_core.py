# connx_core.py
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


class ConnxCore:
    """
    Core CONNX access + safety, suitable for use by MCP tools or Flask routes.
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

        if not self.dsn:
            raise ConnxConfigError("CONNX DSN is not configured (config.json or env).")

        # Safety / governance
        self.allow_ddl: bool = bool(safety_cfg.get("allow_ddl", False))
        self.allow_dml: bool = bool(safety_cfg.get("allow_dml", False))
        self.max_rows: int = int(safety_cfg.get("max_rows", 500))

        # Allow-lists
        self.allowed_tables: List[str] = [
            t.lower() for t in safety_cfg.get("allowed_tables", [])
        ]
        self.masked_columns: List[str] = [
            c.lower() for c in safety_cfg.get("masked_columns", [])
        ]

        # Precompiled regexes
        self._forbidden_ddl = re.compile(
            r"\b(CREATE|ALTER|DROP|TRUNCATE|RENAME)\b", re.IGNORECASE
        )
        self._forbidden_dml = re.compile(
            r"\b(INSERT|UPDATE|DELETE|MERGE)\b", re.IGNORECASE
        )
        self._select_star = re.compile(r"\bSELECT\s+\*", re.IGNORECASE)

    # ---------- Internal helpers ----------

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
        return pyodbc.connect(self._connection_string())

    # ---------- Safety checks ----------

    def _check_sql_safety(self, sql: str) -> None:
        """Very simple safety layer: block DDL, DML, SELECT *; enforce allow-lists."""
        stripped = re.sub(
            r"--.*?$|/\*.*?\*/", "", sql, flags=re.MULTILINE | re.DOTALL
        )

        if not self.allow_ddl and self._forbidden_ddl.search(stripped):
            raise ConnxSafetyError(
                "DDL statements (CREATE/ALTER/DROP/...) are not allowed."
            )

        if not self.allow_dml and self._forbidden_dml.search(stripped):
            raise ConnxSafetyError(
                "DML statements (INSERT/UPDATE/DELETE/MERGE) are not allowed."
            )

        if self._select_star.search(stripped):
            raise ConnxSafetyError("SELECT * is not allowed. Use explicit columns.")

        if self.allowed_tables:
            lowered = stripped.lower()
            for disallowed in self._detect_disallowed_tables(lowered):
                raise ConnxSafetyError(
                    f"Access to table '{disallowed}' is not allowed by configuration."
                )

    def _detect_disallowed_tables(self, sql_lowered: str) -> List[str]:
        """
        Extract table tokens after FROM/JOIN and compare their *base* names
        (last component after .) against allowed_tables.
        """
        if not self.allowed_tables:
            return []

        table_tokens: List[str] = []

        for kw in (" from ", " join "):
            start = 0
            while True:
                idx = sql_lowered.find(kw, start)
                if idx == -1:
                    break
                start = idx + len(kw)

                end = len(sql_lowered)
                for sep in (" ", "\n", "\r", "\t", ","):
                    sep_idx = sql_lowered.find(sep, start)
                    if sep_idx != -1:
                        end = min(end, sep_idx)

                token = sql_lowered[start:end].strip()
                if token:
                    # strip punctuation and get base name
                    token = token.strip('";[]')
                    base = token.split(".")[-1]
                    table_tokens.append(base)
                start = end

        disallowed = [
            t for t in table_tokens if t and t not in self.allowed_tables
        ]
        return disallowed
    def _mask_row(self, columns: List[str], row: Tuple[Any, ...]) -> Dict[str, Any]:
        data = {}
        for col_name, value in zip(columns, row):
            if col_name.lower() in self.masked_columns and value is not None:
                data[col_name] = "***MASKED***"
            else:
                data[col_name] = value
        return data

    # ---------- Public operations ----------

    def safe_query(self, sql: str, max_rows: int | None = None) -> Dict[str, Any]:
        self._check_sql_safety(sql)

        limit = max_rows if max_rows and max_rows > 0 else self.max_rows

        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(sql)

            columns = [col[0] for col in cur.description] if cur.description else []
            rows: List[Tuple[Any, ...]] = []
            for _ in range(limit):
                r = cur.fetchone()
                if not r:
                    break
                rows.append(r)

        data = [self._mask_row(columns, r) for r in rows]
        return {
            "columns": columns,
            "rows": data,
            "row_count": len(data),
        }

    def list_tables(self, schema: str | None = None) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            cur = conn.cursor()
            cur.tables()
            result: List[Dict[str, Any]] = []
            for row in cur:
                schema_name = getattr(row, "table_schem", None)
                table_name = getattr(row, "table_name", None)
                if schema and schema_name and schema_name.lower() != schema.lower():
                    continue
                if self.allowed_tables and table_name and table_name.lower() not in self.allowed_tables:
                    continue

                result.append(
                    {
                        "table_cat": getattr(row, "table_cat", None),
                        "table_schem": schema_name,
                        "table_name": table_name,
                        "table_type": getattr(row, "table_type", None),
                        "remarks": getattr(row, "remarks", None),
                    }
                )
        return result

    def describe_table(self, table_name: str) -> List[Dict[str, Any]]:
        base = table_name.split(".")[-1].lower()
        if self.allowed_tables and base not in self.allowed_tables:
            raise ConnxSafetyError(
                f"Access to table '{table_name}' is not allowed by configuration."
            )

        with self.connect() as conn:
            cur = conn.cursor()
            cur.columns(table=table_name)
            cols: List[Dict[str, Any]] = []
            for row in cur:
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