import importlib
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import pyodbc

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MODULE_UNDER_TEST = "connx_server_adabas"


def load_module():
    from mcp.server.fastmcp import FastMCP

    def noop_decorator(*args, **kwargs):
        def _wrap(fn):
            return fn

        return _wrap

    FastMCP.tool = noop_decorator
    FastMCP.resource = noop_decorator

    if MODULE_UNDER_TEST in sys.modules:
        del sys.modules[MODULE_UNDER_TEST]

    return importlib.import_module(MODULE_UNDER_TEST)


mod = load_module()


class TestConfig(unittest.TestCase):
    def test_assert_config_raises_when_missing(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                mod._assert_config()
        self.assertIn("missing required config values", str(ctx.exception).lower())


class TestSqlHelpers(unittest.TestCase):
    def test_sql_fingerprint_is_stable_and_short(self):
        self.assertEqual(mod._sql_fingerprint("SELECT 1"), mod._sql_fingerprint("SELECT 1"))
        self.assertEqual(len(mod._sql_fingerprint("SELECT 1")), 12)

    def test_is_single_statement_rejects_semicolon(self):
        self.assertFalse(mod._is_single_statement("SELECT 1; SELECT 2"))

    def test_is_select_only_rejects_update(self):
        self.assertFalse(mod._is_select_only("UPDATE T SET A=1"))

    def test_resolve_entity_matches_alias(self):
        self.assertEqual(mod.resolve_entity("employees"), "DAEA.dbo.EMPLOYEES")
        self.assertEqual(mod.resolve_entity("cars"), "DAEA.dbo.VEHICLES")


class TestConnxConnection(unittest.TestCase):
    @patch.dict(
        os.environ,
        {"CONNX_DSN_ADABAS": "dummy", "CONNX_USER": "dummy", "CONNX_PASS": "dummy"},
        clear=False,
    )
    @patch(f"{MODULE_UNDER_TEST}.pyodbc.connect")
    def test_get_connx_connection_success(self, mock_connect):
        fake_conn = MagicMock()
        mock_connect.return_value = fake_conn

        conn = mod.get_connx_connection()
        self.assertIs(conn, fake_conn)
        mock_connect.assert_called_once()

    @patch.dict(
        os.environ,
        {"CONNX_DSN_ADABAS": "dummy", "CONNX_USER": "dummy", "CONNX_PASS": "dummy"},
        clear=False,
    )
    @patch(f"{MODULE_UNDER_TEST}.pyodbc.connect")
    def test_get_connx_connection_failure_raises_value_error(self, mock_connect):
        mock_connect.side_effect = pyodbc.Error("nope")

        with self.assertRaises(ValueError) as ctx:
            mod.get_connx_connection()

        self.assertIn("failed to connect to connx adabas dsn", str(ctx.exception).lower())


class TestExecuteQuery(unittest.TestCase):
    @patch(f"{MODULE_UNDER_TEST}.get_connx_connection")
    def test_execute_query_success_returns_list_of_dicts(self, mock_get_conn):
        fake_conn = MagicMock()
        fake_cursor = MagicMock()
        mock_get_conn.return_value = fake_conn
        fake_conn.cursor.return_value = fake_cursor

        fake_cursor.description = [("ID",), ("NAME",)]
        fake_cursor.fetchmany.return_value = [(1, "Alice"), (2, "Bob")]

        results = mod.execute_query("SELECT ID, NAME FROM T WHERE ID > ?", params=[0])

        self.assertEqual(results, [{"ID": 1, "NAME": "Alice"}, {"ID": 2, "NAME": "Bob"}])
        fake_cursor.execute.assert_called_once_with("SELECT ID, NAME FROM T WHERE ID > ?", [0])
        fake_conn.close.assert_called_once()


class TestAsyncWrappers(unittest.IsolatedAsyncioTestCase):
    @patch(f"{MODULE_UNDER_TEST}.execute_query")
    async def test_execute_query_async_delegates(self, mock_execute_query):
        mock_execute_query.return_value = [{"X": 1}]
        out = await mod.execute_query_async("SELECT 1")
        self.assertEqual(out, [{"X": 1}])
        mock_execute_query.assert_called_once()


class TestMcpToolsAndResources(unittest.IsolatedAsyncioTestCase):
    @patch(f"{MODULE_UNDER_TEST}.execute_query_async")
    async def test_query_connx_success(self, mock_exec):
        mock_exec.return_value = [{"ID": 1}]
        out = await mod.query_connx("SELECT * FROM T")
        self.assertEqual(out["count"], 1)

    async def test_query_connx_rejects_non_select(self):
        out = await mod.query_connx("DELETE FROM T")
        self.assertIn("only select", out["error"].lower())

    async def test_query_connx_rejects_semicolons(self):
        out = await mod.query_connx("SELECT * FROM T; SELECT * FROM X")
        self.assertIn("single sql statement", out["error"].lower())

    async def test_describe_server_returns_adabas_metadata(self):
        out = await mod.describe_server()
        self.assertEqual(out["backend"], "Adabas")
        self.assertEqual(out["mode"], "read-only")

    async def test_count_employees_success(self):
        fake_rows = [{"TOTAL_EMPLOYEES": 42}]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)):
            out = await mod.count_employees()
        self.assertEqual(out["total_employees"], 42)

    async def test_count_vehicles_success(self):
        fake_rows = [{"TOTAL_VEHICLES": 17}]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)):
            out = await mod.count_vehicles()
        self.assertEqual(out["total_vehicles"], 17)

    async def test_get_employee_returns_first_row_or_none(self):
        with patch(
            f"{MODULE_UNDER_TEST}.execute_query_async",
            new=AsyncMock(return_value=[{"PERSONNEL_ID": "50005600"}]),
        ):
            out = await mod.get_employee("50005600")
        self.assertEqual(out["employee"], {"PERSONNEL_ID": "50005600"})

        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=[])):
            out = await mod.get_employee("NOPE")
        self.assertIsNone(out["employee"])

    async def test_get_vehicles_for_employee_returns_results(self):
        fake_rows = [{"REG_NUM": "34AL37"}]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)):
            out = await mod.get_vehicles_for_employee("50005600")
        self.assertEqual(out["count"], 1)
        self.assertEqual(out["vehicles"], fake_rows)

    async def test_find_employees_by_city_uses_city_param(self):
        fake_rows = [{"PERSONNEL_ID": "50005600"}]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)) as mock_exec:
            out = await mod.find_employees_by_city("Paris")
        self.assertEqual(out["count"], 1)
        args, kwargs = mock_exec.call_args
        self.assertIn("WHERE UPPER(CITY) = UPPER(?)", args[0].upper())
        self.assertEqual(kwargs.get("params"), ["Paris"])

    async def test_employees_with_vehicles_returns_joined_rows(self):
        fake_rows = [{"PERSONNEL_ID": "50005600", "REG_NUM": "34AL37"}]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)):
            out = await mod.employees_with_vehicles()
        self.assertEqual(out["count"], 1)

    async def test_vehicles_by_department_returns_rows(self):
        fake_rows = [{"DEPARTMENT": "SALES", "VEHICLE_COUNT": 4}]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)):
            out = await mod.vehicles_by_department()
        self.assertEqual(out["departments"], fake_rows)

    async def test_leased_vehicles_by_department_returns_rows(self):
        fake_rows = [{"DEPARTMENT": "SALES", "LEASED_VEHICLE_COUNT": 2}]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)):
            out = await mod.leased_vehicles_by_department()
        self.assertEqual(out["departments"], fake_rows)

    async def test_vehicles_by_country_returns_rows(self):
        fake_rows = [{"COUNTRY": "FRANCE", "VEHICLE_COUNT": 6}]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)):
            out = await mod.vehicles_by_country()
        self.assertEqual(out["countries"], fake_rows)

    async def test_vehicle_summary_by_make_returns_rows(self):
        fake_rows = [{"MAKE": "PEUGEOT", "VEHICLE_COUNT": 3}]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)):
            out = await mod.vehicle_summary_by_make()
        self.assertEqual(out["makes"], fake_rows)

    async def test_describe_entities_returns_employees_and_vehicles(self):
        out = await mod.describe_entities()
        self.assertTrue(any(e.get("entity") == "employees" for e in out["entities"]))
        self.assertTrue(any(e.get("entity") == "vehicles" for e in out["entities"]))

    async def test_count_entities_known_entity_calls_db(self):
        fake_rows = [{"TOTAL_COUNT": 99}]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)):
            out = await mod.count_entities("employees")
        self.assertEqual(out["total"], 99)

    async def test_count_entities_unknown_entity_returns_error(self):
        out = await mod.count_entities("orders")
        self.assertIn("unknown entity", out["error"].lower())

    @patch(f"{MODULE_UNDER_TEST}.execute_query_async")
    async def test_get_schema_success(self, mock_exec):
        mock_exec.return_value = [{"TABLE_NAME": "X"}]
        out = await mod.get_schema()
        self.assertEqual(out["schemas"], [{"TABLE_NAME": "X"}])

    @patch(f"{MODULE_UNDER_TEST}.execute_query_async")
    async def test_get_schema_for_table_uses_param_query(self, mock_exec):
        mock_exec.return_value = [{"TABLE_NAME": "Sales", "COLUMN_NAME": "ID"}]
        await mod.get_schema_for_table("Sales")
        args, kwargs = mock_exec.call_args
        self.assertIn("WHERE TABLE_NAME = ?", args[0].upper())
        self.assertEqual(kwargs.get("params"), ["Sales"])

    async def test_datasets_resource_mentions_adabas(self):
        out = await mod.datasets()
        self.assertEqual(out["backend"], "Adabas")
        self.assertEqual(len(out["datasets"]), 2)

    async def test_employees_domain_metadata_shape(self):
        out = await mod.employees_domain_metadata()
        self.assertEqual(out["entity"], "employees")
        self.assertEqual(out["primary_key"], "PERSONNEL_ID")

    async def test_vehicles_domain_metadata_shape(self):
        out = await mod.vehicles_domain_metadata()
        self.assertEqual(out["entity"], "vehicles")
        self.assertEqual(out["join_key"], "PERSONNEL_ID")

    async def test_semantic_entities_include_relationship(self):
        out = await mod.get_semantic_entities()
        self.assertEqual(len(out["entities"]), 2)
        vehicles = next(e for e in out["entities"] if e["entity"] == "vehicles")
        self.assertIn("PERSONNEL_ID", vehicles["foreign_keys"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
