# tests/test_server.py
import importlib
import os
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import pyodbc

MODULE_UNDER_TEST = "connx_server"


def load_module():
    """
    Import the module under test while neutralizing FastMCP decorators so that
    import-time tool/resource registration doesn't break pytest collection.
    """
    from mcp.server.fastmcp import FastMCP

    def noop_decorator(*args, **kwargs):
        def _wrap(fn):
            return fn
        return _wrap

    # Patch decorators BEFORE importing the server module
    FastMCP.tool = noop_decorator
    FastMCP.resource = noop_decorator

    # Force a clean re-import
    if MODULE_UNDER_TEST in sys.modules:
        del sys.modules[MODULE_UNDER_TEST]

    return importlib.import_module(MODULE_UNDER_TEST)


# Import safely for use in tests
mod = load_module()


class TestConfig(unittest.TestCase):
    def test_assert_config_raises_when_missing(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as ctx:
                mod._assert_config()
        self.assertIn("missing required config values", str(ctx.exception).lower())


class TestSqlHelpers(unittest.TestCase):
    def test_sql_fingerprint_is_stable_and_short(self):
        a = mod._sql_fingerprint("SELECT 1")
        b = mod._sql_fingerprint("SELECT 1")
        self.assertEqual(a, b)
        self.assertEqual(len(a), 12)

    def test_is_single_statement_rejects_semicolon(self):
        self.assertFalse(mod._is_single_statement("SELECT 1; SELECT 2"))

    def test_is_single_statement_accepts_simple(self):
        self.assertTrue(mod._is_single_statement("SELECT 1"))

    def test_is_select_only_accepts_select(self):
        self.assertTrue(mod._is_select_only("SELECT * FROM T"))

    def test_is_select_only_rejects_update(self):
        self.assertFalse(mod._is_select_only("UPDATE T SET A=1"))


class TestEntityAliases(unittest.TestCase):
    def test_resolve_entity_matches_alias(self):
        self.assertEqual(
            mod.resolve_entity("customers"),
            "daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM",
        )
        self.assertEqual(
            mod.resolve_entity("Client"),
            "daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM",
        )
        self.assertEqual(
            mod.resolve_entity("companies"),
            "daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM",
        )

    def test_resolve_entity_unknown_returns_none(self):
        self.assertIsNone(mod.resolve_entity("employees"))
        self.assertIsNone(mod.resolve_entity(""))

    def test_resolve_entity_none_returns_none(self):
        self.assertIsNone(mod.resolve_entity(None))


class TestStateNormalization(unittest.TestCase):
    def test_normalize_state_full_name_to_code(self):
        self.assertEqual(mod._normalize_state("Virginia"), "VA")
        self.assertEqual(mod._normalize_state("  virginia  "), "VA")

    def test_normalize_state_empty(self):
        self.assertEqual(mod._normalize_state(""), "")
        self.assertEqual(mod._normalize_state("   "), "")

    def test_normalize_state_unknown_passthrough(self):
        self.assertEqual(mod._normalize_state("PR"), "PR")  # not in dict, pass through


class TestConnxConnection(unittest.TestCase):
    @patch.dict(os.environ, {"CONNX_DSN": "dummy", "CONNX_USER": "dummy", "CONNX_PASS": "dummy"}, clear=False)
    @patch(f"{MODULE_UNDER_TEST}.pyodbc.connect")
    def test_get_connx_connection_success(self, mock_connect):
        fake_conn = MagicMock()
        mock_connect.return_value = fake_conn

        conn = mod.get_connx_connection()
        self.assertIs(conn, fake_conn)
        mock_connect.assert_called_once()

    @patch.dict(os.environ, {"CONNX_DSN": "dummy", "CONNX_USER": "dummy", "CONNX_PASS": "dummy"}, clear=False)
    @patch(f"{MODULE_UNDER_TEST}.pyodbc.connect")
    def test_get_connx_connection_failure_raises_value_error(self, mock_connect):
        mock_connect.side_effect = pyodbc.Error("nope")

        with self.assertRaises(ValueError) as ctx:
            mod.get_connx_connection()

        self.assertIn("failed to connect to connx", str(ctx.exception).lower())


class TestExecuteQuery(unittest.TestCase):
    @patch(f"{MODULE_UNDER_TEST}.get_connx_connection")
    def test_execute_query_success_returns_list_of_dicts(self, mock_get_conn):
        fake_conn = MagicMock()
        fake_cursor = MagicMock()
        mock_get_conn.return_value = fake_conn
        fake_conn.cursor.return_value = fake_cursor

        fake_cursor.description = [("ID",), ("NAME",)]
        fake_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]

        results = mod.execute_query("SELECT ID, NAME FROM T WHERE ID > ?", params=[0])

        self.assertEqual(results, [{"ID": 1, "NAME": "Alice"}, {"ID": 2, "NAME": "Bob"}])
        fake_cursor.execute.assert_called_once_with("SELECT ID, NAME FROM T WHERE ID > ?", [0])
        fake_conn.close.assert_called_once()

    @patch(f"{MODULE_UNDER_TEST}.get_connx_connection")
    def test_execute_query_raises_when_no_result_set(self, mock_get_conn):
        fake_conn = MagicMock()
        fake_cursor = MagicMock()
        mock_get_conn.return_value = fake_conn
        fake_conn.cursor.return_value = fake_cursor

        fake_cursor.description = None  # simulate no result set

        with self.assertRaises(ValueError) as ctx:
            mod.execute_query("SELECT 1")

        self.assertIn("did not return a result set", str(ctx.exception).lower())
        fake_conn.close.assert_called_once()

    @patch(f"{MODULE_UNDER_TEST}.get_connx_connection")
    def test_execute_query_closes_connection_on_odbc_error(self, mock_get_conn):
        fake_conn = MagicMock()
        fake_cursor = MagicMock()
        mock_get_conn.return_value = fake_conn
        fake_conn.cursor.return_value = fake_cursor

        fake_cursor.execute.side_effect = pyodbc.Error("bad query")

        with self.assertRaises(ValueError) as ctx:
            mod.execute_query("SELECT * FROM X")

        self.assertIn("query execution failed", str(ctx.exception).lower())
        fake_conn.close.assert_called_once()


class TestExecuteUpdate(unittest.TestCase):
    @patch(f"{MODULE_UNDER_TEST}.get_connx_connection")
    def test_execute_update_success_commits_and_returns_rowcount(self, mock_get_conn):
        fake_conn = MagicMock()
        fake_cursor = MagicMock()
        mock_get_conn.return_value = fake_conn
        fake_conn.cursor.return_value = fake_cursor
        fake_cursor.rowcount = 3

        affected = mod.execute_update("UPDATE T SET A = 1")

        self.assertEqual(affected, 3)
        fake_conn.commit.assert_called_once()
        fake_conn.rollback.assert_not_called()
        fake_conn.close.assert_called_once()

    @patch(f"{MODULE_UNDER_TEST}.get_connx_connection")
    def test_execute_update_failure_rolls_back_and_raises(self, mock_get_conn):
        fake_conn = MagicMock()
        fake_cursor = MagicMock()
        mock_get_conn.return_value = fake_conn
        fake_conn.cursor.return_value = fake_cursor
        fake_cursor.execute.side_effect = pyodbc.Error("boom")

        with self.assertRaises(ValueError) as ctx:
            mod.execute_update("DELETE FROM T")

        self.assertIn("update execution failed", str(ctx.exception).lower())
        fake_conn.rollback.assert_called_once()
        fake_conn.commit.assert_not_called()
        fake_conn.close.assert_called_once()


class TestAsyncWrappers(unittest.IsolatedAsyncioTestCase):
    @patch(f"{MODULE_UNDER_TEST}.execute_query")
    async def test_execute_query_async_delegates(self, mock_execute_query):
        mock_execute_query.return_value = [{"X": 1}]
        out = await mod.execute_query_async("SELECT 1")
        self.assertEqual(out, [{"X": 1}])
        mock_execute_query.assert_called_once()

    @patch(f"{MODULE_UNDER_TEST}.execute_update")
    async def test_execute_update_async_delegates(self, mock_execute_update):
        mock_execute_update.return_value = 5
        out = await mod.execute_update_async("UPDATE T SET A=1")
        self.assertEqual(out, 5)
        mock_execute_update.assert_called_once()


class TestMcpToolsAndResources(unittest.IsolatedAsyncioTestCase):
    # ----------------
    # query_connx tool
    # ----------------
    @patch(f"{MODULE_UNDER_TEST}.execute_query_async")
    async def test_query_connx_success(self, mock_exec):
        mock_exec.return_value = [{"ID": 1}, {"ID": 2}]
        out = await mod.query_connx("SELECT * FROM T")
        self.assertEqual(out["count"], 2)
        self.assertEqual(out["results"], [{"ID": 1}, {"ID": 2}])

    async def test_query_connx_rejects_non_select(self):
        out = await mod.query_connx("DELETE FROM T")
        self.assertIn("error", out)
        self.assertIn("only select", out["error"].lower())

    async def test_query_connx_rejects_semicolons(self):
        out = await mod.query_connx("SELECT * FROM T; SELECT * FROM X")
        self.assertIn("error", out)
        self.assertIn("single sql statement", out["error"].lower())

    @patch(f"{MODULE_UNDER_TEST}.execute_query_async")
    async def test_query_connx_value_error_returns_error_dict(self, mock_exec):
        mock_exec.side_effect = ValueError("no db")
        out = await mod.query_connx("SELECT * FROM T")
        self.assertIn("error", out)
        self.assertIn("no db", out["error"].lower())

    # -----------------
    # update_connx tool
    # -----------------
    async def test_update_connx_rejects_when_writes_disabled(self):
        out = await mod.update_connx("update", "UPDATE T SET A=1")
        self.assertIn("error", out)
        self.assertIn("writes are disabled", out["error"].lower())

    @patch(f"{MODULE_UNDER_TEST}.CONNX_ALLOW_WRITES", True)
    async def test_update_connx_rejects_invalid_operation_when_writes_enabled(self):
        out = await mod.update_connx("merge", "UPDATE T SET A=1")
        self.assertIn("error", out)
        self.assertIn("invalid operation", out["error"].lower())

    @patch(f"{MODULE_UNDER_TEST}.CONNX_ALLOW_WRITES", True)
    async def test_update_connx_rejects_semicolons_when_writes_enabled(self):
        out = await mod.update_connx("update", "UPDATE T SET A=1; UPDATE T SET A=2")
        self.assertIn("error", out)
        self.assertIn("single sql statement", out["error"].lower())

    @patch(f"{MODULE_UNDER_TEST}.CONNX_ALLOW_WRITES", True)
    @patch(f"{MODULE_UNDER_TEST}.execute_update_async")
    async def test_update_connx_success_when_writes_enabled(self, mock_exec):
        mock_exec.return_value = 7
        out = await mod.update_connx("update", "UPDATE T SET A=1")
        self.assertEqual(out["affected_rows"], 7)
        self.assertIn("completed successfully", out["message"].lower())

    @patch(f"{MODULE_UNDER_TEST}.CONNX_ALLOW_WRITES", True)
    @patch(f"{MODULE_UNDER_TEST}.execute_update_async")
    async def test_update_connx_error_when_writes_enabled(self, mock_exec):
        mock_exec.side_effect = ValueError("bad update")
        out = await mod.update_connx("delete", "DELETE FROM T")
        self.assertIn("error", out)
        self.assertIn("bad update", out["error"].lower())

    # -------------------
    # count_customers tool
    # -------------------
    async def test_count_customers_success(self):
        fake_rows = [{"TOTAL_CUSTOMERS": 999}]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)):
            out = await mod.count_customers()
        self.assertEqual(out["total_customers"], 999)

    async def test_count_customers_value_error_returns_error_dict(self):
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(side_effect=ValueError("db down"))):
            out = await mod.count_customers()
        self.assertIn("error", out)
        self.assertIn("db down", out["error"].lower())

    # -----------------
    # schema resources
    # -----------------
    @patch(f"{MODULE_UNDER_TEST}.execute_query_async")
    async def test_get_schema_success(self, mock_exec):
        mock_exec.return_value = [{"TABLE_NAME": "X"}]
        out = await mod.get_schema()
        self.assertIn("schemas", out)
        self.assertEqual(out["schemas"], [{"TABLE_NAME": "X"}])

    @patch(f"{MODULE_UNDER_TEST}.execute_query_async")
    async def test_get_schema_value_error_returns_error_dict(self, mock_exec):
        mock_exec.side_effect = ValueError("schema fail")
        out = await mod.get_schema()
        self.assertIn("error", out)
        self.assertIn("schema fail", out["error"].lower())

    @patch(f"{MODULE_UNDER_TEST}.execute_query_async")
    async def test_get_schema_for_table_uses_param_query(self, mock_exec):
        mock_exec.return_value = [{"TABLE_NAME": "Sales", "COLUMN_NAME": "ID"}]
        out = await mod.get_schema_for_table("Sales")
        self.assertIn("schemas", out)

        args, kwargs = mock_exec.call_args
        query_sent = args[0].upper()
        self.assertIn("WHERE TABLE_NAME = ?", query_sent)
        self.assertEqual(kwargs.get("params"), ["Sales"])

    @patch(f"{MODULE_UNDER_TEST}.execute_query_async")
    async def test_get_schema_for_table_value_error_returns_error_dict(self, mock_exec):
        mock_exec.side_effect = ValueError("schema table fail")
        out = await mod.get_schema_for_table("CUSTOMERS_VSAM")
        self.assertIn("error", out)
        self.assertIn("schema table fail", out["error"].lower())

    # ------------------------
    # domain metadata/resources
    # ------------------------
    async def test_customers_domain_metadata_resource_shape(self):
        out = await mod.customers_domain_metadata()
        self.assertEqual(out.get("entity"), "customers")
        self.assertIn("primary_table", out)
        self.assertIn("common_queries", out)
        self.assertIn("columns", out)

    async def test_datasets_resource_shape(self):
        out = await mod.datasets()
        self.assertIn("datasets", out)
        self.assertTrue(any(d.get("logical_name") == "customers" for d in out["datasets"]))

    async def test_get_semantic_entities_resource_shape(self):
        out = await mod.get_semantic_entities()
        self.assertIn("entities", out)
        self.assertIsInstance(out["entities"], list)
        self.assertGreaterEqual(len(out["entities"]), 3)
        self.assertTrue(any(e.get("entity") == "customers" for e in out["entities"]))

    # -----------------
    # customer demo tools
    # -----------------
    async def test_customers_by_state_returns_states(self):
        fake_rows = [{"STATE": "CA", "CUSTOMER_COUNT": 10}]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)):
            out = await mod.customers_by_state()
        self.assertEqual(out["states"], fake_rows)

    async def test_customer_cities_returns_cities(self):
        fake_rows = [{"CITY": "Richmond"}]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)):
            out = await mod.customer_cities()
        self.assertEqual(out["cities"], fake_rows)

    async def test_customers_missing_phone_returns_results_and_count(self):
        fake_rows = [{"CUSTOMERID": "C1", "CUSTOMERNAME": "X"}]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)):
            out = await mod.customers_missing_phone()
        self.assertEqual(out["count"], 1)
        self.assertEqual(out["results"], fake_rows)

    async def test_get_customer_returns_first_row_or_none(self):
        fake_rows = [{"CUSTOMERID": "C1"}]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)):
            out = await mod.get_customer("C1")
        self.assertEqual(out["customer"], {"CUSTOMERID": "C1"})

        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=[])):
            out2 = await mod.get_customer("NOPE")
        self.assertIsNone(out2["customer"])

    async def test_find_customers_builds_query_and_params_state_only(self):
        fake_rows = [{"CUSTOMERID": "A"}]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)) as mock_exec:
            out = await mod.find_customers("Virginia")  # normalize -> VA

        self.assertEqual(out["count"], 1)
        args, kwargs = mock_exec.call_args
        sql_sent = args[0]
        params_sent = kwargs.get("params")
        self.assertIn("FROM daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM", sql_sent)
        self.assertEqual(params_sent, ["VA"])

    async def test_find_customers_includes_city_filter_when_provided(self):
        fake_rows = [{"CUSTOMERID": "A"}]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)) as mock_exec:
            out = await mod.find_customers("VA", city="Richmond")

        self.assertEqual(out["count"], 1)
        args, kwargs = mock_exec.call_args
        sql_sent = args[0].upper()
        params_sent = kwargs.get("params")
        self.assertIn("CUSTOMERCITY", sql_sent)
        self.assertEqual(params_sent, ["VA", "Richmond"])

    async def test_find_customers_truncates_results(self):
        fake_rows = [{"CUSTOMERID": f"C{i}"} for i in range(150)]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)):
            out = await mod.find_customers(state="CA", max_rows=100)

        self.assertEqual(out["count"], 100)
        self.assertTrue(out["truncated"])
        self.assertEqual(len(out["results"]), 100)

    async def test_find_customers_value_error_returns_error_dict(self):
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(side_effect=ValueError("boom"))):
            out = await mod.find_customers("CA")
        self.assertIn("error", out)
        self.assertIn("boom", out["error"].lower())

    # --------------------
    # describe/count entities
    # --------------------
    async def test_describe_entities_returns_entities(self):
        out = await mod.describe_entities()
        self.assertIn("entities", out)
        self.assertTrue(any(e.get("entity") == "customers" for e in out["entities"]))

    async def test_count_entities_unknown_entity_returns_error(self):
        out = await mod.count_entities("employees")
        self.assertIn("error", out)
        self.assertIn("unknown entity", out["error"].lower())

    async def test_count_entities_known_entity_calls_db(self):
        fake_rows = [{"TOTAL_COUNT": 123}]
        with patch(f"{MODULE_UNDER_TEST}.execute_query_async", new=AsyncMock(return_value=fake_rows)):
            out = await mod.count_entities("customers")

        self.assertEqual(out["total"], 123)
        self.assertEqual(out["table"], "daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM")


if __name__ == "__main__":
    unittest.main(verbosity=2)