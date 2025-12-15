# tests/test_server.py
import unittest
import importlib
import sys
from unittest.mock import MagicMock, patch
import pyodbc
from dotenv import load_dotenv

MODULE_UNDER_TEST = "connx_server"


def load_module():
    """
    Import the module under test while neutralizing FastMCP decorators so that
    import-time registration/validation doesn't break pytest collection.
    """
    # Patch FastMCP decorators BEFORE importing the server module
    from mcp.server.fastmcp import FastMCP

    def noop_decorator(*args, **kwargs):
        def _wrap(fn):
            return fn
        return _wrap

    # Apply patches
    FastMCP.tool = noop_decorator
    FastMCP.resource = noop_decorator

    # Force a clean re-import
    if MODULE_UNDER_TEST in sys.modules:
        del sys.modules[MODULE_UNDER_TEST]

    return importlib.import_module(MODULE_UNDER_TEST)


# Import safely for use in tests
mod = load_module()


class TestSanitizeInput(unittest.TestCase):
    def test_sanitize_removes_common_patterns(self):
        s = "SELECT * FROM users; DROP TABLE users; -- comment"
        out = mod.sanitize_input(s)
        self.assertNotIn("DROP", out.upper())
        self.assertNotIn(";", out)
        self.assertNotIn("--", out)

    def test_sanitize_is_case_insensitive(self):
        s = "DrOp TaBlE X"
        out = mod.sanitize_input(s)
        self.assertNotIn("DROP", out.upper())

    def test_sanitize_keeps_normal_text(self):
        s = "Region = ?"
        out = mod.sanitize_input(s)
        self.assertEqual(out, s)


class TestConnxConnection(unittest.TestCase):
    @patch(f"{MODULE_UNDER_TEST}.pyodbc.connect")
    def test_get_connx_connection_success(self, mock_connect):
        fake_conn = MagicMock()
        mock_connect.return_value = fake_conn

        conn = mod.get_connx_connection()
        self.assertIs(conn, fake_conn)
        mock_connect.assert_called_once()

    @patch(f"{MODULE_UNDER_TEST}.pyodbc.connect")
    def test_get_connx_connection_failure_raises_value_error(self, mock_connect):
        mock_connect.side_effect = pyodbc.Error("nope")

        with self.assertRaises(ValueError) as ctx:
            mod.get_connx_connection()

        self.assertIn("Failed to connect to CONNX", str(ctx.exception))


class TestExecuteQuery(unittest.TestCase):
    @patch(f"{MODULE_UNDER_TEST}.get_connx_connection")
    def test_execute_query_success_returns_list_of_dicts(self, mock_get_conn):
        fake_conn = MagicMock()
        fake_cursor = MagicMock()
        mock_get_conn.return_value = fake_conn
        fake_conn.cursor.return_value = fake_cursor

        fake_cursor.description = [("ID",), ("NAME",)]
        fake_cursor.fetchall.return_value = [
            (1, "Alice"),
            (2, "Bob"),
        ]

        results = mod.execute_query("SELECT ID, NAME FROM T WHERE ID > ?", params=[0])

        self.assertEqual(
            results,
            [{"ID": 1, "NAME": "Alice"}, {"ID": 2, "NAME": "Bob"}],
        )
        fake_cursor.execute.assert_called_once()
        fake_conn.close.assert_called_once()

    @patch(f"{MODULE_UNDER_TEST}.get_connx_connection")
    def test_execute_query_closes_connection_on_error(self, mock_get_conn):
        fake_conn = MagicMock()
        fake_cursor = MagicMock()
        mock_get_conn.return_value = fake_conn
        fake_conn.cursor.return_value = fake_cursor

        fake_cursor.execute.side_effect = pyodbc.Error("bad query")

        with self.assertRaises(ValueError) as ctx:
            mod.execute_query("SELECT * FROM X")

        self.assertIn("Query execution failed", str(ctx.exception))
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

        self.assertIn("Update execution failed", str(ctx.exception))
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


class TestMcpToolFunctions(unittest.IsolatedAsyncioTestCase):
    @patch(f"{MODULE_UNDER_TEST}.execute_query_async")
    async def test_query_connx_success(self, mock_exec):
        mock_exec.return_value = [{"ID": 1}, {"ID": 2}]
        out = await mod.query_connx("SELECT * FROM T")
        self.assertEqual(out["count"], 2)
        self.assertEqual(out["results"], [{"ID": 1}, {"ID": 2}])

    @patch(f"{MODULE_UNDER_TEST}.execute_query_async")
    async def test_query_connx_error_returns_error_dict(self, mock_exec):
        mock_exec.side_effect = ValueError("no db")
        out = await mod.query_connx("SELECT * FROM T")
        self.assertIn("error", out)
        self.assertIn("no db", out["error"])

    async def test_update_connx_rejects_invalid_operation(self):
        out = await mod.update_connx("merge", "UPDATE T SET A=1")
        self.assertIn("error", out)
        self.assertIn("Invalid operation", out["error"])

    @patch(f"{MODULE_UNDER_TEST}.execute_update_async")
    async def test_update_connx_success(self, mock_exec):
        mock_exec.return_value = 7
        out = await mod.update_connx("update", "UPDATE T SET A=1")
        self.assertEqual(out["affected_rows"], 7)
        self.assertIn("completed successfully", out["message"].lower())

    @patch(f"{MODULE_UNDER_TEST}.execute_update_async")
    async def test_update_connx_error(self, mock_exec):
        mock_exec.side_effect = ValueError("bad update")
        out = await mod.update_connx("delete", "DELETE FROM T")
        self.assertIn("error", out)
        self.assertIn("bad update", out["error"])

    @patch(f"{MODULE_UNDER_TEST}.execute_query_async")
    async def test_get_schema_without_table_name(self, mock_exec):
        mock_exec.return_value = [{"TABLE_NAME": "X"}]
        out = await mod.get_schema()
        self.assertIn("schemas", out)
        self.assertEqual(out["schemas"], [{"TABLE_NAME": "X"}])

    @patch(f"{MODULE_UNDER_TEST}.execute_query_async")
    async def test_get_schema_for_table_filters_query(self, mock_exec):
        mock_exec.return_value = [{"TABLE_NAME": "Sales", "COLUMN_NAME": "ID"}]

        # NOTE: this must match the function name in connx_server.py
        out = await mod.get_schema_for_table("Sales")

        self.assertIn("schemas", out)
        args, _kwargs = mock_exec.call_args
        query_sent = args[0]
        self.assertIn("WHERE TABLE_NAME", query_sent.upper())


if __name__ == "__main__":
    unittest.main(verbosity=2)