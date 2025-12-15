# tests/test_integration_connx.py
import pytest
from connx_server import get_connx_connection
from dotenv import load_dotenv

load_dotenv()  # ensure .env is loaded for the test run

from connx_server import get_connx_connection


@pytest.mark.integration
def test_real_connx_connection():
    try:
        conn = get_connx_connection()
    except ValueError as e:
        # Typical when DSN isn't installed/configured on the machine running tests
        pytest.skip(f"CONNX ODBC not configured / not reachable: {e}")

    try:
        assert conn is not None
    finally:
        conn.close()