# tests/test_integration_connx.py
import pytest
from dotenv import load_dotenv

load_dotenv()

@pytest.mark.integration
def test_real_connx_connection():
    from connx_server import get_connx_connection  # import inside test

    try:
        conn = get_connx_connection()
    except (ValueError, RuntimeError) as e:
        pytest.skip(f"CONNX ODBC not configured / not reachable: {e}")

    try:
        assert conn is not None
    finally:
        conn.close()