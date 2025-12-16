import sys
from pathlib import Path

# Add project root to PYTHONPATH
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv()

from connx_server import get_connx_connection


def main():
    conn = get_connx_connection()
    try:
        print("Successfully connected to CONNX")
    finally:
        conn.close()


if __name__ == "__main__":
    main()