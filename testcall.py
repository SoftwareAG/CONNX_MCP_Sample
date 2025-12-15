# ask_connx_cli.py
from connx_core import ConnxCore, ConnxSafetyError

core = ConnxCore("config.json")

def main():
    print("Type a SQL query (or 'exit'):")
    while True:
        sql = input("SQL> ")
        if sql.lower() in ("exit", "quit"):
            break
        try:
            result = core.safe_query(sql)
            print(f"{result['row_count']} rows")
            for row in result["rows"]:
                print(row)
        except ConnxSafetyError as se:
            print("Safety error:", se)
        except Exception as e:
            print("Execution error:", e)

if __name__ == "__main__":
    main()