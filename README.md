![CI](https://github.com/djecon-sag/CONNX_MCP_Sample/actions/workflows/ci.yml/badge.svg)

# CONNX MCP Server

An unofficial MCP (Model Context Protocol) server for integrating with CONNX databases. This allows AI agents (e.g., Claude) to securely query and update data via standardized tools.

## Features
- ODBC connection to CONNX for unified database access.
- MCP tools: `query_connx`, `update_connx`.
- Resources: Schema discovery.
- Async support for efficiency.

## Installation
1. Clone the repo: `git clone https://github.com/yourusername/connx-mcp-server.git`
2. Install dependencies: `pip install -r requirements.txt`
3. Configure CONNX DSN in `connx_server.py` (use env vars for production).

## Usage
Run: `python connx_server.py`

## MCP Tools

This server exposes functionality through **MCP tools**, allowing clients to execute database operations against CONNX-connected data sources using structured, validated entry points.

MCP tools provide a safe, well-defined interface for interacting with CONNX-backed data without exposing raw database connections to clients.

---
## MCP Tools

This server exposes functionality through **MCP tools**, allowing clients to execute database operations against CONNX-connected data sources using structured, validated entry points.

MCP tools provide a safe, well-defined interface for interacting with CONNX-backed data without exposing raw database connections to clients.

## Currently Available Tools

### `query_connx`

Purpose
Executes a SQL SELECT statement against a CONNX-connected database and returns the results.

Parameters
	•	query (str): SQL SELECT statement

Behavior
	•	Executes asynchronously
	•	Uses parameterized execution internally
	•	Returns results as a list of dictionaries
	•	Automatically sanitizes input to reduce SQL injection risk

```python
@mcp.tool()
async def query_connx(query: str) -> Dict[str, Any]:
```
## Purpose
Executes a SQL SELECT statement against a CONNX-connected database and returns the results.

## Parameters
query (str): SQL SELECT statement

## Behavior
-	Executes asynchronously
-	Uses parameterized execution internally
-	Returns results as a list of dictionaries
-	Automatically sanitizes input to reduce SQL injection risk

## Return format
```python
{
  "results": [
    { "COLUMN1": "value", "COLUMN2": 123 },
    ...
  ],
  "count": 10
}
```

## Example
```sql
SELECT CUSTOMER_ID, CUSTOMER_NAME
FROM CUSTOMERS
WHERE STATE = 'CA'
```
---

## Integrate in MCP host config:
```python
@mcp.tool()
async def query_connx(query: str) -> Dict[str, Any]:

# Testing
This project uses pytest for unit testing. Tests mock database interactions to run without a real CONNX setup.

- Install test deps: `pip install pytest pytest-mock pytest-asyncio`
- Run tests: `pytest tests/`

Coverage includes connection handling, query/update execution, sanitization, and MCP tools/resources.

# Integrate in MCP host config:
	```json
	{
	  "mcpServers": {
		"connx-database-server": {
		  "command": "python",
		  "args": ["connx_server.py"]
		}
	  }
	}
```
---
## Example: Add a count_connx Tool

```python
@mcp.tool()
async def count_connx(table_name: str) -> Dict[str, Any]:
    """
    Return the number of rows in a table.
    """
    query = f"SELECT COUNT(*) AS ROW_COUNT FROM {sanitize_input(table_name)}"

    try:
        results = await execute_query_async(query)
        return {
            "table": table_name,
            "row_count": results[0]["ROW_COUNT"]
        }
    except ValueError as e:
        return {"error": str(e)}
```
## Usage
```python
{
  "table": "CUSTOMERS"
}
```
---
## Extending MCP Tools

Adding new tools is intentionally simple and testable.

General Pattern:
1. Create a Python function
2. Decorate it with @mcp.tool()
3. Call existing helper functions (execute_query_async, execute_update_async)
4. Return a JSON-serializable dictionary
