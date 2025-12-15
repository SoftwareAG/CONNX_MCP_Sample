![CI](https://github.com/djecon-sag/CONNX_MCP_Sample/actions/workflows/ci.yml/badge.svg)

## Table of Contents

- [Overview](#connx-mcp-server)
- [Sample Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [MCP Tools](#mcp-tools)
  - [`query_connx`](#query_connx)
  - [`update_connx`](#update_connx)
- [MCP Resources](#mcp-resources)
- [MCP Client Examples](#mcp-client-examples)
- [Extending MCP Tools](#extending-mcp-tools)
- [Summary](#summary)
- [What is MCP?](#what-is-mcp)


## CONNX MCP Server

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
```json
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
## update_connx
```python
@mcp.tool()
async def update_connx(operation: str, query: str) -> Dict[str, Any]:
```
## Purpose
Executes data-modifying SQL statements (INSERT, UPDATE, DELETE) via CONNX.

## Parameters
	•	operation (str): One of insert, update, delete
	•	query (str): Full SQL statement

## Behavior
	•	Validates the operation type before execution
	•	Executes inside a transaction
	•	Commits on success, rolls back on failure

## Return format
```json
{
  "affected_rows": 5,
  "message": "Update completed successfully."
}
```
## Example
```sql
UPDATE CUSTOMERS
SET STATUS = 'INACTIVE'
WHERE LAST_LOGIN < '2022-01-01'
```
## MCP Client Examples

Below are examples of how MCP-compatible clients (such as Claude Desktop or other MCP hosts) can invoke the CONNX MCP Server.

### Example: Query Data

```json
{
  "tool": "query_connx",
  "arguments": {
    "query": "SELECT CUSTOMER_ID, CUSTOMER_NAME FROM CUSTOMERS WHERE STATE = 'CA'"
  }
}
```
## Response
```json
{
  "results": [
    { "CUSTOMER_ID": "C001", "CUSTOMER_NAME": "Acme Corp" }
  ],
  "count": 1
}
```
---

## Integrate in MCP host config
```python
@mcp.tool()
async def query_connx(query: str) -> Dict[str, Any]:
```

# Testing
This project uses pytest for unit testing. Tests mock database interactions to run without a real CONNX setup.

- Install test deps: `pip install pytest pytest-mock pytest-asyncio`
- Run tests: `pytest tests/`

Coverage includes connection handling, query/update execution, sanitization, and MCP tools/resources.

## Integrate in MCP host config
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

## Summary
- query_connx is used for read-only SQL queries
- update_connx is used for data modification
- tools are asynchronous, safe, and testable
- extending the toolset follows a simple, repeatable pattern
- CI and test coverage protect against regressions
---
## Extending MCP Tools

Adding new tools is intentionally simple and testable.

General Pattern:
1. Create a Python function
2. Decorate it with @mcp.tool()
3. Call existing helper functions (execute_query_async, execute_update_async)
4. Return a JSON-serializable dictionary

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
## What is MCP?
The Model Context Protocol (MCP) is an open-source standard developed by Anthropic and launched in November 2024. It enables AI models and applications to securely connect to and interact with external data sources, tools, and workflows through a standardized interface. 

MCP acts as a universal "USB-C" port for AI, allowing seamless integrations without the need for custom code for each connection. This protocol builds on existing concepts like tool use and function calling but standardizes them, reducing the fragmentation in AI integrations. By providing access to live, real-world data, MCP empowers large language models (LLMs) like Claude to perform tasks, deliver accurate insights, and handle actions that extend beyond their original training data.

MCP addresses the challenge of AI models being isolated from real-time data and external capabilities. It enables LLMs to:
- Access current data from diverse sources.
- Perform actions on behalf of users, such as querying databases or sending emails.
- Utilize specialized tools and workflows without custom integrations.


---
## Building Blocks
MCP servers expose capabilities through three primary building blocks, which standardize how AI applications interact with external systems:

| Feature   | Explanation                                                                 | Examples                          | Who Controls It |
|-----------|-----------------------------------------------------------------------------|-----------------------------------|-----------------|
| **Tools** | Active functions that the LLM can invoke based on user requests. These can perform actions like writing to databases, calling APIs, or modifying files. Hosts must obtain user consent before invocation. | Search flights, send messages, create calendar events | Model (LLM decides when to call) |
| **Resources** | Passive, read-only data sources providing context, such as file contents, database schemas, or API documentation. | Retrieve documents, access knowledge bases, read calendars | Application (host manages access) |
| **Prompts** | Pre-built templates or workflows that guide the LLM in using tools and resources effectively. | Plan a vacation, summarize meetings, draft an email | User (selects or customizes) |

---
## How MCP Works
At its core, MCP allows an LLM to request assistance from external systems to fulfill user queries. The process involves discovery, invocation, execution, and response.

### Simplified Workflow Example
Consider a user query: "Find the latest sales report in our database and email it to my manager."

1. **Request and Discovery**: The LLM recognizes it needs external access (e.g., database query and email sending). Via the MCP client, it discovers available servers and relevant tools, such as `database_query` and `email_sender`.

2. **Tool Invocation**: The LLM generates a structured request. The client sends it to the appropriate server (e.g., first invoking `database_query` with the report details).

3. **External Action and Response**: The server translates the request (e.g., into a secure SQL query), executes it on the backend system, retrieves the data, and returns it in a formatted response to the client.

4. **Subsequent Actions**: With the data, the LLM invokes the next tool (e.g., `email_sender`), and the server confirms completion.

5. **Final Response**: The LLM replies to the user: "I have found the latest sales report and emailed it to your manager."

This bidirectional flow ensures efficient, secure interactions. Real-world examples include generating web apps from Figma designs, analyzing data across multiple databases via natural language, or creating 3D models in Blender for printing.
