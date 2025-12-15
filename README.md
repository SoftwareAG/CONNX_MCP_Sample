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
