# connx_client.py
import asyncio
import sys
import json
import re
import time
from typing import Optional
from contextlib import AsyncExitStack

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from openai import OpenAI
from dotenv import load_dotenv

from trace_capture import RpcTape

load_dotenv()


def _tool_result_to_text(tool_result) -> str:
    """
    MCP CallToolResult.content is typically a LIST of content items (e.g., TextContent).
    Convert it into a single string safely.
    """
    content = getattr(tool_result, "content", None)

    if isinstance(content, str):
        return content

    if isinstance(content, list):
        parts = []
        for item in content:
            if hasattr(item, "text"):
                parts.append(item.text)
            elif isinstance(item, dict) and "text" in item:
                parts.append(str(item["text"]))
            else:
                parts.append(str(item))
        return "\n".join(p for p in parts if p)

    return str(tool_result)


# ---- SQL-92 guardrail (hard stop before CONNX sees vendor SQL) ----
_SQL92_FORBIDDEN = re.compile(
    r"(?is)\b("
    r"top|limit|offset|fetch\s+first|fetch\s+next|qualify|ilike|distinct\s+on|returning"
    r")\b"
)


def _assert_sql92(sql: str) -> None:
    if _SQL92_FORBIDDEN.search(sql or ""):
        raise ValueError(
            "Non ANSI SQL-92 detected in SQL text (e.g., TOP/LIMIT/OFFSET/FETCH/QUALIFY/ILIKE). "
            "Please rewrite using ANSI SQL-92 only."
        )


# ---- Tee wrappers for MCP stdio so RpcTape captures raw JSON-RPC ----
class _TeeReceiveStream:
    """
    Wrap an anyio ByteReceiveStream-like object.

    MCP may use:
      - async with reader:
      - async for chunk in reader:
      - await reader.receive()
    """

    def __init__(self, inner, tape: RpcTape):
        self._inner = inner
        self._tape = tape

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def __aenter__(self):
        if hasattr(self._inner, "__aenter__"):
            await self._inner.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if hasattr(self._inner, "__aexit__"):
            return await self._inner.__aexit__(exc_type, exc, tb)
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        data = await self.receive()
        # anyio streams return b"" on EOF
        if data == b"" or data is None:
            raise StopAsyncIteration
        return data

    async def receive(self, *args, **kwargs):
        data = await self._inner.receive(*args, **kwargs)

        try:
            s = data.decode("utf-8", errors="replace") if isinstance(data, (bytes, bytearray)) else str(data)
        except Exception:
            s = str(data)

        if s.strip():
            self._tape.add("<-", s.rstrip("\r\n"))

        return data


class _TeeSendStream:
    """
    Wrap an anyio ByteSendStream-like object.

    MCP may use:
      - async with writer:
      - await writer.send(...)
    """

    def __init__(self, inner, tape: RpcTape):
        self._inner = inner
        self._tape = tape

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def __aenter__(self):
        if hasattr(self._inner, "__aenter__"):
            await self._inner.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if hasattr(self._inner, "__aexit__"):
            return await self._inner.__aexit__(exc_type, exc, tb)
        return False

    async def send(self, data, *args, **kwargs):
        try:
            s = data.decode("utf-8", errors="replace") if isinstance(data, (bytes, bytearray)) else str(data)
        except Exception:
            s = str(data)

        if s.strip():
            self._tape.add("->", s.rstrip("\r\n"))

        return await self._inner.send(data, *args, **kwargs)


class MCPClient:
    def __init__(self, llm_provider=None):
        self.tape = RpcTape(maxlen=2000)
        self.session: Optional[ClientSession] = None
        self.exit_stack = AsyncExitStack()
        self.llm = llm_provider or OpenAI()

    async def connect_to_server(self, server_script_path: str):
        if not server_script_path.endswith(".py"):
            raise ValueError("Server script must be a .py file for this demo")

        pyexe = sys.executable
        print("[MCP] Connecting to Server")
        print(f"[MCP] connect_to_server() using python={pyexe}")
        print(f"[MCP] launching server script: {server_script_path}")

        server_params = StdioServerParameters(
            command=pyexe,
            args=[server_script_path],
            env=None,
        )

        print("[MCP] starting stdio_client()...")
        read_stream, write_stream = await self.exit_stack.enter_async_context(stdio_client(server_params))
        print("[MCP] stdio_client started")

        # IMPORTANT: wrap transport, so we can capture JSON-RPC on the tape
        read_stream = _TeeReceiveStream(read_stream, self.tape)
        write_stream = _TeeSendStream(write_stream, self.tape)

        print("[MCP] creating ClientSession...")
        self.session = await self.exit_stack.enter_async_context(ClientSession(read_stream, write_stream))
        print("[MCP] created ClientSession")

        print("[MCP] initializing session (ClientSession.initialize)...")
        await asyncio.wait_for(self.session.initialize(), timeout=30)
        print("[MCP] session.initialize OK")

        print("[MCP] listing tools...")
        resp = await self.session.list_tools()
        tools = [t.name for t in resp.tools]
        print(f"[MCP] Connected. Tools={tools}")

    async def call_tool_direct(self, tool_name: str, tool_args: dict) -> str:
        """Direct MCP tool invocation (no LLM)."""
        if not self.session:
            raise ValueError("Not connected to a server")

        if tool_name == "query_database":
            sql = (tool_args or {}).get("sql_query", "")
            _assert_sql92(sql)

        tool_result = await self.session.call_tool(tool_name, tool_args)
        return _tool_result_to_text(tool_result)

    def _route_intent(self, user_text: str):
        """Return (tool_name, tool_args) if we should force a tool call, else None."""
        t = user_text.strip()

        m = re.match(r"(?i)^\s*(list|show)\s+tables(?:\s+in\s+schema\s+(\w+))?(?:\s+like\s+(.+))?\s*$", t)
        if m:
            schema = m.group(2)
            like = m.group(3)
            args = {}
            if schema:
                args["schema"] = schema.strip()
            if like:
                args["table_name_like"] = like.strip().strip("'\"")
            return ("list_tables", args)

        m = re.match(r"(?i)^\s*(describe|desc)\s+table\s+(.+)\s*$", t)
        if m:
            qualified = m.group(2).strip().strip("'\"")
            return ("describe_table", {"qualified_table": qualified})

        return None

    async def process_query(self, query: str) -> str:
        if not self.session:
            raise ValueError("Not connected to a server")

        # Deterministic tool routing for demo commands
        routed = self._route_intent(query)
        if routed:
            tool_name, tool_args = routed
            return await self.call_tool_direct(tool_name, tool_args)

        messages = [
            {
                "role": "system",
                "content": (
                    "You are an MCP client connected to enterprise data via CONNX. "
                    "For any database/schema/table/data request you MUST call an MCP tool. "
                    "If you generate SQL, it MUST be ANSI SQL-92 compliant. "
                    "Avoid vendor SQL like TOP/LIMIT/OFFSET/FETCH/QUALIFY/ILIKE. "
                    "Do not narrate intentions."
                ),
            },
            {"role": "user", "content": query},
        ]

        resp = await self.session.list_tools()
        available_tools = [
            {
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.inputSchema,
                },
            }
            for tool in resp.tools
        ]

        llm_response = self.llm.chat.completions.create(
            model="gpt-4o",
            temperature=0,
            max_tokens=800,
            messages=messages,
            tools=available_tools,
            tool_choice="required",
        )

        response_message = llm_response.choices[0].message
        messages.append(response_message.model_dump())

        if response_message.tool_calls:
            for tool_call in response_message.tool_calls:
                tool_name = tool_call.function.name
                tool_args = json.loads(tool_call.function.arguments or "{}")

                if tool_name == "query_database":
                    _assert_sql92(tool_args.get("sql_query", ""))

                tool_result = await self.session.call_tool(tool_name, tool_args)
                tool_content = _tool_result_to_text(tool_result)

                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "name": tool_name,
                        "content": tool_content,
                    }
                )

            final_llm_response = self.llm.chat.completions.create(
                model="gpt-4o",
                temperature=0,
                max_tokens=800,
                messages=messages,
            )
            return final_llm_response.choices[0].message.content or "No response generated."

        return "Model did not return tool calls."

    async def cleanup(self):
        await self.exit_stack.aclose()

    def tape_tail(self, limit: int = 60):
        """
        Return last N raw JSON-RPC lines for the web UI.
        RpcTape.snapshot() in your file returns *all* lines, so we slice here.
        """
        snap = self.tape.snapshot()
        return snap[-limit:]


async def main():
    if len(sys.argv) < 2:
        print("Usage: python connx_client.py path/to/connx_db_server.py")
        sys.exit(1)

    client = MCPClient()
    try:
        await client.connect_to_server(sys.argv[1])
        print("Enter your chat query or 'exit' to quit.")
        while True:
            query = input("Query: ")
            if query.lower() == "exit":
                break
            response = await client.process_query(query)
            print("Response:", response)
    finally:
        await client.cleanup()


if __name__ == "__main__":
    asyncio.run(main())