# web_app.py
import atexit
import asyncio
import threading
import re
import json
from flask import Flask, request, jsonify
import os

from connx_client import MCPClient

app = Flask(__name__)

# ---- Background asyncio loop + persistent MCP client ----
_loop = None
_loop_thread = None
_loop_ready = threading.Event()

_client = None
_client_ready = threading.Event()
_client_error = None

_start_lock = threading.Lock()
_query_lock = threading.Lock()  # serialize MCP calls (safer for stdio)


def _start_background_loop():
    """Start an asyncio event loop in a background thread."""
    global _loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _loop = loop
    _loop_ready.set()
    loop.run_forever()


    async def _init_mcp_client():
        global _client
        _client = MCPClient()
        SERVER_PATH = os.path.join(os.path.dirname(__file__), "connx_db_server.py")
        await _client.connect_to_server(SERVER_PATH)

        resp = await _client.session.list_tools()
        print("MCP server tools:", [t.name for t in resp.tools])

    async def _init_mcp_client():
        global _client
        _client = MCPClient()
        await _client.connect_to_server("connx_db_server.py")

        # DEBUG: print available tools from the server we actually connected to
        resp = await _client.session.list_tools()
        print("MCP server tools:", [t.name for t in resp.tools])


async def _shutdown_mcp_client():
    """Cleanup MCP client."""
    global _client
    if _client is not None:
        await _client.cleanup()
        _client = None


def _ensure_started():
    """Start loop thread and connect MCP client exactly once."""
    global _loop_thread, _client_error

    with _start_lock:
        if _loop_thread is None:
            _loop_thread = threading.Thread(target=_start_background_loop, daemon=True)
            _loop_thread.start()

        if not _loop_ready.wait(timeout=10):
            _client_error = "Async loop failed to start"
            return

        if not _client_ready.is_set() and _client_error is None:
            try:
                fut = asyncio.run_coroutine_threadsafe(_init_mcp_client(), _loop)
                fut.result(timeout=30)
                _client_ready.set()
            except Exception as e:
                _client_error = str(e)


@app.before_request
def _startup_hook():
    _ensure_started()


@atexit.register
def _cleanup_on_exit():
    """Best-effort cleanup on process exit."""
    try:
        if _loop is not None:
            fut = asyncio.run_coroutine_threadsafe(_shutdown_mcp_client(), _loop)
            fut.result(timeout=5)
            _loop.call_soon_threadsafe(_loop.stop)
    except Exception:
        pass


def _route_intent(user_text: str):
    """
    Return (tool_name, tool_args) if we should force a direct MCP tool call (no LLM),
    else None.
    """
    t = user_text.strip()

    # list tables [in schema dbo] [like CUSTOMER]
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

    # describe table <qualified>
    m = re.match(r"(?i)^\s*(describe|desc)\s+table\s+(.+)\s*$", t)
    if m:
        qualified = m.group(2).strip().strip("'\"")
        return ("describe_table", {"qualified_table": qualified})

    # very simple NL -> SQL shortcut for demo: CUSTOMERSTATE = 'XX'
    m = re.match(r'(?i)^\s*which\s+customers\s+have\s+customerstate\s*=\s*["\']?([A-Za-z]{2})["\']?\s*$', t)
    if m:
        st = m.group(1).upper()
        return (
            "query_database",
            {"sql_query": f"SELECT * FROM daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM WHERE CUSTOMERSTATE = '{st}'"}
        )

    return None


@app.get("/")
def home():
    if _client_error:
        return f"MCP init failed: {_client_error}", 500
    if not _client_ready.is_set():
        return "Starting MCP client...", 503
    return "GUI is up. Open /ui for the demo page.", 200


@app.post("/query")
def query():
    if _client_error:
        return jsonify({"ok": False, "error": f"MCP init failed: {_client_error}"}), 500
    if not _client_ready.is_set():
        return jsonify({"ok": False, "error": "MCP client not ready yet"}), 503

    data = request.get_json(force=True) or {}
    text = (data.get("sql") or "").strip()
    if not text:
        return jsonify({"ok": False, "error": "Missing 'sql' in JSON body"}), 400

    async def _run():
        # Route a few demo commands to direct tool calls (no LLM)
        routed = _route_intent(text)
        if routed:
            tool_name, tool_args = routed
            return await _client.call_tool_direct(tool_name, tool_args)

        # If user input looks like SQL, call query_database directly (no LLM narration)
        if re.match(r"(?i)^\s*(select|with|update|insert|delete)\b", text):
            return await _client.call_tool_direct("query_database", {"sql_query": text})

        # Otherwise, let the LLM orchestrate tool usage
        return await _client.process_query(text)

    try:
        with _query_lock:
            fut = asyncio.run_coroutine_threadsafe(_run(), _loop)
            result = fut.result(timeout=120)
        return jsonify({"ok": True, "result": result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/ui")
def ui():
    return """
<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>CONNX MCP Demo</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 24px; }
      textarea { width: 900px; max-width: 95vw; }
      button { padding: 8px 12px; margin-right: 8px; margin-bottom: 8px; }
      #out { margin-top: 12px; }
      .note { margin: 10px 0; padding: 8px 10px; background: #fff7d6; border: 1px solid #f0d36b; }
      table { border-collapse: collapse; margin-top: 10px; width: min(1100px, 95vw); }
      th, td { border: 1px solid #ddd; padding: 6px 8px; vertical-align: top; }
      th { background: #f4f4f4; position: sticky; top: 0; }
      pre { background: #f4f4f4; padding: 12px; white-space: pre-wrap; }
      code { background: #f4f4f4; padding: 2px 4px; }
      .muted { color: #666; font-size: 0.95em; }
      input[type="text"] { padding: 8px; width: 420px; max-width: 90vw; }
    </style>
  </head>
  <body>
    <h1>CONNX MCP Demo</h1>

    <p>
      Try:
      <code>list tables</code>,
      <code>list tables like CUSTOMER</code>,
      <code>describe table daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM</code>,
      <code>which customers have CUSTOMERSTATE = "VA"</code>,
      or paste a SQL query.
    </p>

    <textarea id="sql" rows="6">list tables</textarea><br><br>

    <button onclick="setCmd('list tables')">List Tables</button>
    <button onclick="setCmd('list tables like CUSTOMER')">List Tables Like CUSTOMER</button>
    <button onclick="setCmd('describe table daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM')">Describe Customers</button>
    <button onclick="setCmd('which customers have CUSTOMERSTATE = &quot;VA&quot;')">Customers in VA</button>
    <button onclick="run()">Run</button>

    <div id="out"></div>

    <script>
  function setCmd(v) {
    document.getElementById("sql").value = v;
  }

  function escapeHtml(s) {
    return String(s)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  // ---------- SELECT renderer ----------
  function renderSelect(payload) {
    const cols = payload.columns || [];
    const rows = payload.rows || [];

    let html = "";
    html += "<div><b>Rows returned:</b> " + rows.length + "</div>";

    if (payload.truncated) {
      html += "<div class='note'><b>Truncated:</b> showing up to " + payload.max_rows +
              " rows. Add a WHERE clause to narrow results.</div>";
    }

    html += "<div style='overflow:auto; max-height: 65vh; border: 1px solid #eee; padding: 6px;'>";
    html += "<table><thead><tr>" +
            cols.map(c => "<th>" + escapeHtml(c) + "</th>").join("") +
            "</tr></thead><tbody>";

    for (const r of rows) {
      html += "<tr>" + r.map(v => "<td>" + escapeHtml(v ?? "") + "</td>").join("") + "</tr>";
    }

    html += "</tbody></table></div>";
    return html;
  }

  // ---------- TABLE LIST renderer + filter ----------
  function renderTablesList(payload) {
    const rows = payload.tables || [];
    const count = payload.count ?? rows.length;
    console.log("FILTERED renderTablesList LOADED");
    
    // store for filtering
    window.__tablesPayload = { rows };

    let html = "<div><b>Tables:</b> " + count + "</div>";
    html += "<div class='muted'>Filter by Catalog, Schema, Table, or Remarks</div>";

    html += "<div style='margin:10px 0;'>";
    html += "  <input id='tblFilter' type='text' " +
            "placeholder='Type to filter (e.g., VSAM, dbo, CUSTOMERS)' " +
            "oninput='applyTablesFilter()' />";
    html += "  <span class='muted' style='margin-left:10px;' id='tblFilterCount'></span>";
    html += "</div>";

    html += "<div style='overflow:auto; max-height: 65vh; border: 1px solid #eee; padding: 6px;'>";
    html += "  <table>";
    html += "    <thead><tr><th>Catalog</th><th>Schema</th><th>Table</th><th>Remarks</th></tr></thead>";
    html += "    <tbody id='tblBody'></tbody>";
    html += "  </table>";
    html += "</div>";

    // defer initial fill until DOM is updated
    setTimeout(() => applyTablesFilter(), 0);

    return html;
  }

  function applyTablesFilter() {
    const state = window.__tablesPayload;
    if (!state) return;

    const input = document.getElementById("tblFilter");
    const q = (input?.value || "").trim().toLowerCase();

    const body = document.getElementById("tblBody");
    const countEl = document.getElementById("tblFilterCount");
    if (!body) return;

    const filtered = state.rows.filter(t => {
      const cat = (t.table_cat ?? "").toString().toLowerCase();
      const sch = (t.table_schem ?? "").toString().toLowerCase();
      const name = (t.table_name ?? "").toString().toLowerCase();
      const rem = (t.remarks ?? "").toString().toLowerCase();
      return !q || cat.includes(q) || sch.includes(q) || name.includes(q) || rem.includes(q);
    });

    body.innerHTML = filtered.map(t => (
      "<tr>" +
        "<td>" + escapeHtml(t.table_cat ?? "") + "</td>" +
        "<td>" + escapeHtml(t.table_schem ?? "") + "</td>" +
        "<td><code>" + escapeHtml(t.table_name ?? "") + "</code></td>" +
        "<td>" + escapeHtml(t.remarks ?? "") + "</td>" +
      "</tr>"
    )).join("");

    if (countEl) {
      countEl.textContent = q
        ? `Showing ${filtered.length} of ${state.rows.length}`
        : `Showing ${state.rows.length}`;
    }
  }

  // ---------- DESCRIBE renderer ----------
  function renderDescribe(payload) {
    const cols = payload.columns || [];
    const count = payload.count ?? cols.length;

    let html = "<div><b>Describe:</b> <code>" + escapeHtml(payload.qualified_table ?? "") + "</code></div>";
    html += "<div><b>Columns:</b> " + count + "</div>";

    html += "<div style='overflow:auto; max-height: 65vh; border: 1px solid #eee; padding: 6px;'>";
    html += "<table><thead><tr>" +
            "<th>Column</th><th>Type</th><th>Size</th><th>Scale</th><th>Nullable</th><th>Remarks</th>" +
            "</tr></thead><tbody>";

    for (const c of cols) {
      html += "<tr>" +
        "<td><code>" + escapeHtml(c.column_name ?? "") + "</code></td>" +
        "<td>" + escapeHtml(c.type_name ?? "") + "</td>" +
        "<td>" + escapeHtml(c.column_size ?? "") + "</td>" +
        "<td>" + escapeHtml(c.decimal_digits ?? "") + "</td>" +
        "<td>" + escapeHtml(c.nullable ?? "") + "</td>" +
        "<td>" + escapeHtml(c.remarks ?? "") + "</td>" +
        "</tr>";
    }

    html += "</tbody></table></div>";
    return html;
  }

  // ---------- RUN ----------
  async function run() {
    const sql = document.getElementById("sql").value;
    const out = document.getElementById("out");
    out.innerHTML = "<pre>Running...</pre>";

    try {
      const r = await fetch("/query", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({sql})
      });
      const j = await r.json();
      const resultText = (j.result || j.error || "").toString();

      try {
        const payload = JSON.parse(resultText);

        if (payload && payload.ok && payload.type === "select") {
          out.innerHTML = renderSelect(payload);
          return;
        }
        if (payload && payload.ok && payload.type === "tables") {
          out.innerHTML = renderTablesList(payload);
          return;
        }
        if (payload && payload.ok && payload.type === "describe") {
          out.innerHTML = renderDescribe(payload);
          return;
        }
        if (payload && payload.ok && payload.type === "non_select") {
          out.innerHTML = "<pre>OK. Rows affected: " + escapeHtml(payload.rows_affected ?? "") + "</pre>";
          return;
        }
        if (payload && payload.ok === false) {
          out.innerHTML = "<pre>Error: " + escapeHtml(payload.error || "Unknown error") + "</pre>";
          return;
        }
      } catch (e) {
        // Not JSON; fall through
      }

      out.innerHTML = "<pre>" + escapeHtml(resultText || JSON.stringify(j, null, 2)) + "</pre>";
    } catch (e) {
      out.innerHTML = "<pre>Request failed: " + escapeHtml(e) + "</pre>";
    }
  }
</script>
  </body>
</html>
    """
    resp = make_response(html)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

if __name__ == "__main__":
    # IMPORTANT: disable reloader for persistent background threads
    app.run(host="127.0.0.1", port=4999, debug=True, use_reloader=False)