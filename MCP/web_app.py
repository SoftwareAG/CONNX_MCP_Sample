# web_app.py
import atexit
import asyncio
import threading
import re
import os
import time
from flask import Flask, request, jsonify, make_response

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

# Cache tools list for optional UI display
_tools_cache = []
_tools_cache_lock = threading.Lock()


def _start_background_loop():
    """Start an asyncio event loop in a background thread."""
    global _loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _loop = loop
    _loop_ready.set()
    loop.run_forever()


async def _init_mcp_client():
    """Create and connect the MCP client once."""
    global _client, _tools_cache

    _client = MCPClient()

    # Use absolute path so running from another working dir still works
    server_path = os.path.join(os.path.dirname(__file__), "connx_db_server.py")
    print(f"[WEB] MCP server path: {server_path}")

    await _client.connect_to_server(server_path)

    # Cache tools for UI and trace
    resp = await _client.session.list_tools()
    with _tools_cache_lock:
        _tools_cache = [t.name for t in resp.tools]
    print("[WEB] MCP server tools:", _tools_cache)


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
                fut.result(timeout=45)
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
    t = (user_text or "").strip()

    # ---------------------------
    # CONNX deterministic routes
    # ---------------------------

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
        # ANSI SQL-92 note: no TOP / LIMIT here; query_database tool controls max_rows server-side.
        return (
            "query_database",
            {"sql_query": f"SELECT * FROM daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM WHERE CUSTOMERSTATE = '{st}'"},
        )

    # ---------------------------
    # Kafka deterministic routes
    # ---------------------------

    # "kafka topics" / "kafka list topics"
    if re.match(r"(?i)^\s*kafka\s+(topics|list\s+topics)\s*$", t):
        return ("kafka_list_topics", {})

    # "kafka tail <topic> [n 10] [timeout 1500]"
    m = re.match(r"(?i)^\s*kafka\s+tail\s+([^\s]+)(?:\s+n\s+(\d+))?(?:\s+timeout\s+(\d+))?\s*$", t)
    if m:
        topic = m.group(1)
        n = int(m.group(2) or "10")
        timeout_ms = int(m.group(3) or "1500")
        return ("kafka_tail", {"topic": topic, "n": n, "timeout_ms": timeout_ms})

    # "kafka publish <topic> <value...> [key <key...>]"
    # value is everything after topic, optionally stopping before " key "
    m = re.match(r"(?is)^\s*kafka\s+publish\s+([^\s]+)\s+(.+?)\s*(?:\s+key\s+(.+))?\s*$", t)
    if m:
        topic = m.group(1).strip()
        value = (m.group(2) or "").strip()
        key = (m.group(3) or "").strip() if m.group(3) else None
        args = {"topic": topic, "value": value}
        if key:
            args["key"] = key
        return ("kafka_publish", args)

    return None


@app.get("/")
def home():
    if _client_error:
        return f"MCP init failed: {_client_error}", 500
    if not _client_ready.is_set():
        return "Starting MCP client...", 503
    return "GUI is up. Open /ui for the demo page.", 200


@app.get("/health")
def health():
    """Quick readiness probe for scripts/curl."""
    if _client_error:
        return jsonify({"ok": False, "ready": False, "error": _client_error}), 500
    if not _client_ready.is_set():
        return jsonify({"ok": True, "ready": False}), 503
    with _tools_cache_lock:
        tools = list(_tools_cache)
    return jsonify({"ok": True, "ready": True, "tools": tools}), 200


@app.post("/query")
def query():
    if _client_error:
        return jsonify({"ok": False, "error": _client_error}), 500
    if not _client_ready.is_set():
        return jsonify({"ok": False, "error": "MCP client not ready"}), 503

    data = request.get_json(force=True) or {}
    text = (data.get("sql") or "").strip()
    show_raw = bool(data.get("show_raw", False))
    if not text:
        return jsonify({"ok": False, "error": "Missing sql"}), 400

    trace = []
    t_total0 = time.perf_counter()

    async def _run():
        # 1) List tools (per request; makes the “MCP behind the scenes” obvious)
        t0 = time.perf_counter()
        tools_resp = await _client.session.list_tools()
        t1 = time.perf_counter()
        tool_names = [t.name for t in tools_resp.tools]

        trace.append({
            "stage": "list_tools",
            "ms": int((t1 - t0) * 1000),
            "tools": tool_names,
        })

        # 2) Deterministic routing -> direct tool call
        routed = _route_intent(text)
        if routed:
            tool_name, tool_args = routed
            trace.append({
                "stage": "tool_call",
                "tool": tool_name,
                "args": tool_args,
            })

            t2 = time.perf_counter()
            result = await _client.call_tool_direct(tool_name, tool_args)
            t3 = time.perf_counter()

            trace.append({
                "stage": "tool_result",
                "tool": tool_name,
                "ms": int((t3 - t2) * 1000),
                "preview": (result or "")[:600],
            })
            return result

        # 3) If input looks like SQL, call query_database directly
        if re.match(r"(?i)^\s*(select|with|update|insert|delete)\b", text):
            tool_name = "query_database"
            tool_args = {"sql_query": text}
            trace.append({
                "stage": "tool_call",
                "tool": tool_name,
                "args": tool_args,
            })

            t2 = time.perf_counter()
            result = await _client.call_tool_direct(tool_name, tool_args)
            t3 = time.perf_counter()

            trace.append({
                "stage": "tool_result",
                "tool": tool_name,
                "ms": int((t3 - t2) * 1000),
                "preview": (result or "")[:600],
            })
            return result

        # 4) LLM path (still MCP-driven), but we record this stage explicitly
        trace.append({"stage": "llm_orchestration"})
        t2 = time.perf_counter()
        result = await _client.process_query(text)
        t3 = time.perf_counter()
        trace.append({
            "stage": "llm_done",
            "ms": int((t3 - t2) * 1000),
            "preview": (result or "")[:600],
        })
        return result

    try:
        with _query_lock:
            fut = asyncio.run_coroutine_threadsafe(_run(), _loop)
            result = fut.result(timeout=120)

        t_total1 = time.perf_counter()
        trace.append({"stage": "total_time", "ms": int((t_total1 - t_total0) * 1000)})

        raw_rpc = None
        if show_raw and hasattr(_client, "tape") and _client.tape is not None:
            # snapshot may return list of dicts
            try:
                raw_rpc = _client.tape.snapshot()
            except Exception:
                raw_rpc = None

        return jsonify({
            "ok": True,
            "result": result,
            "trace": trace,
            "raw_rpc": raw_rpc,
        })
    except Exception as e:
        t_total1 = time.perf_counter()
        trace.append({"stage": "error", "error": str(e)})
        trace.append({"stage": "total_time", "ms": int((t_total1 - t_total0) * 1000)})
        return jsonify({"ok": False, "error": str(e), "trace": trace}), 500


@app.get("/ui")
def ui():
    # Prevent browser caching during iterative demo edits
    html = r"""<!doctype html>
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
      .grid { display: grid; grid-template-columns: 1fr; gap: 16px; margin-top: 14px; }
      .card { border: 1px solid #eee; padding: 12px; border-radius: 8px; }
      .card h3 { margin: 0 0 8px 0; }
      .kv { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; font-size: 12px; }
      .badge { display:inline-block; padding:2px 6px; border:1px solid #ddd; border-radius: 999px; font-size: 12px; margin-right: 6px; }
      .tracepre { background:#111; color:#0f0; padding: 10px; border-radius: 6px; overflow:auto; max-height: 55vh; }
      .row { display:flex; align-items:center; gap:12px; flex-wrap:wrap; }
      .pill { display:inline-block; padding: 4px 10px; border:1px solid #ddd; border-radius: 999px; font-size: 12px; margin: 2px 6px 2px 0; background: #fafafa; }
      .split { display:grid; grid-template-columns: 1fr; gap: 12px; }
      .small { font-size: 12px; }
      
      .trace-options {
          margin-top: 10px;
        }
        .trace-options label {
          display: inline-block;
          margin-right: 20px;
        }
      
    </style>
  </head>
  <body>
    <h1>CONNX MCP Demo</h1>

    <p>
      CONNX examples:
      <code>list tables</code>,
      <code>list tables like CUSTOMER</code>,
      <code>describe table daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM</code>,
      <code>which customers have CUSTOMERSTATE = "VA"</code>,
      or paste an ANSI SQL-92 query.
    </p>

    <p>
      Kafka examples:
      <code>kafka topics</code>,
      <code>kafka tail mytopic n 10</code>,
      <code>kafka publish mytopic {"hello":"world"}</code>,
      <code>kafka publish mytopic hello key mykey</code>
    </p>

    <textarea id="sql" rows="6">list tables</textarea><br><br>

    <!-- CONNX quick buttons -->
    <button onclick="setCmd('list tables')">List Tables</button>
    <button onclick="setCmd('list tables like CUSTOMER')">List Tables Like CUSTOMER</button>
    <button onclick="setCmd('describe table daea_Mainframe_VSAM.dbo.CUSTOMERS_VSAM')">Describe Customers</button>
    <button onclick="setCmd('which customers have CUSTOMERSTATE = &quot;VA&quot;')">Customers in VA</button>

    <!-- Kafka quick buttons -->
    <button onclick="setCmd('kafka topics')">Kafka Topics</button>
    <button onclick="setCmd('kafka tail mytopic n 10')">Kafka Tail (edit topic)</button>
    <button onclick="setCmd('kafka publish mytopic {&quot;hello&quot;:&quot;world&quot;}')">Kafka Publish JSON (edit topic)</button>

    <button onclick="run()">Run</button>
    
    
    <br>
    
    <label style="margin-left:12px;">
      <input type="checkbox" id="showTrace" checked />
      Show MCP Trace
    </label>
    
    <label style="margin-left:12px;">
      <input type="checkbox" id="showRaw" />
      Show Raw JSON-RPC
    </label>

    <div class="grid">
      <div class="card">
        <h3>Results</h3>
        <div id="out"></div>
      </div>

      <div class="card">
        <div class="row">
          <h3 style="margin:0;">🔍 MCP Trace</h3>
          <span class="muted">Tool discovery + calls + timing</span>
        </div>
        <div id="trace" class="tracepre"></div>
      </div>

      <div class="card">
        <div class="row">
          <h3 style="margin:0;">🧾 Raw MCP JSON-RPC</h3>
          <span class="muted">Actual JSON lines over stdio (MCP protocol)</span>
        </div>
        <div id="rawrpc" class="tracepre"></div>
      </div>
    </div>

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

      // ---------- Kafka renderers ----------
      function renderKafkaTopics(payload) {
        const topics = payload.topics || [];
        let html = "";
        html += "<div><b>Kafka bootstrap:</b> <code>" + escapeHtml(payload.bootstrap ?? "") + "</code></div>";
        html += "<div><b>Topics:</b> " + topics.length + "</div>";
        html += "<div style='margin-top:8px;'>";
        for (const t of topics) {
          html += "<span class='pill'>" + escapeHtml(t) + "</span>";
        }
        html += "</div>";
        return html;
      }

      function renderKafkaTail(payload) {
        const msgs = payload.messages || [];
        let html = "<div><b>Topic:</b> <code>" + escapeHtml(payload.topic ?? "") + "</code></div>";
        html += "<div><b>Messages:</b> " + msgs.length + "</div>";

        html += "<div style='overflow:auto; max-height: 65vh; border: 1px solid #eee; padding: 6px;'>";
        html += "<table><thead><tr>" +
                "<th>Partition</th><th>Offset</th><th>Timestamp</th><th>Key</th><th>Value</th>" +
                "</tr></thead><tbody>";

        for (const m of msgs) {
          const ts = m.timestamp_ms ? new Date(m.timestamp_ms).toISOString() : "";
          html += "<tr>" +
            "<td>" + escapeHtml(m.partition ?? "") + "</td>" +
            "<td>" + escapeHtml(m.offset ?? "") + "</td>" +
            "<td class='small'>" + escapeHtml(ts) + "</td>" +
            "<td>" + escapeHtml(m.key ?? "") + "</td>" +
            "<td><pre class='kv' style='margin:0;'>" + escapeHtml(m.value ?? "") + "</pre></td>" +
            "</tr>";
        }

        html += "</tbody></table></div>";
        return html;
      }

      function renderKafkaPublish(payload) {
        let html = "<div><b>Published</b> to <code>" + escapeHtml(payload.topic ?? "") + "</code></div>";
        html += "<div class='kv'>partition=" + escapeHtml(payload.partition ?? "") +
                " offset=" + escapeHtml(payload.offset ?? "") + "</div>";
        return html;
      }

      // ---------- TRACE renderer ----------
      function renderTrace(trace) {
        if (!Array.isArray(trace) || trace.length === 0) return "(no MCP trace)";

        let lines = [];
        for (const item of trace) {
          const stage = item.stage || "stage";
          if (stage === "list_tools") {
            lines.push(`[list_tools] ${item.ms ?? ""}ms tools=${JSON.stringify(item.tools || [])}`);
          } else if (stage === "tool_call") {
            lines.push(`[tool_call] tool=${item.tool || ""} args=${JSON.stringify(item.args || {})}`);
          } else if (stage === "tool_result") {
            lines.push(`[tool_result] tool=${item.tool || ""} ${item.ms ?? ""}ms preview=${(item.preview || "").replaceAll("\n"," ")}`);
          } else if (stage === "llm_orchestration") {
            lines.push(`[llm_orchestration]`);
          } else if (stage === "llm_done") {
            lines.push(`[llm_done] ${item.ms ?? ""}ms preview=${(item.preview || "").replaceAll("\n"," ")}`);
          } else if (stage === "total_time") {
            lines.push(`[total_time] ${item.ms ?? ""}ms`);
          } else if (stage === "error") {
            lines.push(`[error] ${item.error || ""}`);
          } else {
            lines.push(`[${stage}] ${JSON.stringify(item)}`);
          }
        }
        return lines.join("\n");
      }

      // ---------- Raw RPC renderer ----------
      function renderRawRpc(raw) {
        if (!Array.isArray(raw) || raw.length === 0) return "(no RPC captured)";
        // Expect items like: {ts_ms, direction, text}
        return raw.map(x => {
          const ts = x.ts_ms ? new Date(x.ts_ms).toISOString() : "";
          const dir = x.direction || "";
          const txt = x.text || "";
          return `${ts} ${dir} ${txt}`;
        }).join("\n");
      }

      // ---------- RUN ----------
      async function run() {
        const sql = document.getElementById("sql").value;
        const out = document.getElementById("out");
        const traceEl = document.getElementById("trace");
        const rawEl = document.getElementById("rawrpc");

        const showTrace = document.getElementById("showTrace").checked;
        const showRaw = document.getElementById("showRaw").checked;

        out.innerHTML = "<pre>Running...</pre>";
        traceEl.textContent = showTrace ? "Waiting for trace..." : "(trace hidden)";
        rawEl.textContent = showRaw ? "Waiting for raw RPC..." : "(raw hidden)";

        try {
          const r = await fetch("/query", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({sql, show_raw: showRaw})
          });

          const j = await r.json();
          const resultText = (j.result || j.error || "").toString();

          // Trace
          traceEl.textContent = showTrace ? renderTrace(j.trace || []) : "(trace hidden)";

          // Raw RPC
          rawEl.textContent = showRaw ? renderRawRpc(j.raw_rpc || []) : "(raw hidden)";

          // If server says not OK
          if (j.ok === false) {
            out.innerHTML = "<pre>Error: " + escapeHtml(j.error || "Unknown error") + "</pre>";
            return;
          }

          // Try to parse tool output JSON and render nicely
          try {
            const payload = JSON.parse(resultText);

            // CONNX tool payloads
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

            // Kafka tool payloads
            if (payload && payload.ok && payload.type === "kafka_topics") {
              out.innerHTML = renderKafkaTopics(payload);
              return;
            }
            if (payload && payload.ok && payload.type === "kafka_tail") {
              out.innerHTML = renderKafkaTail(payload);
              return;
            }
            if (payload && payload.ok && payload.type === "kafka_publish") {
              out.innerHTML = renderKafkaPublish(payload);
              return;
            }

            if (payload && payload.ok === false) {
              out.innerHTML = "<pre>Error: " + escapeHtml(payload.error || "Unknown error") + "</pre>";
              return;
            }
          } catch (e) {
            // Not JSON; fall through
          }

          // Plain text fallback
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