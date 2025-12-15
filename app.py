# in app.py
import os
import openai  # or Azure OpenAI if you prefer
from dotenv import load_dotenv


load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")  # or use Azure client

SQL_SYSTEM_PROMPT = """
You are a SQL generator for a CONNX data source.
- Use ANSI SQL-92 syntax.
- Use only these tables: CUSTOMERS_DB2, ORDERS_DB2, PRODUCTS_DB2.
- Never use SELECT *; always list specific columns.
- If you are not sure which columns exist, ask the user or say you are unsure.
- Do not perform INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, or MERGE.
Return ONLY the SQL query, nothing else.
"""

@app.route("/ask", methods=["POST"])
def ask():
    payload = request.get_json(force=True)
    question = payload.get("question", "").strip()
    max_rows = payload.get("max_rows", 200)

    if not question:
        return jsonify({"ok": False, "error": "No question provided."}), 400

    # 1. Use LLM to generate SQL
    messages = [
        {"role": "system", "content": SQL_SYSTEM_PROMPT},
        {"role": "user", "content": f"User question: {question}"}
    ]

    completion = openai.ChatCompletion.create(
        model="gpt-4o-mini",  # or whichever
        messages=messages,
        temperature=0.0,
    )

    sql = completion.choices[0].message["content"].strip()

    try:
        # 2. Run SQL safely via CONNX
        result = core.safe_query(sql, max_rows=max_rows)
    except ConnxSafetyError as se:
        return jsonify({
            "ok": False,
            "error": f"Safety error: {se}",
            "sql": sql
        }), 400
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": f"Execution error: {e}",
            "sql": sql
        }), 500

    # 3. Optionally add a brief explanation using the result
    explanation = f"Executed SQL:\n{sql}\nReturned {result['row_count']} rows."

    return jsonify({
        "ok": True,
        "sql": sql,
        "data": result,
        "explanation": explanation
    })