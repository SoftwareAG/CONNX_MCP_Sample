import json
import logging
import sys
import pyodbc
from mcp.server.fastmcp import FastMCP
import re
from dotenv import load_dotenv
import os
from typing import Any, Dict, List, Optional
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError
import time

logging.basicConfig(level=logging.INFO, stream=sys.stderr)
log = logging.getLogger("connx_mcp_server")

mcp = FastMCP("connx_db")


load_dotenv()

# ----------------------------
# CONNX / ODBC configuration
# ----------------------------
CONNX_DSN = os.getenv("CONNX_DSN", "Share_2025")
CONNX_UID = os.getenv("CONNX_UID")
CONNX_PWD = os.getenv("CONNX_PWD")
CONNX_CONN_STR = f"DSN={CONNX_DSN};UID={CONNX_UID};PWD={CONNX_PWD}"

# ----------------------------
# Kafka configuration (env)
# ----------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "connx-mcp")

_producer: Optional[KafkaProducer] = None


def _kafka_common_kwargs() -> Dict[str, Any]:
    """
    Build kafka-python kwargs based on env vars.
    Keep it minimal and compatible with common Kafka + Confluent Cloud setups.
    """
    kw: Dict[str, Any] = {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "client_id": KAFKA_CLIENT_ID,
    }

    # if KAFKA_SECURITY_PROTOCOL:
    #     kw["security_protocol"] = KAFKA_SECURITY_PROTOCOL
    #
    # if KAFKA_SASL_MECHANISM:
    #     kw["sasl_mechanism"] = KAFKA_SASL_MECHANISM
    # if KAFKA_SASL_USERNAME is not None:
    #     kw["sasl_plain_username"] = KAFKA_SASL_USERNAME
    # if KAFKA_SASL_PASSWORD is not None:
    #     kw["sasl_plain_password"] = KAFKA_SASL_PASSWORD

    return kw

def _get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        kw = _kafka_common_kwargs()
        # Produce bytes; encode at call time.
        _producer = KafkaProducer(**kw)
        log.info("KafkaProducer created (bootstrap=%s)", KAFKA_BOOTSTRAP_SERVERS)
    return _producer


def _get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        kw = _kafka_common_kwargs()
        # Produce bytes; encode at call time.
        _producer = KafkaProducer(**kw)
        log.info("KafkaProducer created (bootstrap=%s)", KAFKA_BOOTSTRAP_SERVERS)
    return _producer

# ----------------------------
# Kafka tools
# ----------------------------

@mcp.tool()
async def kafka_list_topics() -> str:
    """
    List Kafka topics.
    Returns JSON: {ok, type:'kafka_topics', count, topics:[...]}
    """
    try:
        kw = _kafka_common_kwargs()
        admin = KafkaAdminClient(**kw)
        try:
            topics = sorted(list(admin.list_topics()))
        finally:
            admin.close()

        return json.dumps({
            "ok": True,
            "type": "kafka_topics",
            "bootstrap": KAFKA_BOOTSTRAP_SERVERS,
            "count": len(topics),
            "topics": topics
        })
    except Exception as e:
        log.exception("kafka_list_topics error")
        return json.dumps({"ok": False, "error": str(e)})

@mcp.tool()
async def kafka_publish(topic: str, value: str, key: str = None, headers: dict = None) -> str:
    """
    Publish a message to Kafka.
    - value: string payload
    - key: optional string key
    - headers: optional dict of string->string (converted to Kafka headers)

    Returns JSON: {ok, type:'kafka_publish', topic, partition, offset}
    """
    try:
        producer = _get_producer()

        k = key.encode("utf-8") if key is not None else None
        v = value.encode("utf-8") if value is not None else b""

        hdrs = None
        if headers:
            # kafka-python expects List[Tuple[str, bytes]]
            hdrs = []
            for hk, hv in headers.items():
                if hv is None:
                    hdrs.append((str(hk), b""))
                else:
                    hdrs.append((str(hk), str(hv).encode("utf-8")))

        fut = producer.send(topic, key=k, value=v, headers=hdrs)
        record_md = fut.get(timeout=10)  # block until ack

        return json.dumps({
            "ok": True,
            "type": "kafka_publish",
            "topic": topic,
            "partition": record_md.partition,
            "offset": record_md.offset,
            "timestamp_ms": int(time.time() * 1000)
        })
    except KafkaError as e:
        log.exception("kafka_publish KafkaError")
        return json.dumps({"ok": False, "error": f"KafkaError: {e}"})
    except Exception as e:
        log.exception("kafka_publish error")
        return json.dumps({"ok": False, "error": str(e)})


@mcp.tool()
async def kafka_tail(topic: str, n: int = 10, timeout_ms: int = 1500) -> str:
    """
    Tail up to N most recent messages from a topic.
    No consumer group (no coordinator) — uses manual partition assignment.
    Returns JSON:
      {ok, type:'kafka_tail', topic, messages:[{partition, offset, timestamp, key, value}]}
    """
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        return json.dumps({"ok": False, "error": "KAFKA_BOOTSTRAP_SERVERS not set"})

    admin = None
    consumer = None
    try:
        # Discover partitions
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap,
            client_id="connx-mcp-admin",
        )

        topic_meta = admin.describe_topics([topic])
        if not topic_meta or "partitions" not in topic_meta[0]:
            return json.dumps({"ok": False, "error": f"Topic not found or no partitions: {topic}"})

        partitions = [p["partition"] for p in topic_meta[0]["partitions"]]
        tps = [TopicPartition(topic, p) for p in partitions]

        # Manual assignment consumer (no group_id => no coordinator traffic)
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap,
            client_id="connx-mcp-tail",
            group_id=None,
            enable_auto_commit=False,
            auto_offset_reset="latest",
            consumer_timeout_ms=0,  # we control timing via poll loop
            api_version_auto_timeout_ms=10000,
        )

        consumer.assign(tps)

        # Seek near end for each partition
        end_offsets = consumer.end_offsets(tps)
        for tp in tps:
            end = end_offsets.get(tp, 0) or 0
            # seek to max(end - n, 0). Note: this is per-partition; overall may exceed n.
            start = max(end - n, 0)
            consumer.seek(tp, start)

        # Poll until we have enough or timeout
        deadline = time.time() + (timeout_ms / 1000.0)
        out = []

        while time.time() < deadline and len(out) < n:
            records = consumer.poll(timeout_ms=200)  # short poll
            if not records:
                continue

            for tp, msgs in records.items():
                for msg in msgs:
                    out.append({
                        "partition": tp.partition,
                        "offset": msg.offset,
                        "timestamp": msg.timestamp,
                        "key": None if msg.key is None else msg.key.decode("utf-8", "replace") if isinstance(msg.key, (bytes, bytearray)) else str(msg.key),
                        "value": None if msg.value is None else msg.value.decode("utf-8", "replace") if isinstance(msg.value, (bytes, bytearray)) else str(msg.value),
                    })
                    if len(out) >= n:
                        break
                if len(out) >= n:
                    break

        # Sort “tail style”: newest last (or change if you prefer newest first)
        out.sort(key=lambda x: (x["partition"], x["offset"]))

        return json.dumps({
            "ok": True,
            "type": "kafka_tail",
            "topic": topic,
            "messages": out
        })

    except Exception as e:
        return json.dumps({"ok": False, "error": str(e)})

    finally:
        try:
            if consumer:
                consumer.close()
        except Exception:
            pass
        try:
            if admin:
                admin.close()
        except Exception:
            pass
@mcp.tool()
async def query_database(sql_query: str) -> str:
    """
    Execute an ANSI SQL-92 query via CONNX and return JSON.
    For SELECT: {ok, type:'select', columns:[...], rows:[[...]], truncated, max_rows}
    For non-SELECT: {ok, type:'non_select', rows_affected}
    """
    conn = None
    cursor = None
    bad = re.search(r"(?i)\b(top|limit|offset|fetch|qualify)\b", sql_query)
    if bad:
        return json.dumps({
            "ok": False,
            "error": f"Non-ANSI SQL-92 keyword detected: {bad.group(1)}. Please remove it."
        })

    try:
        conn = pyodbc.connect(CONNX_CONN_STR)
        cursor = conn.cursor()
        cursor.execute(sql_query)

        # Non-SELECT statements return no cursor.description
        if cursor.description is None:
            try:
                conn.commit()
            except Exception:
                pass
            return json.dumps({"ok": True, "type": "non_select", "rows_affected": cursor.rowcount})

        columns = [desc[0] for desc in cursor.description] if cursor.description else []

        max_rows = 200
        rows = cursor.fetchmany(max_rows)

        rows_out = []
        for r in rows:
            rows_out.append([None if v is None else str(v) for v in r])

        truncated = bool(cursor.fetchmany(1))

        payload = {
            "ok": True,
            "type": "select",
            "columns": columns,
            "rows": rows_out,
            "row_count_returned": len(rows_out),
            "truncated": truncated,
            "max_rows": max_rows,
        }
        return json.dumps(payload)

    except Exception as e:
        logging.exception("query_database error")
        return json.dumps({"ok": False, "error": str(e)})

    finally:
        try:
            if cursor is not None:
                cursor.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


@mcp.tool()
async def list_tables(schema: str = None, table_name_like: str = None) -> str:
    """
    List available tables using ODBC metadata.
    Returns JSON: {ok, type:'tables', count, tables:[{table_cat, table_schem, table_name, table_type, remarks}]}
    """
    conn = None
    cursor = None
    try:
        conn = pyodbc.connect(CONNX_CONN_STR)
        cursor = conn.cursor()

        rows = cursor.tables(tableType="TABLE").fetchall()

        out = []
        for r in rows:
            out.append({
                "table_cat": getattr(r, "table_cat", None),
                "table_schem": getattr(r, "table_schem", None),
                "table_name": getattr(r, "table_name", None),
                "table_type": getattr(r, "table_type", None),
                "remarks": getattr(r, "remarks", None),
            })

        if schema:
            s = schema.strip().lower()
            out = [t for t in out if (t.get("table_schem") or "").lower() == s]

        if table_name_like:
            pat = table_name_like.strip().lower()
            out = [t for t in out if pat in (t.get("table_name") or "").lower()]

        return json.dumps({"ok": True, "type": "tables", "count": len(out), "tables": out})

    except Exception as e:
        logging.exception("list_tables error")
        return json.dumps({"ok": False, "error": str(e)})

    finally:
        try:
            if cursor is not None:
                cursor.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


@mcp.tool()
async def describe_table(qualified_table: str) -> str:
    """
    Describe columns using ODBC metadata.
    qualified_table can be:
      schema.table  OR  catalog.schema.table  OR  just table
    Returns JSON: {ok, type:'describe', qualified_table, count, columns:[...]}
    """
    conn = None
    cursor = None
    try:
        conn = pyodbc.connect(CONNX_CONN_STR)
        cursor = conn.cursor()

        parts = [p.strip() for p in qualified_table.split(".") if p.strip()]
        catalog = schema = table = None

        if len(parts) == 1:
            table = parts[0]
        elif len(parts) == 2:
            schema, table = parts
        else:
            catalog, schema, table = parts[-3], parts[-2], parts[-1]

        cols = cursor.columns(table=table, schema=schema, catalog=catalog).fetchall()

        out = []
        for c in cols:
            out.append({
                "column_name": getattr(c, "column_name", None),
                "type_name": getattr(c, "type_name", None),
                "column_size": getattr(c, "column_size", None),
                "decimal_digits": getattr(c, "decimal_digits", None),
                "nullable": getattr(c, "nullable", None),
                "remarks": getattr(c, "remarks", None),
            })

        return json.dumps({
            "ok": True,
            "type": "describe",
            "qualified_table": qualified_table,
            "count": len(out),
            "columns": out
        })

    except Exception as e:
        logging.exception("describe_table error")
        return json.dumps({"ok": False, "error": str(e)})

    finally:
        try:
            if cursor is not None:
                cursor.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()