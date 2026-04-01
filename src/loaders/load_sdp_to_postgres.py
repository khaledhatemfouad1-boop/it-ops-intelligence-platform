"""
SDP Tickets – Kafka Consumer → Postgres Loader
Consumes messages from: sdp.tickets.raw
Upserts into Postgres table: sdp_tickets

Run:
    python loader/load_sdp_to_postgres.py
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("sdp_loader")

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER",   "localhost:9092")
KAFKA_TOPIC    = "sdp.tickets.raw"
KAFKA_GROUP    = "sdp-postgres-loader"

PG_DSN = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",       "itopsdb"),
    "user":     os.getenv("PG_USER",     "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}

UPSERT_SQL = """
INSERT INTO sdp_tickets (
    ticket_id, subject, status, priority, urgency, impact,
    category, subcategory, "group", technician, requester,
    site, department, mode,
    created_time, due_by_time, resolved_time, closed_time,
    first_response_time, is_overdue, is_first_response_overdue, sla_violated,
    source_raw, ingested_at
) VALUES (
    %(ticket_id)s, %(subject)s, %(status)s, %(priority)s, %(urgency)s, %(impact)s,
    %(category)s, %(subcategory)s, %(group)s, %(technician)s, %(requester)s,
    %(site)s, %(department)s, %(mode)s,
    %(created_time)s, %(due_by_time)s, %(resolved_time)s, %(closed_time)s,
    %(first_response_time)s, %(is_overdue)s, %(is_first_response_overdue)s, %(sla_violated)s,
    %(source_raw)s, %(ingested_at)s
)
ON CONFLICT (ticket_id) DO UPDATE SET
    subject                    = EXCLUDED.subject,
    status                     = EXCLUDED.status,
    priority                   = EXCLUDED.priority,
    urgency                    = EXCLUDED.urgency,
    impact                     = EXCLUDED.impact,
    category                   = EXCLUDED.category,
    subcategory                = EXCLUDED.subcategory,
    "group"                    = EXCLUDED."group",
    technician                 = EXCLUDED.technician,
    requester                  = EXCLUDED.requester,
    site                       = EXCLUDED.site,
    department                 = EXCLUDED.department,
    mode                       = EXCLUDED.mode,
    due_by_time                = EXCLUDED.due_by_time,
    resolved_time              = EXCLUDED.resolved_time,
    closed_time                = EXCLUDED.closed_time,
    first_response_time        = EXCLUDED.first_response_time,
    is_overdue                 = EXCLUDED.is_overdue,
    is_first_response_overdue  = EXCLUDED.is_first_response_overdue,
    sla_violated               = EXCLUDED.sla_violated,
    source_raw                 = EXCLUDED.source_raw,
    ingested_at                = EXCLUDED.ingested_at;
"""


def connect_pg():
    conn = psycopg2.connect(**PG_DSN)
    conn.autocommit = False
    return conn


def run():
    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BROKER,
        "group.id":           KAFKA_GROUP,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_TOPIC])
    log.info("Subscribed to %s", KAFKA_TOPIC)

    conn   = connect_pg()
    cursor = conn.cursor()
    batch  = []
    BATCH_SIZE = 200

    def flush_batch():
        if not batch:
            return
        psycopg2.extras.execute_batch(cursor, UPSERT_SQL, batch)
        conn.commit()
        log.info("Upserted %d tickets into Postgres", len(batch))
        batch.clear()

    try:
        while True:
            msg = consumer.poll(timeout=5.0)

            if msg is None:
                flush_batch()   # flush on idle
                continue

            if msg.error():
                raise KafkaException(msg.error())

            record = json.loads(msg.value().decode("utf-8"))

            # Map flat dict → DB row
            row = {
                "ticket_id":                 record.get("ticket_id"),
                "subject":                   record.get("subject"),
                "status":                    record.get("status"),
                "priority":                  record.get("priority"),
                "urgency":                   record.get("urgency"),
                "impact":                    record.get("impact"),
                "category":                  record.get("category"),
                "subcategory":               record.get("subcategory"),
                "group":                     record.get("group"),
                "technician":                record.get("technician"),
                "requester":                 record.get("requester"),
                "site":                      record.get("site"),
                "department":                record.get("department"),
                "mode":                      record.get("mode"),
                "created_time":              record.get("created_time"),
                "due_by_time":               record.get("due_by_time"),
                "resolved_time":             record.get("resolved_time"),
                "closed_time":               record.get("closed_time"),
                "first_response_time":       record.get("first_response_time"),
                "is_overdue":                record.get("is_overdue", False),
                "is_first_response_overdue": record.get("is_first_response_overdue", False),
                "sla_violated":              record.get("sla_violated", False),
                "source_raw":                record.get("source_raw"),
                "ingested_at":               record.get("_ingested_at", datetime.utcnow().isoformat()),
            }
            batch.append(row)

            if len(batch) >= BATCH_SIZE:
                flush_batch()
                consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        log.info("Interrupted — flushing final batch…")
        flush_batch()
        consumer.commit(asynchronous=False)
    finally:
        cursor.close()
        conn.close()
        consumer.close()
        log.info("✅  Loader stopped cleanly")


if __name__ == "__main__":
    run()
