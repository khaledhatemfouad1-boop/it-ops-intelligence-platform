"""
Dynamics 365 – Kafka Consumer → Postgres Loader
Consumes:
  - dynamics.roles.raw
  - dynamics.userroles.raw
Upserts into:
  - dynamics_roles
  - dynamics_user_roles
"""

import os
import json
import logging
import argparse

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("dynamics_loader")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

PG_DSN = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "dbname": os.getenv("PG_DB", "itopsdb"),
    "user": os.getenv("PG_USER", "itops"),
    "password": os.getenv("PG_PASSWORD", "Sumerge@Itops2026!"),
}

TOPIC_ROLES = "dynamics.roles.raw"
TOPIC_USERROLES = "dynamics.userroles.raw"
TOPICS = [TOPIC_ROLES, TOPIC_USERROLES]

UPSERT_ROLES = """
INSERT INTO public.dynamics_roles (
    role_id,
    role_name,
    business_unit_id,
    raw_payload,
    ingested_at
) VALUES (
    %(role_id)s,
    %(role_name)s,
    %(business_unit_id)s,
    %(raw_payload)s,
    %(ingested_at)s
)
ON CONFLICT (role_id) DO UPDATE SET
    role_name        = EXCLUDED.role_name,
    business_unit_id = EXCLUDED.business_unit_id,
    raw_payload      = EXCLUDED.raw_payload,
    ingested_at      = EXCLUDED.ingested_at;
"""

UPSERT_USERROLES = """
INSERT INTO public.dynamics_user_roles (
    user_id,
    role_id,
    user_name,
    full_name,
    role_name,
    business_unit,
    assigned_on,
    ingested_at
) VALUES (
    %(user_id)s,
    %(role_id)s,
    %(user_name)s,
    %(full_name)s,
    %(role_name)s,
    %(business_unit)s,
    %(assigned_on)s,
    %(ingested_at)s
)
ON CONFLICT (user_id, role_id) DO UPDATE SET
    user_name     = EXCLUDED.user_name,
    full_name     = EXCLUDED.full_name,
    role_name     = EXCLUDED.role_name,
    business_unit = EXCLUDED.business_unit,
    assigned_on   = EXCLUDED.assigned_on,
    ingested_at   = EXCLUDED.ingested_at;
"""


def connect_pg():
    conn = psycopg2.connect(**PG_DSN)
    conn.autocommit = False
    return conn


def mk_consumer(group_id: str):
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def as_jsonb(value: dict):
    # psycopg2 will adapt Json(...) into JSON/JSONB.
    return psycopg2.extras.Json(value, dumps=lambda o: json.dumps(o, default=str))


def map_role(record: dict) -> dict:
    # Producer sends: role_id, name, business_unit (id), parent_role_id, ...
    return {
        "role_id": record.get("role_id"),
        "role_name": record.get("name"),
        "business_unit_id": record.get("business_unit"),
        "raw_payload": as_jsonb(record),
        # Producer field is "ingested_at" (ISO string). Keep as-is for TIMESTAMPTZ.
        "ingested_at": record.get("ingested_at"),
    }


def map_userrole(record: dict) -> dict:
    # Producer sends: user_id, role_id, ingested_at
    return {
        "user_id": record.get("user_id"),
        "role_id": record.get("role_id"),
        "user_name": record.get("user_name"),
        "full_name": record.get("full_name"),
        "role_name": record.get("role_name"),
        "business_unit": record.get("business_unit"),
        "assigned_on": record.get("assigned_on"),
        "ingested_at": record.get("ingested_at"),
    }


def run(topics: list[str], group_id: str, timeout: int, batch_size: int):
    consumer = mk_consumer(group_id=group_id)
    consumer.subscribe(topics)
    log.info("Subscribed to: %s", topics)

    conn = connect_pg()
    cur = conn.cursor()

    # Fail fast with a clear message if schema wasn't initialized.
    # Use information_schema to reliably detect tables in the public schema
    # (works even when to_regclass may return unexpected values due to search_path).
    cur.execute(
        "SELECT EXISTS ("
        "  SELECT 1 FROM information_schema.tables "
        "  WHERE table_schema = 'public' AND table_name = 'dynamics_roles'"
        "), EXISTS ("
        "  SELECT 1 FROM information_schema.tables "
        "  WHERE table_schema = 'public' AND table_name = 'dynamics_user_roles'"
        ")"
    )
    roles_exists, userroles_exists = cur.fetchone()
    if not roles_exists or not userroles_exists:
        raise RuntimeError(
            "Missing required tables. Expected dynamics_roles and dynamics_user_roles "
            "in the public schema. Run DB init (docker compose up -d, then ensure "
            "sql/00_init.sql has been applied) and re-try."
        )

    batches: dict[str, list[dict]] = {t: [] for t in topics}
    counts: dict[str, int] = {t: 0 for t in topics}
    idle = 0
    saw_message = False

    def safe_commit():
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as e:
            # Can happen if no offsets are stored locally (e.g., no messages consumed,
            # or commit called after revoke). Safe to ignore for shutdown/flush paths.
            if "_NO_OFFSET" in str(e):
                return
            raise

    def flush(topic: str):
        nonlocal cur, conn
        batch = batches[topic]
        if not batch:
            return

        try:
            if topic == TOPIC_ROLES:
                psycopg2.extras.execute_batch(cur, UPSERT_ROLES, batch, page_size=200)
            elif topic == TOPIC_USERROLES:
                psycopg2.extras.execute_batch(cur, UPSERT_USERROLES, batch, page_size=200)
            else:
                raise ValueError(f"Unexpected topic: {topic}")

            conn.commit()
            counts[topic] += len(batch)
            batch.clear()
        except psycopg2.Error as e:
            conn.rollback()
            # Don't keep consuming if the DB is in a bad state (missing tables,
            # permissions, bad types, etc.).
            raise RuntimeError(f"Postgres upsert failed: {e}") from e

    try:
        while idle < timeout:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    idle += 5
                    continue
                raise KafkaException(msg.error())

            idle = 0
            topic = msg.topic()
            saw_message = True
            try:
                record = json.loads(msg.value().decode("utf-8"))
                if topic == TOPIC_ROLES:
                    batches[topic].append(map_role(record))
                elif topic == TOPIC_USERROLES:
                    batches[topic].append(map_userrole(record))
                else:
                    continue

                if len(batches[topic]) >= batch_size:
                    flush(topic)
                    safe_commit()
            except Exception as e:
                # Parsing/mapping issues are message-scoped; DB failures raise above.
                log.warning("Bad message on %s: %s", topic, e)

        for t in topics:
            flush(t)
        if saw_message:
            safe_commit()

    except KeyboardInterrupt:
        log.info("Interrupted — flushing final batches…")
        for t in topics:
            flush(t)
        if saw_message:
            safe_commit()

    finally:
        cur.close()
        conn.close()
        consumer.close()

    log.info("✅  Dynamics load complete:")
    for t in topics:
        log.info("  %-28s %d rows", t, counts[t])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dynamics Kafka → Postgres loader")
    parser.add_argument("--topic", nargs="+", default=TOPICS, choices=TOPICS)
    parser.add_argument("--timeout", type=int, default=60, help="Idle seconds then stop")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--group-id", default="dynamics-postgres-loader")
    args = parser.parse_args()

    run(args.topic, args.group_id, args.timeout, args.batch_size)
