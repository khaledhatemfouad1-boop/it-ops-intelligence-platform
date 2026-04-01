"""
Zabbix Kafka → Postgres Loader
Consumes zabbix.problems.raw and zabbix.hosts.raw topics → Postgres

Run:
    python loaders/load_zabbix_to_postgres.py
"""

import os, json, logging
from datetime import datetime
import psycopg2, psycopg2.extras
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("zabbix_loader")

PG_DSN = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",       "itopsdb"),
    "user":     os.getenv("PG_USER",     "itops"),
    "password": os.getenv("PG_PASSWORD", "itops"),
}
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

UPSERT_PROBLEMS = """
INSERT INTO zabbix_problems (
    problem_id, host_name, host_technical, trigger_id, severity, severity_level,
    problem_name, acknowledged, suppressed, clock, recovered_at,
    duration_seconds, status, raw_payload, ingested_at
) VALUES (
    %(problem_id)s, %(host_name)s, %(host_technical)s, %(trigger_id)s,
    %(severity)s, %(severity_level)s, %(problem_name)s, %(acknowledged)s,
    %(suppressed)s, %(clock)s, %(recovered_at)s, %(duration_seconds)s,
    %(status)s, %(raw_payload)s, NOW()
)
ON CONFLICT (problem_id) DO UPDATE SET
    status = EXCLUDED.status, acknowledged = EXCLUDED.acknowledged,
    recovered_at = EXCLUDED.recovered_at, ingested_at = NOW();
"""

UPSERT_HOSTS = """
INSERT INTO zabbix_hosts (
    host_id, hostname, display_name, host_group, ip_address,
    status, available, last_seen_at, ingested_at
) VALUES (
    %(host_id)s, %(hostname)s, %(display_name)s, %(host_group)s, %(ip_address)s,
    %(status)s, %(available)s, %(last_seen_at)s, NOW()
)
ON CONFLICT (host_id) DO UPDATE SET
    status = EXCLUDED.status, available = EXCLUDED.available,
    last_seen_at = EXCLUDED.last_seen_at, ingested_at = NOW();
"""

def make_consumer(topics):
    c = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "zabbix-loader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    c.subscribe(topics)
    return c

def run(timeout=30):
    conn = psycopg2.connect(**PG_DSN)
    cur  = conn.cursor()

    for topic, upsert_sql, label in [
        ("zabbix.problems.raw", UPSERT_PROBLEMS, "problems"),
        ("zabbix.hosts.raw",    UPSERT_HOSTS,    "hosts"),
    ]:
        consumer = make_consumer([topic])
        count = 0
        idle  = 0
        log.info("Loading %s from %s ...", label, topic)
        while idle < timeout:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                log.error("Kafka error: %s", msg.error())
                idle += 1
                continue
            idle = 0
            data = json.loads(msg.value())
            data["raw_payload"] = json.dumps(data)
            try:
                cur.execute(upsert_sql, data)
                count += 1
                if count % 100 == 0:
                    conn.commit()
                    log.info("  %d %s upserted...", count, label)
            except Exception as e:
                log.warning("Row error: %s | data: %s", e, str(data)[:100])
                conn.rollback()
        conn.commit()
        consumer.close()
        log.info("✅  %d %s loaded into postgres", count, label)

    cur.close()
    conn.close()

if __name__ == "__main__":
    run()
