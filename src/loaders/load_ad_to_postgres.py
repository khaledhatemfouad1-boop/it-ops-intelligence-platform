"""
AD Kafka Consumer → Postgres Loader
Reads from Kafka topics: ad.users.raw, ad.computers.raw, ad.groups.raw, ad.ous.raw
Upserts into Postgres tables: ad_users, ad_computers, ad_groups, ad_ous

Run AFTER: python producers/ad_producer.py --mode all

Usage:
    python loader/load_ad_to_postgres.py
    python loader/load_ad_to_postgres.py --topic ad.users.raw
    python loader/load_ad_to_postgres.py --timeout 30
"""

import os, json, logging, argparse
from datetime import datetime

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ad_loader")

PG_DSN = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",       "itopsdb"),
    "user":     os.getenv("PG_USER",     "itops"),
    "password": os.getenv("PG_PASSWORD", "itops"),
}
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

TOPICS = ["ad.users.raw", "ad.computers.raw", "ad.groups.raw", "ad.ous.raw"]

# ── SQL ──────────────────────────────────────────────────────────
UPSERTS = {
    "ad.users.raw": ("""
        INSERT INTO ad_users (
            sam_account, upn, email, display_name, first_name, last_name,
            department, title, company, office, phone, mobile, description,
            manager_dn, ou, distinguished_name, enabled, locked_out,
            pwd_no_expire, is_service_account, created, modified,
            last_logon, pwd_last_set, ingested_at
        ) VALUES (
            %(sam_account)s, %(upn)s, %(email)s, %(display_name)s,
            %(first_name)s, %(last_name)s, %(department)s, %(title)s,
            %(company)s, %(office)s, %(phone)s, %(mobile)s, %(description)s,
            %(manager_dn)s, %(ou)s, %(distinguished_name)s, %(enabled)s,
            %(locked_out)s, %(pwd_no_expire)s, %(is_service_account)s,
            %(created)s, %(modified)s, %(last_logon)s, %(pwd_last_set)s,
            NOW()
        )
        ON CONFLICT (sam_account) DO UPDATE SET
            email             = EXCLUDED.email,
            display_name      = EXCLUDED.display_name,
            department        = EXCLUDED.department,
            title             = EXCLUDED.title,
            enabled           = EXCLUDED.enabled,
            locked_out        = EXCLUDED.locked_out,
            last_logon        = EXCLUDED.last_logon,
            modified          = EXCLUDED.modified,
            is_service_account= EXCLUDED.is_service_account,
            ingested_at       = NOW();
    """, ["sam_account","upn","email","display_name","first_name","last_name",
          "department","title","company","office","phone","mobile","description",
          "manager_dn","ou","distinguished_name","enabled","locked_out",
          "pwd_no_expire","is_service_account","created","modified",
          "last_logon","pwd_last_set"]),

    "ad.computers.raw": ("""
        INSERT INTO ad_computers (
            computer_name, dns_hostname, os, os_version, description,
            location, ou, distinguished_name, enabled, created, modified,
            last_logon, ingested_at
        ) VALUES (
            %(computer_name)s, %(dns_hostname)s, %(os)s, %(os_version)s,
            %(description)s, %(location)s, %(ou)s, %(distinguished_name)s,
            %(enabled)s, %(created)s, %(modified)s, %(last_logon)s, NOW()
        )
        ON CONFLICT (computer_name) DO UPDATE SET
            os          = EXCLUDED.os,
            os_version  = EXCLUDED.os_version,
            enabled     = EXCLUDED.enabled,
            last_logon  = EXCLUDED.last_logon,
            modified    = EXCLUDED.modified,
            ingested_at = NOW();
    """, ["computer_name","dns_hostname","os","os_version","description",
          "location","ou","distinguished_name","enabled","created","modified","last_logon"]),

    "ad.groups.raw": ("""
        INSERT INTO ad_groups (
            group_name, group_type, description, member_count,
            ou, distinguished_name, created, ingested_at
        ) VALUES (
            %(group_name)s, %(group_type)s, %(description)s, %(member_count)s,
            %(ou)s, %(distinguished_name)s, %(created)s, NOW()
        )
        ON CONFLICT (group_name) DO UPDATE SET
            group_type   = EXCLUDED.group_type,
            member_count = EXCLUDED.member_count,
            ingested_at  = NOW();
    """, ["group_name","group_type","description","member_count",
          "ou","distinguished_name","created"]),

    "ad.ous.raw": ("""
        INSERT INTO ad_ous (
            distinguished_name, ou_name, description, depth,
            parent_dn, created, ingested_at
        ) VALUES (
            %(distinguished_name)s, %(ou_name)s, %(description)s, %(depth)s,
            %(parent_dn)s, %(created)s, NOW()
        )
        ON CONFLICT (distinguished_name) DO UPDATE SET
            ou_name     = EXCLUDED.ou_name,
            description = EXCLUDED.description,
            ingested_at = NOW();
    """, ["distinguished_name","ou_name","description","depth","parent_dn","created"]),
}


def safe_record(msg: dict, fields: list) -> dict:
    """Extract only the fields we need, defaulting missing ones to None."""
    return {f: msg.get(f) for f in fields}


def run(topics: list, timeout: int = 60):
    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BROKER,
        "group.id":           "ad-loader",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(topics)
    log.info("Subscribed to: %s", topics)

    conn   = psycopg2.connect(**PG_DSN)
    cursor = conn.cursor()

    counts   = {t: 0 for t in topics}
    batches  = {t: [] for t in topics}
    BATCH_SZ = 200
    idle     = 0

    def flush(topic):
        if not batches[topic]:
            return
        sql, fields = UPSERTS[topic]
        recs = [safe_record(r, fields) for r in batches[topic]]
        psycopg2.extras.execute_batch(cursor, sql, recs, page_size=100)
        conn.commit()
        counts[topic] += len(batches[topic])
        batches[topic] = []

    try:
        while idle < timeout:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    idle += 10
                else:
                    log.error("Kafka error: %s", msg.error())
                continue

            idle  = 0
            topic = msg.topic()
            try:
                record = json.loads(msg.value().decode("utf-8"))
                batches[topic].append(record)
                if len(batches[topic]) >= BATCH_SZ:
                    flush(topic)
            except Exception as e:
                log.warning("Parse error on %s: %s", topic, e)

        # Flush remaining
        for topic in topics:
            flush(topic)

    finally:
        consumer.close()
        cursor.close()
        conn.close()

    log.info("✅  AD load complete:")
    for topic, count in counts.items():
        log.info("  %-30s %d rows", topic, count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AD Kafka → Postgres loader")
    parser.add_argument("--topic", nargs="+", default=TOPICS,
                        choices=TOPICS, help="Topics to consume (default: all)")
    parser.add_argument("--timeout", type=int, default=60,
                        help="Idle seconds before stopping (default: 60)")
    args = parser.parse_args()
    run(args.topic, args.timeout)
