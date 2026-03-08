"""
Dynamics 365 → Kafka Producer
Pulls security roles and user-role assignments from Dataverse REST API.
Uses OAuth2 client credentials flow (same Azure app as MS Graph producer).

Kafka topics:
  dynamics.roles.raw          → all security roles
  dynamics.userroles.raw      → user ↔ role assignments

Run:
    python producers/dynamics_producer.py
    python producers/dynamics_producer.py --mode roles
    python producers/dynamics_producer.py --mode userroles
"""

import os, json, time, logging, argparse
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("dynamics_producer")

# ── Config ─────────────────────────────────────────────────────
TENANT_ID      = os.getenv("TENANT_ID")
CLIENT_ID      = os.getenv("CLIENT_ID")
CLIENT_SECRET  = os.getenv("CLIENT_SECRET")
DATAVERSE_URL  = os.getenv("DATAVERSE_URL", "https://sumerge.crm4.dynamics.com").rstrip("/")
KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "localhost:9092")

TOPIC_ROLES    = "dynamics.roles.raw"
TOPIC_URROLES  = "dynamics.userroles.raw"

TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
SCOPE     = f"{DATAVERSE_URL}/.default"


# ── OAuth2 ─────────────────────────────────────────────────────
def get_access_token() -> str:
    resp = requests.post(TOKEN_URL, data={
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope":         SCOPE,
    }, timeout=30)
    resp.raise_for_status()
    token = resp.json()["access_token"]
    log.info("✅  OAuth2 token acquired")
    return token


def dataverse_get(token: str, entity: str, select: str = None, top: int = 5000) -> list:
    """Page through OData results and return all records."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept":        "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version":    "4.0",
        "Prefer":           f"odata.maxpagesize={top}",
    }
    url = f"{DATAVERSE_URL}/api/data/v9.2/{entity}"
    params = {}
    if select:
        params["$select"] = select

    results = []
    while url:
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        results.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
        params = {}   # nextLink already has params encoded
        log.info("  fetched %d records so far…", len(results))

    return results


# ── Kafka ───────────────────────────────────────────────────────
def make_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BROKER})


def publish(producer: Producer, topic: str, key: str, record: dict):
    producer.produce(
        topic,
        key=key,
        value=json.dumps(record, default=str),
    )


# ── Fetch helpers ───────────────────────────────────────────────
def fetch_roles(token: str) -> list:
    log.info("Fetching security roles from Dataverse…")
    raw = dataverse_get(
        token, "roles",
        select="roleid,name,businessunitid,ismanaged,iscustomizable,_parentroleid_value"
    )
    ts  = datetime.now(timezone.utc).isoformat()
    out = []
    for r in raw:
        out.append({
            "role_id":        r.get("roleid"),
            "name":           r.get("name"),
            "business_unit":  r.get("_businessunitid_value"),
            "is_managed":     r.get("ismanaged"),
            "is_customizable": r.get("iscustomizable", {}).get("Value") if isinstance(r.get("iscustomizable"), dict) else r.get("iscustomizable"),
            "parent_role_id": r.get("_parentroleid_value"),
            "ingested_at":    ts,
        })
    log.info("✅  %d roles fetched", len(out))
    return out


def fetch_userroles(token: str) -> list:
    log.info("Fetching user-role assignments from Dataverse…")
    raw = dataverse_get(
        token, "systemuserrolescollection",
        select="systemuserid,roleid"
    )
    ts  = datetime.now(timezone.utc).isoformat()
    out = []
    for r in raw:
        out.append({
            "user_id":     r.get("systemuserid"),
            "role_id":     r.get("roleid"),
            "ingested_at": ts,
        })
    log.info("✅  %d user-role pairs fetched", len(out))
    return out


# ── Main ────────────────────────────────────────────────────────
def run(mode: str = "all"):
    token    = get_access_token()
    producer = make_producer()

    if mode in ("all", "roles"):
        roles = fetch_roles(token)
        for r in roles:
            publish(producer, TOPIC_ROLES, r["role_id"], r)
        producer.flush()
        log.info("✅  Published %d roles → %s", len(roles), TOPIC_ROLES)

    if mode in ("all", "userroles"):
        pairs = fetch_userroles(token)
        for p in pairs:
            key = f"{p['user_id']}:{p['role_id']}"
            publish(producer, TOPIC_URROLES, key, p)
        producer.flush()
        log.info("✅  Published %d user-role pairs → %s", len(pairs), TOPIC_URROLES)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dynamics 365 → Kafka")
    parser.add_argument("--mode", choices=["all", "roles", "userroles"], default="all")
    args = parser.parse_args()
    run(args.mode)
