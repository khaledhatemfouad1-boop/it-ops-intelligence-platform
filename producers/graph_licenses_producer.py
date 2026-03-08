"""
Microsoft 365 Licenses → Kafka Producer
Uses your existing Azure app registration (same as Dynamics 365)

REQUIRED Azure App Permissions (Application type, not Delegated):
  - Organization.Read.All          → for subscribed SKUs
  - User.Read.All                  → for per-user license assignments
  - Directory.Read.All             → for license details

Add these in Azure Portal:
  App Registrations → your app → API Permissions
  → Add Microsoft Graph → Application permissions → add the above → Grant admin consent

Kafka topics:
  ms365.licenses.skus.raw     → subscribed SKUs (total/consumed/available)
  ms365.licenses.users.raw    → per-user license assignments

Run:
    python producers/graph_licenses_producer.py
"""

import os, json, time, logging
from datetime import datetime

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("graph_licenses")

# ── Config ──────────────────────────────────────────────────────
TENANT_ID     = os.getenv("TENANT_ID")
CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
KAFKA_BROKER  = os.getenv("KAFKA_BROKER", "localhost:9092")

GRAPH_BASE     = "https://graph.microsoft.com/v1.0"
TOKEN_URL      = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

TOPIC_SKUS     = "ms365.licenses.skus.raw"
TOPIC_USERS    = "ms365.licenses.users.raw"

# ── Friendly display names for common SKU IDs ──────────────────
SKU_FRIENDLY = {
    "05e9a617-0261-4cee-bb44-138d3ef5d965": "Microsoft 365 E3",
    "06ebc4ee-1bb5-47dd-8120-11324bc54e06": "Microsoft 365 Business Premium",
    "cbdc14ab-d96c-4c30-b9f4-6ada7cdc1d46": "Microsoft 365 Business Basic",
    "f245ecc8-75af-4f8e-b61f-27d8114de5f3": "Microsoft 365 Business Standard",
    "c7df2760-2c81-4ef7-b578-5b5392b571df": "Office 365 E5",
    "6fd2c87f-b296-42f0-b197-1e91e994b900": "Office 365 E3",
    "18181a46-0d4e-45cd-891e-60aabd171b4e": "Office 365 E1",
    "b05e124f-c7cc-45a0-a6aa-8cf78c946968": "Enterprise Mobility + Security E5",
    "efccb6f7-5641-4e0e-bd10-b4976e1bf68e": "Enterprise Mobility + Security E3",
    "078d2b04-f1bd-4111-bbd4-b4b1b354cef4": "Azure AD Premium P2",
    "84a661c4-e949-4bd2-a560-ed7766fcaf2b": "Azure AD Premium P1",
    "a403ebcc-fae0-4ca2-8c8c-7a907fd6c235": "Power BI Pro",
    "f8a1db68-be16-40ed-86d5-cb42ce701560": "Power BI Premium Per User",
    "b30411f5-fea1-4a59-9ad9-3db7c7ead579": "Microsoft Teams Exploratory",
    "710779e8-3d4a-4c88-adb9-386c958d1fdf": "Microsoft Teams Essentials",
    "4b9405b0-7788-4568-add1-99614e613b69": "Exchange Online Plan 1",
    "19ec0d23-8335-4cbd-94ac-6050e30712fa": "Exchange Online Plan 2",
    "3b555118-da6a-4418-894f-7df1e2096870": "Microsoft 365 F1",
    "66b55226-6b4f-492c-910c-a3b7a3c9d993": "Microsoft 365 F3",
    "e43b5b99-8dfb-405f-9987-dc307f34bcbd": "Microsoft Teams Phone Standard",
    "9c0dab89-a30d-4518-b4cf-20571b5571fc": "Visio Plan 2",
    "c5928f49-12ba-48f7-ada3-0d743a3601d5": "Visio Plan 1",
    "53818b1b-4a27-454b-8896-0dba576410e6": "Project Plan 3",
    "eb3c22d2-6e55-47f4-a238-4c0b4834b6ab": "Dynamics 365 Customer Engagement Plan",
}


# ── Token ───────────────────────────────────────────────────────
_token_cache = {"token": None, "expires_at": 0}

def get_token() -> str:
    if _token_cache["token"] and time.time() < _token_cache["expires_at"] - 60:
        return _token_cache["token"]
    resp = requests.post(TOKEN_URL, data={
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope":         "https://graph.microsoft.com/.default",
    }, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    _token_cache["token"]      = data["access_token"]
    _token_cache["expires_at"] = time.time() + data.get("expires_in", 3600)
    log.info("✅  MS Graph token acquired")
    return _token_cache["token"]


def graph_get(path: str, params: dict = None) -> dict:
    resp = requests.get(
        f"{GRAPH_BASE}{path}",
        headers={"Authorization": f"Bearer {get_token()}"},
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def graph_get_all(path: str, params: dict = None) -> list:
    """Auto-paginate through all pages."""
    results = []
    url = f"{GRAPH_BASE}{path}"
    while url:
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {get_token()}"},
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        results.extend(data.get("value", []))
        url    = data.get("@odata.nextLink")
        params = None  # only on first request
    return results


# ── SKU (subscription) data ─────────────────────────────────────
def fetch_skus() -> list:
    log.info("Fetching subscribed SKUs...")
    skus = graph_get_all("/subscribedSkus")
    records = []
    for sku in skus:
        prepaid  = sku.get("prepaidUnits", {})
        sku_id   = sku.get("skuId", "")
        records.append({
            "sku_id":          sku_id,
            "sku_part_number": sku.get("skuPartNumber", ""),
            "friendly_name":   SKU_FRIENDLY.get(sku_id, sku.get("skuPartNumber", "")),
            "capability_status": sku.get("capabilityStatus", ""),
            "consumed_units":  sku.get("consumedUnits", 0),
            "enabled_units":   prepaid.get("enabled", 0),
            "suspended_units": prepaid.get("suspended", 0),
            "warning_units":   prepaid.get("warning", 0),
            "available_units": prepaid.get("enabled", 0) - sku.get("consumedUnits", 0),
            "utilization_pct": round(
                sku.get("consumedUnits", 0) / prepaid.get("enabled", 1) * 100, 2
            ) if prepaid.get("enabled", 0) > 0 else 0,
            "service_plans":   [sp.get("servicePlanName") for sp in sku.get("servicePlans", [])],
            "_ingested_at":    datetime.utcnow().isoformat(),
        })
    return records


# ── Per-user license assignments ────────────────────────────────
def fetch_user_licenses() -> list:
    log.info("Fetching per-user license assignments (this may take a while)...")
    users = graph_get_all(
        "/users",
        params={
            "$select": "id,displayName,userPrincipalName,department,jobTitle,"
                       "assignedLicenses,assignedPlans,accountEnabled,createdDateTime",
            "$top": "999",
        }
    )
    records = []
    for u in users:
        assigned = u.get("assignedLicenses", [])
        for lic in assigned:
            sku_id = lic.get("skuId", "")
            records.append({
                "user_id":           u.get("id"),
                "display_name":      u.get("displayName", ""),
                "upn":               u.get("userPrincipalName", ""),
                "email":             u.get("userPrincipalName", "").lower(),
                "department":        u.get("department", ""),
                "job_title":         u.get("jobTitle", ""),
                "account_enabled":   u.get("accountEnabled", True),
                "sku_id":            sku_id,
                "friendly_name":     SKU_FRIENDLY.get(sku_id, sku_id),
                "disabled_plans":    lic.get("disabledPlans", []),
                "created_datetime":  u.get("createdDateTime"),
                "_ingested_at":      datetime.utcnow().isoformat(),
            })
        # Users with no licenses
        if not assigned:
            records.append({
                "user_id":          u.get("id"),
                "display_name":     u.get("displayName", ""),
                "upn":              u.get("userPrincipalName", "").lower(),
                "email":            u.get("userPrincipalName", "").lower(),
                "department":       u.get("department", ""),
                "job_title":        u.get("jobTitle", ""),
                "account_enabled":  u.get("accountEnabled", True),
                "sku_id":           None,
                "friendly_name":    "No License",
                "disabled_plans":   [],
                "created_datetime": u.get("createdDateTime"),
                "_ingested_at":     datetime.utcnow().isoformat(),
            })
    return records


# ── Kafka publish ───────────────────────────────────────────────
def run():
    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "client.id":         "graph-licenses-producer",
        "acks":              "all",
    })

    def publish(topic, key, record):
        producer.produce(
            topic=topic, key=key,
            value=json.dumps(record),
            callback=lambda err, _: log.error("Kafka: %s", err) if err else None,
        )
        producer.poll(0)

    # SKUs
    skus = fetch_skus()
    for s in skus:
        publish(TOPIC_SKUS, s["sku_id"], s)
    producer.flush()
    log.info("✅  Published %d SKUs to %s", len(skus), TOPIC_SKUS)

    # Per-user
    users = fetch_user_licenses()
    for u in users:
        key = f"{u['user_id']}_{u['sku_id'] or 'none'}"
        publish(TOPIC_USERS, key, u)
    producer.flush()
    log.info("✅  Published %d user-license rows to %s", len(users), TOPIC_USERS)


if __name__ == "__main__":
    run()
