"""
Microsoft Graph → Postgres Direct Loader
Fixes: skips users with no license (NULL sku_id was violating NOT NULL constraint)

Usage: python loaders/load_graph_direct.py
"""

import os, time, logging
import requests, psycopg2, psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("graph_loader")

PG = dict(host=os.getenv("PG_HOST","localhost"), port=int(os.getenv("PG_PORT","5432")),
          dbname=os.getenv("PG_DB","itopsdb"), user=os.getenv("PG_USER","itops"),
          password=os.getenv("PG_PASSWORD","itops"))

TENANT_ID     = os.getenv("TENANT_ID")
CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TOKEN_URL     = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
GRAPH_BASE    = "https://graph.microsoft.com/v1.0"

SKU_FRIENDLY = {
    "05e9a617-0261-4cee-bb44-138d3ef5d965":"Microsoft 365 E3",
    "06ebc4ee-1bb5-47dd-8120-11324bc54e06":"Microsoft 365 Business Premium",
    "cbdc14ab-d96c-4c30-b9f4-6ada7cdc1d46":"Microsoft 365 Business Basic",
    "f245ecc8-75af-4f8e-b61f-27d8114de5f3":"Microsoft 365 Business Standard",
    "c7df2760-2c81-4ef7-b578-5b5392b571df":"Office 365 E5",
    "6fd2c87f-b296-42f0-b197-1e91e994b900":"Office 365 E3",
    "18181a46-0d4e-45cd-891e-60aabd171b4e":"Office 365 E1",
    "a403ebcc-fae0-4ca2-8c8c-7a907fd6c235":"Power BI Pro",
    "f8a1db68-be16-40ed-86d5-cb42ce701560":"Power BI Premium Per User",
    "4b9405b0-7788-4568-add1-99614e613b69":"Exchange Online Plan 1",
    "3b555118-da6a-4418-894f-7df1e2096870":"Microsoft 365 F1",
    "66b55226-6b4f-492c-910c-a3b7a3c9d993":"Microsoft 365 F3",
    "eb3c22d2-6e55-47f4-a238-4c0b4834b6ab":"Dynamics 365 Customer Engagement Plan",
    "84a661c4-e949-4bd2-a560-ed7766fcaf2b":"Azure AD Premium P1",
    "078d2b04-f1bd-4111-bbd4-b4b1b354cef4":"Azure AD Premium P2",
    "efccb6f7-5641-4e0e-bd10-b4976e1bf68e":"EMS E3",
    "b05e124f-c7cc-45a0-a6aa-8cf78c946968":"EMS E5",
}

_tok = {"t": None, "exp": 0}

def get_token():
    if _tok["t"] and time.time() < _tok["exp"] - 60:
        return _tok["t"]
    r = requests.post(TOKEN_URL, data={"grant_type":"client_credentials",
        "client_id":CLIENT_ID,"client_secret":CLIENT_SECRET,
        "scope":"https://graph.microsoft.com/.default"}, timeout=15)
    if not r.ok:
        raise RuntimeError(f"Token failed: {r.status_code} {r.text[:200]}")
    d = r.json()
    _tok["t"] = d["access_token"]; _tok["exp"] = time.time() + d.get("expires_in",3600)
    log.info("✅  MS Graph token acquired"); return _tok["t"]

def graph_all(path, params=None):
    results, url = [], f"{GRAPH_BASE}{path}"
    while url:
        r = requests.get(url, headers={"Authorization":f"Bearer {get_token()}"}, params=params, timeout=30)
        if not r.ok: raise RuntimeError(f"Graph {r.status_code}: {r.text[:200]}")
        d = r.json(); results.extend(d.get("value",[])); url = d.get("@odata.nextLink"); params = None
    return results

ENSURE_COLS = """
ALTER TABLE ms365_license_skus
    ADD COLUMN IF NOT EXISTS friendly_name     TEXT,
    ADD COLUMN IF NOT EXISTS utilization_pct   NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS available_units   INT,
    ADD COLUMN IF NOT EXISTS capability_status TEXT,
    ADD COLUMN IF NOT EXISTS ingested_at       TIMESTAMPTZ DEFAULT now();
ALTER TABLE ms365_user_licenses
    ADD COLUMN IF NOT EXISTS display_name    TEXT,
    ADD COLUMN IF NOT EXISTS department      TEXT,
    ADD COLUMN IF NOT EXISTS job_title       TEXT,
    ADD COLUMN IF NOT EXISTS account_enabled BOOLEAN,
    ADD COLUMN IF NOT EXISTS friendly_name   TEXT,
    ADD COLUMN IF NOT EXISTS ingested_at     TIMESTAMPTZ DEFAULT now();
"""

def load_skus(cur, conn):
    log.info("Fetching subscribed SKUs...")
    skus = graph_all("/subscribedSkus")
    log.info("  → Got %d SKUs", len(skus))
    for sku in skus:
        pp = sku.get("prepaidUnits",{}); enabled = pp.get("enabled",0); consumed = sku.get("consumedUnits",0); sid = sku.get("skuId","")
        cur.execute("""
            INSERT INTO ms365_license_skus (sku_id,sku_part_number,friendly_name,capability_status,
                consumed_units,enabled_units,available_units,utilization_pct,ingested_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,now())
            ON CONFLICT (sku_id) DO UPDATE SET
                sku_part_number=EXCLUDED.sku_part_number, friendly_name=EXCLUDED.friendly_name,
                capability_status=EXCLUDED.capability_status, consumed_units=EXCLUDED.consumed_units,
                enabled_units=EXCLUDED.enabled_units, available_units=EXCLUDED.available_units,
                utilization_pct=EXCLUDED.utilization_pct, ingested_at=now()
        """, (sid, sku.get("skuPartNumber",""), SKU_FRIENDLY.get(sid,sku.get("skuPartNumber","")),
              sku.get("capabilityStatus",""), consumed, enabled, enabled-consumed,
              round(consumed/enabled*100,2) if enabled>0 else 0))
    conn.commit()
    log.info("✅  Upserted %d SKUs → ms365_license_skus", len(skus))

def load_user_licenses(cur, conn):
    log.info("Fetching user license assignments...")
    users = graph_all("/users", params={"$select":"id,displayName,userPrincipalName,department,jobTitle,assignedLicenses,accountEnabled","$top":"999"})
    log.info("  → Got %d users from Graph", len(users))

    rows = []
    unlicensed = 0
    for u in users:
        assigned = u.get("assignedLicenses", [])
        upn = (u.get("userPrincipalName") or "").lower()
        if assigned:
            for lic in assigned:
                sid = lic.get("skuId","")
                if not sid:
                    continue  # skip entries with no sku_id
                rows.append((u.get("id"), upn, u.get("displayName",""),
                             u.get("department",""), u.get("jobTitle",""),
                             u.get("accountEnabled",True), sid,
                             SKU_FRIENDLY.get(sid, sid)))
        else:
            unlicensed += 1  # count but DON'T insert — sku_id is NOT NULL

    log.info("  → %d licensed assignments, %d users have no license (skipped)", len(rows), unlicensed)

    if rows:
        psycopg2.extras.execute_batch(cur, """
            INSERT INTO ms365_user_licenses
                (user_id,email,display_name,department,job_title,
                 account_enabled,sku_id,friendly_name,ingested_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,now())
            ON CONFLICT (user_id,sku_id) DO UPDATE SET
                email=EXCLUDED.email, display_name=EXCLUDED.display_name,
                department=EXCLUDED.department, job_title=EXCLUDED.job_title,
                account_enabled=EXCLUDED.account_enabled, friendly_name=EXCLUDED.friendly_name,
                ingested_at=now()
        """, rows, page_size=200)
        conn.commit()
    log.info("✅  Upserted %d rows → ms365_user_licenses", len(rows))

def run():
    conn = psycopg2.connect(**PG); cur = conn.cursor()
    try:
        cur.execute(ENSURE_COLS); conn.commit()
    except Exception as e:
        conn.rollback(); log.warning("Column migration: %s", e)
    load_skus(cur, conn)
    load_user_licenses(cur, conn)
    cur.close(); conn.close()
    log.info("🎉 Microsoft Graph load complete!")

if __name__ == "__main__":
    run()
