"""
Zabbix → Postgres Direct Loader
Fixed: removed selectHosts (not supported in problem.get — use host.get separately)

Usage:
    python loaders/load_zabbix_direct.py --token "YOUR_NEW_TOKEN"
    python loaders/load_zabbix_direct.py --user khatem --password "yourpassword"

⚠️  Requires VPN to 10.0.0.240
"""

import os, sys, logging, argparse, requests
from datetime import datetime, timezone
import psycopg2, psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("zabbix_loader")

ZABBIX_URL   = os.getenv("ZABBIX_URL", "http://10.0.0.240/zabbix")
ZABBIX_TOKEN = os.getenv("ZABBIX_API_TOKEN", "")
ZABBIX_USER  = os.getenv("ZABBIX_USER", "khatem")
ZABBIX_PASS  = os.getenv("ZABBIX_PASSWORD", "")

PG = dict(host=os.getenv("PG_HOST","localhost"), port=int(os.getenv("PG_PORT","5432")),
          dbname=os.getenv("PG_DB","itopsdb"), user=os.getenv("PG_USER","itops"),
          password=os.getenv("PG_PASSWORD","itops"))

SEVERITY_MAP = {"0":"Not classified","1":"Information","2":"Warning",
                "3":"Average","4":"High","5":"Disaster"}

ENSURE_SCHEMA = """
DO $$ BEGIN
  BEGIN ALTER TABLE zabbix_hosts ADD COLUMN host_id TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
  BEGIN ALTER TABLE zabbix_hosts ADD COLUMN hostname TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
  BEGIN ALTER TABLE zabbix_hosts ADD COLUMN display_name TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
  BEGIN ALTER TABLE zabbix_hosts ADD COLUMN status TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
  BEGIN ALTER TABLE zabbix_hosts ADD COLUMN available TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
  BEGIN ALTER TABLE zabbix_hosts ADD COLUMN ip_address TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
  BEGIN ALTER TABLE zabbix_hosts ADD COLUMN groups TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
  BEGIN ALTER TABLE zabbix_hosts ADD COLUMN templates TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
  BEGIN ALTER TABLE zabbix_hosts ADD COLUMN ingested_at TIMESTAMPTZ DEFAULT now(); EXCEPTION WHEN duplicate_column THEN NULL; END;
  BEGIN ALTER TABLE zabbix_problems ADD COLUMN event_id TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
  BEGIN ALTER TABLE zabbix_problems ADD COLUMN host_name TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
  BEGIN ALTER TABLE zabbix_problems ADD COLUMN problem_name TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
  BEGIN ALTER TABLE zabbix_problems ADD COLUMN severity TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
  BEGIN ALTER TABLE zabbix_problems ADD COLUMN severity_label TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
  BEGIN ALTER TABLE zabbix_problems ADD COLUMN status TEXT; EXCEPTION WHEN duplicate_column THEN NULL; END;
  BEGIN ALTER TABLE zabbix_problems ADD COLUMN started_at TIMESTAMPTZ; EXCEPTION WHEN duplicate_column THEN NULL; END;
  BEGIN ALTER TABLE zabbix_problems ADD COLUMN acknowledged BOOLEAN DEFAULT false; EXCEPTION WHEN duplicate_column THEN NULL; END;
  BEGIN ALTER TABLE zabbix_problems ADD COLUMN ingested_at TIMESTAMPTZ DEFAULT now(); EXCEPTION WHEN duplicate_column THEN NULL; END;
END $$;
"""

class TokenExpiredError(Exception): pass

def zapi(url, token, method, params):
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    try:
        r = requests.post(f"{url}/api_jsonrpc.php",
                          headers=headers,
                          json={"jsonrpc":"2.0","method":method,"params":params,"id":1},
                          timeout=30)
        r.raise_for_status()
    except requests.exceptions.ConnectionError:
        log.error("❌  Cannot reach %s — connect VPN first!", url)
        sys.exit(1)
    result = r.json()
    if "error" in result:
        msg = result["error"].get("data", "")
        if "expired" in msg.lower() or "session" in msg.lower():
            raise TokenExpiredError(msg)
        raise RuntimeError(f"Zabbix API error: {msg}")
    return result.get("result", [])

def login(url, username, password):
    log.info("Logging in as '%s'...", username)
    r = requests.post(f"{url}/api_jsonrpc.php",
                      headers={"Content-Type":"application/json"},
                      json={"jsonrpc":"2.0","method":"user.login",
                            "params":{"username":username,"password":password},"id":1},
                      timeout=15)
    r.raise_for_status()
    result = r.json()
    if "error" in result:
        raise RuntimeError(f"Login failed: {result['error'].get('data','')}")
    log.info("✅  Got fresh Zabbix token")
    return result["result"]

def fetch_hosts(url, token):
    log.info("Fetching Zabbix hosts...")
    hosts = zapi(url, token, "host.get", {
        "output": ["hostid","host","name","status","available"],
        "selectGroups": ["name"],
        "selectInterfaces": ["ip"],
        "selectParentTemplates": ["name"],
    })
    log.info("  → %d hosts", len(hosts))
    records = []
    for h in hosts:
        ip = (h.get("interfaces") or [{}])[0].get("ip", "")
        groups    = ", ".join(g.get("name","") for g in h.get("groups",[]))
        templates = ", ".join(t.get("name","") for t in h.get("parentTemplates",[]))
        records.append({
            "host_id":      h.get("hostid"),
            "hostname":     h.get("host"),
            "display_name": h.get("name"),
            "status":    "Enabled" if str(h.get("status")) == "0" else "Disabled",
            "available": {"0":"Unknown","1":"Available","2":"Unavailable"}.get(
                         str(h.get("available","0")), "Unknown"),
            "ip_address": ip,
            "groups":     groups,
            "templates":  templates,
        })
    return records

def fetch_problems(url, token):
    log.info("Fetching active problems...")
    # ── FIX: do NOT use selectHosts in problem.get — get host info separately ──
    problems = zapi(url, token, "problem.get", {
        "output":    ["eventid","objectid","name","severity","clock","acknowledged","r_eventid"],
        "recent":    True,
        "sortfield": "eventid",
        "sortorder": "DESC",
    })
    log.info("  → %d problems, resolving host names...", len(problems))

    # Build hostid → hostname map from triggers
    if problems:
        trigger_ids = list({p.get("objectid") for p in problems if p.get("objectid")})
        try:
            triggers = zapi(url, token, "trigger.get", {
                "output":        ["triggerid"],
                "triggerids":    trigger_ids,
                "selectHosts":   ["hostid","host"],
            })
            tid_to_host = {}
            for t in triggers:
                for h in t.get("hosts",[]):
                    tid_to_host[t["triggerid"]] = h.get("host","")
        except Exception as e:
            log.warning("Could not resolve host names: %s", e)
            tid_to_host = {}
    else:
        tid_to_host = {}

    records = []
    for p in problems:
        sev = str(p.get("severity","0"))
        records.append({
            "event_id":     p.get("eventid"),
            "host_name":    tid_to_host.get(p.get("objectid",""), ""),
            "problem_name": p.get("name",""),
            "severity":     sev,
            "severity_label": SEVERITY_MAP.get(sev, "Unknown"),
            "status":       "RESOLVED" if p.get("r_eventid","0") != "0" else "PROBLEM",
            "started_at":   datetime.fromtimestamp(int(p.get("clock",0)), tz=timezone.utc),
            "acknowledged": p.get("acknowledged") == "1",
        })
    return records

def run(url, token, username, password):
    active_token = token

    try:
        hosts = fetch_hosts(url, active_token)
    except TokenExpiredError:
        log.warning("Token expired — trying password login...")
        active_token = login(url, username, password)
        hosts = fetch_hosts(url, active_token)

    try:
        problems = fetch_problems(url, active_token)
    except TokenExpiredError:
        active_token = login(url, username, password)
        problems = fetch_problems(url, active_token)

    conn = psycopg2.connect(**PG)
    cur  = conn.cursor()
    try:
        cur.execute(ENSURE_SCHEMA)
        conn.commit()
    except Exception as e:
        conn.rollback()
        log.warning("Schema migration: %s", e)

    if hosts:
        psycopg2.extras.execute_batch(cur, """
            INSERT INTO zabbix_hosts
                (host_id,hostname,display_name,status,available,ip_address,groups,templates,ingested_at)
            VALUES
                (%(host_id)s,%(hostname)s,%(display_name)s,%(status)s,%(available)s,
                 %(ip_address)s,%(groups)s,%(templates)s,now())
            ON CONFLICT DO NOTHING
        """, hosts, page_size=100)
        conn.commit()
        log.info("✅  Inserted %d hosts → zabbix_hosts", len(hosts))

    if problems:
        psycopg2.extras.execute_batch(cur, """
            INSERT INTO zabbix_problems
                (problem_id,event_id,host_name,problem_name,severity,severity_label,
                 status,started_at,acknowledged,ingested_at)
            VALUES
                (%(event_id)s,%(event_id)s,%(host_name)s,%(problem_name)s,%(severity)s,%(severity_label)s,
                 %(status)s,%(started_at)s,%(acknowledged)s,now())
            ON CONFLICT (problem_id) DO UPDATE SET
                host_name=EXCLUDED.host_name,
                problem_name=EXCLUDED.problem_name,
                severity=EXCLUDED.severity,
                severity_label=EXCLUDED.severity_label,
                status=EXCLUDED.status,
                started_at=EXCLUDED.started_at,
                acknowledged=EXCLUDED.acknowledged,
                ingested_at=now()
        """, problems, page_size=100)
        conn.commit()
        log.info("✅  Inserted %d problems → zabbix_problems", len(problems))

    cur.close(); conn.close()
    log.info("🎉 Zabbix load complete — %d hosts, %d problems", len(hosts), len(problems))

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url",      default=ZABBIX_URL)
    p.add_argument("--token",    default=ZABBIX_TOKEN)
    p.add_argument("--user",     default=ZABBIX_USER)
    p.add_argument("--password", default=ZABBIX_PASS)
    args = p.parse_args()
    run(args.url, args.token, args.user, args.password)