"""
Zabbix → Kafka Producer
Uses Zabbix API token (no user/password needed)
Requires VPN to reach: http://10.0.0.240/zabbix

Pulls:
  - Active problems (with host + severity + duration)
  - Recent resolved problems (last 7 days)
  - All hosts with status
  - Host groups

Kafka topics:
  zabbix.problems.raw   → active + recent problems
  zabbix.hosts.raw      → all monitored hosts

Run:
    python producers/zabbix_producer.py
    python producers/zabbix_producer.py --mode problems
    python producers/zabbix_producer.py --mode hosts
"""

import os, json, time, logging, argparse
from datetime import datetime, timedelta

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("zabbix_producer")

# ── Config ─────────────────────────────────────────────────────
ZABBIX_URL     = os.getenv("ZABBIX_URL", "http://10.0.0.240/zabbix").rstrip("/")
ZABBIX_API_URL = f"{ZABBIX_URL}/api_jsonrpc.php"
ZABBIX_TOKEN   = os.getenv("ZABBIX_API_TOKEN", "7e4a292397caf9662cc365b58ee45785694622f4e57cfee07155f28879cab01b")
KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "localhost:9092")

TOPIC_PROBLEMS = "zabbix.problems.raw"
TOPIC_HOSTS    = "zabbix.hosts.raw"

SEVERITY_MAP = {
    "0": "Not classified",
    "1": "Information",
    "2": "Warning",
    "3": "Average",
    "4": "High",
    "5": "Disaster",
}


# ── Zabbix API helper ──────────────────────────────────────────
class ZabbixAPI:
    def __init__(self):
        self._req_id = 1

    def call(self, method: str, params: dict, auth: bool = True) -> dict:
        headers = {"Content-Type": "application/json-rpc"}
        if auth:
            headers["Authorization"] = f"Bearer {ZABBIX_TOKEN}"
        payload = {
            "jsonrpc": "2.0",
            "method":  method,
            "params":  params,
            "id":      self._req_id,
        }
        self._req_id += 1
        resp = requests.post(ZABBIX_API_URL, json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"Zabbix API error: {data['error']}")
        return data["result"]

    def get_version(self) -> str:
        # apiinfo.version must NOT include Authorization header
        return self.call("apiinfo.version", {}, auth=False)


# ── Fetch problems ─────────────────────────────────────────────
def fetch_problems(api: ZabbixAPI, days_back: int = 7) -> list:
    """Fetch active + recently resolved problems."""
    since_ts = int((datetime.utcnow() - timedelta(days=days_back)).timestamp())

    problems = api.call("problem.get", {
        "output":          "extend",
        "selectAcknowledges": ["message", "clock", "username"],
        "selectTags":      "extend",
        "recent":          True,       # include recently resolved
        "time_from":       since_ts,
        "sortfield":       "eventid",
        "sortorder":       "DESC",
        "limit":           5000,
    })

    # Enrich with trigger + host info
    if not problems:
        return []

    trigger_ids = list({p["objectid"] for p in problems})

    # Get trigger details (host, description)
    triggers = api.call("trigger.get", {
        "output":         ["triggerid", "description", "priority", "status"],
        "triggerids":     trigger_ids,
        "selectHosts":    ["hostid", "name", "host"],
        "selectGroups":   ["name"],
        "expandDescription": True,
    })
    trigger_map = {t["triggerid"]: t for t in triggers}

    enriched = []
    for p in problems:
        trigger = trigger_map.get(p["objectid"], {})
        hosts   = trigger.get("hosts", [{}])
        groups  = trigger.get("groups", [{}])
        host    = hosts[0] if hosts else {}
        group   = groups[0] if groups else {}

        sev_level = int(p.get("severity", "0"))
        clock     = int(p.get("clock", 0))
        r_clock   = int(p.get("r_clock", 0)) if p.get("r_clock") else None

        enriched.append({
            "problem_id":      p["eventid"],
            "object_id":       p["objectid"],
            "host_id":         host.get("hostid"),
            "host_name":       host.get("name", ""),
            "host_technical":  host.get("host", ""),
            "host_group":      group.get("name", ""),
            "severity":        SEVERITY_MAP.get(str(sev_level), "Unknown"),
            "severity_level":  sev_level,
            "problem_name":    trigger.get("description", p.get("name", "")),
            "status":          "RESOLVED" if p.get("r_eventid") else "PROBLEM",
            "acknowledged":    p.get("acknowledged") == "1",
            "suppressed":      p.get("suppressed") == "1",
            "clock":           datetime.utcfromtimestamp(clock).isoformat() if clock else None,
            "recovered_at":    datetime.utcfromtimestamp(r_clock).isoformat() if r_clock else None,
            "duration_seconds": (r_clock - clock) if r_clock else None,
            "tags":            p.get("tags", []),
            "ack_count":       len(p.get("acknowledges", [])),
            "_ingested_at":    datetime.utcnow().isoformat(),
        })

    return enriched


# ── Fetch hosts ────────────────────────────────────────────────
def fetch_hosts(api: ZabbixAPI) -> list:
    hosts = api.call("host.get", {
        "output":       ["hostid", "host", "name", "status", "available",
                         "error", "ipmi_available", "snmp_available",
                         "maintenance_status"],
        "selectGroups": ["name"],
        "selectInterfaces": ["ip", "dns", "port", "type"],
        "selectInventory":  ["os", "os_full", "hardware", "location"],
    })

    enriched = []
    for h in hosts:
        groups     = h.get("groups", [{}])
        interfaces = h.get("interfaces", [{}])
        inv        = h.get("inventory") or {}
        iface      = interfaces[0] if interfaces else {}
        group      = groups[0] if groups else {}

        enriched.append({
            "host_id":       h["hostid"],
            "hostname":      h["host"],
            "display_name":  h["name"],
            "host_group":    group.get("name", ""),
            "status":        "Monitored" if h.get("status") == "0" else "Unmonitored",
            "available":     {"0": "Unknown", "1": "Available", "2": "Unavailable"}.get(
                                 h.get("available", "0"), "Unknown"),
            "ip_address":    iface.get("ip", ""),
            "dns":           iface.get("dns", ""),
            "os":            inv.get("os_full") or inv.get("os", ""),
            "hardware":      inv.get("hardware", ""),
            "location":      inv.get("location", ""),
            "in_maintenance": h.get("maintenance_status") == "1",
            "error":         h.get("error", ""),
            "_ingested_at":  datetime.utcnow().isoformat(),
        })

    return enriched


# ── Kafka publish ──────────────────────────────────────────────
def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "client.id":         "zabbix-producer",
        "acks":              "all",
    })


def publish(producer: Producer, topic: str, key: str, record: dict):
    producer.produce(
        topic    = topic,
        key      = key,
        value    = json.dumps(record),
        callback = lambda err, msg: log.error("Kafka error: %s", err) if err else None,
    )
    producer.poll(0)


# ── Main ───────────────────────────────────────────────────────
def run(mode: str = "all"):
    api      = ZabbixAPI()
    producer = make_producer()

    try:
        version = api.get_version()
        log.info("Connected to Zabbix API version %s", version)
    except Exception as e:
        log.error("Cannot connect to Zabbix at %s — are you on VPN? Error: %s", ZABBIX_API_URL, e)
        return

    if mode in ("problems", "all"):
        log.info("Fetching problems (last 7 days)...")
        problems = fetch_problems(api)
        for p in problems:
            publish(producer, TOPIC_PROBLEMS, str(p["problem_id"]), p)
        producer.flush()
        log.info("✅  Published %d problems to %s", len(problems), TOPIC_PROBLEMS)

    if mode in ("hosts", "all"):
        log.info("Fetching all monitored hosts...")
        hosts = fetch_hosts(api)
        for h in hosts:
            publish(producer, TOPIC_HOSTS, str(h["host_id"]), h)
        producer.flush()
        log.info("✅  Published %d hosts to %s", len(hosts), TOPIC_HOSTS)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Zabbix → Kafka producer")
    parser.add_argument("--mode", choices=["problems", "hosts", "all"], default="all")
    args = parser.parse_args()
    run(mode=args.mode)
