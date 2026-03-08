"""
Kaspersky Security Center — XML Export → Postgres Loader
Parses the XML export from KSC Admin Console and loads into kaspersky_devices table

Usage:
    1. In Kaspersky Security Center Admin Console:
       Managed devices → right-click → Export to XML
    2. Run: python loader/load_kaspersky_xml.py --file path/to/export.xml

Also sends each device to Kafka: kaspersky.devices.raw
"""

import os, json, logging, argparse
from datetime import datetime
from pathlib import Path
import xml.etree.ElementTree as ET

import psycopg2
import psycopg2.extras
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("kaspersky_loader")

PG_DSN = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",       "itopsdb"),
    "user":     os.getenv("PG_USER",     "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC        = "kaspersky.devices.raw"

PROT_STATUS_MAP = {
    "0": "OK",
    "1": "Warning",
    "2": "Critical",
    "3": "Unprotected",
}


def parse_xml(filepath: str) -> list:
    """
    Parse Kaspersky XML export.
    KSC exports use <KLCSP_RESULT> or <HOSTS> as root,
    with <PARAMS> or <HOST> child elements.
    We try multiple known formats.
    """
    log.info("Parsing XML: %s", filepath)
    tree = ET.parse(filepath)
    root = tree.getroot()

    log.info("XML root tag: %s", root.tag)

    # Detect format
    devices = []

    # Format 1: flat HOST list
    hosts = root.findall(".//HOST") or root.findall(".//host")
    # Format 2: PARAMS structure
    if not hosts:
        hosts = root.findall(".//PARAMS") or root.findall(".//Device")

    log.info("Found %d device elements", len(hosts))

    def g(el, *keys):
        """Try multiple attribute/child names."""
        for k in keys:
            v = el.get(k) or el.findtext(k)
            if v:
                return v.strip()
        return ""

    for i, host in enumerate(hosts):
        # Try to extract known Kaspersky fields
        # Field names differ between KSC versions — we try all common variants
        device_id   = g(host, "KLHST_WKS_GROUPID", "id", "Id", "DeviceId") or f"device_{i+1}"
        name        = g(host, "KLHST_WKS_FQDN", "KLHST_WKS_DN", "Name", "DisplayName", "name")
        os_name     = g(host, "KLHST_WKS_OS_NAME", "OsName", "OS")
        os_ver      = g(host, "KLHST_WKS_OS_VER_MAJOR", "OsVersion")
        ip          = g(host, "KLHST_WKS_IP_LONG", "KLHST_WKS_IP", "IpAddress", "IP")
        group       = g(host, "KLHST_WKS_GROUPNAME", "GroupName", "Group", "group")
        domain      = g(host, "KLHST_WKS_DOMAIN", "Domain")
        av_version  = g(host, "KLHST_WKS_AV_VERSION", "AvVersion", "KavVersion")
        last_visible= g(host, "KLHST_WKS_LAST_VISIBLE", "LastVisible", "LastSeen")
        last_scan   = g(host, "KLHST_WKS_LAST_SCAN", "LastScan", "LastFullScan")
        av_bases    = g(host, "KLHST_WKS_AV_BASES_TIME", "AvBasesDate", "BasesDate")
        prot_status = g(host, "KLHST_WKS_PROT_STATE", "ProtectionStatus", "Status")
        rtp         = g(host, "KLHST_WKS_RTP_STATUS", "RtpStatus", "RealTimeProtection")
        threats     = g(host, "KLHST_WKS_DETECTED", "ThreatsDetected", "Threats") or "0"
        cured       = g(host, "KLHST_WKS_CURED", "ThreatsCured", "Cured") or "0"
        vulns       = g(host, "KLHST_WKS_VULNS_COUNT", "VulnerabilitiesCount", "Vulns") or "0"
        enc_status  = g(host, "KLHST_WKS_ENCRYPTION_STATE", "EncryptionStatus", "Encryption")
        patch_status= g(host, "KLHST_WKS_PATCH_STATUS", "PatchStatus", "PatchMgmt")
        managed     = g(host, "KLHST_WKS_IS_MANAGED", "IsManaged") or "1"

        # Normalize protection status
        prot_norm = PROT_STATUS_MAP.get(str(prot_status), prot_status or "Unknown")

        record = {
            "device_id":             device_id or f"ksp_{i+1}",
            "device_name":           name or f"UNKNOWN-{i+1}",
            "os_name":               os_name,
            "os_version":            os_ver,
            "ip_address":            ip,
            "domain":                domain,
            "group_name":            group,
            "kaspersky_version":     av_version,
            "last_visible":          last_visible or None,
            "last_scan":             last_scan or None,
            "av_bases_date":         av_bases or None,
            "protection_status":     prot_norm,
            "real_time_protection":  str(rtp) not in ("0", "false", "False", ""),
            "threats_detected":      int(threats) if str(threats).isdigit() else 0,
            "threats_cured":         int(cured)   if str(cured).isdigit()   else 0,
            "threats_quarantined":   0,
            "encryption_status":     enc_status,
            "patch_status":          patch_status,
            "vulnerabilities_count": int(vulns)  if str(vulns).isdigit()   else 0,
            "is_managed":            str(managed) not in ("0", "false", "False"),
            "source_xml":            ET.tostring(host, encoding="unicode")[:2000],
            "ingested_at":           datetime.utcnow().isoformat(),
        }
        devices.append(record)

    return devices


UPSERT_SQL = """
INSERT INTO kaspersky_devices (
    device_id, device_name, os_name, os_version, ip_address, domain,
    group_name, kaspersky_version, last_visible, last_scan, av_bases_date,
    protection_status, real_time_protection, threats_detected,
    threats_cured, threats_quarantined, encryption_status, patch_status,
    vulnerabilities_count, is_managed, ingested_at
) VALUES (
    %(device_id)s, %(device_name)s, %(os_name)s, %(os_version)s, %(ip_address)s, %(domain)s,
    %(group_name)s, %(kaspersky_version)s, %(last_visible)s, %(last_scan)s, %(av_bases_date)s,
    %(protection_status)s, %(real_time_protection)s, %(threats_detected)s,
    %(threats_cured)s, %(threats_quarantined)s, %(encryption_status)s, %(patch_status)s,
    %(vulnerabilities_count)s, %(is_managed)s, %(ingested_at)s
)
ON CONFLICT (device_id) DO UPDATE SET
    device_name         = EXCLUDED.device_name,
    os_name             = EXCLUDED.os_name,
    protection_status   = EXCLUDED.protection_status,
    real_time_protection= EXCLUDED.real_time_protection,
    threats_detected    = EXCLUDED.threats_detected,
    vulnerabilities_count = EXCLUDED.vulnerabilities_count,
    last_visible        = EXCLUDED.last_visible,
    last_scan           = EXCLUDED.last_scan,
    av_bases_date       = EXCLUDED.av_bases_date,
    ingested_at         = EXCLUDED.ingested_at;
"""


def run(filepath: str):
    devices = parse_xml(filepath)
    if not devices:
        log.error("No devices parsed from XML. Check the file format.")
        return

    # Kafka
    producer = Producer({"bootstrap.servers": KAFKA_BROKER, "client.id": "kaspersky-loader"})
    for d in devices:
        producer.produce(topic=TOPIC, key=d["device_id"],
                         value=json.dumps(d, default=str))
        producer.poll(0)
    producer.flush()
    log.info("✅  Published %d devices to Kafka: %s", len(devices), TOPIC)

    # Postgres
    conn   = psycopg2.connect(**PG_DSN)
    cursor = conn.cursor()
    psycopg2.extras.execute_batch(cursor, UPSERT_SQL, devices)
    conn.commit()
    cursor.close()
    conn.close()
    log.info("✅  Upserted %d Kaspersky devices into Postgres", len(devices))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kaspersky XML → Postgres")
    parser.add_argument("--file", required=True, help="Path to Kaspersky XML export file")
    args = parser.parse_args()
    run(args.file)
