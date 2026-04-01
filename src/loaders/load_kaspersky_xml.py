"""
Kaspersky Security Center — SpreadsheetML Report → Postgres
Handles both report types:
  - "Report on users of infected devices"  (Account, Dangerous objects, ...)
  - "Report on most heavily infected devices" (Device, Dangerous objects, ...)

Usage:
    python loaders/load_kaspersky_xml.py --file "C:\full\path\to\report.xml"
Run both XML files — data merges into the same tables.
"""

import os, logging, argparse
from datetime import datetime
import xml.etree.ElementTree as ET
import psycopg2, psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("kaspersky_loader")

PG_DSN = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",       "itopsdb"),
    "user":     os.getenv("PG_USER",     "itops"),
    "password": os.getenv("PG_PASSWORD", "itops"),
}
NS = {"ss": "urn:schemas-microsoft-com:office:spreadsheet"}

CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS kaspersky_threat_summary (
    id               SERIAL PRIMARY KEY,
    account          TEXT NOT NULL,
    username         TEXT,
    dangerous_objects INT DEFAULT 0,
    devices_infected  INT DEFAULT 0,
    groups_infected   INT DEFAULT 0,
    threats_detected  INT DEFAULT 0,
    first_blocked_at  TIMESTAMPTZ,
    last_blocked_at   TIMESTAMPTZ,
    ingested_at       TIMESTAMPTZ DEFAULT now(),
    UNIQUE(account)
);
CREATE TABLE IF NOT EXISTS kaspersky_threat_events (
    id                    SERIAL PRIMARY KEY,
    account               TEXT,
    username              TEXT,
    group_name            TEXT,
    device_name           TEXT,
    detected_at           TIMESTAMPTZ,
    detected_object       TEXT,
    file_path             TEXT,
    object_type           TEXT,
    action_detail         TEXT,
    application           TEXT,
    version_number        TEXT,
    last_visible          TIMESTAMPTZ,
    last_connected_to_srv TIMESTAMPTZ,
    ip_address            TEXT,
    open_alert            TEXT,
    ipv6_address          TEXT,
    ingested_at           TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_kts_acct ON kaspersky_threat_summary(account);
CREATE INDEX IF NOT EXISTS idx_kte_acct ON kaspersky_threat_events(account);
CREATE INDEX IF NOT EXISTS idx_kte_dev  ON kaspersky_threat_events(device_name);
"""

def cell_text(cell, ns):
    """Safely extract text from a <Cell> element. Returns None if empty."""
    d = cell.find("ss:Data", ns)
    if d is None:
        return None
    return d.text.strip() if d.text else None

def get_rows(ws):
    """Return list-of-lists of cell text values, skipping fully empty rows."""
    result = []
    for row in ws.findall(".//ss:Row", NS):
        cells = [cell_text(c, NS) for c in row.findall("ss:Cell", NS)]
        if any(v is not None for v in cells):
            result.append(cells)
    return result

def find_header_idx(rows):
    """Find the row index that contains column headers."""
    HEADER_WORDS = {"account","device","host","computer","virtual administration server"}
    for i, row in enumerate(rows):
        first = (row[0] or "").lower().strip()
        if first in HEADER_WORDS:
            return i
        # also match if any cell in this row equals a known header
        row_lower = [(v or "").lower() for v in row]
        if "device" in row_lower or "account" in row_lower:
            return i
    # fallback: first row with 3+ non-None values that isn't a title
    SKIP = ("kaspersky", "report", "period", "this report")
    for i, row in enumerate(rows):
        vals = [v for v in row if v]
        if len(vals) >= 3 and not any(s in (vals[0] or "").lower() for s in SKIP):
            return i
    return 0

def safe_int(v):
    try: return int(v) if v else 0
    except: return 0

def safe_ts(v):
    if not v: return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
        try: return datetime.strptime(v, fmt)
        except: pass
    return None

def extract_username(acct):
    if not acct: return ""
    return acct.split("\\", 1)[1].lower() if "\\" in acct else acct.lower()

def gc(row, i):
    """Get cell value safely."""
    try: return row[i] if i is not None and i < len(row) else None
    except: return None

def find_col(headers, *names):
    """Find column index by partial header name match."""
    for name in names:
        for i, h in enumerate(headers):
            if name in (h or "").lower():
                return i
    return None

def parse_summary(ws):
    rows = get_rows(ws)
    log.info("  Summary: %d raw rows", len(rows))
    hi = find_header_idx(rows)
    hdrs = [(h or "").lower() for h in rows[hi]]
    log.info("  Summary headers[%d]: %s", hi, hdrs[:8])

    # This report uses "Device" instead of "Account" — handle both
    c_acct = find_col(hdrs, "account", "device", "host")
    if c_acct is None: c_acct = 0
    c_dang = find_col(hdrs, "dangerous") or 1
    c_devs = find_col(hdrs, "devices infected")  # may not exist in device report
    c_grps = find_col(hdrs, "groups")
    c_thrt = find_col(hdrs, "threats")
    c_fst  = find_col(hdrs, "first")
    c_lst  = find_col(hdrs, "last")

    records = []
    for row in rows[hi+1:]:
        acct = gc(row, c_acct)
        if not acct: continue
        records.append({
            "account":           acct,
            "username":          extract_username(acct),
            "dangerous_objects": safe_int(gc(row, c_dang)),
            "devices_infected":  safe_int(gc(row, c_devs)) if c_devs is not None else 1,
            "groups_infected":   safe_int(gc(row, c_grps)) if c_grps is not None else 0,
            "threats_detected":  safe_int(gc(row, c_thrt)) if c_thrt is not None else 0,
            "first_blocked_at":  safe_ts(gc(row, c_fst)) if c_fst is not None else None,
            "last_blocked_at":   safe_ts(gc(row, c_lst)) if c_lst is not None else None,
        })
    log.info("  Parsed %d summary records", len(records))
    return records

def parse_details(ws):
    rows = get_rows(ws)
    log.info("  Details: %d raw rows", len(rows))
    hi = find_header_idx(rows)
    hdrs = [(h or "").lower() for h in rows[hi]]
    log.info("  Details headers[%d]: %s", hi, hdrs[:8])

    c_acct = find_col(hdrs, "account")
    c_grp  = find_col(hdrs, "group")
    c_dev  = find_col(hdrs, "device", "host", "computer")
    c_det  = find_col(hdrs, "detected at", "detect")
    c_obj  = find_col(hdrs, "detected object", "object name")
    c_path = find_col(hdrs, "path")
    c_type = find_col(hdrs, "object type", "type")
    c_act  = find_col(hdrs, "action")
    c_app  = find_col(hdrs, "application")
    c_ver  = find_col(hdrs, "version")
    c_lvis = find_col(hdrs, "last visible")
    c_lsrv = find_col(hdrs, "last connected")
    c_ip   = find_col(hdrs, "ip address")
    c_alrt = find_col(hdrs, "open alert", "alert")
    c_ip6  = find_col(hdrs, "ipv6")

    # For device-based report: no "account" column — use device as account
    use_device_as_account = (c_acct is None)
    if use_device_as_account:
        log.info("  Device-based report: using Device column as account identifier")

    records = []
    for row in rows[hi+1:]:
        device = gc(row, c_dev)
        acct   = gc(row, c_acct) if not use_device_as_account else device
        if not acct: continue
        records.append({
            "account":               acct,
            "username":              extract_username(acct),
            "group_name":            gc(row, c_grp),
            "device_name":           device,
            "detected_at":           safe_ts(gc(row, c_det)) if c_det is not None else None,
            "detected_object":       gc(row, c_obj),
            "file_path":             gc(row, c_path),
            "object_type":           gc(row, c_type),
            "action_detail":         gc(row, c_act),
            "application":           gc(row, c_app),
            "version_number":        gc(row, c_ver),
            "last_visible":          safe_ts(gc(row, c_lvis)) if c_lvis is not None else None,
            "last_connected_to_srv": safe_ts(gc(row, c_lsrv)) if c_lsrv is not None else None,
            "ip_address":            gc(row, c_ip),
            "open_alert":            gc(row, c_alrt),
            "ipv6_address":          gc(row, c_ip6),
        })
    log.info("  Parsed %d detail records", len(records))
    return records

def load(filepath):
    log.info("Parsing: %s", filepath)
    root = ET.parse(filepath).getroot()

    # Build sheet dict — use explicit is not None (XML elements are always truthy)
    sheets = {}
    for ws in root.findall(".//ss:Worksheet", NS):
        name = ws.get("{urn:schemas-microsoft-com:office:spreadsheet}Name")
        sheets[name] = ws
    log.info("Sheets: %s", list(sheets.keys()))

    sum_ws = sheets.get("Summary")
    det_ws = sheets.get("Details")
    if sum_ws is None and sheets:
        sum_ws = list(sheets.values())[0]
    if det_ws is None and len(sheets) > 1:
        det_ws = list(sheets.values())[1]

    summary = parse_summary(sum_ws) if sum_ws is not None else []
    details = parse_details(det_ws) if det_ws is not None else []

    if not summary and not details:
        log.error("❌ Nothing parsed — debug dump:")
        for nm, ws in sheets.items():
            rows = get_rows(ws)
            log.error("Sheet '%s' (%d rows):", nm, len(rows))
            for i, r in enumerate(rows[:8]):
                log.error("  row%d: %s", i, r[:6])
        return

    conn = psycopg2.connect(**PG_DSN)
    cur  = conn.cursor()
    cur.execute(CREATE_TABLES)
    conn.commit()

    if summary:
        psycopg2.extras.execute_batch(cur, """
            INSERT INTO kaspersky_threat_summary
                (account,username,dangerous_objects,devices_infected,
                 groups_infected,threats_detected,first_blocked_at,last_blocked_at)
            VALUES (%(account)s,%(username)s,%(dangerous_objects)s,%(devices_infected)s,
                    %(groups_infected)s,%(threats_detected)s,%(first_blocked_at)s,%(last_blocked_at)s)
            ON CONFLICT (account) DO UPDATE SET
                username=EXCLUDED.username,
                dangerous_objects=EXCLUDED.dangerous_objects,
                devices_infected=EXCLUDED.devices_infected,
                groups_infected=EXCLUDED.groups_infected,
                threats_detected=EXCLUDED.threats_detected,
                first_blocked_at=EXCLUDED.first_blocked_at,
                last_blocked_at=EXCLUDED.last_blocked_at,
                ingested_at=now()
        """, summary)
        conn.commit()
        log.info("✅  Upserted %d → kaspersky_threat_summary", len(summary))

    inserted, batch = 0, []
    for r in details:
        batch.append(r)
        if len(batch) >= 200:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO kaspersky_threat_events
                    (account,username,group_name,device_name,detected_at,detected_object,
                     file_path,object_type,action_detail,application,version_number,
                     last_visible,last_connected_to_srv,ip_address,open_alert,ipv6_address)
                VALUES (%(account)s,%(username)s,%(group_name)s,%(device_name)s,%(detected_at)s,
                        %(detected_object)s,%(file_path)s,%(object_type)s,%(action_detail)s,
                        %(application)s,%(version_number)s,%(last_visible)s,%(last_connected_to_srv)s,
                        %(ip_address)s,%(open_alert)s,%(ipv6_address)s)
                ON CONFLICT DO NOTHING
            """, batch)
            conn.commit(); inserted += len(batch); batch = []
    if batch:
        psycopg2.extras.execute_batch(cur, """
            INSERT INTO kaspersky_threat_events
                (account,username,group_name,device_name,detected_at,detected_object,
                 file_path,object_type,action_detail,application,version_number,
                 last_visible,last_connected_to_srv,ip_address,open_alert,ipv6_address)
            VALUES (%(account)s,%(username)s,%(group_name)s,%(device_name)s,%(detected_at)s,
                    %(detected_object)s,%(file_path)s,%(object_type)s,%(action_detail)s,
                    %(application)s,%(version_number)s,%(last_visible)s,%(last_connected_to_srv)s,
                    %(ip_address)s,%(open_alert)s,%(ipv6_address)s)
            ON CONFLICT DO NOTHING
        """, batch)
        conn.commit(); inserted += len(batch)

    log.info("✅  Upserted %d → kaspersky_threat_events", inserted)
    cur.close(); conn.close()
    log.info("🎉 Done — %d summary + %d events", len(summary), inserted)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--file", required=True, help="Full path to Kaspersky XML report")
    args = p.parse_args()
    load(args.file)
