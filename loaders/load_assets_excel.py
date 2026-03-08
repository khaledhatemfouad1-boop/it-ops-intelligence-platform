"""
IT_Hardware_List.xlsx → Postgres Loader
Loads all 537 notebook lifecycle records from the Excel asset inventory.

Run:
    python loader/load_assets_excel.py
    python loader/load_assets_excel.py --file path/to/IT_Hardware_List.xlsx
"""

import os, json, logging, argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("assets_loader")

PG_DSN = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",       "itopsdb"),
    "user":     os.getenv("PG_USER",     "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}

DEFAULT_FILE = Path(__file__).parent.parent / "IT_Hardware_List.xlsx"

# ── Helpers ─────────────────────────────────────────────────────
def clean(val):
    if pd.isna(val):
        return None
    v = str(val).strip()
    return v if v and v.lower() not in ("nan", "none", "-", "n/a") else None


def parse_year(val):
    try:
        y = int(str(val).strip()[:4])
        return y if 2000 <= y <= 2030 else None
    except Exception:
        return None


def parse_date(val):
    if pd.isna(val):
        return None
    try:
        return pd.to_datetime(val).date().isoformat()
    except Exception:
        return None


# ── Sheet parsers ────────────────────────────────────────────────
def load_delivered(xl) -> list:
    """Notebooks (Delivered) — 429 active assigned laptops."""
    try:
        df = xl.parse("Notebooks (Delivered)", header=1)
    except Exception:
        df = xl.parse(0, header=1)

    records = []
    for _, row in df.iterrows():
        asset_tag = clean(row.get("Asset Tag") or row.get("asset tag") or row.get("S/N") or row.get("Serial"))
        assigned  = clean(row.get("Assigned To") or row.get("Employee") or row.get("User"))
        if not assigned:
            continue
        records.append({
            "asset_tag":       asset_tag or f"UNK-{len(records)+1}",
            "brand":           clean(row.get("Brand") or row.get("Manufacturer")),
            "model":           clean(row.get("Model")),
            "serial_number":   clean(row.get("S/N") or row.get("Serial Number")),
            "production_year": parse_year(row.get("Production Year") or row.get("Year")),
            "assigned_to":     assigned,
            "department":      clean(row.get("Department") or row.get("Dept")),
            "site":            clean(row.get("Site") or row.get("Location") or row.get("Country")),
            "delivery_date":   parse_date(row.get("Delivery Date") or row.get("Date")),
            "status":          "On Hand",
            "lifecycle_stage": "Delivered",
            "notes":           clean(row.get("Notes") or row.get("Remarks")),
            "ingested_at":     datetime.utcnow().isoformat(),
        })
    log.info("Delivered sheet: %d records parsed", len(records))
    return records


def load_stock(xl) -> list:
    """Notebooks (Stock) sheets — ready/not-ready/retired."""
    records = []
    for sheet_name in xl.sheet_names:
        if "stock" not in sheet_name.lower() or "delivered" in sheet_name.lower():
            continue
        try:
            df = xl.parse(sheet_name, header=1)
        except Exception:
            continue
        for _, row in df.iterrows():
            asset_tag = clean(row.get("Asset Tag") or row.get("S/N") or row.get("Serial"))
            status_raw = str(row.get("Status", "")).strip().lower()
            if "ready" in status_raw and "not" not in status_raw:
                stage, status = "Stock", "Stock Ready"
            elif "not ready" in status_raw or "not" in status_raw:
                stage, status = "Stock", "Stock Not Ready"
            elif "retire" in status_raw:
                stage, status = "Retired", "Retired"
            else:
                stage, status = "Stock", "Stock"
            records.append({
                "asset_tag":       asset_tag or f"STK-{len(records)+1}",
                "brand":           clean(row.get("Brand") or row.get("Manufacturer")),
                "model":           clean(row.get("Model")),
                "serial_number":   clean(row.get("S/N") or row.get("Serial Number")),
                "production_year": parse_year(row.get("Production Year") or row.get("Year")),
                "assigned_to":     None,
                "department":      None,
                "site":            None,
                "delivery_date":   None,
                "status":          status,
                "lifecycle_stage": stage,
                "notes":           clean(row.get("Notes") or row.get("Remarks")),
                "ingested_at":     datetime.utcnow().isoformat(),
            })
    log.info("Stock sheets: %d records parsed", len(records))
    return records


def load_maintenance(xl) -> list:
    """Notebooks (Ext. Maintenance) sheet."""
    records = []
    for sheet_name in xl.sheet_names:
        if "maintenance" not in sheet_name.lower():
            continue
        try:
            df = xl.parse(sheet_name, header=1)
        except Exception:
            continue
        for _, row in df.iterrows():
            asset_tag = clean(row.get("Asset Tag") or row.get("S/N") or row.get("Serial"))
            if not asset_tag:
                continue
            records.append({
                "asset_tag":       asset_tag,
                "brand":           clean(row.get("Brand") or row.get("Manufacturer")),
                "model":           clean(row.get("Model")),
                "serial_number":   clean(row.get("S/N") or row.get("Serial Number")),
                "production_year": parse_year(row.get("Production Year") or row.get("Year")),
                "assigned_to":     clean(row.get("Assigned To") or row.get("Employee")),
                "department":      clean(row.get("Department")),
                "site":            clean(row.get("Site") or row.get("Location")),
                "delivery_date":   None,
                "status":          "Under Maintenance",
                "lifecycle_stage": "External Maintenance",
                "notes":           clean(row.get("Notes") or row.get("Vendor") or row.get("Remarks")),
                "ingested_at":     datetime.utcnow().isoformat(),
            })
    log.info("Maintenance sheet: %d records parsed", len(records))
    return records


def load_scrapped(xl) -> list:
    """Scrap sheet."""
    records = []
    for sheet_name in xl.sheet_names:
        if "scrap" not in sheet_name.lower():
            continue
        try:
            df = xl.parse(sheet_name, header=1)
        except Exception:
            continue
        for _, row in df.iterrows():
            asset_tag = clean(row.get("Asset Tag") or row.get("S/N") or row.get("Serial"))
            if not asset_tag:
                continue
            records.append({
                "asset_tag":       asset_tag,
                "brand":           clean(row.get("Brand") or row.get("Manufacturer")),
                "model":           clean(row.get("Model")),
                "serial_number":   clean(row.get("S/N") or row.get("Serial Number")),
                "production_year": parse_year(row.get("Production Year") or row.get("Year")),
                "assigned_to":     None,
                "department":      None,
                "site":            None,
                "delivery_date":   None,
                "status":          "Scrapped",
                "lifecycle_stage": "Scrapped",
                "notes":           clean(row.get("Notes") or row.get("Remarks")),
                "ingested_at":     datetime.utcnow().isoformat(),
            })
    log.info("Scrap sheet: %d records parsed", len(records))
    return records


def load_donated(xl) -> list:
    """Old Notebooks (Donation) sheet."""
    records = []
    for sheet_name in xl.sheet_names:
        if "donat" not in sheet_name.lower():
            continue
        try:
            df = xl.parse(sheet_name, header=1)
        except Exception:
            continue
        for _, row in df.iterrows():
            asset_tag = clean(row.get("Asset Tag") or row.get("S/N") or row.get("Serial"))
            if not asset_tag:
                continue
            records.append({
                "asset_tag":       asset_tag,
                "brand":           clean(row.get("Brand") or row.get("Manufacturer")),
                "model":           clean(row.get("Model")),
                "serial_number":   clean(row.get("S/N") or row.get("Serial Number")),
                "production_year": parse_year(row.get("Production Year") or row.get("Year")),
                "assigned_to":     None,
                "department":      None,
                "site":            None,
                "delivery_date":   None,
                "status":          "Donated",
                "lifecycle_stage": "Donated",
                "notes":           clean(row.get("Notes") or row.get("Donated To") or row.get("Remarks")),
                "ingested_at":     datetime.utcnow().isoformat(),
            })
    log.info("Donation sheet: %d records parsed", len(records))
    return records


# ── SQL ──────────────────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS assets_laptops (
    asset_tag        TEXT PRIMARY KEY,
    brand            TEXT,
    model            TEXT,
    serial_number    TEXT,
    production_year  INT,
    assigned_to      TEXT,
    department       TEXT,
    site             TEXT,
    delivery_date    DATE,
    status           TEXT,
    lifecycle_stage  TEXT,
    notes            TEXT,
    ingested_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_assets_dept   ON assets_laptops(department);
CREATE INDEX IF NOT EXISTS idx_assets_site   ON assets_laptops(site);
CREATE INDEX IF NOT EXISTS idx_assets_brand  ON assets_laptops(brand);
CREATE INDEX IF NOT EXISTS idx_assets_stage  ON assets_laptops(lifecycle_stage);
CREATE INDEX IF NOT EXISTS idx_assets_year   ON assets_laptops(production_year);
"""

UPSERT_SQL = """
INSERT INTO assets_laptops (
    asset_tag, brand, model, serial_number, production_year,
    assigned_to, department, site, delivery_date,
    status, lifecycle_stage, notes, ingested_at
) VALUES (
    %(asset_tag)s, %(brand)s, %(model)s, %(serial_number)s, %(production_year)s,
    %(assigned_to)s, %(department)s, %(site)s, %(delivery_date)s,
    %(status)s, %(lifecycle_stage)s, %(notes)s, %(ingested_at)s
)
ON CONFLICT (asset_tag) DO UPDATE SET
    brand           = EXCLUDED.brand,
    model           = EXCLUDED.model,
    status          = EXCLUDED.status,
    lifecycle_stage = EXCLUDED.lifecycle_stage,
    assigned_to     = EXCLUDED.assigned_to,
    department      = EXCLUDED.department,
    delivery_date   = EXCLUDED.delivery_date,
    ingested_at     = EXCLUDED.ingested_at;
"""


def run(filepath: str):
    log.info("Opening: %s", filepath)
    xl = pd.ExcelFile(filepath, engine="openpyxl")
    log.info("Sheets found: %s", xl.sheet_names)

    all_records = []
    all_records.extend(load_delivered(xl))
    all_records.extend(load_stock(xl))
    all_records.extend(load_maintenance(xl))
    all_records.extend(load_scrapped(xl))
    all_records.extend(load_donated(xl))

    if not all_records:
        log.error("No records parsed — check sheet names/headers match expected format")
        return

    # Deduplicate by asset_tag (keep first occurrence)
    seen, deduped = set(), []
    for r in all_records:
        if r["asset_tag"] not in seen:
            seen.add(r["asset_tag"])
            deduped.append(r)

    log.info("Total records: %d (after dedup: %d)", len(all_records), len(deduped))

    conn   = psycopg2.connect(**PG_DSN)
    cursor = conn.cursor()

    # Ensure table exists
    cursor.execute(CREATE_TABLE_SQL)
    conn.commit()

    psycopg2.extras.execute_batch(cursor, UPSERT_SQL, deduped, page_size=100)
    conn.commit()

    # Summary
    cursor.execute("SELECT lifecycle_stage, COUNT(*) FROM assets_laptops GROUP BY lifecycle_stage ORDER BY COUNT(*) DESC")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    log.info("✅  Upserted %d asset records into assets_laptops", len(deduped))
    log.info("Breakdown by lifecycle stage:")
    for stage, count in rows:
        log.info("  %-30s %d", stage, count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IT Hardware Excel → Postgres")
    parser.add_argument("--file", default=str(DEFAULT_FILE),
                        help="Path to IT_Hardware_List.xlsx")
    args = parser.parse_args()
    run(args.file)
