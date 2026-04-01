"""
IT Hardware List Excel → PostgreSQL (EC2)
Loads all sheets into public.assets_laptops + public.assets_other

Run: python load_assets_full.py
"""
import os, logging, pandas as pd, psycopg2, psycopg2.extras
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("assets-loader")

PG = dict(host=os.getenv("PG_HOST","localhost"), port=int(os.getenv("PG_PORT","5432")),
          dbname=os.getenv("PG_DB","itopsdb"), user=os.getenv("PG_USER","itops"),
          password=os.getenv("PG_PASSWORD","itops"))

FILE = Path(r"C:\Users\khatem\Downloads\IT Hardware List (2).xlsx")
if not FILE.exists():
    FILE = Path(r"C:\Users\khatem\Downloads\IT_Hardware_List.xlsx")

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS public.assets_laptops (
    id               SERIAL PRIMARY KEY,
    asset_id         TEXT,
    computer_name    TEXT,
    brand            TEXT,
    model            TEXT,
    serial_number    TEXT,
    assigned_to      TEXT,
    department       TEXT,
    location         TEXT,
    status           TEXT,
    lifecycle_stage  TEXT,
    purchase_date    DATE,
    production_year  INT,
    given_date       DATE,
    condition_notes  TEXT,
    ingested_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_assets_dept     ON public.assets_laptops(department);
CREATE INDEX IF NOT EXISTS idx_assets_location ON public.assets_laptops(location);
CREATE INDEX IF NOT EXISTS idx_assets_brand    ON public.assets_laptops(brand);
CREATE INDEX IF NOT EXISTS idx_assets_stage    ON public.assets_laptops(lifecycle_stage);
CREATE INDEX IF NOT EXISTS idx_assets_assigned ON public.assets_laptops(assigned_to);

CREATE TABLE IF NOT EXISTS public.assets_other (
    id           SERIAL PRIMARY KEY,
    asset_id     TEXT,
    asset_type   TEXT,
    manufacturer TEXT,
    model        TEXT,
    serial_no    TEXT,
    location     TEXT,
    ingested_at  TIMESTAMPTZ DEFAULT NOW()
);
"""

UPSERT_LAPTOP = """
INSERT INTO public.assets_laptops
    (asset_id,computer_name,brand,model,serial_number,assigned_to,department,
     location,status,lifecycle_stage,purchase_date,production_year,given_date,condition_notes,ingested_at)
VALUES
    (%(asset_id)s,%(computer_name)s,%(brand)s,%(model)s,%(serial_number)s,%(assigned_to)s,
     %(department)s,%(location)s,%(status)s,%(lifecycle_stage)s,%(purchase_date)s,
     %(production_year)s,%(given_date)s,%(condition_notes)s,NOW())
ON CONFLICT DO NOTHING
"""

UPSERT_OTHER = """
INSERT INTO public.assets_other
    (asset_id,asset_type,manufacturer,model,serial_no,location,ingested_at)
VALUES
    (%(asset_id)s,%(asset_type)s,%(manufacturer)s,%(model)s,%(serial_no)s,%(location)s,NOW())
ON CONFLICT DO NOTHING
"""

def clean(v):
    if v is None: return None
    if str(v) in ('NaT','nan','None',''): return None
    try:
        import pandas as _pd
        if _pd.isna(v): return None
    except: pass
    s = str(v).strip()
    return s if s and s.lower() not in ('nan','none','-','n/a','','nat') else None

def parse_date(v):
    if v is None: return None
    if str(v) in ('NaT','nan','None',''): return None
    try:
        if hasattr(v,'date'): 
            import pandas as _pd
            if _pd.isna(v): return None
            return v.date()
        return pd.to_datetime(str(v)).date()
    except: return None

def parse_year(v):
    try:
        y = int(str(v).strip()[:4])
        return y if 2000 <= y <= 2030 else None
    except: return None

def get(row, *keys):
    for k in keys:
        for col in row.index:
            if str(col).strip().lower() == k.strip().lower():
                v = clean(row[col])
                if v: return v
    return None

def sanitize_rows(rows: list[dict]) -> list[dict]:
    out: list[dict] = []
    for r in rows:
        rr = {}
        for k, v in r.items():
            if v is None:
                rr[k] = None
                continue
            # Catch pandas/numpy NA + NaT aggressively so psycopg2 never sees them.
            try:
                if pd.isna(v):
                    rr[k] = None
                    continue
            except Exception:
                pass
            s = str(v).strip()
            if not s or s.lower() in ("nat", "nan", "none", "null"):
                rr[k] = None
                continue
            # Convert pandas Timestamp to native python types
            try:
                if isinstance(v, pd.Timestamp):
                    rr[k] = None if pd.isna(v) else v.to_pydatetime()
                    continue
            except Exception:
                pass
            rr[k] = v
        out.append(rr)
    return out

def load():
    log.info("Opening: %s", FILE)
    xl = pd.ExcelFile(str(FILE), engine='openpyxl')
    log.info("Sheets: %s", xl.sheet_names)

    laptops = []

    # ── Delivered ──────────────────────────────────────────
    df = xl.parse('Notebooks (Delivered)')
    for _, row in df.iterrows():
        assigned = get(row,'Associated to','Assigned To','User','Employee')
        laptops.append({
            'asset_id':       get(row,'Asset ID','Asset Tag') or get(row,'Computer Name'),
            'computer_name':  get(row,'Computer Name'),
            'brand':          get(row,'Brand','Manufacturer'),
            'model':          get(row,' Model','Model'),
            'serial_number':  get(row,'Serial Number','S/N'),
            'assigned_to':    assigned,
            'department':     get(row,'Department','Dept'),
            'location':       get(row,'Location','Site','Country'),
            'status':         get(row,'Status') or 'On Hand',
            'lifecycle_stage':'Delivered',
            'purchase_date':  parse_date(row.get('Purchase Date')),
            'production_year':parse_year(row.get('Production Year')),
            'given_date':     parse_date(row.get('Given Date') or row.get('Delivery Date')),
            'condition_notes':get(row,'Notes','Remarks'),
        })
    log.info("Delivered: %d", len(laptops))

    # ── Stock sheets ───────────────────────────────────────
    for sheet in ['Notebooks (Stock)','Notebooks (Stock) (2)']:
        try:
            df = xl.parse(sheet)
            for _, row in df.iterrows():
                cn = get(row,'Computer Name') or get(row,'Serial Number','S/N')
                if not cn: continue
                status_raw = str(get(row,'Status') or '').lower()
                if 'ready' in status_raw and 'not' not in status_raw: stage,status = 'Stock','Stock Ready'
                elif 'not' in status_raw: stage,status = 'Stock','Stock Not Ready'
                else: stage,status = 'Stock','Stock'
                laptops.append({
                    'asset_id':cn,'computer_name':cn,
                    'brand':get(row,'Brand','Manufacturer'),
                    'model':get(row,' Model','Model','model'),
                    'serial_number':get(row,'Serial Number','S/N'),
                    'assigned_to':None,'department':None,
                    'location':get(row,'Location','Site'),
                    'status':status,'lifecycle_stage':stage,
                    'purchase_date':None,'production_year':None,'given_date':None,
                    'condition_notes':get(row,'Condition','Notes','Remarks'),
                })
        except Exception as e:
            log.warning("Sheet %s: %s", sheet, e)

    # ── Maintenance ────────────────────────────────────────
    try:
        df = xl.parse('Notebooks (Ext. Maintenance)')
        for _, row in df.iterrows():
            cn = get(row,'Computer Name') or get(row,'Serial Number')
            if not cn: continue
            laptops.append({
                'asset_id':cn,'computer_name':cn,
                'brand':get(row,'Brand'),'model':get(row,' Model','Model'),
                'serial_number':get(row,'Serial Number','S/N'),
                'assigned_to':None,'department':None,
                'location':get(row,'Location'),
                'status':'Under Maintenance','lifecycle_stage':'Maintenance',
                'purchase_date':None,'production_year':None,'given_date':None,
                'condition_notes':get(row,'State','Notes'),
            })
    except Exception as e:
        log.warning("Maintenance: %s", e)

    # ── Donation ───────────────────────────────────────────
    try:
        df = xl.parse('Old Notebooks (Donation)')
        for _, row in df.iterrows():
            sn = get(row,'Serial Number','S/N')
            if not sn: continue
            laptops.append({
                'asset_id':sn,'computer_name':None,
                'brand':get(row,'Brand'),'model':get(row,'Model ','Model'),
                'serial_number':sn,'assigned_to':None,'department':None,
                'location':None,'status':'Donated','lifecycle_stage':'Donated',
                'purchase_date':None,'production_year':None,'given_date':None,
                'condition_notes':get(row,'Notes','Status'),
            })
    except Exception as e:
        log.warning("Donation: %s", e)

    # ── Other assets (servers, switches, printers) ─────────
    other_assets = []
    for sheet in ['Others (Stock)','Server Room','Others']:
        try:
            df = xl.parse(sheet)
            asset_type = 'Server' if sheet=='Server Room' else 'Network/Other'
            for _, row in df.iterrows():
                t = get(row,'Type') or asset_type
                other_assets.append({
                    'asset_id':    get(row,'Asset ID','Asset Tag'),
                    'asset_type':  t,
                    'manufacturer':get(row,'Manufacturer','Brand'),
                    'model':       get(row,'Model'),
                    'serial_no':   get(row,'Serial No','Serial Number','S/N'),
                    'location':    get(row,'Location','Site'),
                })
        except Exception as e:
            log.warning("Sheet %s: %s", sheet, e)

    # Deduplicate
    seen, deduped = set(), []
    for r in laptops:
        key = (r['asset_id'] or '') + '|' + (r['serial_number'] or '')
        if key not in seen:
            seen.add(key); deduped.append(r)

    log.info("Total laptops: %d (deduped) | Other assets: %d", len(deduped), len(other_assets))

    # Insert
    conn = psycopg2.connect(**PG)
    cur  = conn.cursor()
    cur.execute(CREATE_SQL); conn.commit()

    # Truncate and reload
    cur.execute("TRUNCATE public.assets_laptops RESTART IDENTITY")
    cur.execute("TRUNCATE public.assets_other  RESTART IDENTITY")
    conn.commit()

    deduped = sanitize_rows(deduped)
    other_assets = sanitize_rows(other_assets)

    psycopg2.extras.execute_batch(cur, UPSERT_LAPTOP, deduped, page_size=200)
    conn.commit()
    log.info("✅ Inserted %d rows → assets_laptops", len(deduped))

    if other_assets:
        psycopg2.extras.execute_batch(cur, UPSERT_OTHER, other_assets, page_size=200)
        conn.commit()
        log.info("✅ Inserted %d rows → assets_other", len(other_assets))

    # Summary
    cur.execute("SELECT lifecycle_stage, COUNT(*) FROM public.assets_laptops GROUP BY lifecycle_stage ORDER BY COUNT(*) DESC")
    for row in cur.fetchall(): log.info("  %-20s %d", row[0], row[1])
    cur.execute("SELECT asset_type, COUNT(*) FROM public.assets_other GROUP BY asset_type ORDER BY COUNT(*) DESC")
    for row in cur.fetchall(): log.info("  %-20s %d", row[0], row[1])

    cur.close(); conn.close()
    log.info("🎉 Assets load complete")

if __name__ == "__main__":
    load()