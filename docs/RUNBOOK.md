# Sumerge IT Ops Intelligence Platform — Complete Runbook
## Every file, every step, in order

---

## CURRENT STATUS AT A GLANCE

| Component | Status | File |
|---|---|---|
| Docker (Redpanda + Postgres) | ✅ Built | `docker-compose.yml` |
| Dynamics 365 producer | ✅ Working | `producers/dynamics_producer.py` |
| ManageEngine SDP OAuth2 | ✅ Token saved | `auth/token_manager.py` |
| SDP tickets producer | ✅ Working | `producers/sdp_producer.py` |
| Asset Excel loader | ✅ Loaded (429 laptops) | `loader/load_assets_excel.py` |
| MS Graph licenses producer | ✅ Ready | `producers/graph_licenses_producer.py` |
| Zabbix producer | ✅ Ready (needs VPN) | `producers/zabbix_producer.py` |
| AD LDAP producer | ✅ Ready | `producers/ad_producer.py` |
| Kaspersky XML loader | ⏳ Needs XML upload | `loader/load_kaspersky_xml.py` |
| SQL schema (base) | ⏳ Needs Postgres running | `sql/00_init.sql` |
| SQL schema (extension) | ⏳ Needs 00 first | `sql/01_extension_schema.sql` |
| HTML Dashboard | ✅ Delivered | `index.html` |
| Power BI Theme | ✅ Ready | `sumerge-it-ops-theme.json` |
| Power BI Model | ✅ Ready | `POWERBI_MODEL.md` |

---

## STEP 0 — START INFRASTRUCTURE

```powershell
# Start Postgres (if not running)
Start-Service -Name "postgresql-x64-18"

# Start Redpanda (Kafka)
cd C:\Users\khatem\it-ops-intelligence-platform
docker-compose up -d

# Verify containers
docker ps
# Should show: redpanda, postgres (itopsdb)
```

---

## STEP 1 — RUN SQL SCHEMAS

**Run once. Creates all tables and views.**

```powershell
$psql = "C:\Program Files\PostgreSQL\18\bin\psql.exe"
$conn = "-h localhost -U postgres -d itopsdb"

# Base schema (laptops, sdp_tickets, dynamics tables)
& $psql $conn.Split() -f sql/00_init.sql

# Extension schema (AD, MS365 licenses, Zabbix, Kaspersky)
& $psql $conn.Split() -f sql/01_extension_schema.sql
```

**Verify:**
```powershell
& $psql $conn.Split() -c "\dt"
# Should list: assets_laptops, sdp_tickets, dynamics_roles,
#              ad_users, ad_computers, ms365_license_skus,
#              ms365_user_licenses, zabbix_hosts, kaspersky_devices
```

---

## STEP 2 — LOAD ASSET EXCEL (IT_Hardware_List.xlsx)

**Loads 429 delivered laptops + full lifecycle inventory into Postgres.**

```powershell
pip install pandas openpyxl psycopg2-binary confluent-kafka python-dotenv

python loader/load_assets_excel.py
```

**What it loads:**
- 429 delivered notebooks → `assets_laptops`
- 52 stock (old) + 60 stock (new) units
- 9 under external maintenance
- 54 scrapped, 17 donated

**Expected output:**
```
✅  Loaded 429 delivered laptops
✅  Full lifecycle: 537 notebook records total
```

---

## STEP 3 — DYNAMICS 365 (Entra roles + users)

```powershell
python producers/dynamics_producer.py
python loader/load_dynamics_to_postgres.py
```

**What it loads:**
- 6,402 Dynamics roles → `dynamics_roles`
- ~3,999 user-role pairs → `dynamics_user_roles`

---

## STEP 4 — MANAGEENGINE SDP TICKETS

**Token already saved from setup. Just run:**

```powershell
# Pull last 90 days of tickets
python producers/sdp_producer.py --days 90

# Load from Kafka into Postgres
python loader/load_sdp_to_postgres.py
```

**What it loads:** All helpdesk tickets with requester, category, priority, SLA status, technician, resolution time → `sdp_tickets`

---

## STEP 5 — MICROSOFT GRAPH (M365 Licenses)

**First: add Graph API permissions to your Azure app (one time):**

1. Go to: https://portal.azure.com
2. Azure Active Directory → App registrations → your app (`ee2f64bd...`)
3. API permissions → Add permission → Microsoft Graph → **Application permissions**
4. Add these three:
   - `Organization.Read.All`
   - `User.Read.All`
   - `Directory.Read.All`
5. Click **Grant admin consent for Sumerge**

**Then run:**
```powershell
python producers/graph_licenses_producer.py
python loader/load_graph_licenses_to_postgres.py
```

**What it loads:**
- All M365 license SKUs (total / consumed / available) → `ms365_license_skus`
- Per-user license assignments → `ms365_user_licenses`

---

## STEP 6 — ACTIVE DIRECTORY (LDAP)

**Install dependency:**
```powershell
pip install ldap3 gssapi
```

**Run (domain-joined PC — uses your Windows login automatically):**
```powershell
# Test single user first
python producers/ad_producer.py --lookup khatem --no-kafka

# Full sync to Kafka + Postgres
python producers/ad_producer.py --mode all

# Load from Kafka into Postgres
python loader/load_ad_to_postgres.py
```

**What it loads:**
- All AD users (employees + service accounts) → `ad_users`
- All computer objects → `ad_computers`
- All security/distribution groups → `ad_groups`
- Full OU tree → `ad_ous`

**If Kerberos fails** (not on domain-joined machine), add to `.env`:
```
AD_USER=khatem@sumergedc.local
AD_PASSWORD=your_password_here
```
Script falls back to NTLM automatically.

---

## STEP 7 — ZABBIX (Infrastructure Monitoring)

**Requires VPN to reach 10.0.0.240.**

```powershell
# Connect VPN first, then:
python producers/zabbix_producer.py --mode all

# Load from Kafka into Postgres
python loader/load_zabbix_to_postgres.py
```

**What it loads:**
- Active problems (last 7 days) with severity, host, duration → `zabbix_problems`
- All monitored hosts with status/availability → `zabbix_hosts`

**Topics produced:**
- `zabbix.problems.raw`
- `zabbix.hosts.raw`

---

## STEP 8 — KASPERSKY (Endpoint Security)

**⏳ PENDING: Upload your Kaspersky XML export first.**

To export from Kaspersky Security Center:
1. KSC Admin Console → **Managed devices**
2. Right-click any group → **Export to file** → XML
3. Upload the XML file here

Then run:
```powershell
python loader/load_kaspersky_xml.py --file path\to\your_export.xml
```

**What it loads:**
- All managed endpoints with protection status, AV version, last scan, threats detected → `kaspersky_devices`

---

## STEP 9 — POWER BI SETUP

### 9a. Apply Theme
1. Open Power BI Desktop
2. View → Themes → **Browse for themes**
3. Select `sumerge-it-ops-theme.json`

### 9b. Connect to Postgres
1. Get Data → **PostgreSQL**
2. Server: `localhost`  Database: `itopsdb`
3. DirectQuery mode (for live data)
4. Load these tables/views:

| Table/View | Purpose |
|---|---|
| `vw_employee_master` | Central identity bridge |
| `assets_laptops` | Hardware inventory |
| `ad_users` | AD directory |
| `ad_computers` | Computer objects |
| `ad_ous` | OU structure |
| `ms365_license_skus` | License SKU totals |
| `ms365_user_licenses` | Per-user licenses |
| `sdp_tickets` | Helpdesk tickets |
| `dynamics_roles` | D365 roles |
| `dynamics_user_roles` | User-role assignments |
| `zabbix_problems` | Active alerts |
| `zabbix_hosts` | Monitored hosts |
| `kaspersky_devices` | Endpoint protection |
| `vw_license_summary` | License utilization |
| `vw_ad_entra_gap` | AD vs Entra discrepancies |
| `vw_service_accounts` | Service account audit |
| `vw_asset_ad_match` | Asset ↔ computer match |
| `vw_zabbix_active_problems` | Active alerts enriched |

### 9c. Create Relationships
See `POWERBI_MODEL.md` for the full relationship map.

Key relationships to create manually:
```
ms365_user_licenses[email]      → vw_employee_master[canonical_email]  (Many→One)
assets_laptops[assigned_email]  → vw_employee_master[canonical_email]  (Many→One)
sdp_tickets[requester_email]    → vw_employee_master[canonical_email]  (Many→One)
ms365_user_licenses[sku_id]     → ms365_license_skus[sku_id]           (Many→One)
zabbix_problems[host_technical] → zabbix_hosts[hostname]               (Many→One)
kaspersky_devices[device_name]  → ad_computers[computer_name]          (Many→One)
```

### 9d. Paste DAX Measures
Open `POWERBI_MODEL.md` → copy each DAX block → New Measure in Power BI.

---

## FULL FILE INVENTORY

```
it-ops-intelligence-platform/
│
├── docker-compose.yml                    # Redpanda + Postgres
├── .env                                  # All secrets (never commit)
├── .env.example                          # Template (safe to commit)
├── requirements.txt                      # All Python deps
│
├── auth/
│   └── token_manager.py                  # SDP OAuth2 token manager
│
├── producers/
│   ├── dynamics_producer.py              # Dynamics 365 → Kafka
│   ├── sdp_producer.py                   # ManageEngine SDP → Kafka
│   ├── graph_licenses_producer.py        # MS Graph licenses → Kafka
│   ├── ad_producer.py                    # Active Directory → Kafka
│   └── zabbix_producer.py               # Zabbix → Kafka
│
├── loader/
│   ├── load_assets_excel.py              # IT_Hardware_List.xlsx → Postgres
│   ├── load_sdp_to_postgres.py           # Kafka sdp.tickets → Postgres
│   ├── load_dynamics_to_postgres.py      # Kafka dynamics.* → Postgres
│   ├── load_graph_licenses_to_postgres.py# Kafka ms365.* → Postgres
│   ├── load_ad_to_postgres.py            # Kafka ad.* → Postgres
│   ├── load_zabbix_to_postgres.py        # Kafka zabbix.* → Postgres
│   └── load_kaspersky_xml.py             # Kaspersky XML → Postgres
│
├── sql/
│   ├── 00_init.sql                       # Base schema
│   └── 01_extension_schema.sql          # AD, M365, Zabbix, Kaspersky tables + views
│
├── index.html                            # HTML Dashboard (GitHub Pages)
├── sumerge-it-ops-theme.json            # Power BI theme
├── POWERBI_MODEL.md                      # Relationships + all DAX measures
└── AD_KERBEROS_SETUP.md                  # AD integration guide
```

---

## ENVIRONMENT VARIABLES NEEDED

```env
# Kafka
KAFKA_BROKER=localhost:9092

# Postgres
PG_HOST=localhost
PG_PORT=5432
PG_DB=itopsdb
PG_USER=postgres
PG_PASSWORD=your_postgres_password

# Dynamics 365 / Entra
TENANT_ID=4714a8d7-fffd-4c0f-9a98-8698ad2fd930
CLIENT_ID=ee2f64bd-98d1-4b42-b016-e1dd5ba40f48
CLIENT_SECRET=<regenerate — was shared in chat>
DATAVERSE_URL=https://sumerge.crm4.dynamics.com

# ManageEngine SDP
SDP_CLIENT_ID=1000.DM86PXGZRQES47VGV3G1VRICIYZ1VU
SDP_CLIENT_SECRET=<regenerate — was shared in chat>
SDP_REFRESH_TOKEN=<auto-filled by token_manager.py --init>

# AD (only needed if Kerberos fails)
AD_USER=khatem@sumergedc.local
AD_PASSWORD=<your AD password — only in .env, never in chat>

# Zabbix (VPN required)
ZABBIX_URL=http://10.0.0.240/zabbix
ZABBIX_API_TOKEN=7e4a292397caf9662cc365b58ee45785694622f4e57cfee07155f28879cab01b
```

> ⚠️ **IMPORTANT:** Regenerate `CLIENT_SECRET` and `SDP_CLIENT_SECRET` in their
> respective portals — both were accidentally shared in this conversation.

---

## RECOMMENDED RUN ORDER (fresh start)

```
Step 0:  docker-compose up -d
Step 1:  psql → 00_init.sql → 01_extension_schema.sql
Step 2:  load_assets_excel.py
Step 3:  dynamics_producer.py + load_dynamics_to_postgres.py
Step 4:  sdp_producer.py + load_sdp_to_postgres.py
Step 5:  graph_licenses_producer.py + load_graph_licenses_to_postgres.py
Step 6:  ad_producer.py + load_ad_to_postgres.py
Step 7:  (VPN) zabbix_producer.py + load_zabbix_to_postgres.py
Step 8:  (XML upload) load_kaspersky_xml.py --file export.xml
Step 9:  Power BI → connect → apply theme → create relationships → paste DAX
```
