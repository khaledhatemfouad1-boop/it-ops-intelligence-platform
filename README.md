# 🖥️ Sumerge IT Ops Intelligence Platform

> **Real-time IT operations data pipeline** — 7 enterprise data sources → Apache Kafka → PostgreSQL → Power BI  
> Built to unify identity, asset, security, license, and helpdesk data across the Sumerge organization.

![Architecture](docs/architecture.svg)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Producers](#producers)
- [Loaders](#loaders)
- [SQL Schema & Views](#sql-schema--views)
- [Power BI Model](#power-bi-model)
- [Dashboard Demo](#dashboard-demo)
- [Security Notes](#security-notes)
- [Roadmap](#roadmap)

---

## Overview

The Sumerge IT Ops Intelligence Platform is a **production data engineering project** that ingests data from 7 enterprise systems into a unified analytics layer. It solves the core problem that IT operations teams face: data siloed across Active Directory, Dynamics 365, Zabbix, ManageEngine, Kaspersky, Microsoft 365, and Excel spreadsheets — with no single source of truth.

**Key outcomes:**
- 🧑‍💼 **Employee Identity Bridge** — one canonical record per employee joining AD, Entra ID, SDP, assets, and licenses
- 📊 **License Cost Visibility** — which users have which M365 SKUs, surfaced by department
- 💻 **Asset Coverage** — 429 delivered laptops matched to AD accounts; gaps identified
- 🛡️ **Security Posture** — Zabbix active alerts + Kaspersky AV status per device
- 🎫 **Service Desk Analytics** — SLA trends, ticket volume by department and category

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  DATA SOURCES          KAFKA TOPICS          POSTGRESQL VIEWS     │
│                                                                    │
│  Dynamics 365    ──►  dynamics.roles.raw ──► vw_employee_master  │
│  ManageEngine    ──►  sdp.tickets.raw    ──► vw_license_summary  │
│  Active Dir.     ──►  ad.users.raw       ──► vw_ad_entra_gap     │
│  Zabbix          ──►  zabbix.problems    ──► vw_asset_ad_match   │
│  Kaspersky       ──►  kaspersky.devices  ──► vw_service_accounts │
│  MS Graph/M365   ──►  ms365.licenses    ──►  (+ 4 more views)    │
│  Excel Assets    ──►  assets.hardware    │                        │
│                                          ▼                        │
│                                     Power BI                      │
└──────────────────────────────────────────────────────────────────┘
```

Central join key: **`canonical_email`** (lowercase email) — present in every table.

---

## Data Sources

| Source | Method | Topics | Status |
|--------|--------|--------|--------|
| **Dynamics 365** | OAuth2 + Dataverse REST | `dynamics.roles.raw`, `dynamics.userroles.raw` | ✅ Live |
| **ManageEngine SDP** | OAuth2 + REST API | `sdp.tickets.raw` | ✅ Live |
| **Active Directory** | LDAP3 (Simple bind / Kerberos) | `ad.users.raw`, `ad.ous.raw`, `ad.groups.raw`, `ad.computers.raw` | ✅ Live |
| **Zabbix** | Zabbix API + Bearer token | `zabbix.problems.raw`, `zabbix.hosts.raw` | 🔄 VPN required |
| **Kaspersky** | XML export parser | `kaspersky.devices.raw` | 🔄 Export pending |
| **Microsoft 365** | MS Graph API | `ms365.licenses.skus.raw`, `ms365.licenses.users.raw` | 🔄 Permissions pending |
| **Asset Register** | Excel (openpyxl) | `assets.hardware.raw` | ✅ 429 laptops |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Message broker | **Redpanda** (Kafka-compatible) |
| Data store | **PostgreSQL 15** |
| Producers | **Python 3.11** — `confluent-kafka`, `ldap3`, `requests`, `openpyxl` |
| Identity | **Microsoft Entra ID** (OAuth2 client credentials) |
| Directory | **LDAP3** with Kerberos GSSAPI → Simple bind fallback |
| Orchestration | **Docker Compose** |
| Analytics | **Power BI Desktop** + DirectQuery |
| Visualization | Custom HTML dashboard (Chart.js 4.4) |

---

## Project Structure

```
it-ops-intelligence-platform/
├── producers/
│   ├── ad_producer.py             # Active Directory → Kafka (Kerberos/LDAP)
│   ├── dynamics_producer.py       # Dynamics 365 → Kafka (OAuth2)
│   ├── sdp_producer.py            # ManageEngine SDP → Kafka (OAuth2)
│   ├── zabbix_producer.py         # Zabbix → Kafka (API token)
│   ├── graph_licenses_producer.py # MS Graph → Kafka (OAuth2)
├── loaders/
│   ├── load_ad_to_postgres.py     # Kafka consumer: AD topics → PostgreSQL
│   ├── load_assets_excel.py       # Excel parser → PostgreSQL
│   ├── load_kaspersky_xml.py      # XML parser → PostgreSQL
│   ├── load_sdp_to_postgres.py    # Kafka consumer: SDP topics → PostgreSQL
├── sql/
│   ├── 00_init.sql                # Database init + extensions
│   ├── 01_extension_schema.sql    # All tables, indexes, and views
├── powerbi/
│   ├── sumerge-it-ops-theme.json  # Power BI theme (M365 palette)
│   └── POWERBI_MODEL.md           # Full model: star schema, DAX, relationships
├── dashboard/
│   └── index.html                 # Standalone HTML demo dashboard
├── docs/
│   ├── architecture.svg           # Architecture diagram
│   └── RUNBOOK.md                 # Step-by-step deployment guide
├── docker-compose.yml             # Redpanda + PostgreSQL
├── requirements.txt
├── .env.example                   # Template — never commit .env
└── .gitignore
```

---

## Quick Start

### Prerequisites

- Docker Desktop
- Python 3.11+
- VPN access (for Zabbix and Active Directory)
- Azure app registration with Dataverse + MS Graph permissions

### 1. Clone & configure

```bash
git clone https://github.com/YOUR_USERNAME/it-ops-intelligence-platform.git
cd it-ops-intelligence-platform
cp .env.example .env
# Edit .env with your credentials
```

### 2. Start infrastructure

```bash
docker compose up -d
# Verify:
docker compose ps
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Initialize database

```bash
psql -h localhost -p 5432 -U postgres -c "CREATE DATABASE itopsdb;"
psql -h localhost -p 5432 -U postgres -d itopsdb -f sql/00_init.sql
psql -h localhost -p 5432 -U postgres -d itopsdb -f sql/01_extension_schema.sql
```

### 5. Run producers (in order)

```bash
# Active Directory (requires VPN or internal network)
python producers/ad_producer.py

# Dynamics 365
python producers/dynamics_producer.py

# ManageEngine SDP
python producers/sdp_producer.py

# Zabbix (requires VPN to 10.0.0.240)
python producers/zabbix_producer.py --mode all

# Microsoft 365 licenses
python producers/graph_licenses_producer.py

# Assets from Excel
python loaders/load_assets_excel.py
```

### 6. Load to PostgreSQL

```bash
python loaders/load_ad_to_postgres.py
python loaders/load_sdp_to_postgres.py
# Kafka consumers will also auto-load Zabbix and Kaspersky data
```

### 7. Connect Power BI

1. Open Power BI Desktop
2. **Get Data → PostgreSQL** → `localhost:5432` → `itopsdb`
3. Import theme: **View → Browse for themes** → `powerbi/sumerge-it-ops-theme.json`
4. See `powerbi/POWERBI_MODEL.md` for the full star-schema model and DAX measures

---

## Producers

### `ad_producer.py` — Active Directory

Connects via LDAP3 using a Kerberos GSSAPI → Simple bind fallback chain. Extracts Users, OUs, Groups, and Computer objects into four Kafka topics.

```bash
# Test a single user lookup (no Kafka required)
python producers/ad_producer.py --lookup khatem --no-kafka

# Full sync
python producers/ad_producer.py
```

**Auth chain:** Kerberos GSSAPI (Linux) → NTLM → Simple bind (Windows)

### `zabbix_producer.py` — Zabbix Monitoring

Uses Zabbix API token auth. **Important:** `apiinfo.version` must be called *without* the Authorization header — this is handled automatically in the fixed version.

```bash
python producers/zabbix_producer.py --mode all     # problems + hosts
python producers/zabbix_producer.py --mode problems
python producers/zabbix_producer.py --mode hosts
```

### `dynamics_producer.py` — Dynamics 365

Paginates through Dataverse OData API to pull all security roles and user-role assignments.

```bash
python producers/dynamics_producer.py --mode all
```

### `graph_licenses_producer.py` — Microsoft 365 Licenses

Requires MS Graph Application permissions: `Organization.Read.All`, `User.Read.All`, `Directory.Read.All` (grant admin consent in Azure Portal).

```bash
python producers/graph_licenses_producer.py
```

---

## Loaders

| Loader | Source | Target Table |
|--------|--------|-------------|
| `load_ad_to_postgres.py` | Kafka (4 AD topics) | `ad_users`, `ad_computers`, `ad_groups`, `ad_ous` |
| `load_assets_excel.py` | Excel file | `assets_laptops` |
| `load_kaspersky_xml.py` | Kaspersky XML export | `kaspersky_devices` |
| `load_sdp_to_postgres.py` | Kafka `sdp.tickets.raw` | `sdp_tickets` |

---

## SQL Schema & Views

The schema in `sql/01_extension_schema.sql` creates **9 raw tables** and **8 analytical views**:

### Key Views

| View | Purpose |
|------|---------|
| `vw_employee_master` | **Identity bridge** — one row per employee, joins AD + Entra |
| `vw_license_summary` | M365 license SKUs: total / consumed / available / cost |
| `vw_license_by_department` | License consumption grouped by AD department |
| `vw_ad_entra_gap` | Users in AD but missing from Entra (and vice versa) |
| `vw_service_accounts` | Accounts with no email / no manager (service/orphan accounts) |
| `vw_asset_ad_match` | Laptop → AD user → Zabbix host coverage |
| `vw_users_no_laptop` | Employees with no assigned hardware |
| `vw_zabbix_active_problems` | Active Zabbix alerts enriched with host metadata |

---

## Power BI Model

See [`powerbi/POWERBI_MODEL.md`](powerbi/POWERBI_MODEL.md) for:

- Full star schema with `vw_employee_master` as the central fact table
- 10 table relationships
- 20+ DAX measures (headcount, license cost, SLA %, ticket volume, etc.)
- Conditional formatting rules
- 7 recommended report pages

**Theme:** Microsoft 365 color palette (`#0078D4` primary), Segoe UI font.

---

## Dashboard Demo

A standalone HTML dashboard (`dashboard/index.html`) demonstrates the key metrics using Chart.js.  
👉 **[Live Demo](https://YOUR_USERNAME.github.io/it-ops-intelligence-platform/dashboard/)**

It includes:
- Executive KPI summary
- License utilization by SKU
- Ticket trends (SDP)
- Asset coverage chart
- Active alerts summary

---

## Security Notes

> ⚠️ All credentials must be stored in `.env` only — **never commit secrets to git.**

- `.env` is in `.gitignore`
- Use a **read-only service account** for Active Directory LDAP queries
- Rotate `CLIENT_SECRET`, `SDP_CLIENT_SECRET`, `ZABBIX_API_TOKEN`, and `AD_PASSWORD` regularly
- The Zabbix producer uses API token authentication (no user/password stored)
- MS Graph uses client credentials flow — grant only required permissions

---

## Roadmap

- [ ] Kafka consumer auto-loader for all topics (replace manual scripts)
- [ ] Scheduled runs with Apache Airflow or Windows Task Scheduler
- [ ] Kaspersky XML live import via Kaspersky Security Center API
- [ ] Alerting: send Power Automate notification when Zabbix disaster-level event fires
- [ ] GitHub Actions CI: lint all Python producers on push
- [ ] Anonymized demo dataset for public portfolio version

---

## Author

**Khaled Hatem** — Associate Infrastructure Engineer, Sumerge  
[LinkedIn](https://linkedin.com/in/YOUR_PROFILE) · [GitHub](https://github.com/YOUR_USERNAME)

---

*Built as part of an internal IT operations modernization initiative. Data sources and credentials have been removed for public sharing.*
