# Power BI — Full Data Model
## Sumerge IT Ops Intelligence Platform
### Sources: AD · Entra/D365 · SDP · IT Assets (429 laptops) · MS365 Licenses · Zabbix · Kaspersky

---

## Complete Star Schema

```
                         ┌──────────────────────────┐
                         │    vw_employee_master     │
                         │   (canonical_email) 🔑    │
                         │  Central identity bridge  │
                         │  Built from: AD + Entra   │
                         └────────────┬─────────────-┘
                                      │
         ┌──────────────┬─────────────┼──────────────┬──────────────┐
         │              │             │              │              │
         ▼              ▼             ▼              ▼              ▼
  ┌────────────┐ ┌───────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐
  │assets_     │ │ms365_user_│ │sdp_      │ │dynamics_ │ │ad_users      │
  │laptops     │ │licenses   │ │tickets   │ │user_roles│ │(full AD dir) │
  │(429 live + │ │(per-user  │ │(helpdesk)│ │(D365     │               │
  │108 stock/  │ │ SKU assignments)       │ │ roles)   │ └──────────────┘
  │scrap/donate│ └─────┬─────┘ └──────────┘ └────┬─────┘
  └─────┬──────┘       │                          │
        │              ▼                          ▼
        │       ┌──────────────┐          ┌──────────────┐
        │       │ms365_license_│          │dynamics_roles│
        │       │skus          │          │(6,402 roles) │
        │       │(SKU totals)  │          └──────────────┘
        ▼       └──────────────┘
  ┌─────────────┐
  │ad_computers │ ◄── kaspersky_devices (device_name → computer_name)
  │(AD objects) │
  └─────────────┘

  ┌─────────────────┐     ┌──────────────────┐
  │zabbix_problems  │────▶│zabbix_hosts      │
  │(active alerts)  │     │(monitored hosts) │
  └─────────────────┘     └──────────────────┘

  ┌─────────────┐
  │ad_ous        │  (standalone hierarchy — used in treemap)
  └─────────────┘
```

---

## Relationships Table (create manually in Power BI)

| From Table | From Column | To Table | To Column | Cardinality | Cross-filter |
|---|---|---|---|---|---|
| `ms365_user_licenses` | `email` | `vw_employee_master` | `canonical_email` | Many → One | Single |
| `ms365_user_licenses` | `sku_id` | `ms365_license_skus` | `sku_id` | Many → One | Single |
| `sdp_tickets` | `requester_email` | `vw_employee_master` | `canonical_email` | Many → One | Single |
| `dynamics_user_roles` | `user_upn` | `vw_employee_master` | `canonical_email` | Many → One | Single |
| `dynamics_user_roles` | `role_id` | `dynamics_roles` | `role_id` | Many → One | Single |
| `assets_laptops` | `assigned_email` | `vw_employee_master` | `canonical_email` | Many → One | Single |
| `assets_laptops` | `asset_tag` | `ad_computers` | `computer_name` | One → One | Single |
| `kaspersky_devices` | `device_name` | `ad_computers` | `computer_name` | Many → One | Single |
| `zabbix_problems` | `host_technical` | `zabbix_hosts` | `hostname` | Many → One | Single |
| `ad_users` | `email` | `vw_employee_master` | `canonical_email` | One → One | Single |

---

## Power BI Pages — Recommended Layout

### Page 1 — Executive Overview
KPI cards: Active Employees · Open Tickets · Unprotected Devices · Active Alerts · License Utilization % · Laptops Delivered · AD/Entra Gap
Charts: Monthly delivery trend (line) · Dept headcount (bar) · Asset lifecycle donut

### Page 2 — IT Assets (429 Laptops + Full Inventory)
KPI: Delivered 429 · Stock Ready 27 · Maintenance 9 · Scrapped 54 · Donated 17 · Server Room 31
Charts: Brand distribution donut · Age risk column · Dept vs location stacked bar · Top models bar
Table: Recent deliveries with asset tag, brand, model, assigned to, dept, site, date

### Page 3 — Employee Identity (AD + Entra)
KPI: Active Users · Service Accounts · Disabled · Stale (>90d) · Pwd Never Expires · AD/Entra Gap
Charts: OU treemap · Dept distribution bar · Account status donut
Table: AD/Entra gap — users in AD missing in Entra or vice versa

### Page 4 — M365 Licenses
KPI: Total Purchased · Total Consumed · Available · Utilization % · Users No License · Multi-Licensed
Charts: SKU utilization horizontal bar · Dept × license matrix · Consumed vs available per SKU

### Page 5 — Helpdesk (SDP Tickets)
KPI: Open · Overdue · SLA Compliance % · Avg Resolution Hours · Closed This Month
Charts: Tickets by category bar · SLA compliance gauge · Technician performance matrix · Trend line

### Page 6 — Infrastructure (Zabbix + Kaspersky)
KPI: Active Alerts · Disaster · Unacknowledged · Unprotected Devices · Critical · Outdated AV
Charts: Alert severity donut · Hosts availability bar · Kaspersky protection status bar · Threats timeline

### Page 7 — Governance & Access
KPI: Orphan Roles · Over-privileged Users · Service Accounts · Stale Accounts · Users No Laptop
Charts: Role count per user histogram · Dept × role heatmap · Accounts needing review table

---

## DAX Measures — All Sources

### Assets (IT_Hardware_List.xlsx — 429 laptops)

```dax
Laptops Delivered =
    CALCULATE(COUNTROWS(assets_laptops), assets_laptops[status] = "On Hand")

Laptops In Stock Ready =
    CALCULATE(COUNTROWS(assets_laptops), assets_laptops[status] = "Stock Ready")

Laptops Under Maintenance =
    CALCULATE(COUNTROWS(assets_laptops), assets_laptops[lifecycle_stage] = "External Maintenance")

Laptops Scrapped =
    CALCULATE(COUNTROWS(assets_laptops), assets_laptops[lifecycle_stage] = "Scrapped")

Laptops Donated =
    CALCULATE(COUNTROWS(assets_laptops), assets_laptops[lifecycle_stage] = "Donated")

Devices Critical Age =
    CALCULATE(COUNTROWS(assets_laptops), assets_laptops[production_year] <= 2020,
              NOT ISBLANK(assets_laptops[production_year]))

Devices Aging =
    CALCULATE(COUNTROWS(assets_laptops),
              assets_laptops[production_year] >= 2021,
              assets_laptops[production_year] <= 2022)

Devices Active =
    CALCULATE(COUNTROWS(assets_laptops),
              assets_laptops[production_year] >= 2023,
              assets_laptops[production_year] <= 2024)

Egypt Laptops = CALCULATE(COUNTROWS(assets_laptops), assets_laptops[site] = "Egypt")
KSA Laptops   = CALCULATE(COUNTROWS(assets_laptops), assets_laptops[site] = "KSA")

Asset Refresh Urgency % =
    DIVIDE([Devices Critical Age], [Laptops Delivered], 0) * 100
```

### Kaspersky (Endpoint Security)

```dax
Unprotected Devices =
    CALCULATE(COUNTROWS(kaspersky_devices),
              kaspersky_devices[protection_status] = "Unprotected")

Critical Protection =
    CALCULATE(COUNTROWS(kaspersky_devices),
              kaspersky_devices[protection_status] = "Critical")

Warning Devices =
    CALCULATE(COUNTROWS(kaspersky_devices),
              kaspersky_devices[protection_status] = "Warning")

Protected Devices =
    CALCULATE(COUNTROWS(kaspersky_devices),
              kaspersky_devices[protection_status] = "OK")

Kaspersky Coverage % =
    DIVIDE([Protected Devices], COUNTROWS(kaspersky_devices), 0) * 100

Outdated AV Bases =
    CALCULATE(COUNTROWS(kaspersky_devices),
              kaspersky_devices[av_bases_date] < TODAY() - 3)

Not Scanned 7 Days =
    CALCULATE(COUNTROWS(kaspersky_devices),
              kaspersky_devices[last_scan] < TODAY() - 7)

Total Threats Detected = SUM(kaspersky_devices[threats_detected])
Total Vulnerabilities  = SUM(kaspersky_devices[vulnerabilities_count])

RTP Disabled =
    CALCULATE(COUNTROWS(kaspersky_devices),
              kaspersky_devices[real_time_protection] = FALSE())
```

### Zabbix (Infrastructure Monitoring)

```dax
Active Alerts =
    CALCULATE(COUNTROWS(zabbix_problems),
              zabbix_problems[status] = "PROBLEM")

Disaster Alerts =
    CALCULATE(COUNTROWS(zabbix_problems),
              zabbix_problems[severity_level] = 5,
              zabbix_problems[status] = "PROBLEM")

High Alerts =
    CALCULATE(COUNTROWS(zabbix_problems),
              zabbix_problems[severity_level] = 4,
              zabbix_problems[status] = "PROBLEM")

Unacknowledged Alerts =
    CALCULATE(COUNTROWS(zabbix_problems),
              zabbix_problems[acknowledged] = FALSE(),
              zabbix_problems[status] = "PROBLEM")

Hosts Available =
    CALCULATE(COUNTROWS(zabbix_hosts),
              zabbix_hosts[available] = "Available")

Hosts Unavailable =
    CALCULATE(COUNTROWS(zabbix_hosts),
              zabbix_hosts[available] = "Unavailable")

Host Availability % =
    DIVIDE([Hosts Available], COUNTROWS(zabbix_hosts), 0) * 100

Avg Alert Age Hours =
    AVERAGEX(
        FILTER(zabbix_problems, zabbix_problems[status] = "PROBLEM"),
        DATEDIFF(zabbix_problems[clock], NOW(), HOUR)
    )
```

### M365 Licenses (Microsoft Graph)

```dax
Total Licenses Purchased = SUM(ms365_license_skus[enabled_units])
Total Licenses Consumed  = SUM(ms365_license_skus[consumed_units])
Available Licenses       = SUM(ms365_license_skus[available_units])

License Utilization % =
    DIVIDE([Total Licenses Consumed], [Total Licenses Purchased], 0) * 100

Unlicensed Active Users =
    CALCULATE(COUNTROWS(vw_employee_master),
              vw_employee_master[license_count] = 0,
              vw_employee_master[entra_enabled] = TRUE())

Users Multi Licensed =
    COUNTROWS(
        FILTER(
            ADDCOLUMNS(
                SUMMARIZE(ms365_user_licenses, ms365_user_licenses[email]),
                "LicCount", CALCULATE(COUNTROWS(ms365_user_licenses))
            ),
            [LicCount] >= 2
        )
    )

License Waste % =
    DIVIDE([Available Licenses], [Total Licenses Purchased], 0) * 100
```

### Active Directory

```dax
Active Employees =
    CALCULATE(COUNTROWS(ad_users),
              ad_users[enabled] = TRUE(),
              ad_users[is_service_account] = FALSE())

Service Accounts =
    CALCULATE(COUNTROWS(ad_users), ad_users[is_service_account] = TRUE())

Disabled Accounts =
    CALCULATE(COUNTROWS(ad_users), ad_users[enabled] = FALSE())

Stale Accounts =
    CALCULATE(COUNTROWS(ad_users),
              ad_users[last_logon] < TODAY() - 90,
              ad_users[enabled] = TRUE(),
              ad_users[is_service_account] = FALSE())

Pwd Never Expires =
    CALCULATE(COUNTROWS(ad_users),
              ad_users[pwd_no_expire] = TRUE(),
              ad_users[is_service_account] = FALSE())

AD Entra Gap =
    CALCULATE(COUNTROWS(vw_ad_entra_gap),
              vw_ad_entra_gap[sync_status] <> "Synced")

Users Without Laptop = COUNTROWS(vw_users_no_laptop)
```

### SDP Tickets (ManageEngine)

```dax
Open Tickets =
    CALCULATE(COUNTROWS(sdp_tickets), sdp_tickets[status] = "Open")

Overdue Tickets =
    CALCULATE(COUNTROWS(sdp_tickets),
              sdp_tickets[is_overdue] = TRUE(),
              sdp_tickets[status] = "Open")

SLA Compliance % =
    DIVIDE(
        CALCULATE(COUNTROWS(sdp_tickets), sdp_tickets[sla_violated] = FALSE()),
        COUNTROWS(sdp_tickets), 0
    ) * 100

Avg Resolution Hours =
    AVERAGEX(
        FILTER(sdp_tickets, NOT ISBLANK(sdp_tickets[resolved_time])),
        DATEDIFF(sdp_tickets[created_time], sdp_tickets[resolved_time], HOUR)
    )

Tickets This Month =
    CALCULATE(COUNTROWS(sdp_tickets),
              MONTH(sdp_tickets[created_time]) = MONTH(TODAY()),
              YEAR(sdp_tickets[created_time]) = YEAR(TODAY()))
```

### Dynamics 365 (Governance)

```dax
Orphan Roles =
    CALCULATE(COUNTROWS(dynamics_roles),
              ISBLANK(RELATED(dynamics_user_roles[user_id])))

Users Excessive Roles =
    COUNTROWS(
        FILTER(
            ADDCOLUMNS(
                SUMMARIZE(dynamics_user_roles, dynamics_user_roles[user_id]),
                "RoleCount", CALCULATE(COUNTROWS(dynamics_user_roles))
            ),
            [RoleCount] > 5
        )
    )
```

---

## Conditional Formatting Rules

| Measure | Green ✅ | Yellow ⚠️ | Red 🔴 |
|---|---|---|---|
| License Utilization % | < 85% | 85–95% | > 95% |
| Kaspersky Coverage % | > 95% | 85–95% | < 85% |
| Host Availability % | > 98% | 90–98% | < 90% |
| SLA Compliance % | > 90% | 80–90% | < 80% |
| Disaster Alerts | = 0 | — | ≥ 1 |
| Unprotected Devices | = 0 | 1–5 | > 5 |
| AD/Entra Gap | = 0 | 1–10 | > 10 |
| Stale Accounts | = 0 | 1–20 | > 20 |
| Asset Refresh Urgency % | < 10% | 10–25% | > 25% |
