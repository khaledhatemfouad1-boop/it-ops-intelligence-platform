-- ============================================================
-- IT Ops Intelligence Platform – Full Database Schema
-- Database: itopsdb  |  Sumerge
-- ============================================================

-- ──────────────────────────────────────────────────────────────
-- 1. DYNAMICS 365 ROLES
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dynamics_roles (
    role_id          UUID PRIMARY KEY,
    role_name        TEXT NOT NULL,
    business_unit_id UUID,
    business_unit    TEXT,
    is_inherited     BOOLEAN DEFAULT FALSE,
    component_state  INT,
    created_on       TIMESTAMPTZ,
    modified_on      TIMESTAMPTZ,
    raw_payload      JSONB,
    ingested_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dynamics_user_roles (
    id               BIGSERIAL PRIMARY KEY,
    user_id          UUID NOT NULL,
    user_name        TEXT,
    full_name        TEXT,
    role_id          UUID NOT NULL,
    role_name        TEXT,
    business_unit    TEXT,
    assigned_on      TIMESTAMPTZ,
    ingested_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (user_id, role_id)
);

-- ──────────────────────────────────────────────────────────────
-- 2. SERVICEDESK PLUS TICKETS
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sdp_tickets (
    ticket_id                  BIGINT PRIMARY KEY,
    subject                    TEXT,
    status                     TEXT,
    priority                   TEXT,
    urgency                    TEXT,
    impact                     TEXT,
    category                   TEXT,
    subcategory                TEXT,
    "group"                    TEXT,
    technician                 TEXT,
    requester                  TEXT,
    site                       TEXT,
    department                 TEXT,
    mode                       TEXT,
    created_time               TIMESTAMPTZ,
    due_by_time                TIMESTAMPTZ,
    resolved_time              TIMESTAMPTZ,
    closed_time                TIMESTAMPTZ,
    first_response_time        TIMESTAMPTZ,
    is_overdue                 BOOLEAN DEFAULT FALSE,
    is_first_response_overdue  BOOLEAN DEFAULT FALSE,
    sla_violated               BOOLEAN DEFAULT FALSE,
    source_raw                 TEXT,
    ingested_at                TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sdp_tickets_status    ON sdp_tickets(status);
CREATE INDEX IF NOT EXISTS idx_sdp_tickets_priority  ON sdp_tickets(priority);
CREATE INDEX IF NOT EXISTS idx_sdp_tickets_created   ON sdp_tickets(created_time);
CREATE INDEX IF NOT EXISTS idx_sdp_tickets_technician ON sdp_tickets(technician);

-- ──────────────────────────────────────────────────────────────
-- 3. LAPTOPS & ASSET INVENTORY
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assets_laptops (
    id               BIGSERIAL PRIMARY KEY,
    asset_tag        TEXT UNIQUE NOT NULL,
    serial_number    TEXT,
    brand            TEXT,
    model            TEXT,
    processor        TEXT,
    ram_gb           INT,
    storage_gb       INT,
    os               TEXT,
    os_version       TEXT,
    status           TEXT,          -- Delivered, In Stock, Under Repair, Retired
    assigned_to      TEXT,          -- Employee name
    employee_id      TEXT,
    department       TEXT,
    site             TEXT,
    delivery_date    DATE,
    purchase_date    DATE,
    warranty_expiry  DATE,
    notes            TEXT,
    ingested_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_laptops_status     ON assets_laptops(status);
CREATE INDEX IF NOT EXISTS idx_laptops_department ON assets_laptops(department);
CREATE INDEX IF NOT EXISTS idx_laptops_assigned   ON assets_laptops(assigned_to);

-- ──────────────────────────────────────────────────────────────
-- 4. ZABBIX PROBLEMS / ALERTS
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS zabbix_problems (
    problem_id       BIGINT PRIMARY KEY,
    event_id         BIGINT,
    host_name        TEXT,
    host_group       TEXT,
    ip_address       TEXT,
    severity         TEXT,   -- Not classified, Info, Warning, Average, High, Disaster
    severity_level   INT,    -- 0-5
    problem_name     TEXT,
    status           TEXT,   -- PROBLEM, RESOLVED
    acknowledged     BOOLEAN DEFAULT FALSE,
    suppressed       BOOLEAN DEFAULT FALSE,
    clock            TIMESTAMPTZ,
    recovered_at     TIMESTAMPTZ,
    duration_seconds INT,
    tags             JSONB,
    ingested_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_zabbix_severity  ON zabbix_problems(severity_level);
CREATE INDEX IF NOT EXISTS idx_zabbix_host      ON zabbix_problems(host_name);
CREATE INDEX IF NOT EXISTS idx_zabbix_clock     ON zabbix_problems(clock);
CREATE INDEX IF NOT EXISTS idx_zabbix_status    ON zabbix_problems(status);

-- ──────────────────────────────────────────────────────────────
-- 5. KASPERSKY SECURITY CENTER – DEVICES
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS kaspersky_devices (
    device_id              TEXT PRIMARY KEY,
    device_name            TEXT,
    os_name                TEXT,
    os_version             TEXT,
    ip_address             TEXT,
    domain                 TEXT,
    group_name             TEXT,
    kaspersky_version      TEXT,
    last_visible           TIMESTAMPTZ,
    last_scan              TIMESTAMPTZ,
    av_bases_date          TIMESTAMPTZ,
    protection_status      TEXT,   -- OK, Warning, Critical, Unprotected
    real_time_protection   BOOLEAN,
    threats_detected       INT DEFAULT 0,
    threats_cured          INT DEFAULT 0,
    threats_quarantined    INT DEFAULT 0,
    encryption_status      TEXT,
    patch_status           TEXT,
    vulnerabilities_count  INT DEFAULT 0,
    is_managed             BOOLEAN DEFAULT TRUE,
    ingested_at            TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_kasp_protection ON kaspersky_devices(protection_status);
CREATE INDEX IF NOT EXISTS idx_kasp_group      ON kaspersky_devices(group_name);
CREATE INDEX IF NOT EXISTS idx_kasp_last_scan  ON kaspersky_devices(last_scan);


-- ============================================================
-- VIEWS FOR POWER BI
-- ============================================================

-- ── Dynamics: Role distribution by BU ────────────────────────
CREATE OR REPLACE VIEW vw_role_user_counts AS
SELECT
    dr.role_name,
    dr.business_unit,
    COUNT(DISTINCT dur.user_id) AS user_count,
    dr.is_inherited
FROM dynamics_roles dr
LEFT JOIN dynamics_user_roles dur ON dr.role_id = dur.role_id
GROUP BY dr.role_name, dr.business_unit, dr.is_inherited;

-- ── Dynamics: Orphan roles (no users assigned) ───────────────
CREATE OR REPLACE VIEW vw_orphan_roles AS
SELECT
    dr.role_id,
    dr.role_name,
    dr.business_unit,
    dr.created_on
FROM dynamics_roles dr
LEFT JOIN dynamics_user_roles dur ON dr.role_id = dur.role_id
WHERE dur.user_id IS NULL;

-- ── Dynamics: Users with excessive roles (> 5) ──────────────
CREATE OR REPLACE VIEW vw_users_excessive_roles AS
SELECT
    user_id,
    user_name,
    full_name,
    business_unit,
    COUNT(role_id) AS role_count
FROM dynamics_user_roles
GROUP BY user_id, user_name, full_name, business_unit
HAVING COUNT(role_id) > 5
ORDER BY role_count DESC;

-- ── Tickets: Daily volume trend ───────────────────────────────
CREATE OR REPLACE VIEW vw_sdp_daily_volume AS
SELECT
    DATE_TRUNC('day', created_time)::date  AS ticket_date,
    status,
    priority,
    category,
    COUNT(*) AS ticket_count
FROM sdp_tickets
WHERE created_time IS NOT NULL
GROUP BY 1, 2, 3, 4;

-- ── Tickets: SLA compliance by technician ────────────────────
CREATE OR REPLACE VIEW vw_sdp_sla_by_technician AS
SELECT
    technician,
    "group",
    COUNT(*)                                             AS total_tickets,
    SUM(CASE WHEN sla_violated     THEN 1 ELSE 0 END)  AS sla_violated,
    SUM(CASE WHEN NOT sla_violated THEN 1 ELSE 0 END)  AS sla_met,
    ROUND(
        100.0 * SUM(CASE WHEN NOT sla_violated THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2
    )                                                    AS sla_pct,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (resolved_time - created_time)) / 3600
    )::NUMERIC, 2)                                       AS avg_resolution_hours
FROM sdp_tickets
WHERE technician IS NOT NULL
GROUP BY technician, "group";

-- ── Tickets: Open tickets by category & priority ─────────────
CREATE OR REPLACE VIEW vw_sdp_open_tickets AS
SELECT
    category,
    priority,
    "group",
    technician,
    COUNT(*) AS open_count,
    SUM(CASE WHEN is_overdue THEN 1 ELSE 0 END) AS overdue_count
FROM sdp_tickets
WHERE status NOT IN ('Closed', 'Resolved', 'Cancelled')
GROUP BY category, priority, "group", technician;

-- ── Assets: Laptop status summary ────────────────────────────
CREATE OR REPLACE VIEW vw_laptops_summary AS
SELECT
    department,
    site,
    status,
    brand,
    COUNT(*)                                                      AS device_count,
    SUM(CASE WHEN warranty_expiry < CURRENT_DATE THEN 1 ELSE 0 END) AS expired_warranty,
    SUM(CASE WHEN warranty_expiry BETWEEN CURRENT_DATE
                                      AND CURRENT_DATE + 90 THEN 1 ELSE 0 END) AS expiring_90d
FROM assets_laptops
GROUP BY department, site, status, brand;

-- ── Assets: Unassigned laptops ────────────────────────────────
CREATE OR REPLACE VIEW vw_laptops_unassigned AS
SELECT *
FROM assets_laptops
WHERE (assigned_to IS NULL OR assigned_to = '')
  AND status = 'In Stock';

-- ── Zabbix: Active problems by severity ──────────────────────
CREATE OR REPLACE VIEW vw_zabbix_active_problems AS
SELECT
    severity,
    severity_level,
    host_group,
    COUNT(*) AS problem_count,
    SUM(CASE WHEN acknowledged THEN 1 ELSE 0 END) AS acknowledged_count,
    SUM(CASE WHEN NOT acknowledged THEN 1 ELSE 0 END) AS unacknowledged_count
FROM zabbix_problems
WHERE status = 'PROBLEM'
GROUP BY severity, severity_level, host_group
ORDER BY severity_level DESC;

-- ── Zabbix: Problem timeline (last 30 days) ───────────────────
CREATE OR REPLACE VIEW vw_zabbix_problem_trend AS
SELECT
    DATE_TRUNC('day', clock)::date AS alert_date,
    severity,
    COUNT(*) AS problem_count,
    AVG(duration_seconds) / 3600.0 AS avg_duration_hours
FROM zabbix_problems
WHERE clock >= NOW() - INTERVAL '30 days'
GROUP BY 1, 2;

-- ── Kaspersky: Protection status overview ────────────────────
CREATE OR REPLACE VIEW vw_kaspersky_overview AS
SELECT
    group_name,
    protection_status,
    COUNT(*) AS device_count,
    SUM(threats_detected) AS total_threats,
    SUM(vulnerabilities_count) AS total_vulnerabilities,
    SUM(CASE WHEN real_time_protection THEN 0 ELSE 1 END) AS rt_protection_off,
    SUM(CASE WHEN last_scan < NOW() - INTERVAL '7 days' THEN 1 ELSE 0 END) AS not_scanned_7d,
    SUM(CASE WHEN av_bases_date < NOW() - INTERVAL '3 days' THEN 1 ELSE 0 END) AS outdated_bases
FROM kaspersky_devices
WHERE is_managed = TRUE
GROUP BY group_name, protection_status;

-- ── Cross-domain: Ticket-to-alert correlation ─────────────────
CREATE OR REPLACE VIEW vw_ticket_alert_correlation AS
SELECT
    DATE_TRUNC('day', t.created_time)::date AS day,
    COUNT(DISTINCT t.ticket_id)             AS tickets_created,
    COUNT(DISTINCT z.problem_id)            AS zabbix_alerts,
    COUNT(DISTINCT k.device_id)
        FILTER (WHERE k.protection_status = 'Critical') AS kaspersky_critical
FROM sdp_tickets t
FULL OUTER JOIN zabbix_problems z
    ON DATE_TRUNC('day', z.clock) = DATE_TRUNC('day', t.created_time)
FULL OUTER JOIN kaspersky_devices k ON TRUE
WHERE t.created_time >= NOW() - INTERVAL '30 days'
   OR z.clock        >= NOW() - INTERVAL '30 days'
GROUP BY 1
ORDER BY 1;
