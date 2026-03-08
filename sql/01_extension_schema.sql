-- ═══════════════════════════════════════════════════════════════════
-- Sumerge IT Ops Intelligence Platform
-- Schema Extension: AD + MS365 Licenses + Zabbix Hosts
-- Run after 00_init.sql
-- ═══════════════════════════════════════════════════════════════════

-- ── Active Directory: Users ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ad_users (
    sam_account       TEXT PRIMARY KEY,
    upn               TEXT,
    email             TEXT,
    display_name      TEXT,
    first_name        TEXT,
    last_name         TEXT,
    department        TEXT,
    title             TEXT,
    company           TEXT,
    office            TEXT,
    phone             TEXT,
    mobile            TEXT,
    description       TEXT,
    manager_dn        TEXT,
    ou                TEXT,
    distinguished_name TEXT,
    enabled           BOOLEAN DEFAULT TRUE,
    locked_out        BOOLEAN DEFAULT FALSE,
    pwd_no_expire     BOOLEAN DEFAULT FALSE,
    is_service_account BOOLEAN DEFAULT FALSE,
    created           TIMESTAMPTZ,
    modified          TIMESTAMPTZ,
    last_logon        TIMESTAMPTZ,
    pwd_last_set      TIMESTAMPTZ,
    ingested_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ad_users_email      ON ad_users(email);
CREATE INDEX IF NOT EXISTS idx_ad_users_dept        ON ad_users(department);
CREATE INDEX IF NOT EXISTS idx_ad_users_ou          ON ad_users(ou);
CREATE INDEX IF NOT EXISTS idx_ad_users_enabled     ON ad_users(enabled);
CREATE INDEX IF NOT EXISTS idx_ad_users_service_acc ON ad_users(is_service_account);

-- ── Active Directory: Computers ────────────────────────────────────
CREATE TABLE IF NOT EXISTS ad_computers (
    computer_name      TEXT PRIMARY KEY,
    dns_hostname       TEXT,
    os                 TEXT,
    os_version         TEXT,
    description        TEXT,
    location           TEXT,
    ou                 TEXT,
    distinguished_name TEXT,
    enabled            BOOLEAN DEFAULT TRUE,
    created            TIMESTAMPTZ,
    modified           TIMESTAMPTZ,
    last_logon         TIMESTAMPTZ,
    ingested_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ad_computers_os  ON ad_computers(os);
CREATE INDEX IF NOT EXISTS idx_ad_computers_ou  ON ad_computers(ou);

-- ── Active Directory: Groups ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS ad_groups (
    group_name         TEXT PRIMARY KEY,
    group_type         TEXT,
    description        TEXT,
    member_count       INT DEFAULT 0,
    ou                 TEXT,
    distinguished_name TEXT,
    created            TIMESTAMPTZ,
    ingested_at        TIMESTAMPTZ DEFAULT NOW()
);

-- ── Active Directory: OUs ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ad_ous (
    distinguished_name TEXT PRIMARY KEY,
    ou_name            TEXT,
    description        TEXT,
    depth              INT DEFAULT 1,
    parent_dn          TEXT,
    created            TIMESTAMPTZ,
    ingested_at        TIMESTAMPTZ DEFAULT NOW()
);

-- ── Microsoft 365: License SKUs ────────────────────────────────────
CREATE TABLE IF NOT EXISTS ms365_license_skus (
    sku_id              TEXT PRIMARY KEY,
    sku_part_number     TEXT,
    friendly_name       TEXT,
    capability_status   TEXT,
    consumed_units      INT DEFAULT 0,
    enabled_units       INT DEFAULT 0,
    suspended_units     INT DEFAULT 0,
    warning_units       INT DEFAULT 0,
    available_units     INT DEFAULT 0,
    utilization_pct     NUMERIC(5,2) DEFAULT 0,
    ingested_at         TIMESTAMPTZ DEFAULT NOW()
);

-- ── Microsoft 365: Per-User License Assignments ────────────────────
CREATE TABLE IF NOT EXISTS ms365_user_licenses (
    user_id           TEXT,
    sku_id            TEXT,
    upn               TEXT,
    email             TEXT,
    display_name      TEXT,
    department        TEXT,
    job_title         TEXT,
    account_enabled   BOOLEAN DEFAULT TRUE,
    friendly_name     TEXT,
    created_datetime  TIMESTAMPTZ,
    ingested_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, sku_id)
);

CREATE INDEX IF NOT EXISTS idx_ms365_ul_email  ON ms365_user_licenses(email);
CREATE INDEX IF NOT EXISTS idx_ms365_ul_sku    ON ms365_user_licenses(sku_id);
CREATE INDEX IF NOT EXISTS idx_ms365_ul_dept   ON ms365_user_licenses(department);

-- ── Zabbix: Monitored Hosts ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS zabbix_hosts (
    host_id       TEXT PRIMARY KEY,
    hostname      TEXT,
    display_name  TEXT,
    host_group    TEXT,
    status        TEXT,
    available     TEXT,
    ip_address    TEXT,
    dns           TEXT,
    os            TEXT,
    hardware      TEXT,
    location      TEXT,
    in_maintenance BOOLEAN DEFAULT FALSE,
    error         TEXT,
    ingested_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_zabbix_hosts_group     ON zabbix_hosts(host_group);
CREATE INDEX IF NOT EXISTS idx_zabbix_hosts_available ON zabbix_hosts(available);

-- ═══════════════════════════════════════════════════════════════════
-- POWER BI VIEWS
-- Central employee identity view — the JOIN hub for all data
-- ═══════════════════════════════════════════════════════════════════

-- ── Master Employee View (the bridge table) ────────────────────────
-- Links AD user → Entra/Dynamics user → M365 license → Asset
CREATE OR REPLACE VIEW vw_employee_master AS
SELECT
    a.sam_account,
    a.upn,
    LOWER(COALESCE(a.email, a.upn))        AS canonical_email,
    a.display_name,
    a.first_name,
    a.last_name,
    COALESCE(a.department, u.department)   AS department,
    COALESCE(a.title, u.job_title)         AS job_title,
    a.ou                                   AS ad_ou,
    a.enabled                              AS ad_enabled,
    a.locked_out,
    a.is_service_account,
    a.last_logon                           AS ad_last_logon,
    a.pwd_last_set,

    -- Entra/Dynamics presence
    u.upn                                  AS entra_upn,
    u.display_name                         AS entra_display_name,
    u.account_enabled                      AS entra_enabled,

    -- License count
    (SELECT COUNT(DISTINCT sku_id)
     FROM ms365_user_licenses ul
     WHERE LOWER(ul.email) = LOWER(COALESCE(a.email, a.upn))
       AND ul.sku_id IS NOT NULL)          AS license_count,

    -- Asset laptop (name match — fuzzy on display_name)
    (SELECT asset_tag
     FROM assets_laptops al
     WHERE LOWER(al.assigned_to) LIKE '%' || LOWER(SPLIT_PART(a.display_name, ' ', 1)) || '%'
     LIMIT 1)                              AS laptop_asset_tag

FROM ad_users a
LEFT JOIN ms365_user_licenses u
    ON LOWER(u.email) = LOWER(COALESCE(a.email, a.upn))
WHERE a.is_service_account = FALSE
GROUP BY
    a.sam_account, a.upn, a.email, a.display_name, a.first_name,
    a.last_name, a.department, a.title, a.ou, a.enabled, a.locked_out,
    a.is_service_account, a.last_logon, a.pwd_last_set,
    u.upn, u.display_name, u.account_enabled;


-- ── License Consumption Summary ────────────────────────────────────
CREATE OR REPLACE VIEW vw_license_summary AS
SELECT
    s.sku_id,
    s.friendly_name,
    s.sku_part_number,
    s.consumed_units,
    s.enabled_units,
    s.available_units,
    s.utilization_pct,
    s.suspended_units,
    s.capability_status,
    COUNT(DISTINCT ul.user_id)             AS assigned_users,
    COUNT(DISTINCT ul.department)          AS departments_using
FROM ms365_license_skus s
LEFT JOIN ms365_user_licenses ul ON ul.sku_id = s.sku_id
GROUP BY
    s.sku_id, s.friendly_name, s.sku_part_number,
    s.consumed_units, s.enabled_units, s.available_units,
    s.utilization_pct, s.suspended_units, s.capability_status;


-- ── License by Department ──────────────────────────────────────────
CREATE OR REPLACE VIEW vw_license_by_department AS
SELECT
    ul.department,
    s.friendly_name,
    COUNT(DISTINCT ul.user_id) AS user_count
FROM ms365_user_licenses ul
JOIN ms365_license_skus s ON s.sku_id = ul.sku_id
WHERE ul.sku_id IS NOT NULL
  AND ul.department IS NOT NULL
  AND ul.department != ''
GROUP BY ul.department, s.friendly_name;


-- ── AD vs Entra Discrepancy ────────────────────────────────────────
-- Shows users present in AD but missing in Entra, or vice versa
CREATE OR REPLACE VIEW vw_ad_entra_gap AS
SELECT
    a.sam_account,
    a.display_name                AS ad_display_name,
    a.email                       AS ad_email,
    a.enabled                     AS ad_enabled,
    a.ou,
    a.department,
    u.upn                         AS entra_upn,
    u.display_name                AS entra_display_name,
    u.account_enabled             AS entra_enabled,
    CASE
        WHEN u.user_id IS NULL         THEN 'AD-only — missing in Entra'
        WHEN NOT a.enabled             THEN 'Disabled in AD'
        WHEN NOT u.account_enabled     THEN 'Disabled in Entra'
        WHEN a.enabled
         AND NOT u.account_enabled    THEN 'Enabled AD / Disabled Entra'
        WHEN NOT a.enabled
         AND u.account_enabled        THEN 'Disabled AD / Enabled Entra'
        ELSE 'Synced'
    END AS sync_status
FROM ad_users a
LEFT JOIN ms365_user_licenses u
    ON LOWER(u.email) = LOWER(COALESCE(a.email, a.upn))
WHERE a.is_service_account = FALSE;


-- ── Service Accounts in AD ─────────────────────────────────────────
CREATE OR REPLACE VIEW vw_service_accounts AS
SELECT
    sam_account,
    display_name,
    description,
    ou,
    enabled,
    last_logon,
    pwd_last_set,
    pwd_no_expire,
    CASE
        WHEN last_logon < NOW() - INTERVAL '90 days'  THEN 'Stale (>90d)'
        WHEN last_logon < NOW() - INTERVAL '30 days'  THEN 'Inactive (>30d)'
        ELSE 'Active'
    END AS activity_status
FROM ad_users
WHERE is_service_account = TRUE;


-- ── Zabbix Active Alerts ───────────────────────────────────────────
CREATE OR REPLACE VIEW vw_zabbix_active_problems AS
SELECT
    p.problem_id,
    p.host_name,
    h.host_group,
    h.ip_address,
    p.severity,
    p.severity_level,
    p.problem_name,
    p.acknowledged,
    p.suppressed,
    p.clock                                AS started_at,
    EXTRACT(EPOCH FROM (NOW() - p.clock::TIMESTAMPTZ))::INT
                                           AS age_seconds,
    ROUND(EXTRACT(EPOCH FROM (NOW() - p.clock::TIMESTAMPTZ)) / 3600.0, 1)
                                           AS age_hours
FROM zabbix_problems p
LEFT JOIN zabbix_hosts h ON h.hostname = p.host_technical
WHERE p.status = 'PROBLEM'
ORDER BY p.severity_level DESC, p.clock ASC;


-- ── Asset ↔ AD Computer match ──────────────────────────────────────
-- Matches asset tags to AD computer objects via hostname
CREATE OR REPLACE VIEW vw_asset_ad_match AS
SELECT
    al.asset_tag,
    al.brand,
    al.model,
    al.assigned_to,
    al.department         AS asset_dept,
    al.site               AS asset_site,
    ac.computer_name      AS ad_computer_name,
    ac.os                 AS ad_os,
    ac.os_version         AS ad_os_version,
    ac.enabled            AS ad_enabled,
    ac.last_logon         AS ad_last_logon,
    ac.ou                 AS ad_ou,
    CASE
        WHEN ac.computer_name IS NULL  THEN 'No AD record'
        WHEN NOT ac.enabled            THEN 'Disabled in AD'
        ELSE 'Matched'
    END AS match_status
FROM assets_laptops al
LEFT JOIN ad_computers ac
    ON LOWER(ac.computer_name) = LOWER(al.asset_tag)
    OR LOWER(ac.computer_name) = LOWER(REPLACE(al.asset_tag, '-', ''));


-- ── Users with no laptop assigned ─────────────────────────────────
CREATE OR REPLACE VIEW vw_users_no_laptop AS
SELECT
    e.canonical_email,
    e.display_name,
    e.department,
    e.ad_ou,
    e.entra_enabled,
    e.license_count
FROM vw_employee_master e
WHERE e.laptop_asset_tag IS NULL
  AND e.entra_enabled = TRUE
  AND e.is_service_account = FALSE;
