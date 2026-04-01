-- ============================================================
-- fix_views_v2.sql  — final patch for itopsdb
-- Run:  docker cp fix_views_v2.sql itops-postgres:/fix_views_v2.sql
--       docker exec -it itops-postgres psql -U itops -d itopsdb -f /fix_views_v2.sql
-- ============================================================

-- ── Step 1: Add ALL missing columns to old tables ────────────

ALTER TABLE dynamics_roles
    ADD COLUMN IF NOT EXISTS business_unit   TEXT,
    ADD COLUMN IF NOT EXISTS is_inherited    BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS created_on      TIMESTAMPTZ;

ALTER TABLE dynamics_user_roles
    ADD COLUMN IF NOT EXISTS user_name       TEXT,
    ADD COLUMN IF NOT EXISTS full_name       TEXT,
    ADD COLUMN IF NOT EXISTS business_unit   TEXT;

ALTER TABLE ad_users
    ADD COLUMN IF NOT EXISTS manager_dn      TEXT,
    ADD COLUMN IF NOT EXISTS is_service_account BOOLEAN DEFAULT FALSE;

-- ── Step 2: Dynamics views ───────────────────────────────────

DROP VIEW IF EXISTS vw_role_user_counts CASCADE;
CREATE OR REPLACE VIEW vw_role_user_counts AS
SELECT
    dr.role_name,
    dr.business_unit,
    COUNT(DISTINCT dur.user_id) AS user_count,
    dr.is_inherited
FROM dynamics_roles dr
LEFT JOIN dynamics_user_roles dur ON dr.role_id = dur.role_id
GROUP BY dr.role_name, dr.business_unit, dr.is_inherited;

DROP VIEW IF EXISTS vw_orphan_roles CASCADE;
CREATE OR REPLACE VIEW vw_orphan_roles AS
SELECT
    dr.role_id,
    dr.role_name,
    dr.business_unit,
    dr.created_on
FROM dynamics_roles dr
LEFT JOIN dynamics_user_roles dur ON dr.role_id = dur.role_id
WHERE dur.user_id IS NULL;

DROP VIEW IF EXISTS vw_users_excessive_roles CASCADE;
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

-- ── Step 3: vw_employee_master (no GROUP BY needed) ──────────

DROP VIEW IF EXISTS vw_employee_master CASCADE;
CREATE OR REPLACE VIEW vw_employee_master AS
SELECT
    a.sam_account,
    a.upn,
    LOWER(COALESCE(a.email, a.upn))                     AS canonical_email,
    a.display_name,
    a.first_name,
    a.last_name,
    COALESCE(a.department,
        (SELECT u2.department FROM ms365_user_licenses u2
         WHERE LOWER(u2.email) = LOWER(COALESCE(a.email, a.upn))
         LIMIT 1))                                       AS department,
    COALESCE(a.title,
        (SELECT u2.job_title FROM ms365_user_licenses u2
         WHERE LOWER(u2.email) = LOWER(COALESCE(a.email, a.upn))
         LIMIT 1))                                       AS job_title,
    a.ou                                                 AS ad_ou,
    a.enabled                                            AS ad_enabled,
    a.locked_out,
    a.is_service_account,
    a.last_logon                                         AS ad_last_logon,
    a.pwd_last_set,
    (SELECT u2.upn FROM ms365_user_licenses u2
     WHERE LOWER(u2.email) = LOWER(COALESCE(a.email, a.upn))
     LIMIT 1)                                            AS entra_upn,
    (SELECT u2.display_name FROM ms365_user_licenses u2
     WHERE LOWER(u2.email) = LOWER(COALESCE(a.email, a.upn))
     LIMIT 1)                                            AS entra_display_name,
    (SELECT u2.account_enabled FROM ms365_user_licenses u2
     WHERE LOWER(u2.email) = LOWER(COALESCE(a.email, a.upn))
     LIMIT 1)                                            AS entra_enabled,
    (SELECT COUNT(DISTINCT sku_id)
     FROM ms365_user_licenses ul
     WHERE LOWER(ul.email) = LOWER(COALESCE(a.email, a.upn))
       AND ul.sku_id IS NOT NULL)                        AS license_count,
    (SELECT asset_tag
     FROM assets_laptops al
     WHERE LOWER(al.assigned_to) LIKE '%' || LOWER(SPLIT_PART(a.display_name,' ',1)) || '%'
     LIMIT 1)                                            AS laptop_asset_tag
FROM ad_users a
WHERE a.is_service_account = FALSE;

-- ── Step 4: Zabbix view (host_technical → host_name) ─────────

DROP VIEW IF EXISTS vw_zabbix_active_problems CASCADE;
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
    p.clock                                              AS started_at,
    EXTRACT(EPOCH FROM (NOW() - p.clock::TIMESTAMPTZ))::INT
                                                         AS age_seconds,
    ROUND(EXTRACT(EPOCH FROM (NOW() - p.clock::TIMESTAMPTZ)) / 3600.0, 1)
                                                         AS age_hours
FROM zabbix_problems p
LEFT JOIN zabbix_hosts h ON h.hostname = p.host_name
WHERE p.status = 'PROBLEM'
ORDER BY p.severity_level DESC, p.clock ASC;

-- ── Step 5: Dependent views (recreate after CASCADE drops) ────

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
  AND e.entra_enabled IS NOT FALSE
  AND e.is_service_account = FALSE;

DROP VIEW IF EXISTS vw_ad_entra_gap;
CREATE OR REPLACE VIEW vw_ad_entra_gap AS
SELECT
    a.display_name,
    COALESCE(a.email, a.upn)   AS email,
    a.department,
    'In AD, not in Entra'      AS gap_type
FROM ad_users a
LEFT JOIN ms365_user_licenses u
    ON LOWER(COALESCE(a.email, a.upn)) = LOWER(u.email)
WHERE u.email IS NULL
  AND a.enabled = TRUE
  AND a.is_service_account = FALSE
UNION ALL
SELECT
    u.display_name,
    u.email,
    u.department,
    'In Entra, not in AD'      AS gap_type
FROM ms365_user_licenses u
LEFT JOIN ad_users a
    ON LOWER(u.email) = LOWER(COALESCE(a.email, a.upn))
WHERE a.sam_account IS NULL
  AND u.account_enabled = TRUE;

DROP VIEW IF EXISTS vw_service_accounts;
CREATE OR REPLACE VIEW vw_service_accounts AS
SELECT
    sam_account,
    display_name,
    upn,
    department,
    enabled,
    last_logon,
    CASE
        WHEN email IS NULL OR email = ''                    THEN 'No email'
        WHEN manager_dn IS NULL OR manager_dn = ''         THEN 'No manager'
        WHEN is_service_account = TRUE                     THEN 'Flagged as service account'
        ELSE 'Other'
    END AS reason
FROM ad_users
WHERE is_service_account = TRUE
   OR email IS NULL OR email = ''
   OR manager_dn IS NULL OR manager_dn = '';

-- ── Final verification ────────────────────────────────────────
SELECT view_name, row_count FROM (
    SELECT 'vw_employee_master'        AS view_name, COUNT(*) AS row_count FROM vw_employee_master        UNION ALL
    SELECT 'vw_zabbix_active_problems',              COUNT(*) FROM vw_zabbix_active_problems              UNION ALL
    SELECT 'vw_users_no_laptop',                     COUNT(*) FROM vw_users_no_laptop                     UNION ALL
    SELECT 'vw_ad_entra_gap',                        COUNT(*) FROM vw_ad_entra_gap                        UNION ALL
    SELECT 'vw_service_accounts',                    COUNT(*) FROM vw_service_accounts                    UNION ALL
    SELECT 'vw_role_user_counts',                    COUNT(*) FROM vw_role_user_counts                    UNION ALL
    SELECT 'vw_orphan_roles',                        COUNT(*) FROM vw_orphan_roles
) t ORDER BY view_name;
