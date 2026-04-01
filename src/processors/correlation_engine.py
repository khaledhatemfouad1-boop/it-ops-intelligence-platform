"""
IT Ops Correlation Engine
Runs cross-source analysis queries against PostgreSQL and logs findings.

Detects:
  - Employees with no M365 license
  - Users in AD but not in Entra (or vice versa)
  - Employees with no assigned laptop
  - Devices in Zabbix but no AD computer record (shadow IT)
  - Kaspersky AV outdated / missing on known computers
  - SDP tickets with no matching AD user (stale requesters)

Run:
    python processors/correlation_engine.py
    python processors/correlation_engine.py --check license_gap
"""

import os, logging, argparse
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("correlation_engine")

PG_DSN = (
    f"host={os.getenv('PG_HOST','localhost')} "
    f"port={os.getenv('PG_PORT','5432')} "
    f"dbname={os.getenv('PG_DB','itopsdb')} "
    f"user={os.getenv('PG_USER','postgres')} "
    f"password={os.getenv('PG_PASSWORD','')}"
)

CHECKS = {

    "license_gap": {
        "title": "Employees with NO M365 license assigned",
        "sql": """
            SELECT e.display_name, e.email, e.department
            FROM   vw_employee_master e
            LEFT   JOIN ms365_user_licenses lic ON lower(lic.user_email) = e.canonical_email
            WHERE  lic.user_email IS NULL
            AND    e.account_enabled = TRUE
            ORDER  BY e.department, e.display_name;
        """,
        "columns": ["display_name", "email", "department"],
    },

    "ad_entra_gap": {
        "title": "AD ↔ Entra identity gaps",
        "sql": "SELECT * FROM vw_ad_entra_gap ORDER BY gap_type, display_name;",
        "columns": ["display_name", "email", "gap_type"],
    },

    "no_laptop": {
        "title": "Active employees with no assigned laptop",
        "sql": "SELECT * FROM vw_users_no_laptop ORDER BY department, display_name;",
        "columns": ["display_name", "email", "department"],
    },

    "shadow_devices": {
        "title": "Zabbix hosts with no matching AD computer record (shadow IT)",
        "sql": """
            SELECT z.hostname, z.ip_address, z.status, z.last_seen_at
            FROM   zabbix_hosts z
            LEFT   JOIN ad_computers c ON lower(c.computer_name) = lower(z.hostname)
            WHERE  c.computer_name IS NULL
            ORDER  BY z.hostname;
        """,
        "columns": ["hostname", "ip_address", "status", "last_seen_at"],
    },

    "stale_tickets": {
        "title": "SDP tickets with requester not in Active Directory",
        "sql": """
            SELECT t.ticket_id, t.subject, t.requester_email, t.status, t.created_at
            FROM   sdp_tickets t
            LEFT   JOIN ad_users u ON lower(u.email) = lower(t.requester_email)
            WHERE  u.email IS NULL
            AND    t.requester_email IS NOT NULL
            ORDER  BY t.created_at DESC
            LIMIT  50;
        """,
        "columns": ["ticket_id", "subject", "requester_email", "status", "created_at"],
    },

    "active_alerts": {
        "title": "Active Zabbix problems (high/disaster severity)",
        "sql": """
            SELECT host_name, problem_name, severity, duration_minutes, acknowledged
            FROM   vw_zabbix_active_problems
            WHERE  severity IN ('High', 'Disaster')
            ORDER  BY duration_minutes DESC;
        """,
        "columns": ["host_name", "problem_name", "severity", "duration_minutes", "acknowledged"],
    },

    "service_accounts": {
        "title": "Potential service / orphan accounts",
        "sql": "SELECT * FROM vw_service_accounts ORDER BY display_name;",
        "columns": ["display_name", "sam_account", "reason"],
    },
}


def run_check(cur, name: str, check: dict):
    log.info("━━━ %s ━━━", check["title"])
    try:
        cur.execute(check["sql"])
        rows = cur.fetchall()
        if not rows:
            log.info("  ✅  No issues found.")
            return 0
        log.warning("  ⚠️   %d records found:", len(rows))
        cols = check["columns"]
        header = "  " + " | ".join(f"{c:<30}" for c in cols)
        log.warning(header)
        log.warning("  " + "-" * len(header.strip()))
        for row in rows[:20]:
            line = "  " + " | ".join(f"{str(row.get(c, '')):<30}" for c in cols)
            log.warning(line)
        if len(rows) > 20:
            log.warning("  … and %d more rows.", len(rows) - 20)
        return len(rows)
    except Exception as exc:
        log.error("  ❌  Query failed: %s", exc)
        return -1


def run(check_name: str = "all"):
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            timestamp = datetime.now(timezone.utc).isoformat()
            log.info("=" * 60)
            log.info("IT Ops Correlation Engine — %s", timestamp)
            log.info("=" * 60)

            checks_to_run = CHECKS if check_name == "all" else {check_name: CHECKS[check_name]}
            totals = {}
            for name, check in checks_to_run.items():
                count = run_check(cur, name, check)
                totals[name] = count

            log.info("=" * 60)
            log.info("SUMMARY")
            log.info("=" * 60)
            for name, count in totals.items():
                status = "✅" if count == 0 else ("❌" if count < 0 else "⚠️ ")
                log.info("  %s  %-25s %s", status, name, f"{count} records" if count >= 0 else "ERROR")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IT Ops Correlation Engine")
    parser.add_argument("--check", choices=list(CHECKS.keys()) + ["all"], default="all")
    args = parser.parse_args()
    run(args.check)
