"""Apply repo/sql/00_init.sql to the target Postgres using env vars.

Reads PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD (or PGPASSWORD) from environment.
Executes each top-level statement in the SQL file.
"""
import os
import sys
import psycopg2

PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "itopsdb")
PG_USER = os.environ.get("PG_USER", "itops")
PG_PASSWORD = os.environ.get("PG_PASSWORD") or os.environ.get("PGPASSWORD")

if not PG_PASSWORD:
    print("Error: PG_PASSWORD or PGPASSWORD environment variable is not set.")
    sys.exit(1)

sql_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "sql", "00_init.sql")
if not os.path.exists(sql_path):
    print(f"Error: SQL file not found: {sql_path}")
    sys.exit(1)

print(f"Connecting to {PG_HOST}:{PG_PORT} db={PG_DB} user={PG_USER}")
try:
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    conn.autocommit = False
    cur = conn.cursor()
except Exception as e:
    print(f"Failed to connect: {e}")
    sys.exit(1)

with open(sql_path, "r", encoding="utf-8") as f:
    sql = f.read()

# Naive split on semicolon for top-level statements. This works for typical init scripts.
statements = [s.strip() for s in sql.split(";") if s.strip()]

import re

def column_exists(cursor, table, column):
    cursor.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name=%s AND column_name=%s",
        (table, column),
    )
    return cursor.fetchone() is not None

errors = []
for i, stmt in enumerate(statements, 1):
    # Check for simple CREATE INDEX ... ON table(col) patterns and skip if column missing.
    m = re.search(r"CREATE\s+(?:UNIQUE\s+)?INDEX(?:\s+IF\s+NOT\s+EXISTS)?\s+\S+\s+ON\s+(\S+)\s*\(([^)]+)\)", stmt, flags=re.IGNORECASE)
    if m:
        table = m.group(1).strip().strip('"')
        cols = [c.strip().strip('"') for c in m.group(2).split(',')]
        missing = [c for c in cols if not column_exists(cur, table, c)]
        if missing:
            print(f"Skipping index creation statement #{i}: table '{table}' missing columns: {missing}")
            continue

    try:
        cur.execute(stmt)
    except Exception as e:
        # Don't abort on errors; report and continue so the init script is best-effort/idempotent
        errmsg = str(e).strip()
        print(f"Statement #{i} failed: {errmsg}")
        errors.append((i, errmsg))
        conn.rollback()

try:
    conn.commit()
    if errors:
        print("SQL applied with errors. See summary below:")
        for i, err in errors:
            print(f"  - statement #{i}: {err}")
    else:
        print("SQL applied successfully.")
except Exception as e:
    print(f"Failed to commit changes: {e}")
    conn.rollback()
finally:
    cur.close()
    conn.close()
