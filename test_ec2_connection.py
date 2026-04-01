"""
test_ec2_connection.py
Run this to confirm your Python can reach EC2 PostgreSQL.

Usage:
    # Set env vars first:
    $env:PG_HOST="YOUR_ELASTIC_IP"
    $env:PG_PASSWORD="Sumerge@Itops2026!"
    
    python test_ec2_connection.py
"""

import psycopg2, os
from dotenv import load_dotenv

load_dotenv()

PG = dict(
    host=os.getenv("PG_HOST", "YOUR_ELASTIC_IP"),
    port=int(os.getenv("PG_PORT", "5432")),
    dbname=os.getenv("PG_DB", "itopsdb"),
    user=os.getenv("PG_USER", "itops"),
    password=os.getenv("PG_PASSWORD", "Sumerge@Itops2026!"),
)
    
print(f"Connecting to {PG['host']}:{PG['port']}/{PG['dbname']} as {PG['user']}...")

try:
    conn = psycopg2.connect(**PG, connect_timeout=10)
    cur  = conn.cursor()

    cur.execute("SELECT version();")
    print(f"✅ Connected: {cur.fetchone()[0]}\n")

    cur.execute("""
        SELECT tablename, 
               pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
        FROM pg_tables 
        WHERE schemaname = 'public'
        ORDER BY tablename;
    """)
    rows = cur.fetchall()
    print(f"{'Table':<35} {'Size':>10}")
    print("-" * 47)
    for r in rows:
        print(f"{r[0]:<35} {r[1]:>10}")

    print("\nRow counts:")
    tables = [r[0] for r in rows]
    for t in tables:
        cur.execute(f"SELECT COUNT(*) FROM public.{t};")
        count = cur.fetchone()[0]
        print(f"  {t:<35} {count:>8} rows")

    cur.close()
    conn.close()
    print("\n✅ All done — EC2 PostgreSQL is reachable and ready.")

except psycopg2.OperationalError as e:
    print(f"\n❌ Connection failed: {e}")
    print("\nTroubleshooting:")
    print("  1. Is EC2 running?  → AWS Console → EC2 → check status")
    print("  2. Is port 5432 open?  → EC2 → Security Groups → itops-sg → Inbound rules")
    print("  3. Has your IP changed?  → go to whatismyip.com and update the SG rule")
    print("  4. Is Docker running?  → SSH into EC2 → docker ps → should show itops-postgres Up")
