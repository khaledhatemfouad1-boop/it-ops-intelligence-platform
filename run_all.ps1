# ============================================================
#  Sumerge IT Ops Intelligence Platform — Full Pipeline
#  Run this from: C:\Users\khatem\Downloads\it-ops-intelligence-platform\repo
#  Usage:  .\run_all.ps1
# ============================================================

# ── 1. Set ALL environment variables ───────────────────────────
Write-Host "`n[1/8] Setting environment variables..." -ForegroundColor Cyan
$env:PG_HOST       = "localhost"
$env:PG_PORT       = "5432"
$env:PG_DB         = "itopsdb"
$env:PG_USER       = "itops"
$env:PG_PASSWORD   = "itops"
$env:KAFKA_BROKER  = "localhost:9092"
$env:AD_PASSWORD   = "1121998Fifaultimate14`$`$"
$env:AD_USER       = "khatem@sumerge.com"
$env:AD_SERVER     = "10.0.0.8"
$env:AD_BASE_DN    = "DC=sumergedc,DC=local"
$env:AD_DOMAIN     = "SUMERGE"
$env:LDAP_URI      = "ldap://10.0.0.8:389"
$env:SDP_CLIENT_ID     = "1000.DM86PXGZRQES47VGV3G1VRICIYZ1VU"
$env:SDP_CLIENT_SECRET = "0570c15516b28be50ed8acdc4721cae09e6feca344"
$env:SDP_REFRESH_TOKEN = "1000.8a9d17ee264930e232cea46d7976c953.e0a221f851b810d35957589315b02b3a"
$env:ZABBIX_URL        = "http://10.0.0.240/zabbix"
$env:ZABBIX_API_TOKEN  = "7e4a292397caf9662cc365b58ee45785694622f4e57cfee07155f28879cab01b"
Write-Host "  Done." -ForegroundColor Green

# ── 2. Active Directory ────────────────────────────────────────
Write-Host "`n[2/8] AD Producer → Kafka..." -ForegroundColor Cyan
python producers/ad_producer.py
if ($LASTEXITCODE -ne 0) { Write-Host "  AD producer failed — continuing" -ForegroundColor Yellow }

Write-Host "`n[3/8] AD Kafka → Postgres..." -ForegroundColor Cyan
python loaders/load_ad_to_postgres.py
if ($LASTEXITCODE -ne 0) { Write-Host "  AD loader failed — check error above" -ForegroundColor Yellow }

# ── 3. SDP Tickets ────────────────────────────────────────────
Write-Host "`n[4/8] SDP Kafka → Postgres (already in Kafka)..." -ForegroundColor Cyan
python loaders/load_sdp_to_postgres.py
if ($LASTEXITCODE -ne 0) { Write-Host "  SDP loader failed — run: python producers/sdp_producer.py first" -ForegroundColor Yellow }

# ── 4. Assets Excel ───────────────────────────────────────────
Write-Host "`n[5/8] Loading Excel assets..." -ForegroundColor Cyan
python loaders/load_assets_excel.py --file IT_Hardware_List.xlsx
if ($LASTEXITCODE -ne 0) { Write-Host "  Assets loader failed — check IT_Hardware_List.xlsx is in repo root" -ForegroundColor Yellow }

# ── 5. Dynamics (already published, just confirm) ─────────────
Write-Host "`n[6/8] Dynamics already published — skipping producer (run manually if needed)" -ForegroundColor Cyan
Write-Host "  To re-run: python producers/dynamics_producer.py" -ForegroundColor DarkGray

# ── 6. Zabbix (VPN required) ──────────────────────────────────
Write-Host "`n[7/8] Zabbix (requires VPN to 10.0.0.240)..." -ForegroundColor Cyan
$vpnReply = ping -n 1 10.0.0.240 2>$null
if ($vpnReply -match "TTL=") {
    Write-Host "  VPN detected — running Zabbix producer..." -ForegroundColor Green
    python producers/zabbix_producer.py --mode all
    python loaders/load_zabbix_to_postgres.py
} else {
    Write-Host "  10.0.0.240 unreachable — connect to VPN first, then run:" -ForegroundColor Yellow
    Write-Host "  python producers/zabbix_producer.py --mode all" -ForegroundColor DarkGray
    Write-Host "  python loaders/load_zabbix_to_postgres.py" -ForegroundColor DarkGray
}

# ── 7. Verify all tables ──────────────────────────────────────
Write-Host "`n[8/8] Verifying database..." -ForegroundColor Cyan
docker exec -it itops-postgres psql -U itops -d itopsdb -c "
SELECT 'ad_users'           AS tbl, COUNT(*) AS rows FROM ad_users
UNION ALL SELECT 'assets_laptops',    COUNT(*) FROM assets_laptops
UNION ALL SELECT 'sdp_tickets',       COUNT(*) FROM sdp_tickets
UNION ALL SELECT 'dynamics_roles',    COUNT(*) FROM dynamics_roles
UNION ALL SELECT 'zabbix_hosts',      COUNT(*) FROM zabbix_hosts
UNION ALL SELECT 'vw_employee_master',COUNT(*) FROM vw_employee_master
UNION ALL SELECT 'vw_ad_entra_gap',   COUNT(*) FROM vw_ad_entra_gap
UNION ALL SELECT 'vw_orphan_roles',   COUNT(*) FROM vw_orphan_roles
ORDER BY tbl;"

Write-Host "`n✅  Pipeline complete!" -ForegroundColor Green
Write-Host "Now open Power BI Desktop and connect to localhost:5432 / itopsdb / itops / itops" -ForegroundColor Cyan
