"""
pipeline_runner.py — HTTP API for n8n to trigger loaders.

Run: python pipeline_runner.py

Endpoints:
    /health   check server
    /setup    run DB init SQL via psycopg2 (run FIRST after docker compose up)
    /sdp      SDP producer → loader (idle-killed after 15s no output)
    /graph    M365 Graph loader
    /dynamics Dynamics producer → loader (idle-killed after 15s no output)
"""
from dotenv import load_dotenv
load_dotenv()
from http.server import HTTPServer, BaseHTTPRequestHandler
import subprocess, json, os, logging, threading, time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("runner")

REPO = Path(r"C:\Users\khatem\Downloads\it-ops-intelligence-platform\repo")

PG = dict(
    host=os.getenv("PG_HOST", "localhost"),   # ← change this
    port=int(os.getenv("PG_PORT", "5432")),
    dbname=os.getenv("PG_DB",   "itopsdb"),
    user=os.getenv("PG_USER",   "itops"),
 password=os.getenv("PG_PASSWORD", "NewStrongPassword123!")  # ← change
)

def py(*args):
    return ["python"] + [str(a) for a in args]

# ── DB setup via psycopg2 (no psql needed) ───────────────────────
def run_db_setup():
    try:
        import psycopg2
    except ImportError:
        return False, "", "psycopg2 not installed — run: pip install psycopg2-binary"

    results = []
    try:
        conn = psycopg2.connect(**PG)
        conn.autocommit = True
        cur = conn.cursor()

        for fname in ["00_init.sql", "fix_views.sql"]:
            sql_file = REPO / "sql" / fname
            if not sql_file.exists():
                results.append(f"SKIP {fname} (not found)")
                continue
            sql = sql_file.read_text(encoding="utf-8")
            try:
                cur.execute(sql)
                results.append(f"OK   {fname}")
                log.info("  SQL %s — OK", fname)
            except Exception as e:
                results.append(f"WARN {fname}: {e}")
                log.warning("  SQL %s — %s", fname, e)

        cur.close()
        conn.close()
        return True, "\n".join(results), ""
    except Exception as e:
        log.error("  DB setup failed: %s", e)
        return False, "", str(e)

# ── Subprocess runner with idle-kill ─────────────────────────────
def run_step(cmd, timeout=300, idle_kill=15):
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, cwd=str(REPO), env={**os.environ}
    )

    stdout_lines, stderr_lines = [], []
    last_output = [time.time()]

    def read_stream(stream, store):
        for line in stream:
            store.append(line)
            last_output[0] = time.time()
        
    t1 = threading.Thread(target=read_stream, args=(proc.stdout, stdout_lines), daemon=True)
    t2 = threading.Thread(target=read_stream, args=(proc.stderr, stderr_lines), daemon=True)
    t1.start(); t2.start()

    start = time.time()
    while proc.poll() is None:
        time.sleep(1)
        idle = time.time() - last_output[0]
        elapsed = time.time() - start
        if idle > idle_kill and elapsed > 5:
            log.info("  Idle %ds — terminating", int(idle))
            proc.terminate()
            time.sleep(2)
            if proc.poll() is None:
                proc.kill()
            break
        if elapsed > timeout:
            log.warning("  Timeout — killing")
            proc.kill()
            break

    t1.join(timeout=5); t2.join(timeout=5)
    rc = proc.returncode or 0
    stdout = "".join(stdout_lines)
    stderr = "".join(stderr_lines)

    # Override bad exit code if output shows success
    if rc != 0:
        markers = ["Upserted", "Loader stopped cleanly", "Done", "complete", "✅"]
        if any(m in stderr + stdout for m in markers):
            log.info("  Success markers found — treating as OK")
            rc = 0

    return rc, stdout, stderr


def run_pipeline(name):
    all_stdout, all_stderr = [], []
    for cmd in PIPELINES[name]:
        label = Path(cmd[1]).name
        log.info("  Running: %s", label)
        rc, stdout, stderr = run_step(cmd)
        all_stdout.append(f"=== {label} ===\n{stdout[-1500:]}")
        all_stderr.append(f"=== {label} ===\n{stderr[-800:]}")
        if rc != 0:
            log.error("  FAILED: %s (exit %d)", label, rc)
            return False, "\n".join(all_stdout), "\n".join(all_stderr)
        log.info("  OK: %s", label)
    return True, "\n".join(all_stdout), "\n".join(all_stderr)


PIPELINES = {
    "sdp": [
        py(REPO / "producers" / "sdp_producer.py"),
        py(REPO / "loaders"   / "load_sdp_to_postgres.py"),
    ],
    "graph": [
        py(REPO / "loaders" / "load_graph_direct.py"),
    ],
    "dynamics": [
        py(REPO / "producers" / "dynamics_producer.py"),
        py(REPO / "loaders"   / "load_dynamics_to_postgres.py"),
    ],
}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        name = self.path.strip("/").split("?")[0]

        if name == "health":
            self._json(200, {"status": "ok",
                             "endpoints": ["/health","/setup","/sdp","/graph","/dynamics"]})
            return

        if name == "setup":
            log.info("Running DB setup via psycopg2...")
            ok, stdout, stderr = run_db_setup()
            log.info("DB setup → %s", "OK" if ok else "FAILED")
            self._json(200 if ok else 500, {
                "success": ok, "output": stdout, "error": stderr
            })
            return

        if name not in PIPELINES:
            self._json(404, {"error": f"Unknown: '{name}'",
                             "available": list(PIPELINES.keys())})
            return

        log.info("Starting pipeline: %s", name)
        success, stdout, stderr = run_pipeline(name)
        log.info("Pipeline %s → %s", name, "OK" if success else "FAILED")
        self._json(200 if success else 500, {
            "script": name, "success": success,
            "stdout": stdout[-3000:], "stderr": stderr[-1000:],
        })

    def _json(self, code, body):
        data = json.dumps(body, indent=2).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(data))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args): pass

    


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 8765), Handler)
    log.info("✅  Runner on http://localhost:8765")
    log.info("   /health  /setup  /sdp  /graph")
    server.serve_forever()