"""
pgtantivy approach-2 e2e: multi-replica COORDINATED version release
(coordinator control plane on top of the UNCHANGED plugin primitives).

Topology (all REAL, no mocks):
  writer  : PG cluster on :5444  (builds + exports bundles to MinIO)
  node b  : PG cluster on :5445  (replica)
  node c  : PG cluster on :5446  (replica)
  S3      : MinIO on :9000, bucket fti-bundles, prefix coordinator/

PERSISTENT-CONNECTION MODEL: a real application holds backends open, and the
pgtantivy S3 replica registry is per-backend — so fleet state lives in the
long-lived connections, exactly like production. Each node is modelled by one
open psycopg2 connection (its app backend). Short psql probes would see an
empty registry (fresh backend) and could not represent "node b on v2, node c
on v1".

Proven (8 assertions, A-H):
  A. writer publishes v1 (base x100); b and c load v1 -> gamma=0 both
  B. b converges ALONE (reload_s3 NULL) -> SPLIT: b gamma=100, c gamma=0
     (the per-node status quo WITHOUT coordination)
  C. coordinator `release --op v2` canaries both nodes (isolated sessions),
     publishes target only if all pass -> APPLIED
  D. c follows the TARGET (reload_s3 'rep', <target bundle>) -> CONVERGED:
     both gamma=100
  E. writer publishes v3 (+delta x50); target still v2 -> b and c stay on
     v2 (delta=0, PINNING); a NULL-mode backend auto-adopts v3 (delta=50)
     -> the split a target control point prevents
  F. BLOCKED release: node c cluster stopped; `release --op v3` canary fails
     on c -> target UNCHANGED (atomic gate; no partial fleet state)
  G. c restarted (app reconnects, re-registers from target); `release --op
     v3` with both healthy -> APPLIED; both converge -> delta=50 both
  H. COORDINATED ROLLBACK: `release --op v2` -> APPLIED; both converge ->
     delta=0 both (one control-plane write, fleet-wide revert)

Plugin is never modified: every fleet action is existing SQL
(load_s3 / reload_s3(name,bundle) / search); the coordinator only reads the
target object, canaries in isolated sessions, and PUTs the target atomically.
"""
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, "..", "tools"))
import s3_coordinator as coord  # noqa: E402
import psycopg2  # noqa: E402

SO = "/tmp/pgext/lib/pgtantivy.so"
WRITER_HOST = ("/tmp/pgtsock", "5444")
NODES = {
    "b": ("/tmp/pgtsock_b", "5445", "/tmp/pgt_repb"),
    "c": ("/tmp/pgtsock_c", "5446", "/tmp/pgt_repc"),
}
NODES_CLI = "b=/tmp/pgtsock_b:5445,c=/tmp/pgtsock_c:5446"
IDX = "/tmp/pgt_coord_idx"
CANARY_IDX = "/tmp/pgt_coord_canary"
EP = "http://127.0.0.1:9000"
BUCKET = "fti-bundles"
REGION = "us-east-1"
AK = "minioadmin"
SK = "minioadmin123"
PREFIX = "coordinator"

FNS = """
CREATE OR REPLACE FUNCTION "pgtantivy_load_s3"(
  "endpoint" TEXT, "bucket" TEXT, "region" TEXT,
  "access_key" TEXT, "secret" TEXT, "prefix" TEXT,
  "bundle_name" TEXT, "as_name" TEXT, "warm_terms" TEXT
) RETURNS TEXT LANGUAGE c AS '%s', 'pgtantivy_load_s3_wrapper';
CREATE OR REPLACE FUNCTION "pgtantivy_reload_s3"(
  "name" TEXT, "to_bundle" TEXT
) RETURNS TEXT LANGUAGE c AS '%s', 'pgtantivy_reload_s3_wrapper';
CREATE OR REPLACE FUNCTION "pgtantivy_search"(
  "name" TEXT, "query" TEXT, "top_k" bigint
) RETURNS TABLE ("id" bigint, "score" double precision)
STRICT LANGUAGE c AS '%s', 'pgtantivy_search_wrapper';
CREATE OR REPLACE FUNCTION "pgtantivy_s3_status"(
  "name" TEXT
) RETURNS TEXT LANGUAGE c AS '%s', 'pgtantivy_s3_status_wrapper';
""" % (SO, SO, SO, SO)

FNS_WRITER = """
CREATE OR REPLACE FUNCTION "pgtantivy_create"(
  "name" TEXT, "rows_json" TEXT, "lang" TEXT
) RETURNS TEXT STRICT LANGUAGE c AS '%s', 'pgtantivy_create_wrapper';
CREATE OR REPLACE FUNCTION "pgtantivy_load"(
  "name" TEXT
) RETURNS TEXT STRICT LANGUAGE c AS '%s', 'pgtantivy_load_wrapper';
CREATE OR REPLACE FUNCTION "pgtantivy_add"(
  "name" TEXT, "rows_json" TEXT
) RETURNS TEXT STRICT LANGUAGE c AS '%s', 'pgtantivy_add_wrapper';
CREATE OR REPLACE FUNCTION "pgtantivy_export_to_s3"(
  "name" TEXT, "endpoint" TEXT, "bucket" TEXT, "region" TEXT,
  "access_key" TEXT, "secret" TEXT, "prefix" TEXT,
  "writer_id" TEXT, "keep" bigint
) RETURNS TEXT LANGUAGE c AS '%s', 'pgtantivy_export_to_s3_wrapper';
""" % (SO, SO, SO, SO)

CREDS = f"'{EP}','{BUCKET}','{REGION}','{AK}','{SK}','{PREFIX}'"


def connect(host, port, writer=False):
    conn = psycopg2.connect(host=host, port=port, user="lhy", dbname="postgres")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"LOAD '{SO}'; SET pgtantivy.index_dir='{IDX}';")
    cur.execute(FNS + (FNS_WRITER if writer else ""))
    return conn


def run(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    return [r[0] for r in rows]


def count(conn, term, as_name="rep"):
    rows = run(conn, f"SELECT count(*) FROM pgtantivy_search('{as_name}','{term}',1000);")
    return int(rows[0])


def target_bundle():
    raw = coord.get_target_raw(PREFIX, "snap", BUCKET, EP, REGION, AK, SK)
    return json.loads(raw)["bundle"]


def coord_cli(argv, expect_rc=0):
    cmd = ["uv", "run", "--with", "minio", "python",
           os.path.join(HERE, "..", "tools", "s3_coordinator.py")]
    cmd += ["--endpoint", EP, "--bucket", BUCKET, "--region", REGION,
            "--ak", AK, "--sk", SK, "--prefix", PREFIX,
            "--so", SO, "--idx-dir", CANARY_IDX] + argv
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if r.returncode != expect_rc:
        raise RuntimeError(f"coordinator {' '.join(argv)} rc={r.returncode} "
                           f"(expected {expect_rc})\n{r.stdout}\n{r.stderr}")
    return r.stdout


def _node_pids(sock, port):
    """PIDs of THIS node's postmaster. Must not pgrep a shell command line:
    the pattern appears in the wrapping `bash -lc` cmdline and self-matches."""
    import re as _re
    r = subprocess.run(["ps", "-C", "postgres", "-o", "pid=,args="],
                       capture_output=True, text=True)
    pids = []
    pat = _re.compile(rf"unix_socket_directories={_re.escape(sock)} -p {port}(?:\s|$)")
    for line in r.stdout.splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) == 2 and pat.search(parts[1]):
            pids.append(int(parts[0]))
    return pids


def node_alive(sock, port):
    return bool(_node_pids(sock, port))


def stop_node(name):
    sock, port, _ = NODES[name]
    pids = _node_pids(sock, port)
    if not pids:
        raise RuntimeError(f"no postgres pid for node {name}")
    for pid in pids:
        os.kill(pid, signal.SIGTERM)
    for _ in range(40):
        if not node_alive(sock, port):
            return
        time.sleep(0.5)
    raise RuntimeError(f"node {name} did not stop")


def start_node(name):
    sock, port, datadir = NODES[name]
    # Popen (NOT subprocess.run): postgres is a long-lived daemon that never
    # exits; run() would block waiting for it forever.
    with open(f"/tmp/pgt_rep{name}.log", "a") as logf:
        subprocess.Popen(["nohup", "/usr/lib/postgresql/17/bin/postgres", "-D", datadir,
                          "-c", "listen_addresses=", f"-c", f"unix_socket_directories={sock}",
                          "-p", port],
                         stdout=logf, stderr=subprocess.STDOUT,
                         start_new_session=True)
    for _ in range(40):
        if node_alive(sock, port):
            # wait until it actually accepts queries
            for _ in range(20):
                try:
                    c = psycopg2.connect(host=sock, port=port, user="lhy",
                                         dbname="postgres", connect_timeout=2)
                    c.close()
                    return
                except Exception:
                    time.sleep(0.5)
        time.sleep(0.5)
    raise RuntimeError(f"node {name} did not start")


def main() -> int:
    # --- clean slate --------------------------------------------------------
    from minio import Minio
    c = Minio("127.0.0.1:9000", access_key=AK, secret_key=SK, secure=False)
    for o in c.list_objects(BUCKET, prefix=f"{PREFIX}/", recursive=True):
        c.remove_object(BUCKET, o.object_name)
    for d in (IDX, CANARY_IDX):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d, exist_ok=True)
    for name in ("b", "c"):
        if not node_alive(*NODES[name][:2]):
            start_node(name)

    writer = connect(*WRITER_HOST, writer=True)
    conn_b = connect(*NODES["b"][:2])
    conn_c = connect(*NODES["c"][:2])

    base = [{"id": i, "text": ("alpha" if i % 2 == 0 else "beta") + f" t{i}"}
            for i in range(1, 101)]
    gamma_docs = [{"id": 100 + i, "text": "gamma g" + str(i)} for i in range(1, 101)]
    delta_docs = [{"id": 200 + i, "text": "delta d" + str(i)} for i in range(1, 51)]
    jbase, jgamma, jdelta = (json.dumps(x) for x in (base, gamma_docs, delta_docs))

    # ---- A. publish v1; b and c load it; gamma=0 both ----------------------
    run(writer, f"SELECT pgtantivy_create('snap', '{jbase}', 'chinese');")
    out = run(writer, f"SELECT pgtantivy_export_to_s3('snap', {CREDS}, 'writer-A', 3);")
    op1 = int(re.search(r"opstamp (\d+)", out[0]).group(1))
    run(writer, f"SELECT pgtantivy_add('snap', '{jgamma}');")
    out = run(writer, f"SELECT pgtantivy_export_to_s3('snap', {CREDS}, 'writer-A', 3);")
    op2 = int(re.search(r"opstamp (\d+)", out[0]).group(1))
    b1, b2, b3 = (f"snap-{op1}.bundle", f"snap-{op2}.bundle", None)
    print(f"A. published v1={b1} v2={b2}")

    for name, conn in (("b", conn_b), ("c", conn_c)):
        ld = run(conn, f"SELECT pgtantivy_load_s3({CREDS}, '{b1}', 'rep', NULL);")
        assert "already" not in ld[0], f"{name} load: {ld[0]!r}"
    assert count(conn_b, "gamma") == 0 and count(conn_c, "gamma") == 0
    print("A. b,c loaded v1; gamma=0 on both")

    # ---- B. b converges ALONE (NULL mode) -> fleet SPLIT --------------------
    out = run(conn_b, "SELECT pgtantivy_reload_s3('rep', NULL);")
    print(f"B. b: {out[0]}")
    assert "reloaded" in out[0], out[0]
    gb, gc = count(conn_b, "gamma"), count(conn_c, "gamma")
    stb = run(conn_b, "SELECT pgtantivy_s3_status('rep');")[0]
    stc = run(conn_c, "SELECT pgtantivy_s3_status('rep');")[0]
    print(f"   b status: {stb}")
    print(f"   c status: {stc}")
    assert gb == 100 and gc == 0, (gb, gc)
    assert "UP-TO-DATE" in stb and "LAGGING" in stc, (stb, stc)
    print(f"B. SPLIT without coordination: b gamma={gb} (v2), c gamma={gc} (v1)")

    # ---- C. coordinated release of v2 (canary-gated) -------------------------
    out = coord_cli(["release", "--name", "snap", "--op", str(op2),
                     "--nodes", NODES_CLI])
    print("C. " + out.strip().splitlines()[-1])
    assert "APPLIED" in out, out

    # ---- D. c follows the TARGET -> fleet CONVERGED --------------------------
    tb = target_bundle()
    assert tb == b2, tb
    out = run(conn_c, f"SELECT pgtantivy_reload_s3('rep', '{tb}');")
    print(f"D. c: {out[0]}")
    assert "reloaded" in out[0], out[0]
    gb, gc = count(conn_b, "gamma"), count(conn_c, "gamma")
    assert gb == 100 and gc == 100, (gb, gc)
    print(f"D. CONVERGED on target {tb}: gamma b={gb} c={gc}")

    # ---- E. writer publishes v3 (+delta); target PINNED at v2 ---------------
    run(writer, f"SELECT pgtantivy_add('snap', '{jdelta}');")
    out = run(writer, f"SELECT pgtantivy_export_to_s3('snap', {CREDS}, 'writer-A', 3);")
    op3 = int(re.search(r"opstamp (\d+)", out[0]).group(1))
    b3 = f"snap-{op3}.bundle"
    # both pinned nodes follow the target (v2) -> stay v2, delta=0
    for name, conn in (("b", conn_b), ("c", conn_c)):
        run(conn, f"SELECT pgtantivy_reload_s3('rep', '{tb}');")  # no-op path
    db_, dc_ = count(conn_b, "delta"), count(conn_c, "delta")
    # contrast: NULL mode auto-adopts the newly published v3
    run(conn_b, "SELECT pgtantivy_reload_s3('rep', NULL);")
    db_null = count(conn_b, "delta")
    stb = run(conn_b, "SELECT pgtantivy_s3_status('rep');")[0]
    print(f"E. v3 published (op {op3}); pinned nodes delta={db_}/{dc_}; "
          f"NULL-mode node b delta={db_null} ({stb})")
    assert db_ == 0 and dc_ == 0 and db_null == 50, (db_, dc_, db_null)
    print("E. PINNING: target-followers stayed v2 while NULL-mode auto-adopted v3")

    # ---- F. BLOCKED release: node c cluster down ----------------------------
    # close the app connection FIRST: SIGTERM is a smart stop, which waits for
    # every active backend to exit — a live conn_c would hold the shutdown.
    try:
        conn_c.close()
    except Exception:
        pass
    stop_node("c")
    out = coord_cli(["release", "--name", "snap", "--op", str(op3),
                     "--nodes", NODES_CLI], expect_rc=1)
    print("F. " + out.strip().splitlines()[-1])
    assert "BLOCKED" in out, out
    assert target_bundle() == b2, "target must be UNCHANGED after blocked release"
    print(f"F. release BLOCKED by canary failure; target unchanged ({target_bundle()})")

    # ---- G. c back (reconnect + re-register from target); release v3 --------
    start_node("c")
    conn_c = connect(*NODES["c"][:2])  # app reconnects: fresh backend, empty registry
    tb3 = None
    out = coord_cli(["release", "--name", "snap", "--op", str(op3),
                     "--nodes", NODES_CLI])
    print("G. " + out.strip().splitlines()[-1])
    assert "APPLIED" in out, out
    tb3 = target_bundle()
    assert tb3 == b3, tb3
    run(conn_b, f"SELECT pgtantivy_reload_s3('rep', '{tb3}');")
    # c re-registers from the target (fresh backend has no entry yet)
    run(conn_c, f"SELECT pgtantivy_load_s3({CREDS}, '{tb3}', 'rep', NULL);")
    db_, dc_ = count(conn_b, "delta"), count(conn_c, "delta")
    assert db_ == 50 and dc_ == 50, (db_, dc_)
    print(f"G. fleet converged on v3 (target {tb3}): delta b={db_} c={dc_}")

    # ---- H. COORDINATED ROLLBACK to v2 ---------------------------------------
    out = coord_cli(["release", "--name", "snap", "--op", str(op2),
                     "--nodes", NODES_CLI])
    print("H. " + out.strip().splitlines()[-1])
    assert "APPLIED" in out, out
    tb2 = target_bundle()
    assert tb2 == b2, tb2
    run(conn_b, f"SELECT pgtantivy_reload_s3('rep', '{tb2}');")
    run(conn_c, f"SELECT pgtantivy_reload_s3('rep', '{tb2}');")
    db_, dc_ = count(conn_b, "delta"), count(conn_c, "delta")
    assert db_ == 0 and dc_ == 0, (db_, dc_)
    print(f"H. COORDINATED ROLLBACK to v2 (target {tb2}): delta b={db_} c={dc_} "
          "(one control write, fleet-wide revert)")

    for conn in (writer, conn_b, conn_c):
        try:
            conn.close()
        except Exception:
            pass
    print("\nCOORD E2E: 8/8 passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
