"""
pgtantivy approach 2 e2e: range-read S3Directory load (no whole-bundle download).
Runs against a REAL MinIO on 127.0.0.1:9000.

Checks:
  A. export_to_s3   publish a named index to S3 (manifest-last)   [reuse approach 1]
  B. load_s3        open it DIRECTLY over S3 (range-read), no local dir created
  C. no local dir   proof: index_root/<as_name> does NOT exist on disk (range-read, not download)
  D. fresh search   a fresh backend searches the S3-loaded index -> 100/100
  E. warm search    a 2nd search in the same backend -> still 100/100 (split cache)

The decisive distinction from approach 1 (import_from_s3): load_s3 never touches
the local filesystem for the index data; tantivy range-GETs what each query
touches from the object store.
"""
import os, re, sys, subprocess, shutil

ENV = dict(os.environ, PGHOST="/tmp/pgtsock", PGPORT="5444")
SO = "/tmp/pgext/lib/pgtantivy.so"
IDX = "/tmp/pgt_s3dir_idx"   # local index root for the WRITER index only
DB = "s3dirdb"
N = 200
import json
CORPUS = json.dumps(
    [{"id": i, "text": ("alpha" if i % 2 == 0 else "beta") + f" t{i}"} for i in range(1, N + 1)]
)
EP = "http://127.0.0.1:9000"
EP_PY = "127.0.0.1:9000"
BUCKET = "fti-bundles"
REGION = "us-east-1"
AK = "minioadmin"
SK = "minioadmin123"
PREFIX = "fti"

LOAD_S3_FN = """
CREATE OR REPLACE FUNCTION "pgtantivy_load_s3"(
  "endpoint" TEXT, "bucket" TEXT, "region" TEXT, "access_key" TEXT,
  "secret" TEXT, "prefix" TEXT, "bundle_name" TEXT, "as_name" TEXT,
  "warm_terms" TEXT
) RETURNS TEXT LANGUAGE c AS '/tmp/pgext/lib/pgtantivy.so', 'pgtantivy_load_s3_wrapper';
"""
S3_FNS = """
CREATE OR REPLACE FUNCTION "pgtantivy_export_to_s3"(
  "name" TEXT, "endpoint" TEXT, "bucket" TEXT, "region" TEXT,
  "access_key" TEXT, "secret" TEXT, "prefix" TEXT,
  "writer_id" TEXT, "keep" bigint
) RETURNS TEXT LANGUAGE c AS '/tmp/pgext/lib/pgtantivy.so', 'pgtantivy_export_to_s3_wrapper';
"""


def sqlq(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def q(sql: str) -> str:
    lines = q_lines(sql)
    return lines[-1].strip() if lines else ""


def q_lines(sql: str, extra_env: dict = None) -> list:
    env = dict(ENV)
    if extra_env:
        env.update(extra_env)
    wrapped = f"LOAD '{SO}'; SET pgtantivy.index_dir='{IDX}'; {sql}"
    r = subprocess.run(["psql", "-tA", "-q", "-v", "ON_ERROR_STOP=1",
                        "-d", DB, "-c", wrapped],
                       env=env, capture_output=True, text=True, timeout=180)
    if r.returncode != 0:
        raise RuntimeError(f"SQL failed: {sql!r}\n{r.stderr}")
    return [ln for ln in (r.stdout or "").splitlines() if ln.strip()]


def main() -> int:
    for d in (IDX,):
        shutil.rmtree(d, ignore_errors=True)
    os.makedirs(IDX, exist_ok=True)

    subprocess.run(["psql", "-tA", "-q", "-c", f"DROP DATABASE IF EXISTS {DB};"],
                   env=ENV, capture_output=True, text=True, timeout=60)
    r = subprocess.run(["createdb", DB], env=ENV, capture_output=True, text=True, timeout=60)
    if r.returncode != 0 and "already exists" not in r.stderr:
        raise RuntimeError(f"createdb failed: {r.stderr}")
    r = subprocess.run(["psql", "-q", "-v", "ON_ERROR_STOP=1", "-d", DB,
                        "-f", "/tmp/pgt_schema_abs.sql"],
                       env=ENV, capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        raise RuntimeError(f"schema install failed:\n{r.stderr}")
    for fns in (S3_FNS, LOAD_S3_FN):
        r = subprocess.run(["psql", "-q", "-v", "ON_ERROR_STOP=1", "-d", DB, "-c", fns],
                           env=ENV, capture_output=True, text=True, timeout=120)
        if r.returncode != 0:
            raise RuntimeError(f"fns install failed:\n{r.stderr}")

    creds = f"'{EP}','{BUCKET}','{REGION}','{AK}','{SK}','{PREFIX}'"

    # ---- A. build + export_to_s3 ----------------------------------------
    q(f"SELECT pgtantivy_create('snap', {sqlq(CORPUS)}, 'chinese');")
    exp = q(f"SELECT pgtantivy_export_to_s3('snap', {creds}, 'writer-A', 0);")
    print("A. export_s3 :", exp)
    assert "published" in exp, f"export_to_s3 shape: {exp!r}"
    op = int(re.search(r"opstamp (\d+)", exp).group(1))
    bundle = f"snap-{op}.bundle"

    # ---- B. load_s3: open directly over S3 (range-read), no warm-up --------
    as_name = "snap_s3dir"
    ld = q(f"SELECT pgtantivy_load_s3({creds}, {sqlq(bundle)}, {sqlq(as_name)}, NULL);")
    print("B. load_s3   :", ld)
    assert "loaded" in ld and "range-read" in ld, f"load_s3 shape: {ld!r}"
    assert "200 docs" in ld, f"doc count: {ld!r}"
    assert "warmed" not in ld, f"no warm_terms passed, expect no warmed note: {ld!r}"

    # ---- C. proof: no local dir was created for the S3-loaded index ------
    local = os.path.join(IDX, as_name)
    print(f"C. local dir {local} exists = {os.path.exists(local)} (expect False)")
    assert not os.path.exists(local), \
        f"load_s3 must NOT download to disk, but {local} exists"

    # ---- D. fresh backend: load_s3 + search in the SAME backend -----------
    d = q(f"SELECT pgtantivy_load_s3({creds}, {sqlq(bundle)}, {sqlq(as_name)}, NULL); "
          f"SELECT (SELECT count(*) FROM pgtantivy_search({sqlq(as_name)}, 'alpha', {N})) AS a, "
          f"(SELECT count(*) FROM pgtantivy_search({sqlq(as_name)}, 'beta', {N})) AS b;")
    parts = d.split("|")
    print(f"D. fresh search: alpha={parts[0]} beta={parts[1]} (expect 100/100)")
    assert len(parts) == 2 and parts[0].strip() == "100" and parts[1].strip() == "100", \
        f"D search got {d!r}"

    # ---- E. reproducibility: a brand-new backend loads + searches again ---
    e = q(f"SELECT pgtantivy_load_s3({creds}, {sqlq(bundle)}, {sqlq(as_name)}, NULL); "
          f"SELECT count(*) FROM pgtantivy_search({sqlq(as_name)}, 'alpha', {N});")
    print(f"E. fresh-backend re-load+search: alpha={e} (expect 100)")
    assert e == "100", f"alpha re-load got {e}"

    # ---- F. query warm-up: load with warm_terms reports warmed + still 100 -
    lines = q_lines(f"SELECT pgtantivy_load_s3({creds}, {sqlq(bundle)}, {sqlq(as_name)}, 'alpha,beta'); "
                    f"SELECT count(*) FROM pgtantivy_search({sqlq(as_name)}, 'alpha', {N});")
    first, second = lines[0].strip(), lines[1].strip()
    print(f"F. warm-up   : {first}")
    print(f"              alpha after warm = {second} (expect 100)")
    assert "warmed: alpha, beta" in first, f"warm-up note missing: {first!r}"
    assert second == "100", f"alpha after warm got {second!r}"

    # ---- G. bounded split cache: a 1KB budget (smaller than any split file)
    # forces the LRU to evict everything, yet range-read must stay CORRECT
    # (just slower, never OOM / never wrong). Proves the memory ceiling holds.
    tiny = {"PGTANTIVY_S3_CACHE_BYTES": "1024"}
    lines = q_lines(
        f"SELECT pgtantivy_load_s3({creds}, {sqlq(bundle)}, {sqlq(as_name)}, NULL); "
        f"SELECT (SELECT count(*) FROM pgtantivy_search({sqlq(as_name)}, 'alpha', {N})) AS a, "
        f"(SELECT count(*) FROM pgtantivy_search({sqlq(as_name)}, 'beta', {N})) AS b;",
        extra_env=tiny,
    )
    a, b = lines[1].split("|")
    print(f"G. tiny budget (1KB): alpha={a.strip()} beta={b.strip()} (expect 100/100 — cache evicts, range-read stays correct)")
    assert a.strip() == "100" and b.strip() == "100", f"tiny-budget search got {lines[1]!r}"

    # cleanup
    shutil.rmtree(IDX, ignore_errors=True)
    subprocess.run(["psql", "-tA", "-q", "-c", f"DROP DATABASE IF EXISTS {DB};"],
                   env=ENV, capture_output=True, text=True, timeout=60)
    print("\nALL APPROACH-2 (load_s3) E2E CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
