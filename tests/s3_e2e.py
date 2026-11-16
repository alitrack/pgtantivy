"""
pgtantivy S3 transport e2e (research #2026111601 "共享索引" approach 1 —
cross-node transfer leg). Runs against a REAL MinIO on 127.0.0.1:9000.

Checks:
  A. export_to_s3   publish a named index to S3 (manifest-last)
  B. list_s3        the published version is visible (opstamp + n_docs)
  C. import_from_s3 download + verify (FNV) + register on this instance
  D. fresh search   a fresh backend loads the imported index -> 100/100 hits
  E. S3 independent minio-py proves the objects are really in the bucket
  F. S3 GC keep=1   after a 2nd publish, only the newest version remains
  G. tamper         corrupting an S3 object makes import fail the FNV check

Every SQL statement runs in a fresh backend that first does
  LOAD '<so>'; SET pgtantivy.index_dir='<IDX>'
The 7 snapshot fns come from a pgrx-generated schema; the 3 S3 fns are
registered by hand (same wrappers). MinIO creds are per-call args (no PG env).
"""
import os, re, sys, subprocess, shutil, json

ENV = dict(os.environ, PGHOST="/tmp/pgtsock", PGPORT="5444")
SO = "/tmp/pgext/lib/pgtantivy.so"
IDX = "/tmp/pgt_s3_idx"
DB = "snapdb"
N = 200
CORPUS = json.dumps(
    [{"id": i, "text": ("alpha" if i % 2 == 0 else "beta") + f" t{i}"} for i in range(1, N + 1)]
)
# MinIO connection (per-call args)
EP = "http://127.0.0.1:9000"      # object_store (S3 fns) wants the full URL
EP_PY = "127.0.0.1:9000"          # minio-py wants host:port only (it adds scheme)
BUCKET = "fti-bundles"
REGION = "us-east-1"
AK = "minioadmin"
SK = "minioadmin123"
PREFIX = "fti"

S3_FNS = """
CREATE OR REPLACE FUNCTION "pgtantivy_export_to_s3"(
  "name" TEXT, "endpoint" TEXT, "bucket" TEXT, "region" TEXT,
  "access_key" TEXT, "secret" TEXT, "prefix" TEXT,
  "writer_id" TEXT, "keep" bigint
) RETURNS TEXT LANGUAGE c AS '/tmp/pgext/lib/pgtantivy.so', 'pgtantivy_export_to_s3_wrapper';
CREATE OR REPLACE FUNCTION "pgtantivy_import_from_s3"(
  "endpoint" TEXT, "bucket" TEXT, "region" TEXT, "access_key" TEXT,
  "secret" TEXT, "prefix" TEXT, "bundle_name" TEXT, "as_name" TEXT
) RETURNS TEXT LANGUAGE c STRICT AS '/tmp/pgext/lib/pgtantivy.so', 'pgtantivy_import_from_s3_wrapper';
CREATE OR REPLACE FUNCTION "pgtantivy_list_s3"(
  "endpoint" TEXT, "bucket" TEXT, "region" TEXT, "access_key" TEXT,
  "secret" TEXT, "prefix" TEXT, "name" TEXT
) RETURNS TABLE ("opstamp" bigint, "bundle" TEXT, "n_docs" bigint)
  LANGUAGE c STRICT AS '/tmp/pgext/lib/pgtantivy.so', 'pgtantivy_list_s3_wrapper';
"""


def sqlq(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def q(sql: str) -> str:
    wrapped = f"LOAD '{SO}'; SET pgtantivy.index_dir='{IDX}'; {sql}"
    r = subprocess.run(["psql", "-tA", "-q", "-v", "ON_ERROR_STOP=1",
                        "-d", DB, "-c", wrapped],
                       env=ENV, capture_output=True, text=True, timeout=180)
    if r.returncode != 0:
        raise RuntimeError(f"SQL failed: {sql!r}\n{r.stderr}")
    lines = [ln for ln in (r.stdout or "").splitlines() if ln.strip()]
    return lines[-1].strip() if lines else ""


def q_err(sql: str) -> str:
    wrapped = f"LOAD '{SO}'; SET pgtantivy.index_dir='{IDX}'; {sql}"
    r = subprocess.run(["psql", "-tA", "-q", "-d", DB, "-c", wrapped],
                       env=ENV, capture_output=True, text=True, timeout=180)
    return (r.stderr or "").strip()


def minio(args: list[str]) -> str:
    """Run the minio-py helper (independent S3 verification)."""
    code = "\n".join(args)
    r = subprocess.run(["uv", "run", "--with", "minio", "python", "-c", code],
                       capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        raise RuntimeError(f"minio helper failed: {r.stderr}")
    return (r.stdout or "").strip()


def main() -> int:
    for d in (IDX,):
        shutil.rmtree(d, ignore_errors=True)
    os.makedirs(IDX, exist_ok=True)

    # scratch db + extension fns
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
    r = subprocess.run(["psql", "-q", "-v", "ON_ERROR_STOP=1", "-d", DB, "-c", S3_FNS],
                       env=ENV, capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        raise RuntimeError(f"s3 fns install failed:\n{r.stderr}")

    creds = f"'{EP}','{BUCKET}','{REGION}','{AK}','{SK}','{PREFIX}'"

    # ---- A. build + export_to_s3 ---------------------------------------
    q(f"SELECT pgtantivy_create('snap', {sqlq(CORPUS)}, 'chinese');")
    exp = q(f"SELECT pgtantivy_export_to_s3('snap', {creds}, 'writer-A', 0);")
    print("A. export_s3 :", exp)
    assert "published" in exp and "s3://" in exp, f"export_to_s3 shape: {exp!r}"
    m = re.search(r"opstamp (\d+)", exp)
    assert m, f"no opstamp: {exp!r}"
    op1 = int(m.group(1))

    # ---- B. list_s3 ------------------------------------------------------
    lst = q(f"SELECT pgtantivy_list_s3({creds}, 'snap');")
    print("B. list_s3   :", lst)
    assert str(op1) in lst and "snap-" in lst, f"list_s3 should show opstamp {op1}: {lst!r}"

    # ---- C. import_from_s3 ---------------------------------------------
    imp = q(f"SELECT pgtantivy_import_from_s3({creds}, 'snap-{op1}.bundle', 'snap_s3');")
    print("C. import_s3 :", imp)
    assert "imported" in imp and "s3://" in imp, f"import_from_s3 shape: {imp!r}"
    assert "200 docs" in imp, f"imported doc count: {imp!r}"
    assert "writer writer-A" in imp, f"writer marker: {imp!r}"

    # ---- D. fresh backend: load + search the imported index -------------
    n_alpha = q("SELECT pgtantivy_load('snap_s3'); "
                f"SELECT count(*) FROM pgtantivy_search('snap_s3', 'alpha', {N});")
    n_beta = q("SELECT pgtantivy_load('snap_s3'); "
               f"SELECT count(*) FROM pgtantivy_search('snap_s3', 'beta', {N});")
    print(f"D. fresh search: alpha={n_alpha} beta={n_beta} (expect 100/100)")
    assert n_alpha == "100", f"alpha got {n_alpha}"
    assert n_beta == "100", f"beta got {n_beta}"

    # ---- E. independent minio-py: objects really in the bucket ----------
    n_objs = minio([
        "from minio import Minio",
        f"c = Minio('{EP_PY}', access_key='{AK}', secret_key='{SK}', secure=False)",
        f"objs = list(c.list_objects('{BUCKET}', prefix='{PREFIX}/snap-{op1}.bundle/', recursive=True))",
        "print(len(objs))",
    ])
    print("E. s3 objects:", n_objs, "(independent minio-py count for the bundle)")
    n_objs = int(n_objs.splitlines()[-1])
    assert n_objs >= 18, f"expected manifest + segment files in S3, got {n_objs}"
    # manifest (published marker) present
    has_manifest = minio([
        "from minio import Minio",
        f"c = Minio('{EP_PY}', access_key='{AK}', secret_key='{SK}', secure=False)",
        f"print(c.stat_object('{BUCKET}', '{PREFIX}/snap-{op1}.bundle/manifest.json') is not None)",
    ])
    assert has_manifest.strip().splitlines()[-1] == "True", f"manifest missing in S3: {has_manifest}"

    # ---- F. S3 GC keep=1: publish a 2nd version, keep only the newest ----
    q(f"SELECT pgtantivy_load('snap'); "
      f"SELECT pgtantivy_add('snap', {sqlq(json.dumps([{'id': 9999, 'text': 'alpha z9999'}]))});"
      f" SELECT pgtantivy_export_to_s3('snap', {creds}, 'writer-A', 1);")
    kept = minio([
        "from minio import Minio",
        f"c = Minio('{EP_PY}', access_key='{AK}', secret_key='{SK}', secure=False)",
        f"vers = set(o.object_name.split('/')[1] for o in c.list_objects('{BUCKET}', prefix='{PREFIX}/snap-', recursive=True))",
        "print(len([v for v in vers if v.endswith('.bundle')]))",
    ])
    n_ver = int(kept.splitlines()[-1])
    print(f"F. s3 gc keep=1: {n_ver} bundle version(s) remain (expect 1)")
    assert n_ver == 1, f"keep=1 should leave exactly 1 S3 version, got {n_ver}"

    # ---- G. tamper: corrupt an S3 object -> import must fail FNV --------
    cur = q(f"SELECT pgtantivy_list_s3({creds}, 'snap');")
    cur_op = int(re.search(r"(\d+)", cur).group(1))
    # flip the first byte of one data object (not the manifest)
    tamper = minio([
        "from minio import Minio", "import io",
        f"c = Minio('{EP_PY}', access_key='{AK}', secret_key='{SK}', secure=False)",
        f"prefix = '{PREFIX}/snap-{cur_op}.bundle/'",
        f"objs = [o.object_name for o in c.list_objects('{BUCKET}', prefix=prefix, recursive=True)",
        "         if not o.object_name.endswith('manifest.json')]",
        "target = objs[0]",
        f"r = c.get_object('{BUCKET}', target); data = bytearray(r.read()); r.close(); data[0] ^= 1",
        f"c.put_object('{BUCKET}', target, io.BytesIO(bytes(data)), length=len(data))",
        "print(target)",
    ])
    tgt = tamper.splitlines()[-1].strip()
    print("G. tampered  :", tgt)
    g_err = q_err(f"SELECT pgtantivy_import_from_s3({creds}, 'snap-{cur_op}.bundle', 'snap_bad');")
    print("G. corrupt   :", (g_err.splitlines() or ["(empty)"])[0])
    assert g_err and "checksum mismatch" in g_err, \
        f"corrupted S3 bundle must fail FNV check, got: {g_err!r}"

    # ---- cleanup --------------------------------------------------------
    subprocess.run(["psql", "-tA", "-q", "-c", f"DROP DATABASE IF EXISTS {DB};"],
                   env=ENV, capture_output=True, text=True, timeout=60)
    minio([
        "from minio import Minio",
        f"c = Minio('{EP_PY}', access_key='{AK}', secret_key='{SK}', secure=False)",
        f"for o in c.list_objects('{BUCKET}', prefix='{PREFIX}/', recursive=True):",
        f"    c.remove_object('{BUCKET}', o.object_name)",
        "print('cleaned')",
    ])
    for d in (IDX,):
        shutil.rmtree(d, ignore_errors=True)

    print("\nS3 E2E: all 7 checks passed "
          "(export / list / import / fresh-backend-search 100-100 / "
          "independent-s3-verify / s3-gc-keep1 / fnv-tamper-reject)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
