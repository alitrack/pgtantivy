"""
pgtantivy snapshot bundle e2e (approach 1).

Validates the immutable snapshot-bundle export/import contract:
  A. export        pgtantivy_export_index -> bundle dir + opstamp + file count
  B. bundle_info   manifest JSON (format/name/n_docs/single_writer/files/opstamp)
  C. import        pgtantivy_import_index -> registered, doc count matches, writer preserved
  D. search        a FRESH backend loads the imported index and serves queries
                   (multi-backend / shared-storage semantics)
  E. opstamp bump  an incremental write (pgtantivy_add) raises the opstamp on a 2nd export
  F. integrity     corrupting one bundled byte makes import FAIL the FNV check

Self-contained. Connects to the pgrx data-17 cluster on port 5444 (scratch db
`snapdb`). Every statement runs in a backend that first does
  LOAD '<so>'          -- runs _PG_init, registers the pgtantivy GUCs
  SET pgtantivy.index_dir='<IDX>'   -- redirect persistent indexes to a /tmp dir
so indexes never pollute $PGDATA and cleanup is trivial.
The extension is registered once from a schema.sql (pgrx-generated) with
MODULE_PATHNAME rewritten to the absolute .so path (no CREATE EXTENSION needed,
so no /usr write).

Prereq (set up once, see snapshot_env.sh):
  /tmp/pgext/lib/pgtantivy.so     (fresh build)
  /tmp/pgt_schema_abs.sql         (schema.sql with MODULE_PATHNAME -> the .so)
  data-17 cluster up on 5444, scratch db snapdb exists

Run:  python3 tests/snapshot_e2e.py
"""
import os, re, sys, subprocess, shutil, json

ENV = dict(os.environ, PGHOST="/tmp/pgtsock", PGPORT="5444")
SO = "/tmp/pgext/lib/pgtantivy.so"
IDX = "/tmp/pgt_snap_idx"
B1 = "/tmp/pgt_snap_x1"
B2 = "/tmp/pgt_snap_x2"
DB = "snapdb"
N = 200
# every body is '<alpha|beta> t<id>' -> alpha/beta are standalone tokens
CORPUS = json.dumps(
    [{"id": i, "text": ("alpha" if i % 2 == 0 else "beta") + f" t{i}"} for i in range(1, N + 1)]
)


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


def sqlq(s: str) -> str:
    """Quote a string for safe embedding in a psql single-quoted literal."""
    return "'" + s.replace("'", "''") + "'"


def opstamp_of(blob: str) -> int:
    m = re.search(r"opstamp\s+(\d+)", blob)
    assert m, f"no opstamp in: {blob!r}"
    return int(m.group(1))


def bundle_of(blob: str) -> str:
    """Extract the actual <name>-<opstamp>.bundle path from an export line."""
    m = re.search(r"to (\S+)", blob)
    assert m, f"no bundle path in: {blob!r}"
    return m.group(1)


def main() -> int:
    for d in (B1, B2, IDX):
        shutil.rmtree(d, ignore_errors=True)
    os.makedirs(IDX, exist_ok=True)

    # scratch db, idempotent
    subprocess.run(["psql", "-tA", "-q", "-c", f"DROP DATABASE IF EXISTS {DB};"],
                   env=ENV, capture_output=True, text=True, timeout=60)
    r = subprocess.run(["createdb", DB], env=ENV,
                       capture_output=True, text=True, timeout=60)
    if r.returncode != 0 and "already exists" not in r.stderr:
        raise RuntimeError(f"createdb failed: {r.stderr}")
    # register the 18 extension functions (CREATE FUNCTION, no _PG_init needed)
    r = subprocess.run(["psql", "-q", "-v", "ON_ERROR_STOP=1", "-d", DB,
                        "-f", "/tmp/pgt_schema_abs.sql"],
                       env=ENV, capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        raise RuntimeError(f"schema install failed:\n{r.stderr}")

    # ---- A. build + export --------------------------------------------
    q(f"SELECT pgtantivy_create('snap', {sqlq(CORPUS)}, 'chinese');")
    exp1 = q(f"SELECT pgtantivy_export_index('snap', '{B1}', 'writer-A');")
    print("A. export    :", exp1)
    assert "exported" in exp1 and "files)" in exp1, "export return shape"
    assert "opstamp 1" in exp1, f"first export should be opstamp 1: {exp1!r}"
    op1 = opstamp_of(exp1)
    assert op1 == 1, f"first opstamp must be 1, got {op1}"
    BUNDLE = bundle_of(exp1)  # actual <dest>/snap-1.bundle path

    # ---- B. bundle_info -----------------------------------------------
    info = json.loads(q(f"SELECT pgtantivy_bundle_info('{BUNDLE}');"))
    print("B. info      :", json.dumps(info, ensure_ascii=False))
    assert info["name"] == "snap", "name round-trip"
    assert info["n_docs"] == N, f"n_docs round-trip got {info['n_docs']}"
    assert info["writer_id"] == "writer-A", "writer marker"
    assert info["n_files"] >= 3, f"n_files got {info['n_files']}"
    assert info["opstamp"] == op1, "info opstamp == export opstamp"

    # ---- C. import (same backend registers it) ------------------------
    imp = q(f"SELECT pgtantivy_import_index('{BUNDLE}', 'snap_imp');")
    print("C. import    :", imp)
    assert re.search(r"(\d+)\s+docs", imp).group(1) == str(N), "imported doc count"
    assert "writer writer-A" in imp, "writer marker preserved"

    # ---- D. FRESH backend: load + search the imported index -----------
    # each q() is one fresh backend: LOAD (registers GUCs) + register the
    # imported index in-process + search. The count (last row) is the result.
    n_alpha = q("SELECT pgtantivy_load('snap_imp'); "
                f"SELECT count(*) FROM pgtantivy_search('snap_imp', 'alpha', {N});")
    n_beta = q("SELECT pgtantivy_load('snap_imp'); "
               f"SELECT count(*) FROM pgtantivy_search('snap_imp', 'beta', {N});")
    print(f"D. fresh search: alpha={n_alpha} beta={n_beta} (expect 100/100)")
    assert n_alpha == "100", f"alpha hits got {n_alpha}"
    assert n_beta == "100", f"beta hits got {n_beta}"

    # ---- E. opstamp bump after an incremental write -------------------
    # 'snap' was created in an earlier backend; this fresh one must load it
    # into its in-process registry before add.
    add_rows = json.dumps([{"id": 9001, "text": "alpha gamma 9001"}])
    q(f"SELECT pgtantivy_load('snap'); SELECT pgtantivy_add('snap', {sqlq(add_rows)});")
    exp2 = q(f"SELECT pgtantivy_export_index('snap', '{B2}', 'writer-A');")
    op2 = opstamp_of(exp2)
    print(f"E. opstamp   : {op1} -> {op2}  (after 1 add)")
    assert op2 > op1, f"opstamp must bump on write: {op1} !> {op2}"

    # ---- F. integrity: corrupt a bundled file -> import must fail -----
    cand = [os.path.join(root, f) for root, _, fs in os.walk(BUNDLE)
            for f in fs if not f.startswith("pgtantivy_meta")]
    assert cand, "no bundle files to corrupt"
    target = sorted(cand)[0]
    with open(target, "r+b") as fh:
        fh.seek(0); b = fh.read(1); fh.seek(0); fh.write(bytes([b[0] ^ 0x01]))
    err = q_err(f"SELECT pgtantivy_import_index('{BUNDLE}', 'snap_bad');")
    print("F. corrupt  :", (err.splitlines() or ["(empty)"])[0])
    assert err and "checksum mismatch" in err, \
        f"corrupted bundle must fail the FNV integrity check, got: {err!r}"

    # ---- cleanup -------------------------------------------------------
    subprocess.run(["psql", "-tA", "-q", "-c", f"DROP DATABASE IF EXISTS {DB};"],
                   env=ENV, capture_output=True, text=True, timeout=60)
    for d in (B1, B2, IDX):
        shutil.rmtree(d, ignore_errors=True)

    print("\nSNAPSHOT E2E: all 6 checks passed "
          "(export / info / import / fresh-backend-search / opstamp-bump / fnv-verify)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
