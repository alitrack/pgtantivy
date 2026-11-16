"""
pgtantivy approach-2 e2e: in-place replica version switching
(pgtantivy_reload_s3). Runs against a REAL MinIO on 127.0.0.1:9000.

Scenario: a writer publishes two bundle versions of the same index (v1 then
v2, where v2 has 100 extra docs). A replica loads v1, then:
  A. export v1           publish the base corpus (100 docs: alpha/beta)
  B. load_s3 @ v1        replica serves v1
  C. add 100 docs        append gamma docs to the writer's index
  D. export v2           publish v2 (keep both versions)
  E. list_s3             two published versions visible (newest first)
  F. reload_s3 NULL      auto-upgrade to NEWEST (v2) in place, same name;
                         search now finds gamma=100
  G. reload_s3 NULL again  no-op fast path ("already serving")
  H. reload_s3 <v1>      explicit ROLLBACK to v1; gamma back to 0

Decisive proof: the replica keeps its registry name across the switch —
pgtantivy_search('rep', ...) follows the version, no re-load_s3 needed.

Per-backend registry: load_s3 / reload_s3 / pgtantivy_search must run in the
SAME psql invocation (one backend) for the registry entry to be visible.
"""
import os, re, sys, subprocess, shutil, json

ENV = dict(os.environ, PGHOST="/tmp/pgtsock", PGPORT="5444")
SO = "/tmp/pgext/lib/pgtantivy.so"
IDX = "/tmp/pgt_s3reload_idx"
DB = "s3reloaddb"
N = 100
V1 = [{"id": i, "text": ("alpha" if i % 2 == 0 else "beta") + f" t{i}"} for i in range(1, N + 1)]
V2_EXTRA = [{"id": N + i, "text": "gamma g" + str(i)} for i in range(1, N + 1)]
CORPUS_V1 = json.dumps(V1)
CORPUS_EXTRA = json.dumps(V2_EXTRA)
EP = "http://127.0.0.1:9000"
BUCKET = "fti-bundles"
REGION = "us-east-1"
AK = "minioadmin"
SK = "minioadmin123"
PREFIX = "fti"

FNS = """
CREATE OR REPLACE FUNCTION "pgtantivy_export_to_s3"(
  "name" TEXT, "endpoint" TEXT, "bucket" TEXT, "region" TEXT,
  "access_key" TEXT, "secret" TEXT, "prefix" TEXT,
  "writer_id" TEXT, "keep" bigint
) RETURNS TEXT LANGUAGE c AS '/tmp/pgext/lib/pgtantivy.so', 'pgtantivy_export_to_s3_wrapper';
CREATE OR REPLACE FUNCTION "pgtantivy_list_s3"(
  "endpoint" TEXT, "bucket" TEXT, "region" TEXT, "access_key" TEXT,
  "secret" TEXT, "prefix" TEXT, "name" TEXT
) RETURNS TABLE (opstamp bigint, bundle text, n_docs bigint)
LANGUAGE c AS '/tmp/pgext/lib/pgtantivy.so', 'pgtantivy_list_s3_wrapper';
CREATE OR REPLACE FUNCTION "pgtantivy_load_s3"(
  "endpoint" TEXT, "bucket" TEXT, "region" TEXT, "access_key" TEXT,
  "secret" TEXT, "prefix" TEXT, "bundle_name" TEXT, "as_name" TEXT,
  "warm_terms" TEXT
) RETURNS TEXT LANGUAGE c AS '/tmp/pgext/lib/pgtantivy.so', 'pgtantivy_load_s3_wrapper';
CREATE OR REPLACE FUNCTION "pgtantivy_reload_s3"(
  "name" TEXT, "to_bundle" TEXT
) RETURNS TEXT LANGUAGE c AS '/tmp/pgext/lib/pgtantivy.so', 'pgtantivy_reload_s3_wrapper';
"""


def sqlq(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def q_lines(sql: str) -> list:
    wrapped = f"LOAD '{SO}'; SET pgtantivy.index_dir='{IDX}'; {sql}"
    r = subprocess.run(["psql", "-tA", "-q", "-v", "ON_ERROR_STOP=1",
                        "-d", DB, "-c", wrapped],
                       env=ENV, capture_output=True, text=True, timeout=180)
    if r.returncode != 0:
        raise RuntimeError(f"SQL failed: {sql!r}\n{r.stderr}")
    return [ln for ln in (r.stdout or "").splitlines() if ln.strip()]


def q(sql: str) -> str:
    lines = q_lines(sql)
    return lines[-1].strip() if lines else ""


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
    r = subprocess.run(["psql", "-q", "-v", "ON_ERROR_STOP=1", "-d", DB, "-c", FNS],
                       env=ENV, capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        raise RuntimeError(f"fns install failed:\n{r.stderr}")

    creds = f"'{EP}','{BUCKET}','{REGION}','{AK}','{SK}','{PREFIX}'"
    as_name = "rep"

    # ---- A. build + export v1 -------------------------------------------
    q(f"SELECT pgtantivy_create('snap', {sqlq(CORPUS_V1)}, 'chinese');")
    exp1 = q(f"SELECT pgtantivy_export_to_s3('snap', {creds}, 'writer-A', 2);")
    op1 = int(re.search(r"opstamp (\d+)", exp1).group(1))
    bundle1 = f"snap-{op1}.bundle"
    print(f"A. export v1  : {exp1}")

    # ---- B. load_s3 @ v1 -------------------------------------------------
    ld = q(f"SELECT pgtantivy_load_s3({creds}, {sqlq(bundle1)}, {sqlq(as_name)}, NULL); "
           f"SELECT count(*) FROM pgtantivy_search({sqlq(as_name)}, 'gamma', {2*N});")
    print(f"B. load v1   : {ld} (gamma expect 0)")
    assert ld == "0", f"v1 has no gamma, got {ld!r}"

    # ---- C. add 100 gamma docs to the WRITER index -----------------------
    # new backend -> re-register 'snap' from disk (pgtantivy_load) before the
    # registry-based pgtantivy_add; commit writes the sidecar on disk, which
    # the next export reads.
    add = q(f"SELECT pgtantivy_load('snap'); "
            f"SELECT pgtantivy_add('snap', {sqlq(CORPUS_EXTRA)});")
    print(f"C. add docs  : {add}")
    assert "added" in add, f"add failed: {add!r}"

    # ---- D. export v2 (keep both) ----------------------------------------
    exp2 = q(f"SELECT pgtantivy_export_to_s3('snap', {creds}, 'writer-A', 2);")
    op2 = int(re.search(r"opstamp (\d+)", exp2).group(1))
    bundle2 = f"snap-{op2}.bundle"
    print(f"D. export v2  : {exp2}")
    assert op2 > op1, f"expected v2 opstamp > v1 (got {op2} <= {op1})"

    # ---- E. list_s3: two published versions, newest first ----------------
    # TableIterator rows render as "(op,bundle,ndocs)" under -tA.
    alltext = "\n".join(q_lines(f"SELECT pgtantivy_list_s3({creds}, 'snap');"))
    ops = [int(x) for x in re.findall(r"\((\d+),", alltext)]
    print(f"E. list_s3    : {alltext!r}")
    assert op2 in ops and op1 in ops, f"both versions must be listed: {ops}"
    assert ops[0] == op2, f"newest first: {ops}"

    # ---- F/G/H: load v1 -> upgrade -> noop -> rollback, ONE backend -------
    # (S3 replica is per-backend registry; all of these must share a backend
    # to see the 'rep' entry. Same psql invocation = one backend.)
    parts = q_lines(
        f"SELECT pgtantivy_load_s3({creds}, {sqlq(bundle1)}, {sqlq(as_name)}, NULL); "
        f"SELECT pgtantivy_reload_s3({sqlq(as_name)}, NULL); "
        f"SELECT (SELECT count(*) FROM pgtantivy_search({sqlq(as_name)}, 'gamma', {2*N})) AS g, "
        f"       (SELECT count(*) FROM pgtantivy_search({sqlq(as_name)}, 'alpha', {2*N})) AS a; "
        f"SELECT pgtantivy_reload_s3({sqlq(as_name)}, NULL); "
        f"SELECT pgtantivy_reload_s3({sqlq(as_name)}, {sqlq(bundle1)}); "
        f"SELECT (SELECT count(*) FROM pgtantivy_search({sqlq(as_name)}, 'gamma', {2*N})) AS g, "
        f"       (SELECT count(*) FROM pgtantivy_search({sqlq(as_name)}, 'alpha', {2*N})) AS a;"
    )
    # parts: 0=load 1=reload-NEW 2=counts(up) 3=noop 4=rollback 5=counts(down)
    assert len(parts) == 6, f"expected 6 result rows, got {len(parts)}: {parts!r}"
    load_msg, up_msg, up_counts, noop_msg, rb_msg, down_counts = parts
    up_counts = up_counts.split("|")
    down_counts = down_counts.split("|")
    print(f"F. load v1   : {load_msg.strip()}")
    print(f"   reload NEW: {up_msg.strip()}")
    print(f"   up counts : gamma={up_counts[0].strip()} alpha={up_counts[1].strip()} (expect 100/50)")
    print(f"G. noop      : {noop_msg.strip()}")
    print(f"H. rollback  : {rb_msg.strip()}")
    print(f"   down counts: gamma={down_counts[0].strip()} alpha={down_counts[1].strip()} (expect 0/50)")
    assert f"reloaded -> {bundle2}" in up_msg, f"upgrade target: {up_msg!r}"
    assert up_counts[0].strip() == "100", f"v2 gamma got {up_counts[0]!r}"
    assert up_counts[1].strip() == "50", f"v2 alpha got {up_counts[1]!r}"
    assert "already serving" in noop_msg, f"expected no-op, got {noop_msg!r}"
    assert f"reloaded -> {bundle1}" in rb_msg, f"rollback target: {rb_msg!r}"
    assert down_counts[0].strip() == "0", f"v1 has no gamma, got {down_counts[0]!r}"
    assert down_counts[1].strip() == "50", f"v1 alpha got {down_counts[1]!r}"

    # ---- I. bad to_bundle -> clear error ---------------------------------
    # (fresh backend: load first so 'rep' exists, then the bad-bundle guard
    # fires — reload errors, which ON_ERROR_STOP surfaces as a failed call)
    try:
        q_lines(f"SELECT pgtantivy_load_s3({creds}, {sqlq(bundle1)}, {sqlq(as_name)}, NULL); "
                f"SELECT pgtantivy_reload_s3({sqlq(as_name)}, 'not-a-bundle');")
        raise RuntimeError("expected error for bad to_bundle, got success")
    except RuntimeError as e:
        assert "to_bundle" in str(e), f"expected to_bundle error, got: {e}"
        print(f"I. bad bundle : errored as expected (to_bundle validation)")

    # cleanup
    shutil.rmtree(IDX, ignore_errors=True)
    subprocess.run(["psql", "-tA", "-q", "-c", f"DROP DATABASE IF EXISTS {DB};"],
                   env=ENV, capture_output=True, text=True, timeout=60)
    print("\nALL RELOAD_S3 E2E CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
