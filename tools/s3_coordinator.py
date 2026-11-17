#!/usr/bin/env python3
"""S3 shared-index multi-replica coordinator (control plane).

Coordinates WHICH version of a published index bundle every replica node
should be serving, on top of the pgtantivy S3 primitives. The plugin is
UNMODIFIED — this tool only orchestrates the existing, idempotent SQL:

    pgtantivy_load_s3(...)        first-time load (no-op if already loaded)
    pgtantivy_reload_s3(n, b)     converge one backend to bundle b (idempotent)
    pgtantivy_search(n, q, k)     canary readability probe

Protocol (all state in S3, no new PG objects):

    <prefix>/control/<name>.target.json
        {"name": ..., "op": N, "bundle": "<name>-N.bundle", "set_at": ...}

A single-object PUT is atomic on object stores, so the target switch is a
single atomic write. Fleet history is whatever S3 object versioning keeps
(enable it on the bucket if you want an audit trail).

App side (the only consumer of the target): each backend's loop reads the
target and calls `pgtantivy_reload_s3(name, target_bundle)` — instead of
`reload_s3(name, NULL)`. That one-line change is what buys pinning /
coordinated rollback / canary. NULL mode (follow newest) CANNOT be pinned
or rolled back; target mode can.

Honest boundary (per-backend registry): the coordinator cannot force
already-running application backends to switch. It (a) publishes the target,
(b) verifies every fleet node CAN load+search the target bundle (canary, in
an isolated session that never touches the app's registry entry), and
(c) gates the release on all canaries passing. Fleet convergence then
happens via the apps' own loops, each backend at its next tick.

Subcommands:
    get-target   --name N                 print current target bundle (or NONE)
    set-target   --name N --op O          publish target op O (ops decision)
    canary       --name N --op O --node "label=sockdir:port"
                 node health probe for op O's bundle
    release      --name N --op O --nodes "b=...:5445,c=...:5446"
                 canary EVERY node; publish target only if all pass
    status       --name N --nodes "..."   fleet table: per node, target +
                 node's registered state (s3_status) if loadable

Run with minio available:  uv run --with minio python tools/s3_coordinator.py ...
"""
import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone

DEFAULT_SO = "/tmp/pgext/lib/pgtantivy.so"

# The minimal SQL surface the canary/tick need (idempotent CREATE OR REPLACE).
FNS = """
CREATE OR REPLACE FUNCTION "pgtantivy_load_s3"(
  "endpoint" TEXT, "bucket" TEXT, "region" TEXT,
  "access_key" TEXT, "secret" TEXT, "prefix" TEXT,
  "bundle_name" TEXT, "as_name" TEXT, "warm_terms" TEXT
) RETURNS TEXT LANGUAGE c AS '%(so)s', 'pgtantivy_load_s3_wrapper';
CREATE OR REPLACE FUNCTION "pgtantivy_reload_s3"(
  "name" TEXT, "to_bundle" TEXT
) RETURNS TEXT LANGUAGE c AS '%(so)s', 'pgtantivy_reload_s3_wrapper';
CREATE OR REPLACE FUNCTION "pgtantivy_search"(
  "name" TEXT, "query" TEXT, "top_k" bigint
) RETURNS TABLE ("id" bigint, "score" double precision)
STRICT LANGUAGE c AS '%(so)s', 'pgtantivy_search_wrapper';
CREATE OR REPLACE FUNCTION "pgtantivy_s3_status"(
  "name" TEXT
) RETURNS TEXT LANGUAGE c AS '%(so)s', 'pgtantivy_s3_status_wrapper';
""" % {"so": DEFAULT_SO}


def client(args):
    from minio import Minio
    # minio-py wants a bare "host:port" (it adds the scheme from `secure`);
    # a URL like "http://127.0.0.1:9000" raises "path in endpoint is not allowed".
    host = args.endpoint.split("//")[-1].split("/")[0]
    return Minio(host, access_key=args.ak, secret_key=args.sk, secure=False)


def target_key(prefix, name):
    return f"{prefix.rstrip('/')}/control/{name}.target.json"


def list_ops(c, bucket, prefix, name):
    """op -> bundle name, from published manifests (mirrors plugin list_versions)."""
    top = prefix.rstrip("/")
    want = f"{top}/{name}-"
    ops = {}
    for o in c.list_objects(bucket, prefix=top, recursive=True):
        loc = o.object_name
        if loc.startswith(want) and loc.endswith("manifest.json"):
            # "<prefix>/<name>-<op>.bundle/manifest.json" -> bundle dir name
            # (strip_suffix, matching the plugin's list_versions exactly)
            bare = loc.split("/")[-2]
            if bare.endswith(".bundle"):
                bare = bare[:-len(".bundle")]
            op = int(bare.rsplit("-", 1)[1])
            ops[op] = f"{name}-{op}.bundle"
    return ops


def get_target(args, name):
    try:
        return json.loads(get_target_raw(args.prefix, name, args.bucket,
                                         args.endpoint, args.region, args.ak, args.sk))
    except Exception:
        return None


def get_target_raw(prefix, name, bucket, endpoint, region, ak, sk):
    """Raw target JSON text (or raises). Module-level so callers (e2e, ops
    scripts) can read the control point without the argparse surface."""
    from minio import Minio
    host = endpoint.split("//")[-1].split("/")[0]
    c = Minio(host, access_key=ak, secret_key=sk, secure=False)
    r = c.get_object(bucket, target_key(prefix, name))
    data = r.read().decode()
    r.close()
    r.release_conn()
    return data


def set_target(args, name, op, bundle):
    c = client(args)
    body = json.dumps({
        "name": name, "op": op, "bundle": bundle,
        "set_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }).encode()
    c.put_object(args.bucket, target_key(args.prefix, name),
                 __import__("io").BytesIO(body), len(body))


def node_env(spec):
    """'b=/tmp/pgtsock_b:5445' -> (label, {PGHOST, PGPORT})."""
    label, rest = spec.split("=", 1)
    sock, port = rest.split(":", 1)
    import os
    env = dict(os.environ, PGHOST=sock, PGPORT=port)
    return label, env


def psql(env, sql, db="postgres"):
    r = subprocess.run(["psql", "-tA", "-q", "-d", db, "-c", sql],
                       env=env, capture_output=True, text=True, timeout=120)
    return r.returncode, r.stdout.strip(), r.stderr.strip()


def canary(args, label_env, name, bundle, idx_dir, probe_term="gamma"):
    """Isolated-session probe: node can LOAD and SEARCH this bundle.

    Uses as_name 'canary_<op>' (unique per version) so a stale canary entry
    from a previous release can never mask this one. Never touches the app's
    registry entry.
    """
    label, env = label_env
    op = bundle.rsplit("-", 1)[1].split(".")[0]
    as_name = f"canary_{name}_{op}"
    sql = (f"LOAD '{args.so}'; SET pgtantivy.index_dir='{idx_dir}'; {FNS}\n"
           f"SELECT pgtantivy_load_s3('{args.endpoint}','{args.bucket}',"
           f"'{args.region}','{args.ak}','{args.sk}','{args.prefix}',"
           f"'{bundle}','{as_name}',NULL);\n"
           f"SELECT count(*) FROM pgtantivy_search('{as_name}','{probe_term}',1);")
    rc, out, err = psql(env, sql)
    if rc != 0:
        return False, f"unreachable/failed: {(err or out).splitlines()[-1] if (err or out) else 'no output'}"
    lines = [l for l in out.splitlines() if l.strip()]
    return True, "; ".join(lines[-2:])


def parse_nodes(s):
    return [x for x in s.split(",") if x.strip()]


def cmd_get_target(args):
    t = get_target(args, args.name)
    if t is None:
        print("NONE (no target published yet)")
        return 1
    print(t["bundle"])
    return 0


def cmd_set_target(args):
    c = client(args)
    ops = list_ops(c, args.bucket, args.prefix, args.name)
    if args.op not in ops:
        print(f"ERROR: op {args.op} not published (have: {sorted(ops)})")
        return 2
    set_target(args, args.name, args.op, ops[args.op])
    print(f"TARGET {args.name} -> {ops[args.op]} (op {args.op})")
    return 0


def cmd_canary(args):
    spec = parse_nodes(args.node)[0]
    ok, detail = canary(args, node_env(spec), args.name,
                        _bundle_for(args, args.op), args.idx_dir)
    print(f"CANARY {spec.split('=')[0]} ({args.name} op {args.op}): "
          f"{'OK' if ok else 'FAILED'} — {detail}")
    return 0 if ok else 1


def _bundle_for(args, op):
    c = client(args)
    ops = list_ops(c, args.bucket, args.prefix, args.name)
    if op not in ops:
        print(f"ERROR: op {op} not published (have: {sorted(ops)})")
        sys.exit(2)
    return ops[op]


def cmd_release(args):
    bundle = _bundle_for(args, args.op)
    cur = get_target(args, args.name)
    cur_bundle = cur["bundle"] if cur else "NONE"
    results = {}
    for spec in parse_nodes(args.nodes):
        le = node_env(spec)
        ok, detail = canary(args, le, args.name, bundle, args.idx_dir)
        results[le[0]] = (ok, detail)
        print(f"CANARY {le[0]} ({bundle}): {'OK' if ok else 'FAILED'} — {detail}")
    passed = [l for l, (ok, _) in results.items() if ok]
    if len(passed) == len(results) and results:
        set_target(args, args.name, args.op, bundle)
        print(f"RELEASE {args.name} -> op {args.op} ({bundle}): APPLIED "
              f"(canaries {len(passed)}/{len(results)}); target {cur_bundle} -> {bundle}")
        return 0
    print(f"RELEASE {args.name} -> op {args.op}: BLOCKED "
          f"(canaries {len(passed)}/{len(results)}); target unchanged ({cur_bundle})")
    return 1


def cmd_status(args):
    t = get_target(args, args.name)
    target_bundle = t["bundle"] if t else "NONE"
    print(f"target: {target_bundle} (op {t['op'] if t else '-'})")
    for spec in parse_nodes(args.nodes):
        label, env = node_env(spec)
        sql = (f"LOAD '{args.so}'; SET pgtantivy.index_dir='{args.idx_dir}'; {FNS}\n"
               f"SELECT pgtantivy_s3_status('{args.name}');")
        rc, out, err = psql(env, sql)
        if rc != 0:
            print(f"{label}: UNREACHABLE ({(err or out).splitlines()[-1] if (err or out) else '?'})")
            continue
        last = [l for l in out.splitlines() if l.strip()][-1]
        mark = "OK" if target_bundle in last else "off-target"
        print(f"{label}: {mark} — {last}")


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--endpoint", required=True)
    p.add_argument("--bucket", required=True)
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--ak", required=True)
    p.add_argument("--sk", required=True)
    p.add_argument("--prefix", required=True)
    p.add_argument("--so", default=DEFAULT_SO)
    p.add_argument("--idx-dir", default="/tmp/pgt_coord_canary")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("get-target"); sp.add_argument("--name", required=True)
    sp = sub.add_parser("set-target"); sp.add_argument("--name", required=True); sp.add_argument("--op", type=int, required=True)
    sp = sub.add_parser("canary"); sp.add_argument("--name", required=True); sp.add_argument("--op", type=int, required=True); sp.add_argument("--node", required=True)
    sp = sub.add_parser("release"); sp.add_argument("--name", required=True); sp.add_argument("--op", type=int, required=True); sp.add_argument("--nodes", required=True)
    sp = sub.add_parser("status"); sp.add_argument("--name", required=True); sp.add_argument("--nodes", required=True)

    args = p.parse_args()
    args.so = args.so
    handlers = {"get-target": cmd_get_target, "set-target": cmd_set_target,
                "canary": cmd_canary, "release": cmd_release, "status": cmd_status}
    return handlers[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
