#!/usr/bin/env python3
"""Fair benchmark: single PG session per engine (warm), 50 queries inside."""
import subprocess, time, statistics

queries = open('/tmp/zh_queries.txt').read().split('\n')
assert len(queries) == 50

PLANTED = {
    "量子比特": [90001], "量子 计算": [90001], "大模型 算力": [90002],
    "航天 新材料": [90003], "电池 能量密度": [90004], "生物医药 基因": [90005],
    "相干时间": [90001], "人工智能 训练": [90002], "新能源汽车 电池": [90004],
}

# ---- pgtantivy: one session, load once, then per-query psql calls are
# separate sessions... no — use a single psql process with a script and time
# each query server-side instead. Simplest robust: connect via python?
# psycopg2 not guaranteed. Use psql with \timing per query in one script.
def build_tantivy_script():
    lines = ["LOAD '/tmp/pgext/lib/pgtantivy.so';",
             "SELECT pgtantivy_load('bench');",
             "SET client_min_messages=warning;"]
    for i, q in enumerate(queries):
        lines.append(f"\\echo Q{i}")
        lines.append(f"SELECT id, score FROM pgtantivy_search('bench', '{q}', 10);")
    return '\n'.join(lines)

def build_pg_script():
    lines = ["SET client_min_messages=warning;"]
    for i, q in enumerate(queries):
        tsq = ' & '.join(q.split())
        lines.append(f"\\echo Q{i}")
        lines.append(f"SELECT doc_id FROM zh_docs WHERE to_tsvector('simple', body) @@ to_tsquery('simple', '{tsq}') ORDER BY doc_id LIMIT 10;")
    return '\n'.join(lines)

def run_and_parse(script, tag):
    t0 = time.time()
    r = subprocess.run(['psql', '-d', 'seama', '-At', '-f', '/dev/stdin'],
                       input=script, capture_output=True, text=True, timeout=1200)
    total = time.time() - t0
    out = r.stdout.split('\n')
    # collect results per Q marker
    results, cur = {}, None
    for l in out:
        if l.startswith('Q') and l[1:].isdigit():
            cur = int(l[1:]); results[cur] = []
        elif cur is not None and l.strip() and '|' in l:
            results[cur].append(l)
    print(f"{tag}: total 50 queries in one session: {total:.2f}s  ({total/50*1000:.1f} ms/q incl. parse overhead)")
    return results

rt = run_and_parse(build_tantivy_script(), "pgtantivy")
rp = run_and_parse(build_pg_script(), "PG FTS")

# recall on planted
rec_t, rec_p = [], []
for q, want in PLANTED.items():
    if q not in queries: continue
    i = queries.index(q)
    ids_t = {int(l.split('|')[0]) for l in rt.get(i, [])}
    ids_p = {int(l.split('|')[0]) for l in rp.get(i, [])}
    rec_t.append(len(ids_t & set(want)) / len(want))
    rec_p.append(len(ids_p & set(want)) / len(want))
print(f"planted recall: pgtantivy={sum(rec_t)/len(rec_t):.2f} ({len(rec_t)} q)  PG simple={sum(rec_p)/len(rec_p):.2f}")

# hit counts for non-planted queries (info)
empty_t = sum(1 for i in range(50) if not rt.get(i))
empty_p = sum(1 for i in range(50) if not rp.get(i))
print(f"empty result queries: pgtantivy={empty_t}/50  PG={empty_p}/50")
