#!/usr/bin/env python3
"""pgtantivy vs PG native FTS: 50-query latency + recall benchmark."""
import subprocess, time, json, statistics

queries = open('/tmp/zh_queries.txt').read().split('\n')
assert len(queries) == 50

# planted ground truth: query substring -> doc ids that must be found
PLANTED = {
    "量子比特": [90001], "量子 计算": [90001], "大模型 算力": [90002],
    "航天 新材料": [90003], "电池 能量密度": [90004], "生物医药 基因": [90005],
    "相干时间": [90001], "人工智能 训练": [90002], "新能源汽车 电池": [90004],
}

def pgtantivy(q, k=10):
    sql = f"""LOAD '/tmp/pgext/lib/pgtantivy.so';
SELECT pgtantivy_load('bench');
SELECT id, score FROM pgtantivy_search('bench', '{q}', {k});"""
    r = subprocess.run(['psql','-d','seama','-At','-c',sql], capture_output=True, text=True)
    if r.returncode != 0:
        return None, r.stderr.strip().split('\n')[0]
    lines = [l for l in r.stdout.strip().split('\n') if l and '|' in l and 'loaded' not in l]
    rows = [l.split('|') for l in lines]
    return [(int(a), float(b)) for a, b in rows], None

def pg_fts(q, k=10):
    # PG 'simple' config: whitespace tokens only — no Chinese word segmentation.
    # & = AND semantics to match pgtantivy
    tsq = ' & '.join(q.split())
    sql = f"""SELECT doc_id, ts_rank(to_tsvector('simple', body), to_tsquery('simple', '{tsq}')) AS score
FROM zh_docs WHERE to_tsvector('simple', body) @@ to_tsquery('simple', '{tsq}')
ORDER BY score DESC LIMIT {k};"""
    r = subprocess.run(['psql','-d','seama','-At','-c',sql], capture_output=True, text=True)
    if r.returncode != 0:
        return None, r.stderr.strip().split('\n')[0]
    rows = [l.split('|') for l in r.stdout.strip().split('\n') if l]
    return [(int(a), float(b)) for a, b in rows], None

# warmup
pgtantivy("量子比特", 5); pg_fts("量子比特", 5); pgtantivy("产业升级", 5); pg_fts("产业升级", 5)

tt, tp = [], []
rec_t, rec_p = [], []
errs_t, errs_p = set(), set()
for q in queries:
    # pgtantivy
    t0=time.time(); hits_t, e1 = pgtantivy(q); t1=time.time()
    t0p=time.time(); hits_p, e2 = pg_fts(q); t1p=time.time()
    tt.append(t1-t0); tp.append(t1p-t0p)
    if e1: errs_t.add(e1[:80])
    if e2: errs_p.add(e2[:80])
    # recall on planted queries
    if q in PLANTED:
        ids_t = {h[0] for h in (hits_t or [])}
        ids_p = {h[0] for h in (hits_p or [])}
        want = set(PLANTED[q])
        rec_t.append(len(ids_t & want)/len(want))
        rec_p.append(len(ids_p & want)/len(want))

def stats(x): return f"p50={statistics.median(x)*1000:.1f}ms p95={sorted(x)[int(len(x)*0.95)-1]*1000:.1f}ms mean={statistics.mean(x)*1000:.1f}ms"
print(f"pgtantivy : {stats(tt)}  errors={len(errs_t)}")
print(f"PG native : {stats(tp)}  errors={len(errs_p)}")
if errs_t: print("  t-err:", list(errs_t)[:2])
if errs_p: print("  p-err:", list(errs_p)[:2])
if rec_t: print(f"planted recall: pgtantivy={sum(rec_t)/len(rec_t):.2f} ({len(rec_t)} q)  PG={sum(rec_p)/len(rec_p):.2f}")

# sample check
h,_ = pgtantivy("量子比特"); print("sample pgtantivy 量子比特:", h[:3] if h else None)
h,_ = pg_fts("量子比特"); print("sample PG 量子比特:", h[:3] if h else None)
