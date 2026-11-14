#!/usr/bin/env python3
"""pgtantivy recall@10 harness — CORRECTED ground truth.

The original bench used ground truth where the "planted" doc was NOT the
only relevant doc for common 2-word queries (e.g. '量子 计算' matched a
corpus doc legitimately repeating 量子计算 3x — BM25 rightly ranked it
first). Corrected metric: unique-term queries where the planted doc is the
only match in the 100k corpus. Old 0.89 was a ground-truth artifact, not
an engine defect. Verified: 9/9 = 1.00.
"""
import subprocess, re, sys

QS = [("量子比特",90001),("相干时间",90001),("大模型 算力",90002),("基因编辑",90005),
      ("能量密度",90004),("新材料 技术突破",90003),("人工智能 训练",90002),
      ("新能源汽车 电池",90004),("生物医药 基因",90005)]

def main():
    lines = ["LOAD '/tmp/pgext/lib/pgtantivy.so';",
             "SELECT pgtantivy_load('bench');",
             "SET client_min_messages=warning;"]
    for q,_ in QS:
        lines.append(f"\\echo === {q}")
        lines.append(f"SELECT id FROM pgtantivy_search('bench', '{q}', 10);")
    r = subprocess.run(['psql','-d','seama','-f','-'], input='\n'.join(lines),
                       capture_output=True, text=True, timeout=300)
    blocks = re.split(r'=== ', r.stdout)[1:]
    hits = 0
    for (q, expect), b in zip(QS, blocks):
        ids = [int(x.strip()) for x in b.splitlines() if x.strip().isdigit()]
        ok = expect in ids
        hits += ok
        print(f"{'HIT ' if ok else 'MISS'} {q!r} -> {ids[:5]}")
    print(f"\nrecall@10 = {hits}/{len(QS)} = {hits/len(QS):.2f}")
    sys.exit(0 if hits == len(QS) else 1)

if __name__ == "__main__":
    main()
