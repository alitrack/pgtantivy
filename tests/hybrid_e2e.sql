-- pgtantivy_hybrid E2E: BM25 + pgvector + RRF
-- Mirrors the duckdb-tantivy tantivy_rrf_hybrid 90k benchmark setup in miniature.
CREATE EXTENSION IF NOT EXISTS vector;

LOAD '/tmp/pgext/lib/pgtantivy.so';
SET client_min_messages = WARNING;

-- 1) SQL-side schema for hybrid: drop old function first (reload-friendly)
-- (schema-managed; do not drop here)

DROP TABLE IF EXISTS hybrid_docs;
CREATE TABLE hybrid_docs (id BIGINT, body TEXT);
INSERT INTO hybrid_docs VALUES
 (1, '量子比特的相干时间是量子计算核心指标'),
 (2, '大模型的训练需要海量算力支撑'),
 (3, '航天工程的推进依赖新材料技术突破'),
 (4, '新能源汽车的电池能量密度持续提升'),
 (5, '生物医药的基因编辑技术日趋成熟');

-- 2) FTS sidecar via AM
CREATE INDEX hybrid_fts ON hybrid_docs USING pgtantivy (body)
  WITH (text_column='body', id_column='id', language='chinese');

-- 3) Vector side in pgvector: planted geometry — doc1..5 laid out so that
--    semantic query "计算机科学" (q_vec) is nearest to ids {1,2,3}
DROP TABLE IF EXISTS hybrid_vec;
CREATE TABLE hybrid_vec (id BIGINT, embedding vector(3));
INSERT INTO hybrid_vec VALUES
 (1, '[1.0, 0.0, 0.0]'),
 (2, '[0.9, 0.1, 0.0]'),
 (3, '[0.8, 0.2, 0.0]'),
 (4, '[0.0, 1.0, 0.0]'),
 (5, '[0.0, 0.0, 1.0]');

SELECT '-- lex query: FTS only ranks 1; hybrid keeps 1 top --' AS step;
SELECT id, round(rrf_score::numeric, 4) AS rrf, fts_rank, vec_rank
  FROM pgtantivy_hybrid('hybrid_fts', '量子比特',
                        'hybrid_vec', 'embedding', 'id',
                        '[0.95, 0.05, 0.0]', 'l2', 5::bigint, 5::bigint, 60::bigint)
 ORDER BY rrf_score DESC;

SELECT '-- semantic-ish query: both sides agree on {1,2,3} --' AS step;
SELECT id, round(rrf_score::numeric, 4) AS rrf, fts_rank, vec_rank
  FROM pgtantivy_hybrid('hybrid_fts', '量子 计算',
                        'hybrid_vec', 'embedding', 'id',
                        '[0.9, 0.1, 0.0]', 'l2', 5::bigint, 5::bigint, 60::bigint)
 ORDER BY rrf_score DESC;

SELECT '-- cosine metric + vec-only hit (FTS misses) --' AS step;
SELECT id, fts_rank, vec_rank
  FROM pgtantivy_hybrid('hybrid_fts', '跨链协议',
                        'hybrid_vec', 'embedding', 'id',
                        '[1.0, 0.0, 0.0]', 'cosine', 5::bigint, 5::bigint, 60::bigint)
 ORDER BY rrf_score DESC;

SELECT '-- error path: bad metric --' AS step;
SELECT count(*) FROM pgtantivy_hybrid('hybrid_fts', 'x',
  'hybrid_vec', 'embedding', 'id', '[1,0,0]', 'euclidean', 5::bigint, 5::bigint, 60::bigint);

DROP INDEX hybrid_fts;
DROP TABLE hybrid_docs;
DROP TABLE hybrid_vec;
