-- planner E2E v2: force index path (small tables default to seqscan)
LOAD '/tmp/pgext/lib/pgtantivy.so';
SET client_min_messages = WARNING;
SET enable_seqscan = off;

DROP TABLE IF EXISTS am_docs;
CREATE TABLE am_docs (id BIGINT, body TEXT);
INSERT INTO am_docs VALUES
 (1, '量子比特的相干时间是量子计算核心指标'),
 (2, '大模型的训练需要海量算力支撑'),
 (3, '航天工程的推进依赖新材料技术突破'),
 (4, '新能源汽车的电池能量密度持续提升'),
 (5, '生物医药的基因编辑技术日趋成熟');

CREATE INDEX am_fts ON am_docs USING pgtantivy (body)
  WITH (text_column='body', id_column='id', language='chinese');

SELECT '-- plan --' AS step;
EXPLAIN (COSTS OFF) SELECT id FROM am_docs WHERE body @@@ '量子比特';

SELECT '-- results via bitmap index scan --' AS step;
SELECT id FROM am_docs WHERE body @@@ '量子比特' ORDER BY id;   -- 1
SELECT id FROM am_docs WHERE body @@@ '基因编辑' ORDER BY id;   -- 5
SELECT id FROM am_docs WHERE body @@@ '大模型' ORDER BY id;     -- 2
SELECT id FROM am_docs WHERE body @@@ '技术' ORDER BY id;       -- 1,3,5

-- mutation + REINDEX keeps @@@ correct
INSERT INTO am_docs VALUES (6, '区块链跨链协议的安全审计完成');
DELETE FROM am_docs WHERE id = 5;
REINDEX INDEX am_fts;
SELECT '-- after INSERT+DELETE+REINDEX --' AS step;
SELECT id FROM am_docs WHERE body @@@ '跨链协议' ORDER BY id;   -- 6
SELECT id FROM am_docs WHERE body @@@ '基因编辑' ORDER BY id;   -- none
SELECT id FROM am_docs WHERE body @@@ '技术' ORDER BY id;       -- 1,3

DROP INDEX am_fts;
DROP TABLE am_docs;
