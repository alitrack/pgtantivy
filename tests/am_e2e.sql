-- index AM MVP E2E (full): build, search, mutate, REINDEX rebuild
LOAD '/tmp/pgext/lib/pgtantivy.so';
SET client_min_messages = WARNING;

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

SELECT count(*) AS in_pg FROM pg_index WHERE indrelid='am_docs'::regclass;

-- all 5 topics searchable
SELECT string_agg(id::text, ',' ORDER BY id) AS hits_all FROM pgtantivy_search('am_fts', '技术', 10);
SELECT id FROM pgtantivy_search('am_fts', '相干时间', 5);

-- mutate heap then REINDEX: sidecar must reflect new heap state
INSERT INTO am_docs VALUES (6, '区块链跨链协议的安全审计完成');
DELETE FROM am_docs WHERE id = 5;
REINDEX INDEX am_fts;

SELECT '-- after INSERT+DELETE+REINDEX --' AS step;
SELECT id FROM pgtantivy_search('am_fts', '跨链协议', 5);      -- expect 6
SELECT id FROM pgtantivy_search('am_fts', '基因编辑', 5);      -- expect empty
SELECT count(*) AS docs_now FROM pgtantivy_search('am_fts', '技术', 10);

-- WITH defaults (no reloptions) also works
DROP INDEX am_fts;
CREATE INDEX am_dflt ON am_docs USING pgtantivy (body);
SELECT id FROM pgtantivy_search('am_dflt', '相干时间', 5);      -- expect 1

-- cleanup
DROP INDEX am_dflt;
DROP TABLE am_docs;
