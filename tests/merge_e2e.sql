-- pgtantivy_merge E2E: multi-segment index -> merge -> verify
LOAD '/tmp/pgext/lib/pgtantivy.so';
SET client_min_messages = warning;

SELECT pgtantivy_create('merge_test',
  '[{"id":1,"text":"量子计算基础"}]', 'chinese');

-- 4 incremental adds => 5 segments
SELECT pgtantivy_add('merge_test', '[{"id":2,"text":"人工智能大模型"}]');
SELECT pgtantivy_add('merge_test', '[{"id":3,"text":"航天新材料"}]');
SELECT pgtantivy_add('merge_test', '[{"id":4,"text":"生物医药基因编辑"}]');

-- delete => tombstones
SELECT pgtantivy_remove('merge_test', ARRAY[1]);

SELECT '-- pre-merge state --' AS m;
SELECT * FROM pgtantivy_list() WHERE name='merge_test';
SELECT id, round(score::numeric,2) FROM pgtantivy_search('merge_test', '量子 OR 人工智能 OR 航天 OR 基因编辑', 10);

SELECT pgtantivy_merge('merge_test');

SELECT '-- post-merge state --' AS m;
SELECT * FROM pgtantivy_list() WHERE name='merge_test';
SELECT id, round(score::numeric,2) FROM pgtantivy_search('merge_test', '量子 OR 人工智能 OR 航天 OR 基因编辑', 10);
SELECT count(*) AS qubit_gone FROM pgtantivy_search('merge_test', '量子', 5);
