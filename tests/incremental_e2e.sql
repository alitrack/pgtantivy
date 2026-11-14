-- pgtantivy incremental add/remove E2E
LOAD '/tmp/pgext/lib/pgtantivy.so';

SELECT pgtantivy_create('incr_test',
  (SELECT json_agg(json_build_object('id', doc_id, 'text', body))::text FROM zh_docs WHERE doc_id <= 3),
  'chinese');

-- baseline: 3 docs
SELECT 'baseline (3 docs)' AS m;
SELECT id, round(score::numeric, 3) FROM pgtantivy_search('incr_test', '量子', 5);

-- add 2 new docs (upsert semantics)
SELECT pgtantivy_add('incr_test',
  '[{"id": 90001, "text": "量子比特的相干时间是量子计算核心指标"},
    {"id": 90002, "text": "人工智能大模型的训练需要海量算力支撑"}]');

SELECT '-- after add: new docs searchable --' AS m;
SELECT id, round(score::numeric, 3) FROM pgtantivy_search('incr_test', '相干时间', 5);
SELECT id, round(score::numeric, 3) FROM pgtantivy_search('incr_test', '大模型', 5);

-- upsert: overwrite doc 90001 with different text
SELECT pgtantivy_add('incr_test',
  '[{"id": 90001, "text": "超导量子芯片的制备工艺日趋成熟"}]');
SELECT '-- after upsert: old term gone, new term hits --' AS m;
SELECT count(*) AS coherent_hits FROM pgtantivy_search('incr_test', '相干时间', 5);
SELECT id FROM pgtantivy_search('incr_test', '超导量子芯片', 5);

-- remove
SELECT pgtantivy_remove('incr_test', ARRAY[90002]);
SELECT '-- after remove --' AS m;
SELECT count(*) AS llm_hits FROM pgtantivy_search('incr_test', '大模型', 5);
SELECT * FROM pgtantivy_list() WHERE name = 'incr_test';

-- persistence: fresh session should still see all changes
SELECT '-- doc count after all ops (should be 4: 3 base + 90001 upsert) --' AS m;
