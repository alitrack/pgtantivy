-- auto-merge E2E: threshold GUC drives automatic compaction on add
LOAD '/tmp/pgext/lib/pgtantivy.so';
SET client_min_messages = WARNING;

SELECT pgtantivy_create('auto_test', '[{"id":1,"text":"量子计算基础"}]', 'chinese');

-- threshold=4: after the 4th segment exists, add() should auto-merge back to 1
SET pgtantivy.merge_threshold = 4;
SELECT pgtantivy_add('auto_test', '[{"id":2,"text":"人工智能"}]');
SELECT pgtantivy_add('auto_test', '[{"id":3,"text":"航天"}]');
SELECT pgtantivy_add('auto_test', '[{"id":4,"text":"生物医药"}]');
-- now 4 segments >= threshold 4: this add should trigger auto-merge
SELECT pgtantivy_add('auto_test', '[{"id":5,"text":"新能源汽车"}]');

SELECT '-- after 5 adds with threshold 4: expect 1 segment (auto-merged) --' AS m;
SELECT json_array_length(pg_read_file('/var/lib/postgresql/17/main/pgtantivy/auto_test/meta.json')::json -> 'segments') AS segments;

-- disabled: threshold=0 keeps every segment
SELECT pgtantivy_create('manual_test', '[{"id":1,"text":"量子"}]', 'chinese');
SET pgtantivy.merge_threshold = 0;
SELECT pgtantivy_add('manual_test', '[{"id":2,"text":"人工智能"}]');
SELECT pgtantivy_add('manual_test', '[{"id":3,"text":"航天"}]');
SELECT json_array_length(pg_read_file('/var/lib/postgresql/17/main/pgtantivy/manual_test/meta.json')::json -> 'segments') AS segments_manual;

-- manual merge still available
SELECT pgtantivy_merge('manual_test');
SELECT json_array_length(pg_read_file('/var/lib/postgresql/17/main/pgtantivy/manual_test/meta.json')::json -> 'segments') AS segments_after_manual;

-- search intact
SELECT id FROM pgtantivy_search('auto_test', '量子 OR 人工智能 OR 新能源', 10);
