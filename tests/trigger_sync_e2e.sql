-- pgtantivy trigger auto-sync E2E
LOAD '/tmp/pgext/lib/pgtantivy.so';
SET client_min_messages = warning;

DROP TABLE IF EXISTS sync_docs CASCADE;
CREATE TABLE sync_docs(doc_id BIGINT PRIMARY KEY, body TEXT);
INSERT INTO sync_docs VALUES
  (1, '量子计算使用量子比特进行运算'),
  (2, '人工智能大模型的训练需要海量算力');

-- build index from table
SELECT pgtantivy_create('sync_idx',
  (SELECT json_agg(json_build_object('id', doc_id, 'text', body))::text FROM sync_docs),
  'chinese');

-- attach trigger
CREATE TRIGGER pgtantivy_sync
  AFTER INSERT OR UPDATE OR DELETE ON sync_docs
  FOR EACH ROW
  EXECUTE PROCEDURE pgtantivy_sync_trigger('sync_idx', 'doc_id', 'body');

SELECT '-- baseline --' AS m;
SELECT count(*) AS base_docs FROM pgtantivy_search('sync_idx', '量子 OR 人工智能 OR 区块链', 10);

-- 1) trigger INSERT syncs
BEGIN;
INSERT INTO sync_docs VALUES (3, '区块链技术的共识机制');
COMMIT;
SELECT '-- after INSERT (in same session, post-commit) --' AS m;
SELECT id FROM pgtantivy_search('sync_idx', '区块链', 5);
SELECT * FROM pgtantivy_list() WHERE name='sync_idx';

-- 2) trigger UPDATE syncs (upsert)
BEGIN;
UPDATE sync_docs SET body = '超导量子芯片的制备工艺' WHERE doc_id = 3;
COMMIT;
SELECT '-- after UPDATE: blockchain gone, chip found --' AS m;
SELECT count(*) AS blockchain_after_update FROM pgtantivy_search('sync_idx', '区块链', 5);
SELECT id FROM pgtantivy_search('sync_idx', '超导量子芯片', 5);

-- 3) trigger DELETE syncs
BEGIN;
DELETE FROM sync_docs WHERE doc_id = 1;
COMMIT;
SELECT '-- after DELETE --' AS m;
SELECT count(*) AS qubit_after_delete FROM pgtantivy_search('sync_idx', '量子', 5);

-- 4) ROLLBACK discards buffered ops
BEGIN;
INSERT INTO sync_docs VALUES (99, '应该被回滚的文本');
ROLLBACK;
SELECT '-- after ROLLBACK: text invisible --' AS m;
SELECT count(*) AS rolled_back FROM pgtantivy_search('sync_idx', '应该被回滚', 5);

-- 5) multi-row single txn = one commit (doc count exact)
BEGIN;
INSERT INTO sync_docs SELECT g, '批量文档编号' || g FROM generate_series(10, 29) g;
COMMIT;
SELECT '-- after 20-row batch --' AS m;
SELECT * FROM pgtantivy_list() WHERE name='sync_idx';
SELECT count(*) AS batch_hits FROM pgtantivy_search('sync_idx', '批量文档', 25);
