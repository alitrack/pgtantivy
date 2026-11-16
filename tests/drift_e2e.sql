-- pgtantivy drift detection E2E (self-contained: registers its own functions)
-- Verifies the drift-detection feature (research #2026111601 "漂移检测" line):
--   * pgtantivy_create records a build-time WAL LSN anchor in the sidecar meta
--   * pgtantivy_drift_check(name) is false right after build, true once the WAL
--     advances past the anchor (real DML on an unrelated table)
--   * pgtantivy_touch / pgtantivy_add push the anchor forward -> false again
--   * a stale index still searches (drift is a signal, not a correctness gate);
--     the load-time STALE warning needs a fresh session, so it is asserted by
--     the harness, not here (one psql = one backend = one in-process registry).
-- Real PG17 cluster, real tantivy sidecar, isolated index dir.
LOAD '/tmp/pgext/lib/pgtantivy.so';

-- register the two drift functions (idempotent; matches the deployed .so convention)
CREATE OR REPLACE FUNCTION pgtantivy_drift_check(name TEXT)
RETURNS BOOLEAN LANGUAGE c STRICT
AS '/tmp/pgext/lib/pgtantivy.so', 'pgtantivy_drift_check_wrapper';
CREATE OR REPLACE FUNCTION pgtantivy_touch(name TEXT)
RETURNS TEXT LANGUAGE c STRICT
AS '/tmp/pgext/lib/pgtantivy.so', 'pgtantivy_touch_wrapper';
SET pgtantivy.index_dir = '/tmp/pgtantivy_drift';

-- 1) build a small persistent index -> anchor recorded
SELECT pgtantivy_create('drift_test',
  (SELECT json_agg(json_build_object('id', doc_id, 'text', body))::text
   FROM zh_docs WHERE doc_id <= 3), 'chinese') AS create_result;

-- 2) immediately after build: WAL has not advanced past the anchor -> not drifted
SELECT pgtantivy_drift_check('drift_test') AS drift_after_build;

-- 3) advance the WAL with real DML on an unrelated table -> anchor now behind
CREATE TABLE IF NOT EXISTS drift_wal_pad (x int);
INSERT INTO drift_wal_pad SELECT generate_series(1, 50);
SELECT pgtantivy_drift_check('drift_test') AS drift_after_wal_advance;

-- 4) pgtantivy_touch pushes the anchor forward -> not drifted again
SELECT pgtantivy_touch('drift_test') AS touch_result;
SELECT pgtantivy_drift_check('drift_test') AS drift_after_touch;

-- 5) an incremental write also refreshes the anchor
INSERT INTO drift_wal_pad SELECT generate_series(100, 130);
SELECT pgtantivy_add('drift_test',
  '[{"id": 99999, "text": "漂移检测验证：量子比特的相干时间"}]') AS add_result;
SELECT pgtantivy_drift_check('drift_test') AS drift_after_add;

-- 6) advance WAL again -> drifted, but search still works (signal, not gate)
INSERT INTO drift_wal_pad SELECT generate_series(200, 230);
SELECT pgtantivy_drift_check('drift_test') AS drift_before_search;
SELECT id, round(score::numeric, 3) AS score
FROM pgtantivy_search('drift_test', '量子', 5);

SELECT * FROM pgtantivy_list() WHERE name = 'drift_test';
RESET pgtantivy.index_dir;
DROP TABLE IF EXISTS drift_wal_pad;
