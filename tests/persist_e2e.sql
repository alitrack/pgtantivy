-- pgtantivy persistence E2E
LOAD '/tmp/pgext/lib/pgtantivy.so';

-- 1) create persistent index
SELECT pgtantivy_create('persist_test',
  (SELECT json_agg(json_build_object('id', doc_id, 'text', body))::text FROM docs),
  'chinese');

-- 2) verify on disk
\! ls -la $(psql -d seama -t -A -c "SHOW data_directory;")/pgtantivy/ 2>/dev/null || ls -la /var/lib/postgresql/17/main/pgtantivy/ 2>/dev/null || echo NO_DIR

-- 3) search before restart (session A)
SELECT '-- search in same session --' AS m;
SELECT * FROM pgtantivy_search('persist_test', '量子', 3);

-- 4) NEW session: registry is empty, load from disk, search again
SELECT '-- fresh session: load + search --' AS m;
SELECT pgtantivy_load('persist_test');
SELECT * FROM pgtantivy_search('persist_test', '量子', 3);

-- 5) list
SELECT * FROM pgtantivy_list() ORDER BY name;

-- 6) GUC visibility
SHOW pgtantivy.index_dir;
SET pgtantivy.index_dir = '/tmp/pgtantivy_alt';
SELECT pgtantivy_create('alt_test',
  (SELECT json_agg(json_build_object('id', doc_id, 'text', body))::text FROM docs),
  'chinese');
\! ls /tmp/pgtantivy_alt/ 2>/dev/null || echo ALT_DIR_MISSING
RESET pgtantivy.index_dir;
