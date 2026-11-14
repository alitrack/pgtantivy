-- pgtantivy E2E: Chinese BM25 in PostgreSQL
LOAD '/tmp/pgext/lib/pgtantivy.so';

CREATE TABLE IF NOT EXISTS docs(doc_id BIGINT PRIMARY KEY, title TEXT, body TEXT);
TRUNCATE docs;
INSERT INTO docs VALUES
 (1, '量子计算入门', '量子计算使用量子比特进行运算，速度远超经典计算机。量子比特可以同时处于多个状态，这称为叠加态。'),
 (2, '机器学习基础', '机器学习是人工智能的一个分支，通过数据训练模型。深度学习使用多层神经网络。'),
 (3, '数据库索引', 'PostgreSQL 支持多种索引类型：B-tree、GIN、GiST。全文检索使用 GIN 索引加速。'),
 (4, '量子通信', '量子密钥分发利用量子纠缠原理实现安全通信，任何窃听都会被察觉。');

-- build index from table (JSON aggregation)
SELECT pgtantivy_create('zh_docs',
  (SELECT json_agg(json_build_object('id', doc_id, 'text', body))::text FROM docs),
  'chinese');

-- 1) Chinese BM25 search: 量子比特
SELECT '== search 量子比特 ==' AS m;
SELECT * FROM pgtantivy_search('zh_docs', '量子比特', 5);
-- 2) multi-term AND: 量子 + 通信
SELECT '== search 量子 通信 ==' AS m;
SELECT * FROM pgtantivy_search('zh_docs', '量子 通信', 5);
-- 3) latin: postgresql GIN
SELECT '== search PostgreSQL 索引 ==' AS m;
SELECT * FROM pgtantivy_search('zh_docs', 'PostgreSQL 索引', 5);
-- 4) snippet highlight
SELECT '== snippet ==' AS m;
SELECT pgtantivy_snippet('zh_docs', '量子比特', body) FROM docs WHERE doc_id = 1;
-- 5) RAG join pattern
SELECT '== join ==' AS m;
SELECT d.doc_id, d.title, r.score, pgtantivy_snippet('zh_docs', '量子', d.body) AS snip
FROM pgtantivy_search('zh_docs', '量子', 5) r
JOIN docs d ON d.doc_id = r.id
ORDER BY r.score DESC;
