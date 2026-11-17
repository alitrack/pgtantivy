# pgtantivy

Chinese-first BM25 full-text search for PostgreSQL — [tantivy](https://github.com/quickwit-oss/tantivy) + [jieba-rs](https://github.com/messense/jieba-rs) via [pgrx](https://github.com/pgcentralfoundation/pgrx). MIT licensed, clean-room design (architecture notes from ParadeDB's BSD-3 pg_search, no code).

## Why

PostgreSQL native FTS (`to_tsvector('simple', ...)`) does not segment Chinese: a 60-char sentence becomes **one token**. On a 100k-row Chinese corpus, 50 real queries returned **0/50 hits** (bench in `tests/`). pgtantivy segments with jieba and ranks with BM25:

| metric | pgtantivy | PG FTS ('simple' + GIN) |
|:---|:---|:---|
| index build (100k rows) | **1.09 s** | 3.16 s |
| warm query latency | 3.1 ms | 0.5 ms* |
| Chinese recall@10 | **9/9 = 1.00** | **0/50 hits** |

\* PG is "fast" because whole-sentence tokens make every query a unique-key probe with zero usable results — a capability gap, not a performance race.

## API surface

```sql
-- build a persistent index from JSON rows (id + text), chinese/english tokenizers
SELECT pgtantivy_create('docs_idx',
  (SELECT json_agg(json_build_object('id', doc_id, 'text', body))::text
   FROM docs),
  'chinese');
-- → persisted to $PGDATA/pgtantivy/<name>/ (MmapDirectory, survives restarts)

SELECT pgtantivy_load('docs_idx');   -- reload after restart / new session
SELECT * FROM pgtantivy_list();      -- name, loaded, doc count
SELECT pgtantivy_version();

-- BM25 search (AND semantics across words), top-k
SELECT id, score FROM pgtantivy_search('docs_idx', '量子 计算', 10);

-- highlighted fragment with [HIGHLIGHT]...[/HIGHLIGHT] markers (Chinese-safe)
SELECT pgtantivy_snippet('docs_idx', '量子比特', body) FROM docs WHERE doc_id = 1;

-- incremental writes (upsert semantics: same id replaces)
SELECT pgtantivy_add('docs_idx',
  '[{"id": 90001, "text": "量子比特的相干时间是量子计算核心指标"}]');
SELECT pgtantivy_remove('docs_idx', ARRAY[90002]);

-- drift detection: the index is a snapshot. Every build/write records the
-- current WAL LSN as a staleness anchor; compare it against the live position.
SELECT pgtantivy_drift_check('docs_idx');  -- false right after a build/write
-- (returns true once the WAL has advanced past the anchor — DML on ANY table
--  moves it, so treat it as a staleness *signal*, not a correctness gate)
SELECT pgtantivy_touch('docs_idx');        -- push the anchor to now (fresh)
-- pgtantivy_add / pgtantivy_remove / trigger flush also refresh the anchor;
-- a stale index warns on pgtantivy_load but is still queryable.

-- snapshot bundle: portable, versioned, integrity-checked index transfer
-- (research #2026111601 "共享索引" approach 1). Export a named index to an
-- immutable bundle dir; verify it; import it on another node as a new index.
-- keep=N retains the N newest bundles (0/NULL = keep all). The first export
-- fences the index to writer_id; a later export under a different identity
-- is refused (dual-writer protection).
SELECT pgtantivy_export_index('docs_idx', '/data/bundles', 'writer-A', 5);
-- → /data/bundles/docs_idx-<opstamp>.bundle  (manifest + all segment files)
SELECT pgtantivy_bundle_info('/data/bundles/docs_idx-1.bundle');  -- manifest JSON
SELECT pgtantivy_import_index('/data/bundles/docs_idx-1.bundle', 'docs_replica');
-- import verifies every file's size + FNV checksum against the manifest;
-- a corrupted/tampered bundle is rejected, never silently loaded. opstamp
-- monotonically increases with each export (bumps on any write).

-- S3 / object-store transfer (research #2026111601 "共享索引" — cross-node).
-- The same bundle, published to S3: each bundle file is an object under
-- <prefix>/<name>-<opstamp>.bundle/ and the manifest.json is uploaded LAST
-- (the "published" marker — a partially-uploaded bundle is invisible to
-- consumers). Endpoint/credentials are per-call args (no PG env needed).
SELECT pgtantivy_export_to_s3(
  'docs_idx', 'http://s3.internal:9000', 'fti-bundles', 'us-east-1',
  'AK', 'SK', 'fti', 'writer-A', 5);   -- keep=5: retain 5 newest S3 versions
SELECT * FROM pgtantivy_list_s3(
  'http://s3.internal:9000', 'fti-bundles', 'us-east-1', 'AK', 'SK', 'fti', 'docs_idx');
-- → (opstamp, bundle, n_docs) rows, newest first
SELECT pgtantivy_import_from_s3(
  'http://s3.internal:9000', 'fti-bundles', 'us-east-1', 'AK', 'SK', 'fti',
  'docs_idx-1.bundle', 'docs_replica');   -- download + verify (FNV) + register
-- a tampered/corrupted S3 object fails the checksum gate on import.

-- trigger auto-sync: index tracks the table, one tantivy commit per txn
CREATE TRIGGER pgtantivy_sync
  AFTER INSERT OR UPDATE OR DELETE ON docs
  FOR EACH ROW EXECUTE PROCEDURE
  pgtantivy_sync_trigger('docs_idx', 'doc_id', 'body');
-- INSERT/UPDATE/DELETE visible after COMMIT; ROLLBACK discards cleanly;
-- a 1000-row INSERT makes ONE segment, not a thousand.

-- index directory (empty default = $PGDATA/pgtantivy)
SET pgtantivy.index_dir = '/data/fti';
```

## Design notes

- **Persistence**: MmapDirectory under `<index_dir>/<name>/`; language recorded in a `pgtantivy_meta.json` sidecar (not `meta.json` — that collides with tantivy's own segment metadata).
- **Incremental correctness**: the `id` field is `INDEXED | STORED | FAST` — `delete_term` needs the term dictionary; FAST-only ids make upsert/remove silently no-op with ghost duplicates.
- **Trigger batching**: row triggers buffer ops; a PRE_COMMIT xact callback flushes per-index (one tantivy commit per index per transaction). An ABORT callback drains the buffer — without it, rolled-back ops leak into the next transaction's flush.
- **Process-level registry**: loaded indexes live in a process-wide map; after a restart or in a new backend, `pgtantivy_load()` re-mmaps from disk. The disk is the source of truth.
- **Drift detection**: each build (`pgtantivy_create` / `CREATE INDEX` / `REINDEX`) and each incremental write (`pgtantivy_add` / `remove` / trigger flush) records the current WAL insertion LSN — read from the live postgres runtime via the exported `GetInsertRecPtr` symbol — plus a timestamp + doc count in the `pgtantivy_meta.json` sidecar. `pgtantivy_drift_check(name)` compares that anchor to the live WAL position: `true` means "the WAL moved since this index was last built/written". Honest caveat: the LSN is global to the cluster, so DML on *unrelated* tables also advances it — `drifted = true` is a staleness *signal*, not "this index's table changed". `pgtantivy_touch(name)` manually pushes the anchor forward (after an out-of-band sync); a stale index warns on `pgtantivy_load` but remains queryable.
- **Snapshot bundles**: `pgtantivy_export_index` copies the index dir into `<dest>/<name>-<opstamp>.bundle/` with a `manifest.json` (format tag, opstamp, writer_id, doc count, build-time LSN anchor, and a per-file size + FNV-1a 64-bit checksum for every segment file). `pgtantivy_import_index` re-verifies all checksums before extracting, so a corrupted or tampered bundle is rejected — never silently loaded. The opstamp is read from / bumped in the index sidecar, so it rises monotonically with each export and is the versioning key for consumers. **Dual-writer fence**: the first export records its `writer_id` in the index sidecar; a later export under a *different* identity is refused (checked before the opstamp bump, so a rejected export consumes no version) — a misconfigured second writer cannot silently corrupt the shared index. **Retention/GC**: `keep` retains the N newest bundles for the name and deletes older ones (0/NULL = retain all), preventing unbounded bundle accumulation. The `writer_id` marker documents the single-writer contract of the source index. Verified end-to-end on a real PG17 cluster (`tests/snapshot_e2e.py`): export → info → import → fresh-backend search (100/100 hits on a 200-doc corpus) → opstamp bump after a write → dual-writer refusal → keep=2 GC retains the two newest → corrupt-a-byte import rejection.
- **S3 transfer** (`src/s3.rs`): the same bundle, published to object storage. Each bundle file becomes one object under `<prefix>/<name>-<opstamp>.bundle/` and `manifest.json` is uploaded **last** — the published marker, so a half-uploaded bundle is never visible to readers (manifest-first would expose incomplete sets). `pgtantivy_export_to_s3` builds the bundle locally (reusing the fence + integrity logic of `snapshot.rs`), uploads it, then applies an S3-side retention policy (keep N newest). `pgtantivy_import_from_s3` downloads to a temp dir and reuses `snapshot::extract_bundle`, so the **identical** FNV integrity gate + doc-count check apply — a corrupted or tampered object is rejected, never silently loaded. `pgtantivy_list_s3` lists a name's published versions (opstamp, bundle, n_docs) newest-first. **`pgtantivy_load_s3`** opens the bundle **directly over S3** via a range-read tantivy `Directory` (approach 2) — no whole-bundle download, no local index files. Production hardening shipped: it reads the bundle `manifest.json` **once** to seed a per-file length cache (so `get_file_handle`/`exists` need no per-file `head` round-trip on the cold path) and an optional `warm_terms` arg pre-fetches the hot splits (term dict + postings + `.store`) for named terms during load, moving the cold-read RTTs off the first real query, and the whole-file split cache is bounded by an LRU byte budget (`PGTANTIVY_S3_CACHE_BYTES`, default 128 MiB, `0` = unbounded) so a large bundle can't OOM a backend. S3 auth is per-call (endpoint/region/keys as args) — no PG environment setup, works against any S3-compatible store (verified on MinIO). Implementation notes: async `object_store` is bridged into the synchronous PG backend via a process-wide single-thread `tokio::runtime` `block_on`; and `object_store` normalises a multi-segment `Path` prefix into a trailing-slash "directory", which would NOT match `<name>-<op>.bundle` — so list/GC list the top prefix and filter client-side by the exact `<prefix>/<name>-` string. Verified end-to-end against a real MinIO bucket (`tests/s3_e2e.py`): export → list → import → fresh-backend cross-node search (100/100 on a 200-doc corpus) → independent `minio-py` object count + manifest presence → S3-side keep=1 GC → corrupt-an-object import rejection. **`pgtantivy_reload_s3(name, to_bundle)`** re-points an S3-loaded replica at another published bundle version *in place* (same registry name): `to_bundle = NULL` upgrades to the newest published version, a concrete bundle name (`'docs_idx-3.bundle'`) rolls back to that version; it reuses the S3 origin stored when the index was loaded, so the caller does not re-supply credentials. The replica keeps its name, so `pgtantivy_search('<name>', ...)` follows the version — no re-`load_s3` needed. **`pgtantivy_s3_status(name)`** reports replica lag health read-only (also reusing the stored S3 origin): `UP-TO-DATE` (serving the newest published opstamp), `LAGGING (serving op X; source is op Y)` (a newer version exists — run `reload_s3(name, NULL)` to catch up), or `SOURCE-UNKNOWN` (no published versions found for the source name). Verified end-to-end (`tests/s3_reload_e2e.py`): load v1 → `status` reports LAGGING → `reload_s3(NULL)` → `status` reports UP-TO-DATE → rollback → `status` reports LAGGING again (the state transitions track the served opstamp). Verified end-to-end (`tests/s3_reload_e2e.py`): publish v1 → load v1 → add 100 docs → publish v2 → `list_s3` shows both (newest first) → `reload_s3(NULL)` auto-upgrades in place (new docs now searchable) → `reload_s3(NULL)` again is a no-op → `reload_s3(<v1>)` explicitly rolls back (new docs gone).
- **Ranking**: conjunction (AND) BM25. Verified 1.00 recall@10 on unique-term ground truth; common-word TF ranking behaves per spec (see `tests/bench_recall.py` for the ground-truth methodology).
- **Multi-replica coordination** (`tools/s3_coordinator.py`, control plane — the plugin is UNMODIFIED): for a fleet of replica nodes, "follow newest" (`reload_s3(name, NULL)`) cannot be pinned or rolled back; a bad source publish is adopted by every node on its next tick. The coordinator adds a single atomic target object — `<prefix>/control/<name>.target.json` (`{"op", "bundle", "set_at"}`) — and four subcommands: `get-target` / `set-target` / `canary` / `release` / `status`. `release --op N` canaries EVERY node in an isolated session first (loads `N`'s bundle under a private `canary_<name>_<op>` registry entry + probe search — it never touches the app's registry entry), and publishes the target only if all canaries pass, so a failed release leaves the target unchanged (no partial fleet state). Following the target = one-line app-loop change: `reload_s3(name, <target bundle>)` instead of `reload_s3(name, NULL)`, which buys **pinning** (a newly published bad version is not adopted until the target moves), **coordinated rollback** (one control write reverts the whole fleet), and **canary gating** (a down or divergent node blocks the release). Honest boundary: the replica registry is per-backend, so the coordinator cannot force already-running application backends — it publishes the target and gates releases; each backend converges at its own loop tick, and a node that restarts re-registers from the target. Verified end-to-end on TWO real replica PG clusters + a real writer + MinIO (`tests/s3_coord_e2e.py`, persistent psycopg2 connections modelling long-lived app backends): split fleet (b v2 / c v1) → canary-gated release APPLIED → fleet converged → pinning (target followers stay v2 while NULL mode auto-adopts v3) → BLOCKED release (node c down → target unchanged) → restart + release APPLIED → fleet converges → coordinated rollback to v2 (one control write, fleet-wide revert).

## Build & install

```bash
cargo build --release --features pg17 --no-default-features
# then place the .so + SQL where your PG can see them, or:
cargo pgrx install --pg17   # if pgrx-managed PG
```

Requires: PostgreSQL 17, pgrx 0.13.1, tantivy 0.24, jieba-rs 0.7.

## Status

**Drift detection** (snapshot staleness, research #2026111601 "漂移检测" line) — the index is a snapshot: after a build or incremental write, heap DML is not re-indexed (documented MVP contract), so staleness is *made visible* instead of silently wrong.

```sql
SELECT pgtantivy_drift_check('docs_idx');  -- f right after build; t once WAL advances
SELECT pgtantivy_touch('docs_idx');        -- push the anchor forward (fresh now)
-- add/remove/trigger flush refresh the anchor; a stale index warns on load.
```

Every build/`REINDEX`/incremental write records the current WAL insertion LSN (read via postgres' exported `GetInsertRecPtr` symbol) + timestamp + doc count in the sidecar; `pgtantivy_drift_check` compares it to the live WAL position. Caveat: the LSN is global to the cluster, so it's a staleness *signal*, not "this table changed". Verified end-to-end on a real PG17 cluster (`tests/drift_e2e.sql`): build→f, WAL advance→t, touch→f, add→f, search still returns correct rows.

**Snapshot bundles** (portable, versioned, integrity-checked index transfer, research #2026111601 "共享索引" approach 1):

```sql
SELECT pgtantivy_export_index('docs_idx', '/data/bundles', 'writer-A');
SELECT pgtantivy_bundle_info('/data/bundles/docs_idx-1.bundle');
SELECT pgtantivy_import_index('/data/bundles/docs_idx-1.bundle', 'docs_replica');
```

Export writes `<dest>/<name>-<opstamp>.bundle/` (manifest + every segment file, each with a size + FNV-1a checksum). Import re-verifies all checksums before extracting — a corrupted/tampered bundle is rejected, never silently loaded. The opstamp is the version key (monotonic, bumps on any write); `writer_id` records the single-writer source (and fences the index against a second writer); `keep` retains the N newest bundles. The extracted replica keeps its build-time drift anchor, so `pgtantivy_drift_check('docs_replica')` still signals staleness. Verified end-to-end (`tests/snapshot_e2e.py`): export → info → import → fresh-backend search 100/100 → opstamp bump after a write → dual-writer refusal → keep GC → corrupt-a-byte rejection.

**S3 / object-store transfer** (cross-node shared index, research #2026111601 "共享索引" approach 1 — the transfer leg):

```sql
SELECT pgtantivy_export_to_s3('docs_idx', 'http://s3:9000', 'fti-bundles',
                              'us-east-1', 'AK', 'SK', 'fti', 'writer-A', 5);
SELECT * FROM pgtantivy_list_s3('http://s3:9000', 'fti-bundles',
                                'us-east-1', 'AK', 'SK', 'fti', 'docs_idx');
SELECT pgtantivy_import_from_s3('http://s3:9000', 'fti-bundles', 'us-east-1',
                                'AK', 'SK', 'fti', 'docs_idx-1.bundle', 'docs_replica');
-- approach 2: open the bundle DIRECTLY over S3 (range-read, no download)
-- warm_terms (optional) pre-fetches hot splits at load time
SELECT pgtantivy_load_s3('http://s3:9000', 'fti-bundles', 'us-east-1',
                         'AK', 'SK', 'fti', 'docs_idx-1.bundle', 'docs_replica_s3dir',
                         'the,highest-frequency,terms');
SELECT * FROM pgtantivy_search('docs_replica_s3dir', 'your query', 20);

-- follow the source index's version in place (same replica name):
--   NULL = upgrade to the NEWEST published version; a bundle name = rollback.
-- reuses the S3 origin stored at load — no credentials re-supplied.
SELECT pgtantivy_reload_s3('docs_replica_s3dir', NULL);              -- -> newest
SELECT pgtantivy_reload_s3('docs_replica_s3dir', 'docs_idx-3.bundle'); -- -> pin v3
-- replica lag health: is it serving the newest published version of its source?
-- returns UP-TO-DATE / LAGGING (serving op X; source is op Y) / SOURCE-UNKNOWN
SELECT pgtantivy_s3_status('docs_replica_s3dir');
SELECT * FROM pgtantivy_search('docs_replica_s3dir', 'your query', 20);
```

The same bundle published to S3 (each file an object under `<prefix>/<name>-<opstamp>.bundle/`, manifest uploaded last as the published marker). Import reuses the identical FNV integrity gate, so a corrupted/tampered S3 object is rejected. S3-side `keep` retains the N newest published versions. Auth is per-call (works against any S3-compatible store — verified on a real MinIO bucket, `tests/s3_e2e.py`): export → list → import → fresh-backend cross-node search 100/100 → independent minio-py object/manifest check → S3 keep=1 GC → corrupt-an-object rejection.

**Shared index: approach 1 vs approach 2.** This repo ships **both**. **Approach 1** (`pgtantivy_import_from_s3`) is the whole-bundle-download path: it materializes the bundle to a temp dir, re-runs the FNV integrity gate, and `mmap`s it locally — production-ready (single-writer fence, monotonic opstamp versioning, FNV integrity, local + S3 retention). **Approach 2** (`pgtantivy_load_s3`) is the range-read optimization: it opens the S3 bundle **directly** through a custom tantivy `Directory` (`S3Directory` in `src/s3.rs`) so tantivy `range`-GETs only what each query touches, caching whole files per-process — **no whole-bundle download, no local index files**. A feasibility + quantification spike (`alitrack/pgtantivy-s3dir-spike`) measured, on a 231 KB / 10k-doc bundle, a single term query: approach 1 pulls **100%** of the bundle every read; approach 2 **COLD** first read pulls **23.2%** (24 range-GETs) and **WARM** (split cache primed) pulls **~1%** with **7.6× lower latency**. The one real risk is **cold-read RTT** (the COLD N range-GETs are N round-trips — invisible on a local MinIO, amplifying on remote S3); `pgtantivy_load_s3` mitigates this with a manifest-seeded length cache (no per-file `head` on the cold path) and an optional `warm_terms` pre-fetch (hot splits pulled at load, not on the first query). Remaining production hardening: **none** — the whole-file split cache is now bounded by an LRU byte budget (`PGTANTIVY_S3_CACHE_BYTES`, default 128 MiB, `0` = unbounded); a tiny budget degrades warm reads to re-fetching (slower, never wrong, never OOM). Pick approach 1 for small bundles / low read rate / same rack; use approach 2 when bundles are big, reads are frequent, or nodes cross availability zones. `load_s3` is verified end-to-end against a real MinIO bucket (`tests/s3dir_e2e.py`, 7 checks): export → `load_s3` range-read → **no local directory is created** (proof it is not a download) → cross-backend search 100/100 → `warm_terms` pre-fetch reports warmed + still 100 → **1KB LRU budget forces full eviction yet range-read stays 100/100** (memory ceiling holds).

**v0.4.0** — hybrid search, server-side:

```sql
-- BM25 (tantivy) + pgvector ANN, fused with Reciprocal Rank Fusion
SELECT id, rrf_score, fts_rank, vec_rank
FROM pgtantivy_hybrid('docs_fts', '量子比特', 'docs_vec', 'embedding', 'id',
                      '[0.95, 0.05, 0.0]', 'cosine', 50, 10, 60);
```

One MIT extension + pgvector = Elasticsearch-style hybrid search without leaving PostgreSQL. Vector side is delegated to pgvector (no second index AM, no extra WAL surface); RRF recipe validated at recall 1.000 / MRR 1.000 on a 90k-doc benchmark.

**v0.3.0** — the extension now drives a real PostgreSQL index access method:

- `CREATE INDEX ... USING pgtantivy (body) WITH (text_column, id_column, language)` builds a tantivy sidecar in one statement
- `WHERE body @@@ '查询'` plans as **Bitmap Index Scan** over the sidecar (BM25 matching, no heap recheck)
- Manual + automatic segment merging (`pgtantivy.merge_threshold`, default 8, 0=off)
- Trigger-based live sync, incremental writes, persistence, snippets, GUCs

v0.2.0 — persistence, incremental writes, trigger sync, snippets, GUCs.
v0.1.0 — initial prototype.

E2E suites in `tests/`: `persist_e2e.sql`, `incremental_e2e.sql`, `trigger_sync_e2e.sql`, `drift_e2e.sql`, `snapshot_e2e.py` (8 checks: export/info/import/fresh-backend search/opstamp bump/dual-writer fence/GC/checksum rejection), `s3_e2e.py` (7 checks against a real MinIO: export/list/import/fresh-backend cross-node search/independent object+manifest verify/S3 keep=1 GC/object-tamper rejection), `bench_*.py` (100k-row corpus, recall harness).
