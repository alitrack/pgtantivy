// pgtantivy — Chinese-first BM25 full-text search for PostgreSQL via tantivy.
// Seam B prototype: tantivy BM25 + jieba tokenization in a pgrx extension.
// MIT. No ParadeDB code — clean-room, architecture-inspired only.
use pgrx::callbacks::{register_xact_callback, PgXactCallbackEvent};
use pgrx::pgbox::WhoAllocated;
use pgrx::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use std::ffi::{CStr, CString};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{
    IndexRecordOption, Schema, TextFieldIndexing, TextOptions, Value, FAST, INDEXED, STORED,
};
use tantivy::snippet::SnippetGenerator;
use tantivy::tokenizer::{
    LowerCaser, SimpleTokenizer, TextAnalyzer, Token, TokenStream, Tokenizer,
};
use tantivy::{doc, Index, IndexReader, ReloadPolicy};

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use unicode_normalization::UnicodeNormalization;
pg_module_magic!();

pub mod am;
pub mod hybrid;
pub mod s3;
pub mod snapshot;

// GUC pgtantivy.index_dir: where persistent indexes live. Empty (the
// default) resolves to $PGDATA/pgtantivy.
static INDEX_DIR_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

// GUC pgtantivy.merge_threshold: auto-compact after an incremental write
// leaves this many searchable segments (0 = never; manual-only via
// pgtantivy_merge). Default 8.
static MERGE_THRESHOLD_GUC: GucSetting<i32> = GucSetting::<i32>::new(8);

// pgrx 0.20's #[pg_guard] emits its own #[unsafe(no_mangle)] (and
// #[allow(non_snake_case)]) for _PG_init, so the explicit attribute is gone.
#[pg_guard]
pub unsafe extern "C-unwind" fn _PG_init() {
    am::init_reloptions();
    GucRegistry::define_string_guc(
        c"pgtantivy.index_dir",
        c"Base directory for persistent pgtantivy indexes",
        c"Where pgtantivy stores persistent indexes. Empty = $PGDATA/pgtantivy.",
        &INDEX_DIR_GUC,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pgtantivy.merge_threshold",
        c"Auto-merge segment threshold for incremental writes",
        c"After an incremental write, if the index has this many searchable segments or more it is auto-compacted. 0 disables (manual pgtantivy_merge only).",
        &MERGE_THRESHOLD_GUC,
        0,
        1024,
        GucContext::Userset,
        GucFlags::default(),
    );
}

/// Auto-compact helper: called after incremental writes (add / trigger
/// flush). If the GUC threshold is >=1 and the index has at least that
/// many searchable segments, run the same full-merge loop as
/// pgtantivy_merge (best effort — errors are warnings, never hard fail
/// the DML that triggered them).
fn maybe_auto_merge(idx: &PgIndex, name: &str) {
    let threshold = MERGE_THRESHOLD_GUC.get();
    if threshold < 1 {
        return;
    }
    let segs = match idx.index.searchable_segment_metas() {
        Ok(m) => m.len(),
        Err(_) => return,
    };
    if (segs as i32) < threshold {
        return;
    }
    info!("pgtantivy: auto-merge '{name}' ({segs} segments >= threshold {threshold})");
    if let Err(e) = merge_all(idx) {
        warning!("pgtantivy auto-merge '{name}' failed: {e}");
    }
}

/// Full merge loop shared by pgtantivy_merge and auto-merge.
fn merge_all(idx: &PgIndex) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = idx
        .index
        .writer::<tantivy::TantivyDocument>(64 * 1024 * 1024)?;
    loop {
        let metas = idx.index.searchable_segment_metas()?;
        if metas.len() <= 1 {
            break;
        }
        let ids: Vec<_> = metas.iter().map(|m| m.id()).collect();
        writer
            .merge(&ids)
            .wait()
            .map_err(|e| format!("merge failed: {e}"))?;
        writer.commit()?;
    }
    drop(writer);
    idx.reader.reload()?;
    Ok(())
}

// ─── jieba tokenizer adapter (tantivy Tokenizer) ─────────────────────────────

#[derive(Clone, Default)]
struct JiebaTokenizer;

impl Tokenizer for JiebaTokenizer {
    type TokenStream<'a> = JiebaTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let jieba = jieba_shared();
        // jieba-rs Token.start/end are CHAR offsets; tantivy expects BYTE
        // offsets (it slices the raw &str with them). Precompute the
        // char-index -> byte-offset table once per text and translate.
        let byte_of_char: Vec<usize> = std::iter::once(0)
            .chain(text.char_indices().skip(1).map(|(b, _)| b))
            .collect();
        let total_chars = byte_of_char.len();
        let clamp_byte = |c: usize| -> usize {
            if c < total_chars {
                byte_of_char[c]
            } else {
                text.len()
            }
        };
        let mut tokens: Vec<Token> = jieba
            .tokenize(text, jieba_rs::TokenizeMode::Search, true)
            .into_iter()
            .map(|t| Token {
                offset_from: clamp_byte(t.start),
                offset_to: clamp_byte(t.end),
                position: usize::MAX,
                text: t.word.to_string(),
                position_length: 1,
            })
            .collect();
        for (i, t) in tokens.iter_mut().enumerate() {
            t.position = i;
        }
        JiebaTokenStream { tokens, idx: 0 }
    }
}

struct JiebaTokenStream {
    tokens: Vec<Token>,
    idx: usize,
}

impl TokenStream for JiebaTokenStream {
    fn advance(&mut self) -> bool {
        if self.idx < self.tokens.len() {
            self.idx += 1;
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        &self.tokens[self.idx.saturating_sub(1)]
    }

    fn token_mut(&mut self) -> &mut Token {
        let i = self.idx.saturating_sub(1);
        &mut self.tokens[i]
    }
}

fn jieba_shared() -> &'static jieba_rs::Jieba {
    static JIEBA: OnceLock<jieba_rs::Jieba> = OnceLock::new();
    JIEBA.get_or_init(jieba_rs::Jieba::new)
}

const JIEBA_TOKENIZER: &str = "pgt_jieba";

fn analyzer(lang: &str) -> TextAnalyzer {
    if lang == "chinese" || lang == "zh" {
        TextAnalyzer::builder(JiebaTokenizer)
            .filter(LowerCaser)
            .build()
    } else {
        TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(LowerCaser {})
            .build()
    }
}

// ─── index registry: named in-memory tantivy indexes ─────────────────────────

/// Connection info for an index loaded from S3 (approach 2) — kept alongside
/// the live handle so `pgtantivy_reload_s3` can list versions / re-open the
/// bundle without the caller re-supplying credentials. `None` = local index.
#[derive(Clone, Debug)]
struct S3Cred {
    endpoint: String,
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
    prefix: String,
    /// The base name (without the `-<opstamp>.bundle` suffix) — this is what
    /// `list_versions` lists against to find published versions.
    name: String,
}

struct PgIndex {
    index: Index,
    reader: IndexReader,
    body: tantivy::schema::Field,
    id: tantivy::schema::Field,
    /// On-disk directory when persistent (None for legacy in-memory only).
    path: Option<String>,
    /// S3 origin when the index was loaded via `pgtantivy_load_s3` (approach 2);
    /// lets `pgtantivy_reload_s3` upgrade/rollback without re-supplying creds.
    s3: Option<S3Cred>,
}

fn index_root() -> String {
    let cur = INDEX_DIR_GUC
        .get()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    if cur.trim().is_empty() {
        format!("{}/pgtantivy", unsafe {
            CStr::from_ptr(pgrx::pg_sys::DataDir).to_string_lossy()
        })
    } else {
        cur
    }
}

fn registry() -> &'static Mutex<HashMap<String, Arc<PgIndex>>> {
    static R: OnceLock<Mutex<HashMap<String, Arc<PgIndex>>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(HashMap::new()))
}

// ── drift detection: build-time anchor in pgtantivy_meta.json ─────────────
// The AM index is a snapshot: after CREATE/REINDEX, DML on the heap is
// never indexed (aminsert is a no-op — documented MVP contract). To make
// staleness *visible* instead of silently wrong, every build records the
// current WAL insertion LSN + timestamp in the sidecar's
// pgtantivy_meta.json, and incremental writers (add/remove/trigger flush)
// refresh it. `pgtantivy_drift_check` compares the anchor against the
// live WAL position.
//
// Honest caveat: the insertion LSN is global to the whole cluster — DML on
// UNRELATED tables also advances it, so `drifted=true` means "WAL moved
// since the index was built", not "this index's table changed". Treat it as
// a staleness signal; `pgtantivy_reindex` refreshes when in doubt.
extern "C" {
    /// Postgres' canonical "current WAL insertion position" accessor. It returns
    /// `XLogRecPtr` (uint64) and IS in postgres' dynamic symbol table, so the
    /// cdylib's LOAD-time dynamic linker can resolve it. (The earlier draft used
    /// `GetCurrentInsertionLSN`, which postgres does NOT export — the cdylib
    /// compiled but failed at `LOAD` with `undefined symbol`.)
    fn GetInsertRecPtr() -> u64;
}

/// Current WAL insertion LSN, read directly from the postgres runtime via the
/// exported `GetInsertRecPtr` symbol (the cdylib is loaded into postgres, so the
/// dynamic linker resolves it at LOAD time — the standard extension pattern).
pub fn current_insertion_lsn() -> u64 {
    unsafe { GetInsertRecPtr() }
}

/// Build-time anchor for drift detection.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct BuildAnchor {
    /// Global WAL insertion LSN at build/last-refresh time.
    pub built_lsn: u64,
    /// Unix millis at build/last-refresh time.
    pub built_at: i64,
    /// Docs indexed at build/last-refresh time.
    pub n_docs: u64,
}

pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Doc count of a freshly-committed index (reader opened, then dropped).
fn reader_docs(index: &tantivy::Index) -> u64 {
    index.reader().map(|r| r.searcher().num_docs()).unwrap_or(0)
}

/// Read + merge the sidecar meta (keeps `lang` and any other keys, adds the
/// anchor). Atomic write via temp-file + rename so a crash never leaves a
/// half-written meta.
pub fn write_build_meta(dir: &std::path::Path, anchor: &BuildAnchor) -> std::io::Result<()> {
    let meta_path = dir.join("pgtantivy_meta.json");
    let mut v = std::fs::read_to_string(&meta_path)
        .ok()
        .and_then(|m| serde_json::from_str::<serde_json::Value>(&m).ok())
        .unwrap_or(serde_json::json!({}));
    let o = v.as_object_mut().ok_or(std::io::Error::new(
        std::io::ErrorKind::Other,
        "meta is not a JSON object",
    ))?;
    o.insert("built_lsn".into(), serde_json::json!(anchor.built_lsn));
    o.insert("built_at".into(), serde_json::json!(anchor.built_at));
    o.insert("n_docs".into(), serde_json::json!(anchor.n_docs));
    let tmp = dir.join(format!(".pgtantivy_meta.{}.tmp", std::process::id()));
    std::fs::write(&tmp, v.to_string())?;
    std::fs::rename(&tmp, &meta_path)?;
    Ok(())
}

/// Parse the build anchor from a sidecar's meta (absent keys → None).
pub fn read_build_meta(dir: &std::path::Path) -> Option<BuildAnchor> {
    let m = std::fs::read_to_string(dir.join("pgtantivy_meta.json")).ok()?;
    let v: serde_json::Value = serde_json::from_str(&m).ok()?;
    let lsn = v["built_lsn"].as_u64()?;
    let at = v["built_at"].as_i64()?;
    let nd = v["n_docs"].as_u64().unwrap_or(0);
    Some(BuildAnchor {
        built_lsn: lsn,
        built_at: at,
        n_docs: nd,
    })
}

/// Compare the anchor against the live WAL position.
#[derive(Clone, Debug)]
pub struct DriftReport {
    pub drifted: bool,
    pub anchor: Option<BuildAnchor>,
    pub current_lsn: u64,
    pub current_at: i64,
}

pub fn check_drift(dir: &std::path::Path) -> DriftReport {
    let cur_lsn = crate::current_insertion_lsn();
    let anchor = read_build_meta(dir);
    let drifted = anchor
        .as_ref()
        .map(|a| cur_lsn > a.built_lsn)
        .unwrap_or(false);
    DriftReport {
        drifted,
        anchor,
        current_lsn: cur_lsn,
        current_at: now_millis(),
    }
}

fn fmt_lsn(lsn: u64) -> String {
    let (hi, lo) = ((lsn >> 32) as u32, (lsn & 0xFFFF_FFFF) as u32);
    format!("{hi:X}/{lo:08X}")
}

/// After an incremental write (add/remove/trigger flush) the index is fresh
/// again — push the anchor forward so a later load doesn't report the index
/// it just wrote as stale. Best-effort: a meta write failure is not an error
/// (the index is still correct, only the staleness signal is stale).
fn refresh_anchor(idx: &PgIndex, name: &str) {
    let Some(path) = idx.path.as_deref() else {
        return;
    };
    let dir = std::path::Path::new(path);
    let anchor = BuildAnchor {
        built_lsn: crate::current_insertion_lsn(),
        built_at: now_millis(),
        n_docs: idx.reader.searcher().num_docs(),
    };
    if let Err(e) = write_build_meta(dir, &anchor) {
        warning!("pgtantivy: refresh anchor for '{name}' failed: {e}");
    }
}

fn safe_name(name: &str) -> bool {
    let b = name.as_bytes();
    !b.is_empty()
        && b.len() <= 128
        && (b[0].is_ascii_alphabetic() || b[0] == b'_')
        && b.iter()
            .all(|c| c.is_ascii_alphanumeric() || *c == b'_' || *c == b'-')
}

#[pg_extern]
fn pgtantivy_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Build a named in-memory BM25 index from (id, text) rows passed as JSON.
#[pg_extern]
fn pgtantivy_create(
    name: String,
    rows_json: String,
    lang: String,
) -> Result<String, Box<dyn std::error::Error>> {
    if !safe_name(&name) {
        return Err("pgtantivy: name must be [A-Za-z_][A-Za-z0-9_-]*, <=128 chars".into());
    }
    let mut sb = Schema::builder();
    // id must be INDEXED so delete_term (upsert/remove) can find prior docs;
    // FAST keeps the column store for fast id decoding in search results.
    let id_field = sb.add_i64_field("id", INDEXED | STORED | FAST);
    let indexing = TextFieldIndexing::default()
        .set_tokenizer(JIEBA_TOKENIZER)
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let body_field = sb.add_text_field(
        "body",
        TextOptions::default().set_indexing_options(indexing),
    );
    let schema = sb.build();
    // Persistent layout: <index_dir>/<name>/ (MmapDirectory, survives
    // restarts). Wiped on rebuild, same contract as create.
    let root = index_root();
    let dir_path = std::path::Path::new(&root).join(&name);
    if dir_path.exists() {
        std::fs::remove_dir_all(&dir_path)?;
    }
    std::fs::create_dir_all(&dir_path)?;
    let dir = tantivy::directory::MmapDirectory::open(&dir_path)?;
    let index = Index::open_or_create(dir, schema)?;
    let a = analyzer(&lang);
    index.tokenizers().register(JIEBA_TOKENIZER, a);

    let mut writer = index.writer(32 * 1024 * 1024)?;
    for row in serde_json::from_str::<Vec<serde_json::Value>>(&rows_json)? {
        let id = row["id"]
            .as_i64()
            .ok_or_else(|| format!("row missing id: {}", row))?;
        let text = row["text"]
            .as_str()
            .ok_or_else(|| format!("row missing text: {}", row))?
            .to_string();
        let text_nfc = nfc(&text);
        writer.add_document(doc!(id_field => id, body_field => text_nfc.as_str()))?;
    }
    writer.commit()?;
    let n_docs = reader_docs(&index);
    // Sidecar: language for load-time tokenizer registration, plus the build
    // anchor for drift detection (write_build_meta merges onto the base).
    std::fs::write(
        dir_path.join("pgtantivy_meta.json"),
        serde_json::json!({"lang": lang}).to_string(),
    )?;
    let anchor = BuildAnchor {
        built_lsn: crate::current_insertion_lsn(),
        built_at: now_millis(),
        n_docs,
    };
    let _ = write_build_meta(&dir_path, &anchor);
    let reader = index.reader()?;
    registry().lock()?.insert(
        name.clone(),
        Arc::new(PgIndex {
            index,
            reader,
            body: body_field,
            id: id_field,
            path: Some(dir_path.to_string_lossy().into_owned()),
            s3: None,
        }),
    );
    Ok(format!(
        "pgtantivy: index '{}' created at {}",
        name,
        dir_path.display()
    ))
}

/// Open a persistent index from disk into the registry (after a restart).
#[pg_extern]
fn pgtantivy_load(name: String) -> Result<String, Box<dyn std::error::Error>> {
    if !safe_name(&name) {
        return Err("pgtantivy: name must be [A-Za-z_][A-Za-z0-9_-]*, <=128 chars".into());
    }
    if registry().lock()?.contains_key(&name) {
        return Ok(format!("pgtantivy: index '{}' already loaded", name));
    }
    let dir_path = std::path::Path::new(&index_root()).join(&name);
    if !dir_path.exists() {
        return Err(format!("pgtantivy: no index '{name}' on disk").into());
    }
    let num_docs = register_index_at(&dir_path, &name)?;
    let lang = std::fs::read_to_string(dir_path.join("pgtantivy_meta.json"))
        .ok()
        .and_then(|m| serde_json::from_str::<serde_json::Value>(&m).ok())
        .and_then(|v| v["lang"].as_str().map(std::string::ToString::to_string))
        .unwrap_or_else(|| "chinese".to_string());
    Ok(format!(
        "pgtantivy: index '{}' loaded ({} docs, lang={})",
        name, num_docs, lang
    ))
}

/// Open an on-disk index directory and register it in this process's registry
/// (tokenizer by sidecar lang, body/id field lookup, reader, drift warning).
/// Shared by `pgtantivy_load` and `pgtantivy_import_index`. Returns doc count.
fn register_index_at(dir_path: &std::path::Path, name: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let dir = tantivy::directory::MmapDirectory::open(dir_path)
        .map_err(|e| format!("pgtantivy: no index '{name}' on disk ({e})"))?;
    let index = Index::open(dir)?;
    // Tokenizer: jieba for any index we created (language recorded in meta.json).
    let lang = std::fs::read_to_string(dir_path.join("pgtantivy_meta.json"))
        .ok()
        .and_then(|m| serde_json::from_str::<serde_json::Value>(&m).ok())
        .and_then(|v| v["lang"].as_str().map(std::string::ToString::to_string))
        .unwrap_or_else(|| "chinese".to_string());
    index
        .tokenizers()
        .register(JIEBA_TOKENIZER, analyzer(&lang));
    let schema = index.schema();
    let body = schema
        .get_field("body")
        .map_err(|_| "pgtantivy: index lacks body field")?;
    let id = schema
        .get_field("id")
        .map_err(|_| "pgtantivy: index lacks id field")?;
    let reader = index.reader()?;
    let num_docs = reader.searcher().num_docs();
    registry().lock()?.insert(
        name.to_string(),
        Arc::new(PgIndex {
            index,
            reader,
            body,
            id,
            path: Some(dir_path.to_string_lossy().into_owned()),
            s3: None,
        }),
    );
    // Drift check: if the WAL has advanced since the index was last built,
    // the snapshot may be stale — warn (not error) so callers can decide.
    let drift = check_drift(dir_path);
    if drift.drifted {
        if let Some(a) = &drift.anchor {
            warning!(
                "pgtantivy: index '{}' may be STALE — built at LSN {}, now at {}; \
                 run pgtantivy_reindex('{name}') to refresh",
                name,
                fmt_lsn(a.built_lsn),
                fmt_lsn(drift.current_lsn),
            );
        }
    }
    Ok(num_docs)
}

/// Drift check for a named sidecar: true if the WAL has advanced since the
/// index was last built/refreshed (its snapshot may be stale).
///
/// Caveat (see the anchor docs at the top of this file): the anchor LSN is
/// global to the cluster — DML on unrelated tables also advances it. Use
/// this as a staleness *signal* in `WHERE`/monitoring; refresh with
/// `REINDEX INDEX` (AM) or `pgtantivy_create` again (function API) when in
/// doubt, then the anchor moves forward on the next build/write.
#[pg_extern]
fn pgtantivy_drift_check(name: String) -> Result<bool, Box<dyn std::error::Error>> {
    if !safe_name(&name) {
        return Err("pgtantivy: name must be [A-Za-z_][A-Za-z0-9_-]*, <=128 chars".into());
    }
    let dir_path = std::path::Path::new(&index_root()).join(&name);
    if !dir_path.exists() {
        return Err(format!("pgtantivy: no index '{name}' on disk").into());
    }
    Ok(check_drift(&dir_path).drifted)
}

/// Manually push the build anchor forward (the index is considered fresh at
/// this moment). Use after an out-of-band sync — e.g. an ETL that refreshed
/// the heap + sidecar outside this extension's write paths — so subsequent
/// loads/drift checks start from now.
#[pg_extern]
fn pgtantivy_touch(name: String) -> Result<String, Box<dyn std::error::Error>> {
    if !safe_name(&name) {
        return Err("pgtantivy: name must be [A-Za-z_][A-Za-z0-9_-]*, <=128 chars".into());
    }
    let dir_path = std::path::Path::new(&index_root()).join(&name);
    if !dir_path.exists() {
        return Err(format!("pgtantivy: no index '{name}' on disk").into());
    }
    let n_docs = registry()
        .lock()
        .ok()
        .and_then(|r| r.get(&name).cloned())
        .map(|i| i.reader.searcher().num_docs())
        .unwrap_or(0);
    let anchor = BuildAnchor {
        built_lsn: crate::current_insertion_lsn(),
        built_at: now_millis(),
        n_docs,
    };
    write_build_meta(&dir_path, &anchor)
        .map_err(|e| format!("pgtantivy: touch '{name}' failed: {e}"))?;
    Ok(format!(
        "pgtantivy: anchor for '{name}' refreshed to {}",
        fmt_lsn(anchor.built_lsn)
    ))
}

// ─── 方案 1：不可变 snapshot bundle 导出/导入（多实例共享索引的承重核心） ──────
// Research #2026111601 §5 推荐路线的第一步："先用「快照 bundle 导出/导入 + 版本号
// + 单写者」把多实例共享跑起来（最小可用、风险可控，复用 duckdb-tantivy 现成
// save/load 思路）"。纯文件层逻辑在 src/snapshot.rs；这里是 SQL 面 + registry 接线。

/// Snapshot a named on-disk index into an immutable, versioned **bundle** at
/// `<dest>/<name>-<opstamp>.bundle/` (a copy of the index dir + a manifest.json
/// with opstamp / writer_id / doc count / per-file size+FNV checksum). Written
/// to a temp dir then atomically renamed, so a crash never leaves a half-written
/// bundle and the writer's live index is untouched (copy-on-export).
///
/// `writer_id` is the single-writer fence tag (default: `<hostname>.<pid>`).
/// Once an export has fenced this index to a writer identity, an export under a
/// different identity is refused (dual-writer protection); rebuild to change it.
/// `keep` retains the N newest `<name>-<opstamp>.bundle` dirs in `dest` and
/// deletes older ones (0/NULL = retain all).
///
/// Bundles are the transfer unit to a reader instance (copy to the other node,
/// then `pgtantivy_import_index`); S3 is 方案 2 (same format, drop-in).
#[pg_extern]
fn pgtantivy_export_index(
    name: String,
    dest: String,
    writer_id: Option<String>,
    keep: Option<i64>,
) -> Result<String, Box<dyn std::error::Error>> {
    if !safe_name(&name) {
        return Err("pgtantivy: name must be [A-Za-z_][A-Za-z0-9_-]*, <=128 chars".into());
    }
    let dir_path = std::path::Path::new(&index_root()).join(&name);
    if !dir_path.exists() {
        return Err(format!("pgtantivy: no index '{name}' on disk").into());
    }
    let (bundle, opstamp, n_files) = snapshot::export_bundle(
        &name,
        &dir_path,
        &dest,
        writer_id,
        keep.unwrap_or(0).max(0) as u64,
    )?;
    Ok(format!(
        "pgtantivy: exported '{name}' opstamp {opstamp} ({n_files} files) to {}",
        bundle.display()
    ))
}

/// Import (verify + extract + register) a bundle as index `as_name` in this
/// instance. Verifies every file's size + checksum against the manifest (a
/// corrupted/tampered bundle is rejected, never silently loaded), extracts to
/// `<index_root>/<as_name>/`, confirms the doc count, and registers it so
/// `pgtantivy_search` works immediately. The extracted index keeps its sidecar
/// drift anchor, so `pgtantivy_drift_check('as_name')` still signals staleness.
#[pg_extern]
fn pgtantivy_import_index(bundle: String, as_name: String) -> Result<String, Box<dyn std::error::Error>> {
    if !safe_name(&as_name) {
        return Err("pgtantivy: import name must be [A-Za-z_][A-Za-z0-9_-]*, <=128 chars".into());
    }
    let bundle_path = std::path::Path::new(bundle.trim());
    if !bundle_path.is_dir() {
        return Err(format!("pgtantivy: no such bundle directory {bundle:?}").into());
    }
    {
        let reg = registry().lock()?;
        if reg.contains_key(&as_name) {
            return Err(format!(
                "pgtantivy: index '{as_name}' already loaded in this session; \
                 import under a different name"
            )
            .into());
        }
    }
    let root = index_root();
    let target = std::path::Path::new(&root).join(&as_name);
    let meta = snapshot::extract_bundle(bundle_path, &target)?;
    let n_docs = register_index_at(&target, &as_name)?;
    Ok(format!(
        "pgtantivy: imported '{as_name}' from {} (opstamp {}, {} docs, writer {})",
        target.display(),
        meta.opstamp,
        n_docs,
        meta.writer_id
    ))
}

/// Describe a bundle (verify its manifest) as a JSON string: name, opstamp,
/// writer_id, doc count, language, file count, and the build-time LSN anchor.
/// Pure (no registry) — usable on any node to inspect a bundle before import.
#[pg_extern]
fn pgtantivy_bundle_info(bundle: String) -> Result<String, Box<dyn std::error::Error>> {
    snapshot::bundle_info(&bundle)
}

/// Publish an on-disk index to S3 as an immutable, versioned bundle
/// (research #2026111601 "共享索引" approach 1 — the cross-node transfer leg).
/// Builds the local bundle (same as `pgtantivy_export_index`: manifest-last,
/// FNV checksums, opstamp, dual-writer fence, local retention) then uploads it
/// object-by-object to S3, with the `manifest.json` uploaded LAST (the
/// "published" marker — a reader listing versions never sees a half-written
/// bundle). Applies the S3-side retention policy for `keep`.
/// `endpoint`/`region`/`access_key`/`secret` are per-call (MinIO-friendly, no
/// PG env dependency); `prefix` is the object-key namespace (e.g. "fti").
#[pg_extern]
fn pgtantivy_export_to_s3(
    name: String,
    endpoint: String,
    bucket: String,
    region: String,
    access_key: String,
    secret: String,
    prefix: String,
    writer_id: Option<String>,
    keep: Option<i64>,
) -> Result<String, Box<dyn std::error::Error>> {
    if !safe_name(&name) {
        return Err("pgtantivy: name must be [A-Za-z_][A-Za-z0-9_-]*, <=128 chars".into());
    }
    let dir_path = std::path::Path::new(&index_root()).join(&name);
    if !dir_path.exists() {
        return Err(format!("pgtantivy: no index '{name}' on disk").into());
    }
    let local_dest = std::env::temp_dir().join("pgt_s3_local").to_string_lossy().into_owned();
    let keep_u = keep.unwrap_or(0).max(0) as u64;
    let (local_bundle, _opstamp, n_files) =
        snapshot::export_bundle(&name, &dir_path, &local_dest, writer_id, keep_u)?;
    let op = s3::upload_bundle(
        &endpoint, &bucket, &region, &access_key, &secret, &prefix,
        &local_bundle, &name, keep_u,
    )?;
    let _ = std::fs::remove_dir_all(&local_dest);
    Ok(format!(
        "pgtantivy: published '{name}' opstamp {op} ({n_files} files) to s3://{bucket}/{prefix}/{name}-{} (region {region})",
        op
    ))
}

/// Download a published S3 bundle and register it as index `as_name` on this
/// instance. Re-verify every file's size + FNV checksum before extracting
/// (a corrupted/tampered object is rejected), extract to
/// `<index_root>/<as_name>/`, confirm the doc count, and register it for
/// immediate search. The replica keeps its build-time drift anchor.
#[pg_extern]
fn pgtantivy_import_from_s3(
    endpoint: String,
    bucket: String,
    region: String,
    access_key: String,
    secret: String,
    prefix: String,
    bundle_name: String,
    as_name: String,
) -> Result<String, Box<dyn std::error::Error>> {
    if !safe_name(&as_name) {
        return Err("pgtantivy: import name must be [A-Za-z_][A-Za-z0-9_-]*, <=128 chars".into());
    }
    if !bundle_name.trim().ends_with(".bundle") {
        return Err("pgtantivy: bundle_name must be '<name>-<opstamp>.bundle'".into());
    }
    let target = std::path::Path::new(&index_root()).join(&as_name);
    if target.exists() {
        return Err(format!(
            "pgtantivy: index '{as_name}' already exists; remove it first"
        )
        .into());
    }
    let meta = s3::download_bundle(
        &endpoint, &bucket, &region, &access_key, &secret, &prefix,
        &bundle_name, &target,
    )?;
    let n_docs = register_index_at(&target, &as_name)?;
    Ok(format!(
        "pgtantivy: imported '{as_name}' from s3://{bucket}/{prefix}/{bundle_name} (opstamp {}, {} docs, writer {})",
        meta.opstamp, n_docs, meta.writer_id
    ))
}

/// Load an index directly from an S3 bundle via the range-read `S3Directory`
/// (approach 2) — NO whole-bundle download to local disk. The index is
/// registered under `as_name` and searchable with `pgtantivy_search` just like
/// a local/index-loaded one; subsequent reads range-GET only what the query
/// touches and cache splits per-process (warm queries pull ~0 bytes).
///
/// `bundle_name` is the published name, e.g. `'snap-3.bundle'`; the objects are
/// addressed under `<prefix>/<bundle_name>/`. Use `pgtantivy_list_s3` to find
/// the current `bundle_name` for a logical `name`.
#[pg_extern]
fn pgtantivy_load_s3(
    endpoint: String,
    bucket: String,
    region: String,
    access_key: String,
    secret: String,
    prefix: String,
    bundle_name: String,
    as_name: String,
    warm_terms: Option<String>,
) -> Result<String, Box<dyn std::error::Error>> {
    if !safe_name(&as_name) {
        return Err("pgtantivy: name must be [A-Za-z_][A-Za-z0-9_-]*, <=128 chars".into());
    }
    if registry().lock()?.contains_key(&as_name) {
        return Ok(format!(
            "pgtantivy: index '{as_name}' already loaded (S3 load is idempotent)"
        ));
    }
    if !bundle_name.trim().ends_with(".bundle") {
        return Err("pgtantivy: bundle_name must be '<name>-<opstamp>.bundle'".into());
    }
    let bundle_key_prefix = format!("{}/{}", prefix.trim_end_matches('/'), bundle_name);
    // Open directly over S3 (no local download) and register; the helper also
    // stores the S3 origin so pgtantivy_reload_s3 can list/rollback later.
    let (num_docs, lang) = open_s3_registered(
        &endpoint, &bucket, &region, &access_key, &secret, &prefix, &bundle_name, &as_name,
    )?;
    // Query warm-up: run a throwaway search per term so the hot splits (term
    // dict + postings + .store footers) are pulled into the per-process cache
    // NOW, instead of on the first real query (which would pay N cold RTTs).
    // Errors warming one term are non-fatal — the index is still usable.
    let warmed: Vec<String> = warm_terms
        .map(|w| {
            w.split(',').map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut warmed_ok = Vec::new();
    for term in &warmed {
        let idx = registry().lock()?.get(&as_name).cloned();
        if let Some(idx) = idx {
            let searcher = idx.reader.searcher();
            let mut parser = QueryParser::for_index(&idx.index, vec![idx.body]);
            parser.set_conjunction_by_default();
            if let Ok(parsed) = parser.parse_query(&sanitize(&nfc(term))) {
                let _ = searcher.search(&parsed, &TopDocs::with_limit(1));
                warmed_ok.push(term.clone());
            }
        }
    }
    let warmed_note = if warmed_ok.is_empty() {
        String::new()
    } else {
        format!(" (warmed: {})", warmed_ok.join(", "))
    };
    Ok(format!(
        "pgtantivy: loaded '{as_name}' from s3://{bucket}/{bundle_key_prefix} (range-read, {} docs, lang={lang}{warmed_note})",
        num_docs
    ))
}

/// Re-point an S3-loaded replica at another published bundle version, in place
/// (same registry name). `to_bundle` = `NULL` upgrades to the NEWEST published
/// version; a concrete bundle name ("snap-12.bundle") rolls back to that
/// version. Reuses the S3 origin stored when the index was loaded via
/// `pgtantivy_load_s3`, so the caller does not re-supply credentials. The
/// replica keeps its name, so `pgtantivy_search('<name>', ...)` follows it.
#[pg_extern]
fn pgtantivy_reload_s3(name: String, to_bundle: Option<String>) -> Result<String, Box<dyn std::error::Error>> {
    let s3 = {
        let reg = registry().lock()?;
        let idx = reg
            .get(&name)
            .ok_or_else(|| format!("pgtantivy: index '{name}' not loaded in this backend"))?;
        let c = idx
            .s3
            .clone()
            .ok_or_else(|| {
                format!("pgtantivy: '{name}' is not an S3 replica (load it with pgtantivy_load_s3 first)")
            })?;
        let cur = idx
            .path
            .as_deref()
            .map(|p| p.rsplit('/').next().unwrap_or("").to_string());
        (c, cur)
    };
    let (cred, cur_bundle) = s3;
    // Resolve the target bundle: explicit arg wins; NULL -> newest published.
    let target = match to_bundle {
        Some(b) if !b.trim().is_empty() => {
            let b = b.trim().to_string();
            if !b.ends_with(".bundle") {
                return Err(format!("pgtantivy: to_bundle must be '<name>-<opstamp>.bundle' (got '{b}')").into());
            }
            b
        }
        _ => {
            let versions = s3::list_versions(
                &cred.endpoint, &cred.bucket, &cred.region,
                &cred.access_key, &cred.secret_key, &cred.prefix, &cred.name,
            )?;
            match versions.first() {
                Some((op, b, _)) => {
                    // list_versions is newest-first; b is "<name>-<op>.bundle"
                    let _ = op;
                    b.clone()
                }
                None => {
                    return Err(format!(
                        "pgtantivy: no published bundles for '{}' ({}-<opstamp>.bundle) under {}/{}",
                        cred.name, cred.name, cred.bucket, cred.prefix.trim_end_matches('/')
                    ).into());
                }
            }
        }
    };
    // No-op fast path: already serving that bundle.
    if Some(target.clone()) == cur_bundle {
        return Ok(format!(
            "pgtantivy: replica '{name}' already serving {target} (no change)"
        ));
    }
    open_s3_registered(
        &cred.endpoint, &cred.bucket, &cred.region, &cred.access_key,
        &cred.secret_key, &cred.prefix, &target, &name,
    )?;
    Ok(format!(
        "pgtantivy: replica '{name}' reloaded -> {target} (was {})",
        cur_bundle.unwrap_or_else(|| "?".into())
    ))
}

/// Open an S3 index directly over the object store (range-read, no local
/// download) and register it in the per-backend registry. Shared by
/// `pgtantivy_load_s3` (first load) and `pgtantivy_reload_s3` (version
/// upgrade / rollback).
fn open_s3_registered(
    endpoint: &str,
    bucket: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
    prefix: &str,
    bundle_name: &str,
    as_name: &str,
) -> Result<(u64, String), Box<dyn std::error::Error>> {
    let bundle_key_prefix = format!("{}/{}", prefix.trim_end_matches('/'), bundle_name);
    let (index, lang) = s3::open_s3_index(
        endpoint, bucket, region, access_key, secret_key, &bundle_key_prefix,
    )?;
    index.tokenizers().register(JIEBA_TOKENIZER, analyzer(&lang));
    let schema = index.schema();
    let body = schema.get_field("body").map_err(|_| "pgtantivy: index lacks body field")?;
    let id = schema.get_field("id").map_err(|_| "pgtantivy: index lacks id field")?;
    // Read-only S3 directory: no watch. Manual reload avoids the default
    // OnCommitWithDelay policy (which would call watch() and fail).
    let reader: IndexReader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let num_docs = reader.searcher().num_docs();
    // Base name ("snap-12.bundle" -> "snap"): what list_versions lists against.
    let base_name = bundle_name
        .strip_suffix(".bundle")
        .and_then(|b| b.rsplit_once('-').map(|(n, _)| n.to_string()))
        .unwrap_or_else(|| bundle_name.to_string());
    registry().lock()?.insert(
        as_name.to_string(),
        Arc::new(PgIndex {
            index,
            reader,
            body,
            id,
            path: Some(format!("s3://{bucket}/{bundle_key_prefix}")),
            s3: Some(S3Cred {
                endpoint: endpoint.to_string(),
                bucket: bucket.to_string(),
                region: region.to_string(),
                access_key: access_key.to_string(),
                secret_key: secret_key.to_string(),
                prefix: prefix.to_string(),
                name: base_name,
            }),
        }),
    );
    Ok((num_docs, lang))
}

/// List the published S3 bundle versions for `name` (those with a manifest),
/// newest first: opstamp, bundle name, doc count.
#[pg_extern]
fn pgtantivy_list_s3(
    endpoint: String,
    bucket: String,
    region: String,
    access_key: String,
    secret: String,
    prefix: String,
    name: String,
) -> Result<TableIterator<'static, (name!(opstamp, i64), name!(bundle, String), name!(n_docs, i64))>, Box<dyn std::error::Error>> {
    let rows = s3::list_versions(
        &endpoint, &bucket, &region, &access_key, &secret, &prefix, &name,
    )?;
    let rows: Vec<(i64, String, i64)> = rows
        .into_iter()
        .map(|(op, b, d)| (op as i64, b, d as i64))
        .collect();
    Ok(TableIterator::new(rows))
}

/// Replication-lag health for an S3-loaded replica: is it serving the newest
/// published version of its source, or is it behind? Read-only, reuses the S3
/// origin stored at load — no credentials re-supplied. Returns a single-line
/// status string: `up-to-date` / `lagging (serving op X, source op Y)` /
/// `source-unknown` (no published versions found). Pair with
/// `pgtantivy_reload_s3(name, NULL)` to catch up.
#[pg_extern]
fn pgtantivy_s3_status(name: String) -> Result<String, Box<dyn std::error::Error>> {
    let cred = {
        let reg = registry().lock()?;
        let idx = reg
            .get(&name)
            .ok_or_else(|| format!("pgtantivy: index '{name}' not loaded in this backend"))?;
        idx.s3
            .clone()
            .ok_or_else(|| format!("pgtantivy: '{name}' is not an S3 replica"))?
    };
    // The bundle the replica is currently serving (last path segment, e.g.
    // "snap-3.bundle"); parse its opstamp.
    let cur_bundle = registry()
        .lock()?
        .get(&name)
        .and_then(|i| i.path.clone())
        .map(|p| p.rsplit('/').next().unwrap_or("").to_string())
        .unwrap_or_default();
    let cur_op: Option<u64> = cur_bundle
        .strip_suffix(".bundle")
        .and_then(|b| b.rsplit_once('-').map(|(_, o)| o.parse::<u64>().ok()))
        .flatten();
    // Newest published version of the source.
    let versions = s3::list_versions(
        &cred.endpoint, &cred.bucket, &cred.region,
        &cred.access_key, &cred.secret_key, &cred.prefix, &cred.name,
    )?;
    let latest = versions.first().map(|(op, b, _)| (*op, b.clone()));
    match (cur_op, latest) {
        (Some(cur), Some((latest_op, _))) if cur == latest_op => {
            Ok(format!(
                "pgtantivy_s3_status('{name}'): UP-TO-DATE (serving {cur_bundle})"
            ))
        }
        (Some(cur), Some((latest_op, _))) => Ok(format!(
            "pgtantivy_s3_status('{name}'): LAGGING (serving op {cur}; source is op {latest_op})"
        )),
        (None, Some((latest_op, latest_bundle))) => Ok(format!(
            "pgtantivy_s3_status('{name}'): LAGGING (serving {cur_bundle} w/o opstamp; source is op {latest_op} / {latest_bundle})"
        )),
        (Some(cur), None) => Ok(format!(
            "pgtantivy_s3_status('{name}'): SOURCE-UNKNOWN (serving op {cur}; no published bundles found for '{}')",
            cred.name
        )),
        (None, None) => Ok(format!(
            "pgtantivy_s3_status('{name}'): SOURCE-UNKNOWN (no published bundles for '{}'; replica path {cur_bundle})",
            cred.name
        )),
    }
}

/// Incrementally append documents to an existing (loaded) index.
#[pg_extern]
fn pgtantivy_add(name: String, rows_json: String) -> Result<String, Box<dyn std::error::Error>> {
    let idx = lookup(&name)?;
    let rows: Vec<serde_json::Value> = serde_json::from_str(&rows_json)?;
    if rows.is_empty() {
        return Ok(format!(
            "pgtantivy: add to '{}' — nothing to do (0 rows)",
            name
        ));
    }
    let body_field = idx.body;
    let id_field = idx.id;
    // writer-per-call: tantivy allows one live writer at a time; the one
    // from create/add was dropped after commit, so this is safe.
    let mut writer = idx
        .index
        .writer::<tantivy::TantivyDocument>(32 * 1024 * 1024)?;
    let mut n = 0u64;
    for row in rows {
        let id = row["id"]
            .as_i64()
            .ok_or_else(|| format!("row missing id: {}", row))?;
        let text = row["text"]
            .as_str()
            .ok_or_else(|| format!("row missing text: {}", row))?
            .to_string();
        let text_nfc = nfc(&text);
        // upsert semantics: delete any prior doc with this id, then add
        writer.delete_term(tantivy::Term::from_field_i64(id_field, id));
        writer.add_document(doc!(id_field => id, body_field => text_nfc.as_str()))?;
        n += 1;
    }
    writer.commit()?;
    drop(writer);
    idx.reader.reload()?;
    refresh_anchor(&*idx, &name);
    maybe_auto_merge(&idx, &name);
    Ok(format!(
        "pgtantivy: {} doc(s) added to '{}' (upsert by id)",
        n, name
    ))
}

/// Remove documents from an existing (loaded) index by id.
#[pg_extern]
fn pgtantivy_remove(name: String, ids: Array<i64>) -> Result<String, Box<dyn std::error::Error>> {
    let idx = lookup(&name)?;
    let id_field = idx.id;
    let ids: Vec<i64> = ids.iter().flatten().collect();
    if ids.is_empty() {
        return Ok(format!(
            "pgtantivy: remove from '{}' — nothing to do (0 ids)",
            name
        ));
    }
    let mut writer = idx
        .index
        .writer::<tantivy::TantivyDocument>(32 * 1024 * 1024)?;
    let mut n = 0u64;
    for id in &ids {
        writer.delete_term(tantivy::Term::from_field_i64(id_field, *id));
        n += 1;
    }
    writer.commit()?;
    drop(writer);
    idx.reader.reload()?;
    refresh_anchor(&*idx, &name);
    Ok(format!("pgtantivy: {} id(s) removed from '{}'", n, name))
}

// ─── trigger auto-sync: buffer DML, flush once at PRE_COMMIT ───────────────
// One tantivy commit per transaction (not per row): a 1000-row INSERT makes
// one segment, not a thousand. Buffer is per-transaction state; the
// PRE_COMMIT callback (registered lazily on first buffered op) flushes all
// pending ops to their indexes; ABORT drops the buffer without touching disk.

enum PendingOp {
    Upsert {
        index: String,
        id: i64,
        text: String,
    },
    Delete {
        index: String,
        id: i64,
    },
}

fn pending_ops() -> &'static Mutex<Vec<PendingOp>> {
    static P: OnceLock<Mutex<Vec<PendingOp>>> = OnceLock::new();
    P.get_or_init(|| Mutex::new(Vec::new()))
}

/// Flush all buffered ops: called exactly once per transaction at PRE_COMMIT.
fn flush_pending() {
    let ops = {
        let mut g = match pending_ops().lock() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        std::mem::take(&mut *g)
    };
    if ops.is_empty() {
        return;
    }
    // group by index: one writer+commit per index
    use std::collections::BTreeMap;
    let mut by_index: BTreeMap<String, Vec<PendingOp>> = BTreeMap::new();
    for op in ops {
        let k = match &op {
            PendingOp::Upsert { index, .. } => index.clone(),
            PendingOp::Delete { index, .. } => index.clone(),
        };
        by_index.entry(k).or_default().push(op);
    }
    for (name, group) in by_index {
        let idx = match registry().lock().ok().and_then(|r| r.get(&name).cloned()) {
            Some(i) => i,
            None => {
                warning!(
                    "pgtantivy sync: index '{name}' not loaded; {} op(s) skipped",
                    group.len()
                );
                continue;
            }
        };
        let mut writer = match idx
            .index
            .writer::<tantivy::TantivyDocument>(32 * 1024 * 1024)
        {
            Ok(w) => w,
            Err(e) => {
                warning!("pgtantivy sync: writer for '{name}' failed: {e}");
                continue;
            }
        };
        for op in &group {
            match op {
                PendingOp::Upsert { id, text, .. } => {
                    let _ = writer.delete_term(tantivy::Term::from_field_i64(idx.id, *id));
                    let text_nfc = nfc(text);
                    let _ = writer.add_document(doc!(
                        idx.id => *id,
                        idx.body => text_nfc.as_str()
                    ));
                }
                PendingOp::Delete { id, .. } => {
                    let _ = writer.delete_term(tantivy::Term::from_field_i64(idx.id, *id));
                }
            }
        }
        if let Err(e) = writer.commit() {
            warning!("pgtantivy sync: commit for '{name}' failed: {e}");
            continue;
        }
        drop(writer);
        let _ = idx.reader.reload();
        refresh_anchor(&idx, &name);
        maybe_auto_merge(&idx, &name);
    }
}

/// Merge all searchable segments into one (manual compaction).
/// After many incremental commits (add/remove/trigger syncs) the index
/// accumulates small segments + tombstones. This rewrites it as a single
/// segment: reclaimed disk, faster queries.
#[pg_extern]
fn pgtantivy_merge(name: String) -> Result<String, Box<dyn std::error::Error>> {
    let idx = lookup(&name)?;
    let before = idx.index.searchable_segment_metas()?.len();
    if before <= 1 {
        return Ok(format!(
            "pgtantivy: '{name}' already compact ({before} segment(s), nothing to merge)"
        ));
    }
    merge_all(&idx)?;
    Ok(format!(
        "pgtantivy: '{name}' merged {before} segment(s) into 1"
    ))
}

/// Drop all buffered ops (xact ABORT).
fn discard_pending() {
    let mut g = match pending_ops().lock() {
        Ok(g) => g,
        Err(e) => e.into_inner(),
    };
    g.clear();
}

/// Row-level trigger: pgtantivy_sync_trigger() — attach with
///   CREATE TRIGGER ... EXECUTE PROCEDURE pgtantivy_sync_trigger();
/// Trigger args: (index_name, id_column [, text_column = 'body'])
/// Buffers INSERT/UPDATE/DELETE ops; a PRE_COMMIT callback flushes them
/// in one tantivy commit per index.
#[pg_trigger]
#[allow(clippy::type_complexity)]
fn pgtantivy_sync_trigger<'a>(
    trigger: &'a PgTrigger<'a>,
) -> Result<Option<PgHeapTuple<'a, impl WhoAllocated>>, Box<dyn std::error::Error>> {
    use pgrx::trigger_support::PgTriggerOperation::*;
    let args = trigger.extra_args()?;
    if args.len() < 2 {
        let e: Box<dyn std::error::Error> =
            "pgtantivy sync trigger needs args: (index_name, id_column [, text_column])".into();
        return Err(e);
    }
    let index = args[0].clone();
    let id_col = args[1].clone();
    let text_col = args.get(2).cloned().unwrap_or_else(|| "body".into());

    let op = trigger.op()?;
    let mut guard = pending_ops()
        .lock()
        .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
    match op {
        Insert | Update => {
            let new = trigger.new().ok_or::<Box<dyn std::error::Error>>(
                "pgtantivy sync: INSERT/UPDATE trigger has no NEW row".into(),
            )?;
            let id: i64 = new
                .get_by_name::<i64>(&id_col)
                .map_err(|e| format!("pgtantivy sync: id col '{id_col}': {e}"))?
                .ok_or_else(|| format!("pgtantivy sync: NEW row lacks '{id_col}'"))?;
            let text: String = new
                .get_by_name::<String>(&text_col)
                .map_err(|e| format!("pgtantivy sync: text col '{text_col}': {e}"))?
                .ok_or_else(|| format!("pgtantivy sync: NEW row lacks '{text_col}'"))?;
            guard.push(PendingOp::Upsert { index, id, text });
        }
        Delete => {
            let old = trigger.old().ok_or::<Box<dyn std::error::Error>>(
                "pgtantivy sync: DELETE trigger has no OLD row".into(),
            )?;
            let id: i64 = old
                .get_by_name::<i64>(&id_col)
                .map_err(|e| format!("pgtantivy sync: id col '{id_col}': {e}"))?
                .ok_or_else(|| format!("pgtantivy sync: OLD row lacks '{id_col}'"))?;
            guard.push(PendingOp::Delete { index, id });
        }
        _ => {}
    }
    drop(guard);
    // lazy one-shot: flush at PRE_COMMIT, drop buffer on ABORT (the static
    // buffer outlives the transaction, so it must be drained explicitly —
    // otherwise rolled-back ops leak into the next transaction's flush)
    register_xact_callback(PgXactCallbackEvent::PreCommit, flush_pending);
    register_xact_callback(PgXactCallbackEvent::Abort, discard_pending);
    Ok(None as Option<PgHeapTuple<'a, pgrx::pgbox::AllocatedByPostgres>>)
}

// impl Error for PgTriggerError so Box<dyn Error> works with `?` on trigger.op()/extra_args()
// (PgTriggerError implements std::error::Error in pgrx 0.13; nothing needed here).

/// List indexes: on-disk (with loaded flag) — one row per index dir.
#[pg_extern]
fn pgtantivy_list(
) -> TableIterator<'static, (name!(name, String), name!(loaded, bool), name!(docs, i64))> {
    let root = index_root();
    let mut rows: Vec<(String, bool, i64)> = Vec::new();
    let reg = registry().lock().unwrap_or_else(|e| e.into_inner());
    if let Ok(rd) = std::fs::read_dir(&root) {
        for e in rd.flatten() {
            let fname = e.file_name().to_string_lossy().into_owned();
            if !safe_name(&fname) {
                continue;
            }
            let loaded = reg.contains_key(&fname);
            let docs = reg
                .get(&fname)
                .map(|i| i.reader.searcher().num_docs() as i64)
                .unwrap_or(-1);
            rows.push((fname, loaded, docs));
        }
    }
    TableIterator::new(rows)
}

/// BM25 search over a named index. AND semantics, sanitized.
#[pg_extern]
fn pgtantivy_search(
    name: String,
    query: String,
    top_k: i64,
) -> Result<TableIterator<'static, (name!(id, i64), name!(score, f64))>, Box<dyn std::error::Error>>
{
    let idx = lookup(&name)?;
    let searcher = idx.reader.searcher();
    let mut parser = QueryParser::for_index(&idx.index, vec![idx.body]);
    parser.set_conjunction_by_default();
    let parsed = parser
        .parse_query(&sanitize(&nfc(&query)))
        .map_err(|e| format!("pgtantivy: bad query: {e}"))?;
    let top = searcher.search(&parsed, &TopDocs::with_limit(top_k.max(1) as usize))?;
    let mut hits: Vec<(i64, f64)> = top
        .into_iter()
        .filter_map(|(s, addr)| {
            let d: tantivy::TantivyDocument = searcher.doc(addr).ok()?;
            Some((d.get_first(idx.id)?.as_i64()?, s as f64))
        })
        .collect();
    hits.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap().then(a.0.cmp(&b.0)));
    Ok(TableIterator::new(hits))
}

/// Highlighted fragment (markers [HIGHLIGHT]/[/HIGHLIGHT]).
#[pg_extern]
fn pgtantivy_snippet(
    name: String,
    query: String,
    text: String,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let idx = lookup(&name)?;
    let searcher = idx.reader.searcher();
    let mut parser = QueryParser::for_index(&idx.index, vec![idx.body]);
    parser.set_conjunction_by_default();
    let parsed = parser
        .parse_query(&sanitize(&nfc(&query)))
        .map_err(|e| format!("pgtantivy: bad query: {e}"))?;
    let mut gen = SnippetGenerator::create(&searcher, &parsed, idx.body)?;
    gen.set_max_num_chars(150);
    let mut snip = gen.snippet(&nfc(&text));
    snip.set_snippet_prefix_postfix("[HIGHLIGHT]", "[/HIGHLIGHT]");
    Ok(Some(snip.to_html()))
}

pub(crate) fn lookup(name: &str) -> Result<Arc<PgIndex>, Box<dyn std::error::Error>> {
    registry().lock()?.get(name).cloned().ok_or_else(|| {
        format!("pgtantivy: no index '{name}' (create with pgtantivy_create)").into()
    })
}

fn nfc(s: &str) -> String {
    s.nfc().collect::<String>()
}

pub(crate) fn sanitize(q: &str) -> String {
    // Map every non-alphanumeric character to a space, so the text handed to
    // tantivy's QueryParser can only contain space-separated alphanumeric
    // "terms" and never a DSL construct.
    //
    // Why an allow-list and not a deny-list of metacharacters: the QueryParser
    // has a large DSL surface (range `> < >= <= [ {`, set `IN`, phrase, prefix
    // `~`, boost `^`, string-literal quote `' "`, ...). A deny-list keeps
    // missing new operators, and each miss is a hard error (e.g. a bare `>`
    // becomes an unbounded *range* on a text field -> "Range query need to
    // target a specific field", and a bare `'` a Syntax Error). Real user
    // queries hit these ("y > x", "can't", "..., FL").
    //
    // This is consistent with the index by construction: the SAME tokenizer
    // (SimpleTokenizer for english / Jieba for chinese, + LowerCaser) tokenizes
    // both the indexed documents and this query string, and it already
    // discards every non-alphanumeric character. So stripping punctuation here
    // cannot change which terms a query matches - it only removes characters
    // the parser would otherwise interpret as operators. Unicode-aware
    // (char::is_alphanumeric), so CJK ideographs are preserved for the jieba
    // path and only Latin-script punctuation is stripped.
    q.chars()
        .map(|c| if c.is_alphanumeric() { c } else { ' ' })
        .collect()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod pg_test_tests {
    use super::*;

    #[pg_test]
    fn test_smoke() {
        let r = pgtantivy_version();
        assert!(r.starts_with("0.4"));
    }
}

#[cfg(test)]
pub mod test {
    #[test]
    fn rust_smoke() {
        assert_eq!("0.1.0", env!("CARGO_PKG_VERSION"));
    }
}
