// pgtantivy — Chinese-first BM25 full-text search for PostgreSQL via tantivy.
// Seam B prototype: tantivy BM25 + jieba tokenization in a pgrx extension.
// MIT. No ParadeDB code — clean-room, architecture-inspired only.
use pgrx::prelude::*;
use pgrx::pgbox::WhoAllocated;
use pgrx::callbacks::{register_xact_callback, PgXactCallbackEvent};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{
    IndexRecordOption, Schema, TextFieldIndexing, TextOptions, Value, FAST, INDEXED, STORED,
};
use tantivy::snippet::SnippetGenerator;
use tantivy::tokenizer::{
    LowerCaser, SimpleTokenizer, TextAnalyzer, Token, TokenStream, Tokenizer,
};
use tantivy::{doc, Index, IndexReader};
use std::ffi::CStr;

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use unicode_normalization::UnicodeNormalization;
pg_module_magic!();

// GUC pgtantivy.index_dir: where persistent indexes live. Empty (the
// default) resolves to $PGDATA/pgtantivy.
static INDEX_DIR_GUC: GucSetting<Option<&'static CStr>> = GucSetting::<Option<&'static CStr>>::new(None);

#[allow(non_snake_case)]
#[no_mangle]
#[pg_guard]
pub unsafe extern "C" fn _PG_init() {
    GucRegistry::define_string_guc(
        "pgtantivy.index_dir",
        "Base directory for persistent pgtantivy indexes",
        "Where pgtantivy stores persistent indexes. Empty = $PGDATA/pgtantivy.",
        &INDEX_DIR_GUC,
        GucContext::Userset,
        GucFlags::default(),
    );
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

struct PgIndex {
    index: Index,
    reader: IndexReader,
    body: tantivy::schema::Field,
    id: tantivy::schema::Field,
    /// On-disk directory when persistent (None for legacy in-memory only).
    path: Option<String>,
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
    // Sidecar: language for load-time tokenizer registration.
    let _ = std::fs::write(
        dir_path.join("pgtantivy_meta.json"),
        serde_json::json!({"lang": lang}).to_string(),
    );
    let reader = index.reader()?;
    registry().lock()?.insert(
        name.clone(),
        Arc::new(PgIndex {
            index,
            reader,
            body: body_field,
            id: id_field,
            path: Some(dir_path.to_string_lossy().into_owned()),
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
fn pgtantivy_load(
    name: String,
) -> Result<String, Box<dyn std::error::Error>> {
    if !safe_name(&name) {
        return Err("pgtantivy: name must be [A-Za-z_][A-Za-z0-9_-]*, <=128 chars".into());
    }
    if registry().lock()?.contains_key(&name) {
        return Ok(format!("pgtantivy: index '{}' already loaded", name));
    }
    let dir_path = std::path::Path::new(&index_root()).join(&name);
    let dir = tantivy::directory::MmapDirectory::open(&dir_path)
        .map_err(|e| format!("pgtantivy: no index '{}' on disk ({e})", name))?;
    let index = Index::open(dir)?;
    let meta = index.tokenizers();
    let _ = meta;
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
        name.clone(),
        Arc::new(PgIndex {
            index,
            reader,
            body,
            id,
            path: Some(dir_path.to_string_lossy().into_owned()),
        }),
    );
    Ok(format!(
        "pgtantivy: index '{}' loaded ({} docs, lang={})",
        name, num_docs, lang
    ))
}

/// Incrementally append documents to an existing (loaded) index.
#[pg_extern]
fn pgtantivy_add(
    name: String,
    rows_json: String,
) -> Result<String, Box<dyn std::error::Error>> {
    let idx = lookup(&name)?;
    let rows: Vec<serde_json::Value> = serde_json::from_str(&rows_json)?;
    if rows.is_empty() {
        return Ok(format!("pgtantivy: add to '{}' — nothing to do (0 rows)", name));
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
    Ok(format!("pgtantivy: {} doc(s) added to '{}' (upsert by id)", n, name))
}

/// Remove documents from an existing (loaded) index by id.
#[pg_extern]
fn pgtantivy_remove(
    name: String,
    ids: Array<i64>,
) -> Result<String, Box<dyn std::error::Error>> {
    let idx = lookup(&name)?;
    let id_field = idx.id;
    let ids: Vec<i64> = ids.iter().flatten().collect();
    if ids.is_empty() {
        return Ok(format!("pgtantivy: remove from '{}' — nothing to do (0 ids)", name));
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
    Ok(format!("pgtantivy: {} id(s) removed from '{}'", n, name))
}

// ─── trigger auto-sync: buffer DML, flush once at PRE_COMMIT ───────────────
// One tantivy commit per transaction (not per row): a 1000-row INSERT makes
// one segment, not a thousand. Buffer is per-transaction state; the
// PRE_COMMIT callback (registered lazily on first buffered op) flushes all
// pending ops to their indexes; ABORT drops the buffer without touching disk.

enum PendingOp {
    Upsert { index: String, id: i64, text: String },
    Delete { index: String, id: i64 },
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
                warning!("pgtantivy sync: index '{name}' not loaded; {} op(s) skipped", group.len());
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
                    let _ = writer
                        .delete_term(tantivy::Term::from_field_i64(idx.id, *id));
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
    }
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

fn lookup(name: &str) -> Result<Arc<PgIndex>, Box<dyn std::error::Error>> {
    registry().lock()?.get(name).cloned().ok_or_else(|| {
        format!("pgtantivy: no index '{name}' (create with pgtantivy_create)").into()
    })
}

fn nfc(s: &str) -> String {
    s.nfc().collect::<String>()
}

fn sanitize(q: &str) -> String {
    q.chars()
        .map(|c| {
            if "+^-&|!(){}[]^\"~*?:\\/".contains(c) {
                ' '
            } else {
                c
            }
        })
        .collect()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod pg_test_tests {
    use super::*;

    #[pg_test]
    fn test_smoke() {
        let r = pgtantivy_version();
        assert!(r.starts_with("0.1"));
    }
}

#[cfg(test)]
pub mod test {
    #[test]
    fn rust_smoke() {
        assert_eq!("0.1.0", env!("CARGO_PKG_VERSION"));
    }
}

