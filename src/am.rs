//! P3: index access method — `USING pgtantivy` on a real PG index.
//!
//! MVP scope (clean-room, no pg_search code):
//! - ambuild: full-table scan of the indexed relation, building a tantivy
//!   sidecar index named after the index relation (via relname), with
//!   reloptions text_column / id_column / language.
//! - ambulkdelete/amvacuumcleanup: no-op (sidecar lifetime = index lifetime).
//! - scan callbacks: minimal, error if used (planner integration is the next
//!   phase; searches go through pgtantivy_load + pgtantivy_search).
//!
//! Storage remains the v0.2 sidecar layout under $PGDATA/pgtantivy/<name>/.
//! WAL/recovery contract unchanged (documented: REINDEX after crash).

use crate::{index_root, lookup, nfc, registry, sanitize, PgIndex, JIEBA_TOKENIZER, analyzer};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use pgrx::prelude::*;
use tantivy::schema::{IndexRecordOption, TextFieldIndexing, TextOptions, Value, INDEXED, STORED, FAST};

// ── handler ──────────────────────────────────────────────────────────────

/// Minimal cost model: query-agnostic constant — BM25 top-N on a tantivy
/// sidecar is sub-ms regardless of table size (mmap + inverted index).
unsafe extern "C" fn amcostestimate(
    _root: *mut pg_sys::PlannerInfo,
    path: *mut pg_sys::IndexPath,
    _loop_count: f64,
    index_startup_cost: *mut pg_sys::Cost,
    index_total_cost: *mut pg_sys::Cost,
    index_selectivity: *mut pg_sys::Selectivity,
    index_correlation: *mut f64,
    index_pages: *mut f64,
) {
    // Pit #9: a constant cheap cost makes the planner pick this AM even for
    // scans with NO index clauses (e.g. SELECT count(*) FROM t) — the scan
    // then runs with no query key and returns nothing. An AM without usable
    // clauses must price itself out.
    let nclauses = if path.is_null() { 0 } else {
        let l = (*path).indexclauses;
        if l.is_null() { 0 } else { (*l).length }
    };
    if nclauses <= 0 {
        *index_startup_cost = f64::MAX / 2.0;
        *index_total_cost = f64::MAX / 2.0;
        *index_selectivity = 1.0;
        *index_correlation = 0.0;
        *index_pages = 0.0;
        return;
    }
    *index_startup_cost = 0.01;
    *index_total_cost = 1.0;
    *index_selectivity = 0.01;
    *index_correlation = 0.0;
    *index_pages = 1.0;
}

/// The @@@ operator implementation. Only used as an index condition seed:
/// the planner matches `body @@@ 'query'` against strategy 1 of pgtantivy_ops
/// and routes it to our bitmap scan. Runtime execution (when no index) is
/// a naive substring/jieba fallback: NOT scored BM25 — use pgtantivy_search
/// for ranking.
#[pg_extern]
fn pgtantivy_match(left: String, right: String) -> bool {
    // Fallback (no index / recheck): naive containment on NFC forms.
    let l = crate::nfc(&left);
    let r = crate::nfc(&right);
    l.contains(&r)
}

#[pg_extern(sql = "
CREATE FUNCTION pgtantivy_am_handler(internal) RETURNS index_am_handler
PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
CREATE ACCESS METHOD pgtantivy TYPE INDEX HANDLER pgtantivy_am_handler;

CREATE OPERATOR FAMILY pgtantivy_ops USING pgtantivy;
CREATE OPERATOR @@@ (LEFTARG = text, RIGHTARG = text, PROCEDURE = pgtantivy_match);
CREATE OPERATOR CLASS pgtantivy_ops DEFAULT FOR TYPE text USING pgtantivy FAMILY pgtantivy_ops AS
  OPERATOR 1 @@@ (text, text);
")]
fn pgtantivy_am_handler(_fcinfo: pg_sys::FunctionCallInfo) -> PgBox<pg_sys::IndexAmRoutine> {
    let amroutine = unsafe {
        let n = pg_sys::palloc0(std::mem::size_of::<pg_sys::IndexAmRoutine>()) as *mut pg_sys::IndexAmRoutine;
        (*n).type_ = pg_sys::NodeTag::T_IndexAmRoutine;
        n
    };
    let amroutine = unsafe {
        let n = pg_sys::palloc0(std::mem::size_of::<pg_sys::IndexAmRoutine>()) as *mut pg_sys::IndexAmRoutine;
        (*n).type_ = pg_sys::NodeTag::T_IndexAmRoutine;
        n
    };

    unsafe {
    (*amroutine).amstrategies = 0;
    (*amroutine).amsupport = 0;
    (*amroutine).amcanmulticol = false;
    (*amroutine).amoptionalkey = true;
    (*amroutine).amcanparallel = false;
    (*amroutine).amkeytype = pg_sys::InvalidOid;

    (*amroutine).amvalidate = Some(validate as _);
    (*amroutine).amoptions = Some(amoptions as _);
    (*amroutine).amcostestimate = Some(amcostestimate as _);
    (*amroutine).ambuild = Some(ambuild as _);
    (*amroutine).ambuildempty = Some(ambuildempty as _);
    (*amroutine).aminsert = Some(aminsert as _);
    (*amroutine).ambulkdelete = Some(ambulkdelete as _);
    (*amroutine).amvacuumcleanup = Some(amvacuumcleanup as _);
    (*amroutine).ambeginscan = Some(ambeginscan as _);
    (*amroutine).amrescan = Some(amrescan as _);
    (*amroutine).amendscan = Some(amendscan as _);
    (*amroutine).amgettuple = None; // bitmap-only for now
    (*amroutine).amgetbitmap = Some(amgetbitmap as _);
    }

    unsafe { PgBox::from_pg(amroutine) }
}

// ── amvalidate ───────────────────────────────────────────────────────────

/// No operator classes defined for this AM in the MVP: the index is driven by
/// reloptions, not by operators, so any opclass is accepted (PG requires
/// amvalidate to exist; CREATE INDEX ... USING pgtantivy (col) will still pass
/// a default opclass through).
unsafe extern "C" fn validate(_opclassoid: pg_sys::Oid) -> bool {
    true
}

// ── reloptions parsing ───────────────────────────────────────────────────

/// Options struct mirrored by the relopt_parse_elt table below.
#[repr(C)]
struct PgtantivyOpts {
    vl_len_: i32,
    text_column_off: i32,
    id_column_off: i32,
    language_off: i32,
}

static mut PGT_KIND: pg_sys::relopt_kind::Type = 0;

/// relname -> tantivy ctid field handle (populated at ambuild).
static CTID_FIELDS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<String, tantivy::schema::Field>>> =
    std::sync::OnceLock::new();

fn ctid_fields() -> &'static std::sync::Mutex<std::collections::HashMap<String, tantivy::schema::Field>> {
    CTID_FIELDS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Register our reloption kind. MUST run inside _PG_init (allocations land
/// in TopMemoryContext); registering mid-query leaves dangling relopt_gen
/// pointers that SEGV build_reloptions later.
pub unsafe fn init_reloptions() {
    unsafe {
        ensure_relopt_kind(&raw mut PGT_KIND);
    }
}

unsafe fn ensure_relopt_kind(pgt_kind: *mut pg_sys::relopt_kind::Type) {
    if *pgt_kind == 0 {
        let kind = pg_sys::add_reloption_kind();
        for (name, desc, dflt) in [
            ("text_column", "text column to index", "body"),
            ("id_column", "id column", "id"),
            ("language", "tokenizer language", "chinese"),
        ] {
            let mut cname = name.as_bytes().to_vec(); cname.push(0);
            let mut cdesc = desc.as_bytes().to_vec(); cdesc.push(0);
            let mut cdflt = dflt.as_bytes().to_vec(); cdflt.push(0);
            pg_sys::add_string_reloption(
                kind,
                cname.as_ptr() as *const std::ffi::c_char,
                cdesc.as_ptr() as *const std::ffi::c_char,
                cdflt.as_ptr() as *const std::ffi::c_char,
                None,
                pg_sys::ShareUpdateExclusiveLock as pg_sys::LOCKMODE,
            );
        }
        *pgt_kind = kind;
    }
}

fn relopt_table() -> [pg_sys::relopt_parse_elt; 3] {
    [
    pg_sys::relopt_parse_elt {
        optname: c"text_column".as_ptr(),
        opttype: pg_sys::relopt_type::RELOPT_TYPE_STRING,
        offset: std::mem::offset_of!(PgtantivyOpts, text_column_off) as i32,
    },
    pg_sys::relopt_parse_elt {
        optname: c"id_column".as_ptr(),
        opttype: pg_sys::relopt_type::RELOPT_TYPE_STRING,
        offset: std::mem::offset_of!(PgtantivyOpts, id_column_off) as i32,
    },
    pg_sys::relopt_parse_elt {
        optname: c"language".as_ptr(),
        opttype: pg_sys::relopt_type::RELOPT_TYPE_STRING,
        offset: std::mem::offset_of!(PgtantivyOpts, language_off) as i32,
    },
    ]
}

/// Read already-parsed options off the relation (rd_options is our PgtantivyOpts bytea).
unsafe fn read_reloptions(index_rel: pg_sys::Relation) -> (Option<String>, Option<String>, String) {
    let rd_opts = (*index_rel).rd_options as *mut PgtantivyOpts;
    if rd_opts.is_null() {
        return (None, None, "chinese".into());
    }
    // String relopt fields store OFFSETS into the struct, not pointers
    // (GET_STRING_RELOPTION semantics).
    let base = rd_opts as *mut u8;
    let take = |off: i32| -> Option<String> {
        if off == 0 { return None; }
        let p = base.offset(off as isize) as *const std::ffi::c_char;
        if p.is_null() { None } else {
            Some(std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned())
        }
    };
    (
        take((*rd_opts).text_column_off),
        take((*rd_opts).id_column_off),
        take((*rd_opts).language_off).unwrap_or_else(|| "chinese".into()),
    )
}

/// amoptions: parse/validate WITH (...) into our options struct (as bytea).
unsafe extern "C" fn amoptions(reloptions: pg_sys::Datum, validate: bool) -> *mut pg_sys::bytea {
    let tbl = relopt_table();
    let r = pg_sys::build_reloptions(
        reloptions,
        validate,
        PGT_KIND,
        std::mem::size_of::<PgtantivyOpts>(),
        tbl.as_ptr(),
        tbl.len() as i32,
    ) as *mut pg_sys::bytea;
    r
}

// ── ambuild ──────────────────────────────────────────────────────────────

struct BuildState {
    n_docs: i64,
}

unsafe extern "C" fn ambuild(
    heap: pg_sys::Relation,
    index_rel: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    let relname = std::ffi::CStr::from_ptr((*(*index_rel).rd_rel).relname.data.as_ptr())
        .to_string_lossy()
        .into_owned();
    let (text_col, id_col, lang) = read_reloptions(index_rel);
    let text_col = text_col.unwrap_or_else(|| "body".into());
    let id_col = id_col.unwrap_or_else(|| "id".into());

    // Resolve column attnums by name on the heap tuple descriptor.
    let tupdesc = (*heap).rd_att;
    let natts = (*tupdesc).natts as i32;
    let mut text_attn = -1i32;
    let mut id_attn = -1i32;
    for i in 0..natts {
        let attr = (*tupdesc).attrs.as_slice(natts as usize)[i as usize];
        let name = std::ffi::CStr::from_ptr(attr.attname.data.as_ptr())
            .to_string_lossy()
            .into_owned();
        if name == text_col {
            text_attn = i + 1;
        }
        if name == id_col {
            id_attn = i + 1;
        }
    }
    if text_attn < 0 || id_attn < 0 {
        error!("pgtantivy: reloptions must name existing columns: text_column={}, id_column={}", text_col, id_col);
    }

    // Build the tantivy sidecar (same layout as pgtantivy_create).
    let mut sb = tantivy::schema::Schema::builder();
    let id_field = sb.add_i64_field("id", INDEXED | STORED | FAST);
    let ctid_field = sb.add_u64_field("ctid", INDEXED | FAST | STORED);
    let indexing = TextFieldIndexing::default()
        .set_tokenizer(JIEBA_TOKENIZER)
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let body_field = sb.add_text_field(
        "body",
        TextOptions::default().set_indexing_options(indexing),
    );
    let schema = sb.build();

    let dir_path = std::path::Path::new(&index_root()).join(&relname);
    let _ = std::fs::remove_dir_all(&dir_path);
    std::fs::create_dir_all(&dir_path).unwrap_or_else(|e| error!("pgtantivy: mkdir failed: {e}"));
    let dir = tantivy::directory::MmapDirectory::open(&dir_path)
        .unwrap_or_else(|e| error!("pgtantivy: open dir failed: {e}"));
    let tindex = tantivy::Index::open_or_create(dir, schema)
        .unwrap_or_else(|e| error!("pgtantivy: create index failed: {e}"));
    tindex
        .tokenizers()
        .register(JIEBA_TOKENIZER, analyzer(&lang));
    let _ = std::fs::write(
        dir_path.join("pgtantivy_meta.json"),
        serde_json::json!({"lang": lang}).to_string(),
    );

    let mut writer = tindex
        .writer(32 * 1024 * 1024)
        .unwrap_or_else(|e| error!("pgtantivy: writer failed: {e}"));

    // Full table scan via heap_getnext (HeapTuple path — avoids slot ops pitfalls).
    let mut state = BuildState { n_docs: 0 };
    let scan = pg_sys::table_beginscan_catalog(heap, 0, std::ptr::null_mut());
    loop {
        let tup = pg_sys::heap_getnext(scan, pg_sys::ScanDirection::ForwardScanDirection);
        if tup.is_null() {
            break;
        }
        if pg_sys::heap_attisnull(tup, id_attn, tupdesc) {
            continue; // NULL id: skip
        }
        let id_datum = pg_sys::nocachegetattr(tup, id_attn, tupdesc);
        if pg_sys::heap_attisnull(tup, text_attn, tupdesc) {
            continue; // NULL text: skip doc
        }
        let text_datum = pg_sys::nocachegetattr(tup, text_attn, tupdesc);
        let id = id_datum.value() as i64;
        let tid = (*tup).t_self;
        let ctid_val = ((tid.ip_blkid.bi_hi as u64) << 48)
            | ((tid.ip_blkid.bi_lo as u64) << 32)
            | (tid.ip_posid as u64);
        let cstr = pg_sys::text_to_cstring(text_datum.cast_mut_ptr::<pg_sys::text>());
        let text = std::ffi::CStr::from_ptr(cstr).to_string_lossy().into_owned();
        let text_nfc = nfc(&text);
        writer
            .add_document(tantivy::doc!(id_field => id, ctid_field => ctid_val, body_field => text_nfc.as_str()))
            .unwrap_or_else(|e| error!("pgtantivy: add_document failed: {e}"));
        state.n_docs += 1;
    }
    pg_sys::heap_endscan(scan);
    writer
        .commit()
        .unwrap_or_else(|e| error!("pgtantivy: commit failed: {e}"));
    let _ = index_info;

    // Register in the in-process registry under the index relname.
    let reader = tindex
        .reader()
        .unwrap_or_else(|e| error!("pgtantivy: reader failed: {e}"));
    ctid_fields().lock().map(|mut m| m.insert(relname.clone(), ctid_field)).ok();
    if let Ok(mut reg) = registry().lock() {
        reg.insert(
            relname.clone(),
            std::sync::Arc::new(PgIndex {
                index: tindex,
                reader,
                body: body_field,
                id: id_field,
                path: Some(dir_path.to_string_lossy().into_owned()),
            }),
        );
    }

    // Build result: PG wants an IndexBuildResult (heap tuples count etc).
    let result = pg_sys::palloc0(std::mem::size_of::<pg_sys::IndexBuildResult>()) as *mut pg_sys::IndexBuildResult;
    (*result).heap_tuples = state.n_docs as f64;
    (*result).index_tuples = state.n_docs as f64;
    result
}

/// MVP contract: the index is a snapshot taken at CREATE INDEX / REINDEX time.
/// Inserts are accepted (returning false = "not indexed here") so DML on the
/// table is never blocked; run REINDEX to refresh, or use the trigger-based
/// sync (pgtantivy_sync_trigger) for live updates on a sidecar name.
unsafe extern "C" fn aminsert(
    _index_rel: pg_sys::Relation,
    _values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    _heap_tuple: pg_sys::ItemPointer,
    _heap_rel: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck::Type,
    _index_unchanged: bool,
    _index_info: *mut pg_sys::IndexInfo,
) -> bool {
    false
}

unsafe extern "C" fn ambuildempty(_index_rel: pg_sys::Relation) {
    // CREATE INDEX on an empty table: create an empty sidecar dir.
    let relname = std::ffi::CStr::from_ptr((*(*_index_rel).rd_rel).relname.data.as_ptr())
        .to_string_lossy()
        .into_owned();
    let dir_path = std::path::Path::new(&index_root()).join(&relname);
    let _ = std::fs::remove_dir_all(&dir_path);
    let _ = std::fs::create_dir_all(&dir_path);
}

// ── vacuum (no-op; sidecar managed by create/merge lifecycle) ───────────

unsafe extern "C" fn ambulkdelete(
    _info: *mut pg_sys::IndexVacuumInfo,
    _stats: *mut pg_sys::IndexBulkDeleteResult,
    _callback: pg_sys::IndexBulkDeleteCallback,
    _callback_state: *mut std::ffi::c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    std::ptr::null_mut()
}

unsafe extern "C" fn amvacuumcleanup(
    _info: *mut pg_sys::IndexVacuumInfo,
    _stats: *mut pg_sys::IndexBulkDeleteResult,
) -> *mut pg_sys::IndexBulkDeleteResult {
    std::ptr::null_mut()
}
// ── scan callbacks (bitmap path) ─────────────────────────────────────────

/// Scan-private state: materialized (ctid, score) cursor.
struct AmScanState {
    hits: Vec<(u64, f32)>,
    pos: usize,
}

/// Extract the @@@ query string from the first scan key.
unsafe fn query_from_scankeys(scan: pg_sys::IndexScanDesc) -> Option<String> {
    let sd = &mut *scan;
    if sd.numberOfKeys < 1 || sd.keyData.is_null() {
        return None;
    }
    let key = &*sd.keyData;
    if key.sk_argument.value() == 0 {
        return None;
    }
    let cstr = pg_sys::text_to_cstring(key.sk_argument.cast_mut_ptr::<pg_sys::text>());
    if cstr.is_null() {
        return None;
    }
    let s = std::ffi::CStr::from_ptr(cstr).to_string_lossy().into_owned();
    pg_sys::pfree(cstr.cast());
    Some(s)
}

unsafe extern "C" fn ambeginscan(
    index_rel: pg_sys::Relation,
    nkeys: i32,
    norderbys: i32,
) -> pg_sys::IndexScanDesc {
    let r = pg_sys::RelationGetIndexScan(index_rel, nkeys, norderbys);
    r
}

unsafe extern "C" fn amrescan(
    scan: pg_sys::IndexScanDesc,
    keys: pg_sys::ScanKey,
    _nkeys: i32,
    _orderbys: pg_sys::ScanKey,
    _norderbys: i32,
) {
    let sd = &mut *scan;
    if !keys.is_null() {
        sd.keyData = keys;
    }
    let hits = match query_from_scankeys(scan) {
        Some(q) => {
            let relname = std::ffi::CStr::from_ptr(
                (*(*sd.indexRelation).rd_rel).relname.data.as_ptr(),
            )
            .to_string_lossy()
            .into_owned();
            let r = run_query(&relname, &q, 10_000);
            r.unwrap_or_default()
        }
        None => vec![],
    };
    sd.opaque = Box::into_raw(Box::new(AmScanState { hits, pos: 0 })) as *mut _;
    sd.xs_recheck = false;
}

unsafe extern "C" fn amendscan(scan: pg_sys::IndexScanDesc) {
    let sd = &mut *scan;
    if !sd.opaque.is_null() {
        drop(Box::from_raw(sd.opaque as *mut AmScanState));
        sd.opaque = std::ptr::null_mut();
    }
}

unsafe extern "C" fn amgetbitmap(scan: pg_sys::IndexScanDesc, tbm: *mut pg_sys::TIDBitmap) -> i64 {
    let sd = &mut *scan;
    if sd.opaque.is_null() {
        return 0;
    }
    let state = &mut *(sd.opaque as *mut AmScanState);
    let n = state.hits.len();
    if n == 0 {
        return 0;
    }
    let mut tids: Vec<pg_sys::ItemPointerData> = Vec::with_capacity(n);
    for (ctid, _score) in &state.hits {
        tids.push(pg_sys::ItemPointerData {
            ip_blkid: pg_sys::BlockIdData {
                bi_hi: ((ctid >> 48) & 0xffff) as u16,
                bi_lo: ((ctid >> 32) & 0xffff) as u16,
            },
            ip_posid: (ctid & 0xffff) as u16,
        });
    }
    pg_sys::tbm_add_tuples(tbm, tids.as_mut_ptr(), n as i32, false);
    n as i64
}

/// Run a sanitized BM25 query on the named sidecar, returning (ctid, score).
unsafe fn run_query(relname: &str, query: &str, limit: usize) -> Option<Vec<(u64, f32)>> {
    if registry().lock().ok()?.contains_key(relname) {
        // already loaded in this backend
    } else {
        // cross-session: sidecar on disk, load it (and the ctid field handle)
        let dir_path = std::path::Path::new(&index_root()).join(relname);
        let dir = tantivy::directory::MmapDirectory::open(&dir_path).ok()?;
        let index = tantivy::Index::open(dir).ok()?;
        let lang = std::fs::read_to_string(dir_path.join("pgtantivy_meta.json"))
            .ok()
            .and_then(|m| serde_json::from_str::<serde_json::Value>(&m).ok())
            .and_then(|v| v["lang"].as_str().map(std::string::ToString::to_string))
            .unwrap_or_else(|| "chinese".to_string());
        index.tokenizers().register(JIEBA_TOKENIZER, analyzer(&lang));
        let schema = index.schema();
        let body = schema.get_field("body").ok()?;
        let id = schema.get_field("id").ok()?;
        let ctid_f = schema.get_field("ctid").ok()?;
        let reader = index.reader().ok()?;
        registry().lock().ok()?.insert(
            relname.to_string(),
            std::sync::Arc::new(PgIndex { index: index.clone(), reader, body, id,
                path: Some(dir_path.to_string_lossy().into_owned()) }),
        );
        ctid_fields().lock().ok()?.insert(relname.to_string(), ctid_f);
    }
    let idx = lookup(relname).ok()?;
    let searcher = idx.reader.searcher();
    let ctid_field = *ctid_fields().lock().ok()?.get(relname)?;
    let mut parser = QueryParser::for_index(&idx.index, vec![idx.body]);
    parser.set_conjunction_by_default();
    let parsed = parser.parse_query(&sanitize(&nfc(query))).ok()?;
    let top = searcher.search(&parsed, &TopDocs::with_limit(limit)).ok()?;
    let mut out = Vec::with_capacity(top.len());
    for (score, addr) in top {
        let d: tantivy::TantivyDocument = searcher.doc(addr).ok()?;
        if let Some(c) = d.get_first(ctid_field).and_then(|v| v.as_u64()) {
            out.push((c, score));
        }
    }
    Some(out)
}
