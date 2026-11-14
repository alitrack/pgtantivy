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

use crate::{index_root, nfc, registry, PgIndex, JIEBA_TOKENIZER, analyzer};
use pgrx::prelude::*;
use tantivy::schema::{IndexRecordOption, TextFieldIndexing, TextOptions, INDEXED, STORED, FAST};

// ── handler ──────────────────────────────────────────────────────────────

#[pg_extern(sql = "
CREATE FUNCTION pgtantivy_am_handler(internal) RETURNS index_am_handler
PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c
AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
CREATE ACCESS METHOD pgtantivy TYPE INDEX HANDLER pgtantivy_am_handler;

CREATE OPERATOR FAMILY pgtantivy_ops USING pgtantivy;
CREATE OPERATOR CLASS pgtantivy_ops DEFAULT FOR TYPE text USING pgtantivy FAMILY pgtantivy_ops AS
  OPERATOR 1 ~~ (text, text),
  OPERATOR 2 ~~* (text, text);
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
        let cstr = pg_sys::text_to_cstring(text_datum.cast_mut_ptr::<pg_sys::text>());
        let text = std::ffi::CStr::from_ptr(cstr).to_string_lossy().into_owned();
        let text_nfc = nfc(&text);
        writer
            .add_document(tantivy::doc!(id_field => id, body_field => text_nfc.as_str()))
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

// ── scan (minimal: not wired to planner in MVP) ─────────────────────────

unsafe extern "C" fn ambeginscan(
    _index_rel: pg_sys::Relation,
    _nkeys: i32,
    _norderbys: i32,
) -> pg_sys::IndexScanDesc {
    error!("pgtantivy: index scans are not yet supported; query via pgtantivy_search (planner integration is next)"
    );
}

unsafe extern "C" fn amrescan(
    _scan: pg_sys::IndexScanDesc,
    _keys: *mut pg_sys::ScanKeyData,
    _nkeys: i32,
    _orderbys: *mut pg_sys::ScanKeyData,
    _norderbys: i32,
) {
}

unsafe extern "C" fn amendscan(_scan: pg_sys::IndexScanDesc) {}

unsafe extern "C" fn amgetbitmap(
    _scan: pg_sys::IndexScanDesc,
    _tbm: *mut pg_sys::TIDBitmap,
) -> i64 {
    0
}
