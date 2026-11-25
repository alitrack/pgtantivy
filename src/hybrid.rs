//! Hybrid retrieval: pgtantivy BM25 + pgvector ANN, fused with Reciprocal
//! Rank Fusion — the same proven recipe as duckdb-tantivy's
//! `tantivy_rrf_hybrid` (recall 1.000 / MRR 1.000 on the 90k benchmark).
//!
//! The vector side lives in pgvector (no second index AM, no WAL surface):
//! this function SPI-queries `ORDER BY <vec_col> <=> <query_vec> LIMIT n`
//! over a user table, then fuses doc ids by rank.
//!
//! IMPORTANT: id alignment — the FTS sidecar's id column and the vector
//! table's id column must come from the same source (same ids, same type).

use crate::{lookup, nfc, sanitize};
use pgrx::prelude::*;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::Value;

/// RRF fusion of pgtantivy BM25 and a pgvector nearest-neighbour query.
///
/// * `fts_name`   — pgtantivy index name (as created/loaded)
/// * `fts_query`  — BM25 query string (same syntax as pgtantivy_search)
/// * `vec_table`  — table holding pgvector embeddings, e.g. 'docs_vec'
/// * `vec_col`    — vector column (type vector), e.g. 'embedding'
/// * `id_col`     — id column in `vec_table` matching the FTS id space
/// * `query_vec`  — query embedding as JSON text, e.g. '[0.1, 0.2, ...]'
/// * `metric`     — 'l2' (default) or 'cosine' or 'ip'
/// * `vec_fetch`  — how many candidates to pull from the vector side
/// * `top_k`      — final fused result size
/// * `rrf_k`      — RRF smoothing constant (classic 60)
///
/// Returns (id, rrf_score, fts_rank, vec_rank, fts_score) — NULL rank/score
/// when that side did not retrieve the doc.
#[pg_extern]
fn pgtantivy_hybrid(
    fts_name: String,
    fts_query: String,
    vec_table: String,
    vec_col: String,
    id_col: String,
    query_vec: String,
    metric: default!(String, "'l2'"),
    vec_fetch: default!(i64, 50),
    top_k: default!(i64, 10),
    rrf_k: default!(i64, 60),
    conjunctive: default!(bool, "false"),
) -> Result<
    TableIterator<
        'static,
        (
            name!(id, i64),
            name!(rrf_score, f64),
            name!(fts_rank, Option<i64>),
            name!(vec_rank, Option<i64>),
            name!(fts_score, Option<f64>),
        ),
    >,
    Box<dyn std::error::Error>,
> {
    if rrf_k <= 0 {
        return Err("pgtantivy: rrf_k must be > 0".into());
    }
    let k = rrf_k as f64;
    let vf = vec_fetch.max(1) as usize;
    let tk = top_k.max(1) as usize;

    // ── FTS side: BM25 over the named sidecar ────────────────────────────
    // OR (disjunction) by default, matching pgtantivy_search; `conjunctive`
    // ANDs all terms for precision-oriented filters.
    let idx = lookup(&fts_name)?;
    let searcher = idx.reader.searcher();
    let mut parser = QueryParser::for_index(&idx.index, vec![idx.body]);
    if conjunctive {
        parser.set_conjunction_by_default();
    }
    let parsed = parser
        .parse_query(&sanitize(&nfc(&fts_query)))
        .map_err(|e| format!("pgtantivy: bad query: {e}"))?;
    let top = searcher.search(&parsed, &TopDocs::with_limit(vf))?;
    let mut fts: Vec<(i64, f64)> = top
        .into_iter()
        .filter_map(|(s, addr)| {
            let d: tantivy::TantivyDocument = searcher.doc(addr).ok()?;
            Some((d.get_first(idx.id)?.as_i64()?, s as f64))
        })
        .collect();
    fts.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap().then(a.0.cmp(&b.0)));

    // ── Vector side: pgvector ANN over SPI ───────────────────────────────
    let op = match metric.to_ascii_lowercase().as_str() {
        "l2" => "<->",
        "cosine" | "cos" => "<=>",
        "ip" | "inner" | "dot" => "<#>",
        other => return Err(format!("pgtantivy: unknown metric '{other}' (l2|cosine|ip)").into()),
    };
    // Validate the vector literal minimally (must look like '[a,b,..]').
    let v = query_vec.trim();
    if !v.starts_with('[') || !v.ends_with(']') || v.len() < 3 {
        return Err("pgtantivy: query_vec must be a JSON array literal like '[0.1,0.2]'".into());
    }
    let sql = format!(
        "SELECT {id}::bigint AS id FROM {tbl} WHERE {vc} IS NOT NULL \
         ORDER BY {vc} {op} '{vec}'::vector LIMIT {n}",
        id = pgrx::spi::quote_identifier(&id_col),
        tbl = pgrx::spi::quote_identifier(&vec_table),
        vc = pgrx::spi::quote_identifier(&vec_col),
        op = op,
        vec = v.replace('\'', "''"),
        n = vf,
    );
    let vec_res: Result<Vec<i64>, String> = Spi::connect(|client| match client.select(&sql, None, &[]) {
        Ok(table) => {
            let mut out: Vec<i64> = Vec::new();
            for row in table {
                if let Ok(Some(id)) = row.get_by_name::<i64, _>("id") {
                    out.push(id);
                }
            }
            Ok(out)
        }
        Err(e) => Err(format!("pgtantivy: vector query failed: {e}")),
    });
    let vec_hits = vec_res?;

    // ── Fuse: RRF over both rank lists ───────────────────────────────────
    let mut score: std::collections::HashMap<i64, f64> = std::collections::HashMap::new();
    let mut fr: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
    let mut fs: std::collections::HashMap<i64, f64> = std::collections::HashMap::new();
    let mut vr: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
    for (rank, (id, s)) in fts.iter().enumerate() {
        score.entry(*id).or_insert(0.0);
        *score.get_mut(id).unwrap() += 1.0 / (k + rank as f64 + 1.0);
        fr.insert(*id, rank as i64 + 1);
        fs.insert(*id, *s);
    }
    for (rank, id) in vec_hits.iter().enumerate() {
        score.entry(*id).or_insert(0.0);
        *score.get_mut(id).unwrap() += 1.0 / (k + rank as f64 + 1.0);
        vr.insert(*id, rank as i64 + 1);
    }
    let mut fused: Vec<(i64, f64, Option<i64>, Option<i64>, Option<f64>)> = score
        .into_iter()
        .map(|(id, s)| (id, s, fr.get(&id).copied(), vr.get(&id).copied(), fs.get(&id).copied()))
        .collect();
    fused.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap()
            .then(a.0.cmp(&b.0))
    });
    fused.truncate(tk);
    Ok(TableIterator::new(fused))
}
