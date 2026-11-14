// pgtantivy — Chinese-first BM25 full-text search for PostgreSQL via tantivy.
// Seam B prototype: tantivy BM25 + jieba tokenization in a pgrx extension.
// MIT. No ParadeDB code — clean-room, architecture-inspired only.
use pgrx::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{
    IndexRecordOption, Schema, TextFieldIndexing, TextOptions, Value, FAST, STORED,
};
use tantivy::snippet::SnippetGenerator;
use tantivy::tokenizer::{
    LowerCaser, SimpleTokenizer, TextAnalyzer, Token, TokenStream, Tokenizer,
};
use tantivy::{doc, Index, IndexReader};
use unicode_normalization::UnicodeNormalization;
pg_module_magic!();



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
    let id_field = sb.add_i64_field("id", STORED | FAST);
    let indexing = TextFieldIndexing::default()
        .set_tokenizer(JIEBA_TOKENIZER)
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let body_field = sb.add_text_field(
        "body",
        TextOptions::default().set_indexing_options(indexing),
    );
    let schema = sb.build();
    let index = Index::create_in_ram(schema);
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
    let reader = index.reader()?;
    registry().lock()?.insert(
        name.clone(),
        Arc::new(PgIndex {
            index,
            reader,
            body: body_field,
            id: id_field,
        }),
    );
    Ok(format!("pgtantivy: index '{}' created", name))
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

#[pg_guard]
extern "C" fn _PG_init() {}
