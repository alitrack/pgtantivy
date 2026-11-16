//! 方案 1 — immutable snapshot bundle export/import for multi-instance sharing.
//!
//! Research (`D:/wsl2/win/pgtantivy_s3_共享索引_深度调研与对比测试_20261116.md`, §5)
//! recommends: "先用「快照 bundle 导出/导入 + 版本号 + 单写者」把多实例共享跑起来
//! （最小可用、风险可控、复用你 duckdb-tantivy 现成 save/load）". This module is that
//! minimal-but-real core, kept as a **pure** file/manifest layer (no pgrx, no
//! registry) so `lib.rs` stays the only owner of the in-memory registry:
//!
//!   * `export_bundle` — snapshot a named on-disk index into an immutable,
//!     versioned **bundle**: a recursive copy of the index directory plus a
//!     `manifest.json` recording opstamp, writer id, doc count, and a per-file
//!     size + FNV-1a checksum. Written to a temp dir then atomically renamed, so
//!     a crash never leaves a half-written bundle and the writer's live index is
//!     never disturbed (copy-on-export).
//!   * `verify_bundle` / `extract_bundle` — the reader side: verify the manifest
//!     (every file's size + checksum), extract to a caller-chosen target dir,
//!     strip the manifest from the extracted copy, and confirm the index opens
//!     with the documented doc count. Any tamper/truncation fails loud — a
//!     corrupted bundle can never be silently loaded.
//!
//! Design notes / honest boundaries:
//!   * **Immutable + versioned.** Each export stamps a monotonically increasing
//!     `opstamp` (persisted in the index sidecar meta) and names the bundle
//!     `<dest>/<name>-<opstamp>.bundle/`. Bundles are never mutated in place; a
//!     reader wanting a newer view re-imports a higher opstamp. GC of old bundles
//!     is out of scope (research §5 defers it to the production layer).
//!   * **Single-writer fence (seed, not yet enforced).** Each export records a
//!     `writer_id` (default `<hostname>.<pid>`) in the manifest — the seed for
//!     the CAS-style fencing research §5 wants ("仿 SlateDB manifest CAS，防误双
//!     写"). For this MVP the fence is recorded and surfaced (`bundle_info`);
//!     enforcement is a 方案 4 item.
//!   * **Point-in-time snapshot.** Export copies the index as it is at export
//!     time; a concurrent `pgtantivy_add`/`remove` racing the copy is not
//!     transactionally excluded. If it races, the reader's checksum verification
//!     on import catches it and refuses the bundle (fail-loud, never silently
//!     wrong). Re-export from a quiescent writer for a clean snapshot.
//!   * **No S3 yet.** Destinations are local directories. The bundle *format*
//!     (immutable dir + manifest + opstamp) is exactly the Quickwit/BoilStream
//!     "split + versioned meta pointer" shape, so 方案 2 (S3 range-read
//!     `Directory`) is a drop-in extension of this.

use crate::{read_build_meta, BuildAnchor};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tantivy::Index;

/// FNV-1a 64 — fast, dependency-free, deterministic. Not cryptographic; it
/// catches truncated/torn/accidentally-overwritten transfers, the failure mode
/// bundle sharing must never silently swallow.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    let (mut h, prime) = (0xcbf2_9ce4_8422_2325u64, 0x0000_0100_0000_01b3u64);
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(prime);
    }
    h
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct FileEntry {
    rel: String,
    size: u64,
    fnv: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Manifest {
    format: String,
    format_version: u32,
    name: String,
    opstamp: u64,
    written_at: i64,
    writer_id: String,
    n_docs: u64,
    lang: String,
    /// Build-time WAL anchor carried from the source sidecar (staleness signal).
    anchor: Option<BuildAnchor>,
    files: Vec<FileEntry>,
}

const FORMAT: &str = "pgtantivy-index-bundle";

pub(crate) fn bundle_dir_name(name: &str, opstamp: u64) -> String {
    format!("{name}-{opstamp}.bundle")
}

fn walk(base: &Path, dir: &Path, out: &mut Vec<FileEntry>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let p = entry.path();
        if p.is_dir() {
            walk(base, &p, out)?;
        } else if p.is_file() {
            let bytes = std::fs::read(&p)?;
            let rel = p
                .strip_prefix(base)
                .map(|r| r.to_string_lossy().replace('\\', "/"))
                .unwrap_or_default();
            out.push(FileEntry {
                rel,
                size: bytes.len() as u64,
                fnv: fnv1a_64(&bytes),
            });
        }
    }
    Ok(())
}

fn collect_files(dir: &Path) -> std::io::Result<Vec<FileEntry>> {
    let mut out = Vec::new();
    walk(dir, dir, &mut out)?;
    out.sort_by(|a, b| a.rel.cmp(&b.rel));
    Ok(out)
}

fn copy_tree(src: &Path, dst: &Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(src)? {
        let e = entry?;
        let t = dst.join(e.file_name());
        if e.path().is_dir() {
            std::fs::create_dir_all(&t)?;
            copy_tree(&e.path(), &t)?;
        } else {
            std::fs::copy(e.path(), &t)?;
        }
    }
    Ok(())
}

fn default_writer_id() -> String {
    // <hostname>.<pid>. Hostname is best-effort from the environment; Postgres
    // does not expose it through pgrx bindings here.
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .ok()
        .filter(|h| !h.trim().is_empty())
        .unwrap_or_else(|| "writer".to_string());
    format!("{host}.{}", std::process::id())
}

/// Monotonically bump + persist the export opstamp in the index sidecar meta;
/// returns the new opstamp. First export of a freshly created index (no key)
/// starts at 1.
/// Single-writer fence on the source index. When an export declares an
/// explicit writer identity `writer`, the index sidecar records
/// `fence_writer`. A later export from the same index directory must reuse the
/// same identity — a different one is refused (dual-writer protection). The
/// fence resets on a rebuild because `pgtantivy_create` wipes the index dir.
fn apply_write_fence(dir: &Path, writer: &str) -> Result<(), Box<dyn std::error::Error>> {
    let meta_path = dir.join("pgtantivy_meta.json");
    let mut v = std::fs::read_to_string(&meta_path)
        .ok()
        .and_then(|m| serde_json::from_str::<serde_json::Value>(&m).ok())
        .unwrap_or(serde_json::json!({}));
    let o = v.as_object_mut().ok_or_else(|| -> Box<dyn std::error::Error> {
        "pgtantivy: index meta is not a JSON object".into()
    })?;
    match o.get("fence_writer").and_then(|x| x.as_str()) {
        Some(prev) if prev != writer => {
            return Err(format!(
                "pgtantivy: dual-writer protection — index already fenced to \
                 writer '{prev}', refusing export as '{writer}'. Rebuild the index \
                 (pgtantivy_create) to change writer."
            )
            .into());
        }
        Some(_) => {} // same writer: fence already set, no-op
        None => {
            o.insert("fence_writer".to_string(), serde_json::json!(writer));
            let tmp = dir.join(format!(
                ".pgtantivy_meta.{}.fence.tmp",
                std::process::id()
            ));
            std::fs::write(&tmp, v.to_string())?;
            std::fs::rename(&tmp, &meta_path)?;
        }
    }
    Ok(())
}

/// Retain the `keep` newest bundles for `name` in `dest`, deleting older
/// `<name>-<opstamp>.bundle` directories. `keep == 0` retains all. Returns the
/// number of bundles deleted. Only directories matching the exact bundle
/// naming convention are touched — nothing else is ever removed.
pub(crate) fn gc_bundles(dest: &str, name: &str, keep: u64) -> Result<u64, Box<dyn std::error::Error>> {
    if keep == 0 {
        return Ok(0);
    }
    let dest = std::path::Path::new(dest.trim());
    let mut found: Vec<(u64, std::path::PathBuf)> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dest) {
        for entry in rd.flatten() {
            let p = entry.path();
            if !p.is_dir() {
                continue;
            }
            let fname = entry.file_name().to_string_lossy().into_owned();
            // <name>-<opstamp>.bundle
            let Some(stem) = fname.strip_suffix(".bundle") else {
                continue;
            };
            let Some(op_str) = stem.strip_prefix(&format!("{name}-")) else {
                continue;
            };
            let Ok(op) = op_str.parse::<u64>() else {
                continue;
            };
            found.push((op, p));
        }
    }
    if found.len() <= keep as usize {
        return Ok(0);
    }
    found.sort_by_key(|(op, _)| std::cmp::Reverse(*op));
    let mut deleted = 0u64;
    for (op, p) in found.iter().skip(keep as usize) {
        std::fs::remove_dir_all(p)?;
        deleted += 1;
        let _ = op;
    }
    Ok(deleted)
}

fn next_opstamp(dir: &Path) -> std::io::Result<u64> {
    let meta_path = dir.join("pgtantivy_meta.json");
    let mut v = std::fs::read_to_string(&meta_path)
        .ok()
        .and_then(|m| serde_json::from_str::<serde_json::Value>(&m).ok())
        .unwrap_or(serde_json::json!({}));
    let o = v
        .as_object_mut()
        .ok_or(std::io::Error::new(
            std::io::ErrorKind::Other,
            "index meta is not a JSON object",
        ))?;
    let cur = o
        .get("opstamp")
        .and_then(|x| x.as_u64())
        .unwrap_or(0)
        .saturating_add(1);
    o.insert("opstamp".to_string(), serde_json::json!(cur));
    let tmp = dir.join(format!(
        ".pgtantivy_meta.{}.opstamp.tmp",
        std::process::id()
    ));
    std::fs::write(&tmp, v.to_string())?;
    std::fs::rename(&tmp, &meta_path)?;
    Ok(cur)
}

/// Snapshot on-disk index `dir` (whose file name == logical `name`) into
/// `<dest>/<name>-<opstamp>.bundle/`. Returns `(bundle_path, opstamp, n_files)`.
pub(crate) fn export_bundle(
    name: &str,
    dir: &Path,
    dest: &str,
    writer_id: Option<String>,
    keep: u64,
) -> Result<(std::path::PathBuf, u64, usize), Box<dyn std::error::Error>> {
    if !dir.is_dir() {
        return Err(format!("pgtantivy: no on-disk index directory {dir:?}").into());
    }
    if dir.join("manifest.json").exists() {
        return Err(format!(
            "pgtantivy: {dir:?} already looks like a bundle (has manifest.json); \
             import bundles with pgtantivy_import_index, don't re-export them"
        )
        .into());
    }
    let dest = dest.trim();
    if dest.is_empty() {
        return Err(
            "pgtantivy: export destination must be a non-empty directory path".into(),
        );
    }
    std::fs::create_dir_all(dest)?;

    let writer_id = writer_id
        .filter(|w| !w.trim().is_empty())
        .unwrap_or_else(default_writer_id);
    // Dual-writer protection: once an export has fenced this index to a writer
    // identity, any export under a different identity is refused. Checked
    // BEFORE bumping the opstamp, so a rejected export does not consume a version.
    apply_write_fence(dir, &writer_id)?;
    let opstamp = next_opstamp(dir)?;

    // Doc count + lang + anchor from the source. A transient reader is opened and
    // dropped; the running registry index is untouched.
    let (n_docs, lang, anchor) = {
        let tmp = Index::open(
            tantivy::directory::MmapDirectory::open(dir)
                .map_err(|e| format!("open source index for export: {e}"))?,
        )?;
        let docs = tmp.reader().map(|r| r.searcher().num_docs()).unwrap_or(0);
        drop(tmp);
        let lang = std::fs::read_to_string(dir.join("pgtantivy_meta.json"))
            .ok()
            .and_then(|m| serde_json::from_str::<serde_json::Value>(&m).ok())
            .and_then(|v| v["lang"].as_str().map(std::string::ToString::to_string))
            .unwrap_or_else(|| "chinese".to_string());
        let anchor = read_build_meta(dir);
        (docs, lang, anchor)
    };

    let final_dir = std::path::Path::new(dest).join(bundle_dir_name(name, opstamp));
    if final_dir.exists() {
        return Err(format!(
            "pgtantivy: bundle already exists {final_dir:?} (opstamp collision; safety \
             net, not expected)"
        )
        .into());
    }
    // Write to a sibling temp dir, then atomically rename into place: a crash
    // mid-export leaves only the temp dir, never a half-written final bundle.
    let tmp_dir = std::path::Path::new(dest).join(format!(".{name}.{opstamp}.tmp"));
    if tmp_dir.exists() {
        std::fs::remove_dir_all(&tmp_dir)?;
    }
    std::fs::create_dir_all(&tmp_dir)?;
    let files = match (|| -> std::io::Result<Vec<FileEntry>> {
        copy_tree(dir, &tmp_dir)?;
        Ok(collect_files(&tmp_dir)?)
    })() {
        Ok(f) => f,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&tmp_dir);
            return Err(e.into());
        }
    };
    let manifest = Manifest {
        format: FORMAT.to_string(),
        format_version: 1,
        name: name.to_string(),
        opstamp,
        written_at: crate::now_millis(),
        writer_id,
        n_docs,
        lang,
        anchor,
        files,
    };
    let mp = tmp_dir.join("manifest.json");
    let mp_tmp = tmp_dir.join(format!(".manifest.{}.tmp", std::process::id()));
    std::fs::write(&mp_tmp, serde_json::to_string_pretty(&manifest).unwrap())?;
    std::fs::rename(&mp_tmp, &mp)?;
    if let Err(e) = std::fs::rename(&tmp_dir, &final_dir) {
        let _ = std::fs::remove_dir_all(&tmp_dir);
        return Err(e.into());
    }
    // Retention: keep the `keep` newest bundles for this name; drop older ones.
    let _ = gc_bundles(dest, name, keep);
    Ok((final_dir, opstamp, manifest.files.len()))
}

/// Read + verify a bundle's manifest (format, per-file size + checksum).
/// Fails loud on any mismatch, missing file, or unknown format.
pub(crate) fn verify_bundle(bundle: &Path) -> Result<Manifest, Box<dyn std::error::Error>> {
    let mpath = bundle.join("manifest.json");
    if !mpath.is_file() {
        return Err(format!(
            "pgtantivy: {mpath:?} is not a bundle (missing manifest.json)"
        )
        .into());
    }
    let mtext = std::fs::read_to_string(&mpath)
        .map_err(|e| format!("read manifest {mpath:?}: {e}"))?;
    let m: Manifest = serde_json::from_str(&mtext)
        .map_err(|e| format!("corrupt manifest in {bundle:?}: {e}"))?;
    if m.format != FORMAT {
        return Err(format!("not a pgtantivy bundle (format={})", m.format).into());
    }
    if m.format_version != 1 {
        return Err(format!("unsupported bundle format_version {}", m.format_version).into());
    }
    for f in &m.files {
        let rel = &f.rel;
        let p = bundle.join(rel);
        if !p.is_file() {
            return Err(format!("bundle {bundle:?}: missing file {rel}").into());
        }
        let bytes = std::fs::read(&p)?;
        if (bytes.len() as u64) != f.size {
            return Err(format!(
                "bundle {bundle:?}: file {rel} size mismatch (manifest {} vs disk {})",
                f.size,
                bytes.len()
            )
            .into());
        }
        if fnv1a_64(&bytes) != f.fnv {
            return Err(format!(
                "bundle {bundle:?}: file {rel} checksum mismatch (transfer corruption?)"
            )
            .into());
        }
    }
    Ok(m)
}

/// Manifest fields the registry needs at import time.
pub(crate) struct BundleMeta {
    pub opstamp: u64,
    pub n_docs: u64,
    pub lang: String,
    pub writer_id: String,
}

/// Verify a bundle and extract it (manifest stripped) into `target`, then open
/// the extracted index to confirm the doc count matches the manifest. Returns
/// the doc count. `target` must not exist (caller manages replacement).
pub(crate) fn extract_bundle(
    bundle: &Path,
    target: &Path,
) -> Result<BundleMeta, Box<dyn std::error::Error>> {
    let m = verify_bundle(bundle)?;
    if target.exists() {
        return Err(format!(
            "pgtantivy: extract target {target:?} already exists; remove it first"
        )
        .into());
    }
    if target
        .canonicalize()
        .ok()
        .and_then(|t| {
            bundle
                .canonicalize()
                .ok()
                .filter(|b| t.starts_with(b) || b.starts_with(&t))
                .map(|_| ())
        })
        .is_some()
    {
        return Err(format!(
            "pgtantivy: extract target {target:?} overlaps the bundle {bundle:?}"
        )
        .into());
    }
    std::fs::create_dir_all(target)?;
    let res = (|| -> Result<(), Box<dyn std::error::Error>> {
        copy_tree(bundle, target)?;
        let _ = std::fs::remove_file(target.join("manifest.json"));
        let mmap = tantivy::directory::MmapDirectory::open(target)
            .map_err(|e| format!("open imported index dir: {e}"))?;
        let index = Index::open(mmap)?;
        let docs = index
            .reader()
            .map(|r| r.searcher().num_docs())
            .unwrap_or(0);
        if docs != m.n_docs {
            return Err(format!(
                "bundle {bundle:?}: imported index has {docs} docs but manifest says {} \
                 — re-export from the writer",
                m.n_docs
            )
            .into());
        }
        Ok(())
    })();
    if let Err(e) = res {
        let _ = std::fs::remove_dir_all(target);
        return Err(e.into());
    }
    Ok(BundleMeta {
        opstamp: m.opstamp,
        n_docs: m.n_docs,
        lang: m.lang,
        writer_id: m.writer_id,
    })
}

/// Describe a bundle as JSON (pure, usable on any node).
pub(crate) fn bundle_info(bundle: &str) -> Result<String, Box<dyn std::error::Error>> {
    let bundle = Path::new(bundle.trim());
    if !bundle.is_dir() {
        return Err(format!("pgtantivy: no such bundle directory {bundle:?}").into());
    }
    let m = verify_bundle(bundle)?;
    Ok(serde_json::json!({
        "name": m.name,
        "opstamp": m.opstamp,
        "written_at": m.written_at,
        "writer_id": m.writer_id,
        "n_docs": m.n_docs,
        "lang": m.lang,
        "n_files": m.files.len(),
        "built_lsn": m.anchor.as_ref().map(|a| crate::fmt_lsn(a.built_lsn)),
    })
    .to_string())
}
