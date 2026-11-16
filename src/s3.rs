//! S3 / object-store transport for snapshot bundles (research #2026111601
//! "共享索引" approach 1 — the cross-node transfer leg).
//!
//! A local bundle (see `snapshot.rs`) is a set of files + a `manifest.json`.
//! Here we move it to / from S3 one object at a time, publishing with the
//! Quickwit-style *manifest-last* convention: every file except
//! `manifest.json` is uploaded first, the manifest is uploaded LAST. The
//! presence of a `manifest.json` for a bundle is therefore its "published"
//! marker — a reader that lists versions never sees a half-written bundle (a
//! crashed upload leaves data objects with no manifest, so it is invisible and
//! can be re-uploaded / GC'd).
//!
//! S3 layout under `<prefix>`:
//!   <prefix>/<name>-<opstamp>.bundle/manifest.json
//!   <prefix>/<name>-<opstamp>.bundle/<each file, rel path>
//!
//! Async is bridged to the synchronous PG backend with a lazily-created
//! single-thread tokio runtime (`block_on`), one worker, no thread-pool spawn
//! from inside a postgres backend.
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use futures::StreamExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as OPath;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload};

use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    Directory, DirectoryLock, FileHandle, Lock, OwnedBytes, WatchCallback, WatchHandle, WritePtr,
};
use tantivy::{HasLen, Index, TantivyError};

/// Lazily constructed single-thread runtime (first S3 call builds it).
static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn rt() -> &'static tokio::runtime::Runtime {
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build single-thread tokio runtime")
    })
}

fn build_store(
    endpoint: &str,
    bucket: &str,
    region: &str,
    access_key: &str,
    secret: &str,
) -> Result<AmazonS3, Box<dyn std::error::Error>> {
    let mut b = AmazonS3Builder::new()
        .with_bucket_name(bucket.to_string())
        .with_endpoint(endpoint.trim())
        .with_access_key_id(access_key.to_string())
        .with_secret_access_key(secret.to_string());
    if !region.trim().is_empty() {
        b = b.with_region(region.to_string());
    }
    // MinIO / local endpoints are plain http, not https.
    if endpoint.trim().to_ascii_lowercase().starts_with("http://") {
        b = b.with_allow_http(true);
    }
    Ok(b.build()?)
}

fn opath(s: &str) -> OPath {
    OPath::from(s)
}

/// Upload a local bundle directory to S3 (manifest-last) and apply the S3-side
/// retention policy. Returns the bundle's opstamp (parsed from the dir name
/// `<name>-<opstamp>.bundle`).
pub(crate) fn upload_bundle(
    endpoint: &str,
    bucket: &str,
    region: &str,
    access_key: &str,
    secret: &str,
    prefix: &str,
    local_bundle: &Path,
    name: &str,
    keep: u64,
) -> Result<u64, Box<dyn std::error::Error>> {
    let s3 = build_store(endpoint, bucket, region, access_key, secret)?;
    let bundle_base = local_bundle
        .file_name()
        .ok_or("bundle has no file name")?
        .to_string_lossy()
        .into_owned();
    let stem = bundle_base
        .strip_suffix(".bundle")
        .ok_or("not a .bundle dir")?;
    let op: u64 = stem
        .rsplit('-')
        .next()
        .ok_or("no opstamp in bundle name")?
        .parse()?;

    let s3_prefix = format!("{}/{}/", prefix.trim_end_matches('/'), bundle_base);
    let mut files: Vec<(String, PathBuf)> = Vec::new();
    walk(local_bundle, local_bundle, &mut files)?;
    let op_final = op;
    let local_bundle = local_bundle.to_path_buf();
    let prefix = prefix.to_string();
    let name = name.to_string();

    rt().block_on(async move {
        for (rel, p) in &files {
            if rel == "manifest.json" {
                continue; // manifest last
            }
            let bytes: Vec<u8> = std::fs::read(p)?;
            let loc = opath(&format!("{}{}", s3_prefix, rel));
            s3.put(&loc, PutPayload::from_bytes(bytes.into())).await?;
        }
        // publish marker
        let mbytes: Vec<u8> = std::fs::read(local_bundle.join("manifest.json"))?;
        let mloc = opath(&format!("{s3_prefix}manifest.json"));
        s3.put(&mloc, PutPayload::from_bytes(mbytes.into())).await?;
        if keep > 0 {
            gc_s3(&s3, &format!("{}/{}", prefix.trim_end_matches('/'), name), keep).await?;
        }
        Ok::<u64, Box<dyn std::error::Error>>(op_final)
    })
}

/// Download + verify + extract an S3 bundle into `target` (an index dir).
/// Reuses `snapshot::extract_bundle`, so the same FNV integrity gate + doc-count
/// check apply. Returns the extracted `BundleMeta`.
pub(crate) fn download_bundle(
    endpoint: &str,
    bucket: &str,
    region: &str,
    access_key: &str,
    secret: &str,
    prefix: &str,
    bundle_name: &str, // "<name>-<opstamp>.bundle"
    target: &Path,
) -> Result<crate::snapshot::BundleMeta, Box<dyn std::error::Error>> {
    let s3 = build_store(endpoint, bucket, region, access_key, secret)?;
    let s3_prefix = format!("{}/{}/", prefix.trim_end_matches('/'), bundle_name);
    let mloc = opath(&format!("{s3_prefix}manifest.json"));

    // 1) manifest (published marker) + 2) all listed files, one runtime block
    // (so `s3` is moved in once and stays usable for the whole download).
    let tmp = std::env::temp_dir().join(format!(
        "pgt_s3dl_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&tmp)?;
    let tmp2 = tmp.clone();
    let (manifest_bytes, m): (Bytes, crate::snapshot::Manifest) =
        rt().block_on(async move {
            let g = s3.get(&mloc).await?;
            let mb = g.bytes().await?;
            let m: crate::snapshot::Manifest = serde_json::from_slice(&mb)?;
            for f in &m.files {
                if f.rel == "manifest.json" {
                    continue;
                }
                let loc = opath(&format!("{}{}", s3_prefix, f.rel));
                let g = s3.get(&loc).await?;
                let b = g.bytes().await?;
                let dest = safe_join(&tmp2, &f.rel)?;
                if let Some(parent) = dest.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&dest, b)?;
            }
            Ok::<(Bytes, crate::snapshot::Manifest), Box<dyn std::error::Error>>((mb, m))
        })?;
    std::fs::write(tmp.join("manifest.json"), &manifest_bytes)?;
    let _ = m;

    // 3) reuse the local verify + extract (FNV gate + doc count + cleanup)
    let meta = crate::snapshot::extract_bundle(&tmp, target)?;
    let _ = std::fs::remove_dir_all(&tmp);
    Ok(meta)
}

/// List published bundle versions for `name` under `prefix` (those with a
/// manifest). Returns `(opstamp, bundle_dir_name, n_docs)` sorted desc.
pub(crate) fn list_versions(
    endpoint: &str,
    bucket: &str,
    region: &str,
    access_key: &str,
    secret: &str,
    prefix: &str,
    name: &str,
) -> Result<Vec<(u64, String, u64)>, Box<dyn std::error::Error>> {
    let s3 = build_store(endpoint, bucket, region, access_key, secret)?;
    // List the TOP prefix (object_store normalises a multi-segment Path into a
    // trailing-slash "directory", which would NOT match "<name>-<op>.bundle");
    // then filter client-side to exactly "<prefix>/<name>-<op>.bundle/manifest.json".
    let top = prefix.trim_end_matches('/').to_string();
    let want = format!("{top}/{name}-");
    rt().block_on(async move {
        let stream = s3.list(Some(&opath(&top)));
        let mut manifests: Vec<ObjectMeta> = Vec::new();
        let mut s = stream;
        while let Some(item) = s.next().await {
            let om = item?;
            let loc = om.location.to_string();
            if loc.starts_with(&want) && loc.ends_with("manifest.json") {
                manifests.push(om);
            }
        }
        let mut res: Vec<(u64, String, u64)> = Vec::new();
        for om in manifests {
            let loc = om.location.to_string();
            let bundle_part = loc
                .rsplit('/')
                .nth(1)
                .ok_or_else(|| format!("odd manifest location {loc}"))?;
            // "snap-12.bundle" -> strip ".bundle" -> "snap-12" -> "12"
            let bare = bundle_part.strip_suffix(".bundle").unwrap_or(bundle_part);
            let op = bare
                .rsplit('-')
                .next()
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or_else(|| format!("no opstamp in {bundle_part}"))?;
            let g = s3.get(&om.location).await?;
            let b = g.bytes().await?;
            let mf: crate::snapshot::Manifest = serde_json::from_slice(&b)?;
            res.push((op, bundle_part.to_string(), mf.n_docs));
        }
        res.sort_by(|a, b| b.0.cmp(&a.0));
        Ok::<Vec<(u64, String, u64)>, Box<dyn std::error::Error>>(res)
    })
}

/// S3-side retention: keep the N newest PUBLISHED bundles for `name` (those
/// with a manifest), delete every object belonging to older bundles. Returns
/// the number of objects deleted.
async fn gc_s3(
    s3: &AmazonS3,
    name_prefix: &str, // "<prefix>/<name>"
    keep: u64,
) -> Result<u64, Box<dyn std::error::Error>> {
    // List the top prefix (a multi-segment Path prefix would be normalised to a
    // trailing-slash directory and miss "<name>-<op>.bundle"), then filter.
    let (top, name) = name_prefix
        .rsplit_once('/')
        .ok_or("gc prefix must be <prefix>/<name>")?;
    let top = top.trim_end_matches('/');
    let want = format!("{top}/{name}-");
    let stream = s3.list(Some(&opath(top)));
    let mut objs: Vec<ObjectMeta> = Vec::new();
    let mut s = stream;
    while let Some(item) = s.next().await {
        let om = item?;
        if om.location.to_string().starts_with(&want) {
            objs.push(om);
        }
    }
    // published = bundles that have a manifest; parse op as "<name>-<op>.bundle"
    let mut published: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
    for om in &objs {
        let loc = om.location.to_string();
        if loc.ends_with("manifest.json") {
            if let Some(op) = parse_op(&loc) {
                published.insert(op);
            }
        }
    }
    let ops: Vec<u64> = published.into_iter().collect();
    if ops.len() <= keep as usize {
        return Ok(0);
    }
    let keep_set: std::collections::BTreeSet<u64> =
        ops.iter().rev().take(keep as usize).copied().collect();
    let mut del = 0u64;
    for om in objs {
        let loc = om.location.to_string();
        if let Some(op) = parse_op(&loc) {
            if !keep_set.contains(&op) {
                s3.delete(&om.location).await?;
                del += 1;
            }
        }
    }
    Ok(del)
}

/// Parse the opstamp out of "<prefix>/<name>-<op>.bundle/...".
fn parse_op(loc: &str) -> Option<u64> {
    let bundle_part = loc.rsplit('/').nth(1)?;
    let bare = bundle_part.strip_suffix(".bundle")?;
    let op = bare.rsplit('-').next()?.parse::<u64>().ok()?;
    Some(op)
}

fn walk(base: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) -> std::io::Result<()> {
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        let p = e.path();
        if p.is_dir() {
            walk(base, &p, out)?;
        } else if p.is_file() {
            let rel = match p.strip_prefix(base) {
                Ok(r) => r.to_string_lossy().replace('\\', "/"),
                Err(_) => continue,
            };
            out.push((rel, p));
        }
    }
    Ok(())
}

fn safe_join(base: &Path, rel: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if rel.starts_with('/') {
        return Err("absolute path in bundle rel path".into());
    }
    if rel.split('/').any(|seg| seg == "..") {
        return Err("path traversal in bundle rel path".into());
    }
    Ok(base.join(rel))
}

// ── Approach 2: range-read S3Directory (shared-index read optimization) ────────
//
// The S3 `Directory` for tantivy reads index files directly from the object
// store via range GET (no full-bundle download to local disk). tantivy issues
// `read_bytes(range)` per chunk (its `stream_file_chunks` uses 1 MB blocks);
// each range is one S3 `get_range`. A per-process split cache turns repeat
// reads of the same file into local cache hits (warm queries pull ~0 bytes).
//
// This is the production landing of the feasibility spike in
// `alitrack/pgtantivy-s3dir-spike` (same tantivy 0.24 + object_store 0.14, code
// moved in). Known follow-ups (not blocking the base feature): file-length
// cache is per-open (a head GET per file on first touch) and there is no
// query pre-warm / footer cache yet.

#[derive(Clone, Debug)]
struct S3FileHandle {
    s3: Arc<AmazonS3>,
    key: String,
    len: u64,
    cache: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl HasLen for S3FileHandle {
    fn len(&self) -> usize {
        self.len as usize
    }
}

impl FileHandle for S3FileHandle {
    fn read_bytes(&self, range: std::ops::Range<usize>) -> std::io::Result<OwnedBytes> {
        let (start, end) = (range.start as u64, range.end as u64);
        // split cache: if the whole file is cached, slice locally (0 S3 bytes)
        {
            let c = self.cache.lock().unwrap();
            if let Some(b) = c.get(&self.key) {
                let s = start as usize;
                let e = (end as usize).min(b.len());
                if e >= s {
                    return Ok(OwnedBytes::new(b[s..e].to_vec()));
                }
            }
        }
        // else: one S3 range GET (the whole point of approach 2)
        let s3 = self.s3.clone();
        let k = self.key.clone();
        let b: Bytes = rt()
            .block_on(async move { s3.get_range(&OPath::from(k.as_str()), start..end).await })
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        if start == 0 && end >= self.len {
            self.cache.lock().unwrap().insert(self.key.clone(), b.clone());
        }
        Ok(OwnedBytes::new(b.to_vec()))
    }
}

#[derive(Clone, Debug)]
struct S3Directory {
    s3: Arc<AmazonS3>,
    prefix: String,
    cache: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl S3Directory {
    fn new(s3: Arc<AmazonS3>, prefix: &str) -> Self {
        Self {
            s3,
            prefix: prefix.trim_end_matches('/').to_string(),
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Directory for S3Directory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let rel = path.to_str().unwrap_or("").trim_start_matches('/').to_string();
        if rel.is_empty() {
            return Err(OpenReadError::FileDoesNotExist(PathBuf::from("empty")));
        }
        let key = format!("{}/{}", self.prefix, rel);
        let s3 = self.s3.clone();
        let k = key.clone();
        let meta = rt()
            .block_on(async move { s3.head(&OPath::from(k.as_str())).await })
            .map_err(|e| OpenReadError::IoError {
                io_error: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("no object {key}: {e}"),
                )),
                filepath: Path::new(&key).to_path_buf(),
            })?;
        Ok(Arc::new(S3FileHandle {
            s3: self.s3.clone(),
            key,
            len: meta.size,
            cache: self.cache.clone(),
        }))
    }
    fn delete(&self, _path: &Path) -> Result<(), DeleteError> {
        Err(DeleteError::IoError {
            io_error: Arc::new(std::io::Error::new(std::io::ErrorKind::Other, "S3Directory is read-only")),
            filepath: Path::new("ro").to_path_buf(),
        })
    }
    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        let rel = path.to_str().unwrap_or("").trim_start_matches('/').to_string();
        if rel.is_empty() {
            return Ok(true);
        }
        let key = format!("{}/{}", self.prefix, rel);
        let s3 = self.s3.clone();
        let k = key.clone();
        let r = rt().block_on(async move { s3.head(&OPath::from(k.as_str())).await });
        Ok(r.is_ok())
    }
    fn open_write(&self, _path: &Path) -> Result<WritePtr, OpenWriteError> {
        Err(OpenWriteError::IoError {
            io_error: Arc::new(std::io::Error::new(std::io::ErrorKind::Unsupported, "S3Directory is read-only")),
            filepath: Path::new("ro").to_path_buf(),
        })
    }
    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let fh = self.get_file_handle(path)?;
        let len = fh.len();
        let b = fh.read_bytes(0..len).map_err(|e| OpenReadError::IoError {
            io_error: Arc::new(e),
            filepath: Path::new("atomic_read").to_path_buf(),
        })?;
        Ok(b[..].to_vec())
    }
    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> std::io::Result<()> {
        Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "S3Directory is read-only"))
    }
    fn sync_directory(&self) -> std::io::Result<()> {
        Ok(())
    }
    fn acquire_lock(&self, _lock: &Lock) -> Result<DirectoryLock, LockError> {
        Ok(DirectoryLock::from(Box::new(())))
    }
    fn watch(&self, _cb: WatchCallback) -> tantivy::Result<WatchHandle> {
        Err(TantivyError::IoError(Arc::new(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "S3Directory: watch not supported (read-only replica)",
        ))))
    }
}

/// Open an S3 bundle with the range-read `S3Directory` (approach 2) — no full
/// download. Returns the opened `Index` and the index's recorded `lang` (from
/// the bundle's `pgtantivy_meta.json`) so the caller can register the matching
/// tokenizer.
pub fn open_s3_index(
    endpoint: &str,
    bucket: &str,
    region: &str,
    access_key: &str,
    secret: &str,
    bundle_key_prefix: &str,
) -> Result<(Index, String), Box<dyn std::error::Error>> {
    let store = Arc::new(build_store(endpoint, bucket, region, access_key, secret)?);
    // Read the lang sidecar (small object) so search registers the right tokenizer.
    let lang = {
        let meta_key = format!("{}/pgtantivy_meta.json", bundle_key_prefix.trim_end_matches('/'));
        let s3 = store.clone();
        let k = meta_key.clone();
        let r = rt().block_on(async move {
            let p = OPath::from(k.as_str());
            let g = s3.get(&p).await?;
            g.bytes().await
        });
        r.ok()
            .and_then(|b| {
                let s = String::from_utf8_lossy(&b).to_string();
                serde_json::from_str::<serde_json::Value>(&s)
                    .ok()
                    .and_then(|v| v["lang"].as_str().map(std::string::ToString::to_string))
            })
            .unwrap_or_else(|| "chinese".to_string())
    };
    let dir = S3Directory::new(store, bundle_key_prefix);
    let index = Index::open(Box::new(dir) as Box<dyn Directory>)?;
    Ok((index, lang))
}
