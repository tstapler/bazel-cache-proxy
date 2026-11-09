//! Pack/manifest persistence of a local disk cache in the GitHub Actions cache.
//!
//! * pack     `{prefix}-pack-{run}-{n}`  — tar+zstd of `ac/<hash>` and `cas/<hash>` files
//! * manifest `{prefix}-manifest-{run}`  — JSON list of the packs that make up the cache
//!
//! GitHub entries are immutable, so each run publishes only its *new* blobs as extra packs
//! and a new manifest that lists old + new packs. When the pack list grows past
//! `max_packs` (or a listed pack was evicted) the whole local cache is rewritten as fresh
//! packs. The manifest is uploaded last, so it never references a pack that failed to upload.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bazel_cache_proxy_core::{
    backend::{FlushReport, StorageBackend},
    entry_kind::EntryKind,
    error::CacheError,
};
use bazel_cache_proxy_disk::DiskBackend;
use futures::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Read;
use tokio::io::AsyncRead;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::transport::{CacheTransport, SaveOutcome};

const MANIFEST_SCHEMA: u32 = 1;
const RESTORE_CONCURRENCY: usize = 4;

#[derive(Debug, Clone)]
pub struct PackConfig {
    /// Key prefix; include anything that should partition caches (OS, workflow, job).
    pub prefix: String,
    /// Local disk cache directory.
    pub root: PathBuf,
    pub max_local_bytes: u64,
    /// Uncompressed bytes per pack; each pack is held in memory while built and uploaded.
    pub max_pack_bytes: u64,
    /// Rewrite everything into fresh packs once the manifest would list more than this.
    pub max_packs: usize,
    /// When false the cache is restored but never published (e.g. pull requests).
    pub upload: bool,
    /// Unique per workflow run attempt; becomes part of every key written.
    pub run_id: String,
}

impl PackConfig {
    pub fn default_run_id() -> String {
        match (
            std::env::var("GITHUB_RUN_ID"),
            std::env::var("GITHUB_RUN_ATTEMPT"),
        ) {
            // The nonce keeps concurrent jobs of one run (matrix legs) from colliding on keys.
            (Ok(id), Ok(attempt)) if !id.is_empty() => {
                format!(
                    "{id}-{attempt}-{}",
                    &uuid::Uuid::new_v4().simple().to_string()[..8]
                )
            }
            _ => uuid::Uuid::new_v4().to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct PackRef {
    key: String,
    bytes: u64,
    blobs: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    schema: u32,
    packs: Vec<PackRef>,
}

#[derive(Default)]
struct State {
    packs: Vec<PackRef>,
    new_blobs: HashSet<(EntryKind, String)>,
    /// A pack listed by the restored manifest is gone or corrupt (evicted); republish all.
    degraded: bool,
    /// The restore hit a transient error, so the chain may be longer than what was loaded.
    /// Publishing a manifest now would hide the packs we failed to see.
    restore_unsafe: bool,
    /// Distinguishes successive flushes of this process in key names.
    flush_seq: u64,
}

struct Restored {
    packs: Vec<PackRef>,
    degraded: bool,
    transient_error: bool,
    blobs: u64,
    bytes: u64,
}

pub struct PackBackend {
    local: DiskBackend,
    transport: Arc<dyn CacheTransport>,
    cfg: PackConfig,
    state: Mutex<State>,
    /// Serialises flushes without holding `state` (which `put` needs) across network I/O.
    flush_lock: Mutex<()>,
}

impl PackBackend {
    /// Restores the newest manifest's packs into `cfg.root`, then opens the local cache.
    /// Restore failures degrade to a cold cache rather than failing the build.
    pub async fn open(
        cfg: PackConfig,
        transport: Arc<dyn CacheTransport>,
    ) -> Result<Self, CacheError> {
        tokio::fs::create_dir_all(&cfg.root)
            .await
            .map_err(|e| CacheError::Io(e.to_string()))?;
        if cfg.max_local_bytes / cfg.max_pack_bytes.max(1) > cfg.max_packs as u64 {
            warn!(
                "max_local_bytes / max_pack_bytes exceeds max_packs: a full cache will be \
                 republished on every run; raise max_packs or max_pack_bytes"
            );
        }
        let started = std::time::Instant::now();
        let mut state = State::default();
        match Self::restore(&cfg, transport.as_ref()).await {
            Ok(Some(r)) => {
                info!(
                    packs = r.packs.len(),
                    blobs = r.blobs,
                    bytes = r.bytes,
                    degraded = r.degraded,
                    transient_error = r.transient_error,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "restored cache packs"
                );
                state.packs = r.packs;
                state.degraded = r.degraded;
                state.restore_unsafe = r.transient_error;
            }
            Ok(None) => info!("no cache manifest found; starting cold"),
            Err(e) => {
                warn!("cache restore failed ({e}); serving cold and not publishing this run");
                state.restore_unsafe = true;
            }
        }
        let local = DiskBackend::new(cfg.root.clone(), cfg.max_local_bytes).await?;
        Ok(Self {
            local,
            transport,
            cfg,
            state: Mutex::new(state),
            flush_lock: Mutex::new(()),
        })
    }

    fn manifest_key(cfg: &PackConfig, seq: u64) -> String {
        format!("{}-manifest-{}-f{seq}", cfg.prefix, cfg.run_id)
    }

    async fn restore(
        cfg: &PackConfig,
        transport: &dyn CacheTransport,
    ) -> Result<Option<Restored>, CacheError> {
        let prefixes = [format!("{}-manifest-", cfg.prefix)];
        let Some(entry) = transport
            .restore(&Self::manifest_key(cfg, 0), &prefixes)
            .await?
        else {
            return Ok(None);
        };
        let manifest: Manifest = serde_json::from_slice(&entry.data).map_err(|e| {
            CacheError::Internal(format!("bad manifest {}: {e}", entry.matched_key))
        })?;
        if manifest.schema != MANIFEST_SCHEMA {
            return Err(CacheError::Internal(format!(
                "unsupported manifest schema {}",
                manifest.schema
            )));
        }
        let results: Vec<_> = stream::iter(manifest.packs.iter().cloned())
            .map(|pack| async move {
                let fetched = transport.restore(&pack.key, &[]).await;
                (pack, fetched)
            })
            // Ordered: packs are applied oldest first so a later AC rewrite wins.
            .buffered(RESTORE_CONCURRENCY)
            .collect()
            .await;

        let mut r = Restored {
            packs: Vec::new(),
            degraded: false,
            transient_error: false,
            blobs: 0,
            bytes: 0,
        };
        for (pack, fetched) in results {
            match fetched {
                Ok(Some(e)) => {
                    let root = cfg.root.clone();
                    let n = tokio::task::spawn_blocking(move || unpack(&root, &e.data))
                        .await
                        .map_err(|e| CacheError::Internal(e.to_string()))?;
                    match n {
                        Ok(stats) => {
                            r.blobs += stats.written;
                            r.bytes += pack.bytes;
                            if stats.rejected > 0 {
                                warn!(pack = %pack.key, rejected = stats.rejected, "rejected invalid pack entries");
                                r.degraded = true;
                            }
                            r.packs.push(pack);
                        }
                        Err(e) => {
                            warn!(pack = %pack.key, "unpack failed: {e}");
                            r.degraded = true;
                        }
                    }
                }
                Ok(None) => {
                    warn!(pack = %pack.key, "pack listed in manifest is gone (evicted?)");
                    r.degraded = true;
                }
                Err(e) => {
                    warn!(pack = %pack.key, "pack download failed: {e}");
                    r.transient_error = true;
                }
            }
        }
        Ok(Some(r))
    }

    fn list_local(root: &Path) -> Vec<(EntryKind, String)> {
        let mut out = Vec::new();
        for kind in [EntryKind::AC, EntryKind::CAS] {
            if let Ok(rd) = std::fs::read_dir(root.join(kind.path_segment())) {
                for e in rd.flatten() {
                    let name = e.file_name().to_string_lossy().into_owned();
                    if is_hash(&name) {
                        out.push((kind, name));
                    }
                }
            }
        }
        out.sort_by(|a, b| a.1.cmp(&b.1));
        out
    }
}

fn is_hash(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

#[derive(Default, Debug)]
struct UnpackStats {
    written: u64,
    rejected: u64,
}

/// Unpacks a pack into `root`. Only `ac/<hash>` and `cas/<hash>` regular files are accepted
/// (no path from the archive is ever joined verbatim) and CAS content must match its name.
fn unpack(root: &Path, data: &[u8]) -> std::io::Result<UnpackStats> {
    let decoder = zstd::stream::read::Decoder::new(data)?;
    let mut archive = tar::Archive::new(decoder);
    let mut stats = UnpackStats::default();
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.to_string_lossy().into_owned();
        let parsed =
            path.split_once('/')
                .and_then(|(k, h)| match (k.parse::<EntryKind>(), is_hash(h)) {
                    (Ok(kind), true) if entry.header().entry_type().is_file() => {
                        Some((kind, h.to_string()))
                    }
                    _ => None,
                });
        let Some((kind, hash)) = parsed else {
            stats.rejected += 1;
            continue;
        };
        let mut content = Vec::new();
        entry.read_to_end(&mut content)?;
        if kind == EntryKind::CAS && format!("{:x}", Sha256::digest(&content)) != hash {
            stats.rejected += 1;
            continue;
        }
        let dir = root.join(kind.path_segment());
        std::fs::create_dir_all(&dir)?;
        let tmp = dir.join(format!("{hash}.tmp"));
        std::fs::write(&tmp, &content)?;
        std::fs::rename(&tmp, dir.join(&hash))?;
        stats.written += 1;
    }
    Ok(stats)
}

/// Builds one pack from `blobs`; returns (bytes, number of blobs actually packed).
fn build_pack(root: &Path, blobs: &[(EntryKind, String)]) -> std::io::Result<(Vec<u8>, u64)> {
    let encoder = zstd::stream::write::Encoder::new(Vec::new(), 3)?;
    let mut builder = tar::Builder::new(encoder);
    let mut packed = 0;
    for (kind, hash) in blobs {
        let path = root.join(kind.path_segment()).join(hash);
        let Ok(content) = std::fs::read(&path) else {
            continue;
        }; // evicted since listing
        let mut header = tar::Header::new_gnu();
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_mtime(0);
        builder.append_data(&mut header, format!("{kind}/{hash}"), content.as_slice())?;
        packed += 1;
    }
    let bytes = builder.into_inner()?.finish()?;
    Ok((bytes, packed))
}

fn chunk_by_size(
    root: &Path,
    blobs: Vec<(EntryKind, String)>,
    max: u64,
) -> Vec<Vec<(EntryKind, String)>> {
    let (mut chunks, mut cur, mut cur_bytes) = (Vec::new(), Vec::new(), 0u64);
    for b in blobs {
        let size = std::fs::metadata(root.join(b.0.path_segment()).join(&b.1))
            .map(|m| m.len())
            .unwrap_or(0);
        if !cur.is_empty() && cur_bytes + size > max {
            chunks.push(std::mem::take(&mut cur));
            cur_bytes = 0;
        }
        cur_bytes += size;
        cur.push(b);
    }
    if !cur.is_empty() {
        chunks.push(cur);
    }
    chunks
}

#[async_trait]
impl StorageBackend for PackBackend {
    async fn contains(
        &self,
        kind: EntryKind,
        hash: &str,
        size: i64,
    ) -> Result<Option<i64>, CacheError> {
        self.local.contains(kind, hash, size).await
    }

    async fn get(
        &self,
        kind: EntryKind,
        hash: &str,
        size: i64,
    ) -> Result<Option<Box<dyn AsyncRead + Send + Unpin>>, CacheError> {
        self.local.get(kind, hash, size).await
    }

    async fn put(
        &self,
        kind: EntryKind,
        hash: &str,
        logical_size: i64,
        data: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<(), CacheError> {
        // An existing CAS blob is identical by construction; an AC entry may be rewritten.
        let existed =
            kind == EntryKind::CAS && self.local.contains(kind, hash, -1).await?.is_some();
        self.local.put(kind, hash, logical_size, data).await?;
        if !existed {
            self.state
                .lock()
                .await
                .new_blobs
                .insert((kind, hash.to_string()));
        }
        Ok(())
    }

    async fn delete(&self, kind: EntryKind, hash: &str) -> Result<(), CacheError> {
        self.local.delete(kind, hash).await
    }

    async fn flush(&self) -> Result<FlushReport, CacheError> {
        let skipped = |why: &str| {
            Ok(FlushReport {
                skipped_reason: Some(why.into()),
                ..Default::default()
            })
        };
        if !self.cfg.upload {
            return skipped("upload disabled");
        }
        let _flushing = self.flush_lock.lock().await;

        // Take the pending set so concurrent `put`s are never blocked by network I/O below.
        let (taken, packs, degraded, seq) = {
            let mut st = self.state.lock().await;
            if st.restore_unsafe {
                return skipped("restore failed earlier; publishing would hide existing packs");
            }
            if st.new_blobs.is_empty() && !st.degraded {
                return skipped("no new blobs");
            }
            st.flush_seq += 1;
            (
                std::mem::take(&mut st.new_blobs),
                st.packs.clone(),
                st.degraded,
                st.flush_seq,
            )
        };
        match self.publish(&taken, packs, degraded, seq).await {
            Ok((report, keep)) => {
                let mut st = self.state.lock().await;
                st.packs = keep;
                st.degraded = false;
                Ok(report)
            }
            Err(e) => {
                self.state.lock().await.new_blobs.extend(taken);
                Err(e)
            }
        }
    }
}

impl PackBackend {
    async fn publish(
        &self,
        taken: &HashSet<(EntryKind, String)>,
        packs: Vec<PackRef>,
        degraded: bool,
        seq: u64,
    ) -> Result<(FlushReport, Vec<PackRef>), CacheError> {
        let root = self.cfg.root.clone();
        let (max_pack, max_packs) = (self.cfg.max_pack_bytes, self.cfg.max_packs);
        let mut new: Vec<_> = taken.iter().cloned().collect();
        new.sort_by(|a, b| a.1.cmp(&b.1));
        let plan_root = root.clone();
        let (chunks, compact) = tokio::task::spawn_blocking(move || {
            let new_chunks = chunk_by_size(&plan_root, new, max_pack);
            if degraded || packs.len() + new_chunks.len() > max_packs {
                let all = Self::list_local(&plan_root);
                (chunk_by_size(&plan_root, all, max_pack), true, packs)
            } else {
                (new_chunks, false, packs)
            }
        })
        .await
        .map(|(c, compact, p)| ((c, p), compact))
        .map_err(|e| CacheError::Internal(e.to_string()))?;
        let (chunks, existing) = chunks;

        let mut keep = if compact { Vec::new() } else { existing };
        let mut report = FlushReport {
            compacted: compact,
            ..Default::default()
        };
        for (i, chunk) in chunks.into_iter().enumerate() {
            let key = format!("{}-pack-{}-f{seq}-{i}", self.cfg.prefix, self.cfg.run_id);
            let r = root.clone();
            let (bytes, blobs) = tokio::task::spawn_blocking(move || build_pack(&r, &chunk))
                .await
                .map_err(|e| CacheError::Internal(e.to_string()))?
                .map_err(|e| CacheError::Io(e.to_string()))?;
            if blobs == 0 {
                continue; // every blob was evicted locally since it was written
            }
            let len = bytes.len() as u64;
            // Keys are unique per process and flush; a collision would silently drop data.
            if self.transport.save(&key, bytes).await? == SaveOutcome::AlreadyExists {
                return Err(CacheError::Internal(format!(
                    "pack key {key} already exists"
                )));
            }
            report.packs_uploaded += 1;
            report.blobs_uploaded += blobs;
            report.bytes_uploaded += len;
            keep.push(PackRef {
                key,
                bytes: len,
                blobs,
            });
        }

        let manifest = Manifest {
            schema: MANIFEST_SCHEMA,
            packs: keep.clone(),
        };
        let body =
            serde_json::to_vec(&manifest).map_err(|e| CacheError::Internal(e.to_string()))?;
        let key = Self::manifest_key(&self.cfg, seq);
        if self.transport.save(&key, body).await? == SaveOutcome::AlreadyExists {
            return Err(CacheError::Internal(format!(
                "manifest key {key} already exists"
            )));
        }
        report.manifest_key = Some(key);
        Ok((report, keep))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use tempfile::TempDir;

    use super::*;
    use crate::transport::RestoredEntry;

    /// In-memory stand-in: immutable keys, newest-prefix-match restore.
    #[derive(Default)]
    struct FakeTransport {
        entries: StdMutex<Vec<(String, Vec<u8>)>>,
        fail_restore: bool,
    }

    impl FakeTransport {
        fn count(&self) -> usize {
            self.entries.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl CacheTransport for FakeTransport {
        async fn restore(
            &self,
            key: &str,
            prefixes: &[String],
        ) -> Result<Option<RestoredEntry>, CacheError> {
            if self.fail_restore {
                return Err(CacheError::BackendUnavailable("down".into()));
            }
            let e = self.entries.lock().unwrap();
            let found = e.iter().find(|(k, _)| k == key).or_else(|| {
                prefixes
                    .iter()
                    .find_map(|p| e.iter().rev().find(|(k, _)| k.starts_with(p.as_str())))
            });
            Ok(found.map(|(k, d)| RestoredEntry {
                matched_key: k.clone(),
                data: d.clone(),
            }))
        }

        async fn save(&self, key: &str, data: Vec<u8>) -> Result<SaveOutcome, CacheError> {
            let mut e = self.entries.lock().unwrap();
            if e.iter().any(|(k, _)| k == key) {
                return Ok(SaveOutcome::AlreadyExists);
            }
            e.push((key.to_string(), data));
            Ok(SaveOutcome::Saved)
        }
    }

    fn cfg(dir: &TempDir, run: &str) -> PackConfig {
        PackConfig {
            prefix: "t".into(),
            root: dir.path().to_path_buf(),
            max_local_bytes: 1 << 30,
            max_pack_bytes: 1 << 20,
            max_packs: 8,
            upload: true,
            run_id: run.into(),
        }
    }

    async fn put(b: &PackBackend, kind: EntryKind, data: &[u8]) -> String {
        let hash = match kind {
            EntryKind::CAS => format!("{:x}", Sha256::digest(data)),
            EntryKind::AC => format!("{:x}", Sha256::digest([b"ac:", data].concat())),
        };
        b.put(
            kind,
            &hash,
            data.len() as i64,
            Box::new(std::io::Cursor::new(data.to_vec())),
        )
        .await
        .unwrap();
        hash
    }

    async fn read(b: &PackBackend, kind: EntryKind, hash: &str) -> Option<Vec<u8>> {
        let mut r = b.get(kind, hash, -1).await.unwrap()?;
        let mut v = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut r, &mut v)
            .await
            .unwrap();
        Some(v)
    }

    #[tokio::test]
    async fn blobs_survive_across_runs_and_manifests_chain() {
        let t = Arc::new(FakeTransport::default());
        let (d1, d2, d3) = (
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
        );

        let run1 = PackBackend::open(cfg(&d1, "1-1"), t.clone()).await.unwrap();
        let cas1 = put(&run1, EntryKind::CAS, b"first").await;
        let ac1 = put(&run1, EntryKind::AC, b"result-one").await;
        let report = run1.flush().await.unwrap();
        assert_eq!((report.packs_uploaded, report.blobs_uploaded), (1, 2));

        let run2 = PackBackend::open(cfg(&d2, "2-1"), t.clone()).await.unwrap();
        assert_eq!(read(&run2, EntryKind::CAS, &cas1).await.unwrap(), b"first");
        let cas2 = put(&run2, EntryKind::CAS, b"second").await;
        put(&run2, EntryKind::CAS, b"first").await; // already restored: must not be re-uploaded
        assert_eq!(run2.flush().await.unwrap().blobs_uploaded, 1);

        let run3 = PackBackend::open(cfg(&d3, "3-1"), t.clone()).await.unwrap();
        assert_eq!(read(&run3, EntryKind::CAS, &cas1).await.unwrap(), b"first");
        assert_eq!(read(&run3, EntryKind::CAS, &cas2).await.unwrap(), b"second");
        assert_eq!(
            read(&run3, EntryKind::AC, &ac1).await.unwrap(),
            b"result-one"
        );
    }

    #[tokio::test]
    async fn flush_without_new_blobs_uploads_nothing() {
        let t = Arc::new(FakeTransport::default());
        let d = TempDir::new().unwrap();
        let b = PackBackend::open(cfg(&d, "1-1"), t.clone()).await.unwrap();
        let r = b.flush().await.unwrap();
        assert_eq!(r.skipped_reason.as_deref(), Some("no new blobs"));
        assert_eq!(t.count(), 0);
    }

    #[tokio::test]
    async fn upload_disabled_never_saves() {
        let t = Arc::new(FakeTransport::default());
        let d = TempDir::new().unwrap();
        let mut c = cfg(&d, "1-1");
        c.upload = false;
        let b = PackBackend::open(c, t.clone()).await.unwrap();
        put(&b, EntryKind::CAS, b"x").await;
        b.flush().await.unwrap();
        assert_eq!(t.count(), 0);
    }

    #[tokio::test]
    async fn pack_list_is_compacted_past_max_packs() {
        let t = Arc::new(FakeTransport::default());
        let mut hashes = Vec::new();
        for run in 1..=4 {
            let d = TempDir::new().unwrap();
            let mut c = cfg(&d, &format!("{run}-1"));
            c.max_packs = 2;
            let b = PackBackend::open(c, t.clone()).await.unwrap();
            hashes.push(put(&b, EntryKind::CAS, format!("blob{run}").as_bytes()).await);
            b.flush().await.unwrap();
        }
        let d = TempDir::new().unwrap();
        let last = PackBackend::open(cfg(&d, "5-1"), t.clone()).await.unwrap();
        for (i, h) in hashes.iter().enumerate() {
            assert_eq!(
                read(&last, EntryKind::CAS, h).await.unwrap(),
                format!("blob{}", i + 1).as_bytes()
            );
        }
        assert!(last.state.lock().await.packs.len() <= 2);
    }

    #[tokio::test]
    async fn evicted_pack_degrades_then_flush_republishes_everything() {
        let t = Arc::new(FakeTransport::default());
        let (d1, d2, d3) = (
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
            TempDir::new().unwrap(),
        );
        let a = PackBackend::open(cfg(&d1, "1-1"), t.clone()).await.unwrap();
        let h1 = put(&a, EntryKind::CAS, b"one").await;
        a.flush().await.unwrap();
        let b = PackBackend::open(cfg(&d2, "2-1"), t.clone()).await.unwrap();
        let h2 = put(&b, EntryKind::CAS, b"two").await;
        b.flush().await.unwrap();

        t.entries
            .lock()
            .unwrap()
            .retain(|(k, _)| k != "t-pack-1-1-f1-0"); // evict run 1's pack

        let c = PackBackend::open(cfg(&d3, "3-1"), t.clone()).await.unwrap();
        assert!(read(&c, EntryKind::CAS, &h1).await.is_none());
        assert!(read(&c, EntryKind::CAS, &h2).await.is_some());
        let r = c.flush().await.unwrap();
        assert!(r.compacted, "a degraded restore must republish");
        assert_eq!(r.blobs_uploaded, 1);
    }

    #[tokio::test]
    async fn restore_failure_starts_cold() {
        let t = Arc::new(FakeTransport {
            fail_restore: true,
            ..Default::default()
        });
        let d = TempDir::new().unwrap();
        let b = PackBackend::open(cfg(&d, "1-1"), t).await.unwrap();
        assert!(b
            .contains(EntryKind::CAS, &"a".repeat(64), -1)
            .await
            .unwrap()
            .is_none());
    }

    fn tar_zst(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut b = tar::Builder::new(zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap());
        for (name, data) in entries {
            let mut h = tar::Header::new_gnu();
            h.set_size(data.len() as u64);
            h.set_mode(0o644);
            b.append_data(&mut h, name, *data).unwrap();
        }
        b.into_inner().unwrap().finish().unwrap()
    }

    #[test]
    fn unpack_rejects_unexpected_names_and_poisoned_cas() {
        let dir = TempDir::new().unwrap();
        let good = format!("{:x}", Sha256::digest(b"ok"));
        let bad_name = "b".repeat(64);
        let pack = tar_zst(&[
            (&format!("cas/{good}"), b"ok"),
            (&format!("cas/{bad_name}"), b"does not hash to name"),
            ("etc/passwd", b"x"),
            ("cas/short", b"x"),
            (&format!("ac/{bad_name}"), b"action result"),
        ]);
        let stats = unpack(dir.path(), &pack).unwrap();
        assert_eq!((stats.written, stats.rejected), (2, 3));
        assert!(dir.path().join("CAS").join(&good).exists());
        assert!(!dir.path().join("CAS").join(&bad_name).exists());
        assert!(dir.path().join("AC").join(&bad_name).exists());
        assert!(!dir.path().join("etc").exists());
    }

    #[tokio::test]
    async fn repeated_flush_in_one_process_keeps_every_blob() {
        let t = Arc::new(FakeTransport::default());
        let (d1, d2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
        let a = PackBackend::open(cfg(&d1, "1-1"), t.clone()).await.unwrap();
        let h1 = put(&a, EntryKind::CAS, b"before-flush").await;
        a.flush().await.unwrap();
        let h2 = put(&a, EntryKind::CAS, b"after-flush").await;
        let r = a.flush().await.unwrap();
        assert_eq!(r.blobs_uploaded, 1);

        let b = PackBackend::open(cfg(&d2, "2-1"), t.clone()).await.unwrap();
        assert!(read(&b, EntryKind::CAS, &h1).await.is_some());
        assert!(read(&b, EntryKind::CAS, &h2).await.is_some());
    }

    #[tokio::test]
    async fn key_collision_is_an_error_not_silent_loss() {
        let t = Arc::new(FakeTransport::default());
        let (d1, d2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
        let a = PackBackend::open(cfg(&d1, "same"), t.clone())
            .await
            .unwrap();
        let b = PackBackend::open(cfg(&d2, "same"), t.clone())
            .await
            .unwrap();
        put(&a, EntryKind::CAS, b"a").await;
        put(&b, EntryKind::CAS, b"b").await;
        a.flush().await.unwrap();
        assert!(b.flush().await.is_err());
        // The failed flush keeps its blobs pending for a retry.
        assert_eq!(b.state.lock().await.new_blobs.len(), 1);
    }

    #[tokio::test]
    async fn newer_ac_entry_wins_across_packs() {
        let t = Arc::new(FakeTransport::default());
        let key = "c".repeat(64);
        for (run, body) in [("1-1", "old"), ("2-1", "new")] {
            let d = TempDir::new().unwrap();
            let b = PackBackend::open(cfg(&d, run), t.clone()).await.unwrap();
            b.put(
                EntryKind::AC,
                &key,
                body.len() as i64,
                Box::new(std::io::Cursor::new(body.as_bytes().to_vec())),
            )
            .await
            .unwrap();
            b.flush().await.unwrap();
        }
        for _ in 0..5 {
            let d = TempDir::new().unwrap();
            let b = PackBackend::open(cfg(&d, "9-1"), t.clone()).await.unwrap();
            assert_eq!(read(&b, EntryKind::AC, &key).await.unwrap(), b"new");
        }
    }

    #[tokio::test]
    async fn failed_restore_never_publishes_a_chain_that_hides_old_packs() {
        let healthy = Arc::new(FakeTransport::default());
        let d0 = TempDir::new().unwrap();
        let seed = PackBackend::open(cfg(&d0, "1-1"), healthy.clone())
            .await
            .unwrap();
        put(&seed, EntryKind::CAS, b"precious").await;
        seed.flush().await.unwrap();

        let flaky = Arc::new(FakeTransport {
            entries: StdMutex::new(healthy.entries.lock().unwrap().clone()),
            fail_restore: true,
        });
        let d = TempDir::new().unwrap();
        let b = PackBackend::open(cfg(&d, "2-1"), flaky.clone())
            .await
            .unwrap();
        put(&b, EntryKind::CAS, b"new work").await;
        let r = b.flush().await.unwrap();
        assert!(r.skipped_reason.is_some());
        assert_eq!(flaky.count(), healthy.count(), "nothing may be published");
    }
}
