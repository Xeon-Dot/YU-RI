use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::{
    fs as tfs,
    io::AsyncWriteExt,
    sync::{Mutex, mpsc},
};
use tracing::debug;

#[cfg(test)]
#[path = "cache_tests.rs"]
mod tests;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(1);
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Meta {
    expires_at: u64,
    created_at: u64,
    size: u64,
    content_type: Option<String>,
    swr_expires_at: Option<u64>,
    last_access_at: u64,
    etag: Option<String>,
    #[serde(default)]
    last_modified: Option<String>,
}
impl Meta {
    fn expired(&self, now: u64) -> bool {
        now > self.expires_at && self.swr_expires_at.is_none_or(|end| now > end)
    }
}

#[derive(Debug, Clone)]
pub struct CacheFileEntry {
    pub path: PathBuf,
    pub size: u64,
    pub content_type: Option<String>,
    pub is_fresh: bool,
    pub etag: Option<String>,
    pub created_at: u64,
    pub last_modified: Option<String>,
    generation: u64,
}
pub struct CacheStoreOptions {
    pub content_type: Option<String>,
    pub ttl: Option<Duration>,
    pub swr: Option<Duration>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}
#[derive(Clone)]
pub struct DiskCache {
    root: PathBuf,
    max_size: u64,
    inner: Arc<Mutex<CacheInner>>,
    // Serialize filesystem mutations, not cache hits or stats.
    mutations: Arc<Mutex<()>>,
    default_ttl: Duration,
    evict_tx: mpsc::Sender<()>,
}
struct CacheInner {
    index: HashMap<String, IndexEntry>,
    total_size: u64,
    access_clock: u64,
}
#[derive(Clone)]
struct IndexEntry {
    meta: Meta,
    generation: u64,
    access_order: u64,
    dirty: bool,
}
impl DiskCache {
    pub async fn new<P: AsRef<Path>>(
        root: P,
        max_size: u64,
        default_ttl: Duration,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        tfs::create_dir_all(&root).await?;
        let (evict_tx, mut evict_rx) = mpsc::channel(1);
        let cache = Self {
            root: root.clone(),
            max_size,
            default_ttl,
            evict_tx,
            inner: Arc::new(Mutex::new(CacheInner {
                index: HashMap::new(),
                total_size: 0,
                access_clock: 0,
            })),
            mutations: Arc::new(Mutex::new(())),
        };
        cache.rebuild_index().await?;
        cache.enforce_size_limit().await?;
        let inner = Arc::downgrade(&cache.inner);
        let mutations = Arc::downgrade(&cache.mutations);
        let sender = cache.evict_tx.downgrade();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;
            loop {
                let maintenance = tokio::select! {
                    request = evict_rx.recv() => { if request.is_none() { break; } false },
                    _ = ticker.tick() => true,
                };
                let (Some(inner), Some(mutations), Some(evict_tx)) =
                    (inner.upgrade(), mutations.upgrade(), sender.upgrade())
                else {
                    break;
                };
                let cache = Self {
                    root: root.clone(),
                    max_size,
                    default_ttl,
                    inner,
                    mutations,
                    evict_tx,
                };
                if maintenance {
                    cache.flush_touches().await;
                }
                if (maintenance || cache.size_info().await.0 > max_size)
                    && let Err(e) = cache.enforce_size_limit().await
                {
                    debug!(error=?e, "cache maintenance failed");
                }
            }
        });
        Ok(cache)
    }
    fn key_hash(key: &str) -> String {
        blake3::hash(key.as_bytes()).to_hex().to_string()
    }
    fn hash_path(&self, hash: &str) -> PathBuf {
        self.root.join(&hash[..2]).join(&hash[2..])
    }
    fn key_path(&self, key: &str) -> PathBuf {
        self.hash_path(&Self::key_hash(key))
    }
    fn temp_path_for(path: &Path) -> PathBuf {
        let extension = path.extension().and_then(|s| s.to_str()).unwrap_or("");
        path.with_extension(format!(
            "{}.{}.{}.tmp",
            extension,
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ))
    }
    pub async fn temp_data_path(&self, key: &str) -> Result<PathBuf> {
        let path = Self::temp_path_for(&self.key_path(key).with_extension("bin"));
        if let Some(parent) = path.parent() {
            tfs::create_dir_all(parent).await?;
        }
        Ok(path)
    }
    async fn write_meta(path: &Path, meta: &Meta, durable: bool) -> Result<()> {
        let temp = Self::temp_path_for(path);
        let result = async {
            let mut file = tfs::File::create(&temp).await?;
            file.write_all(&serde_json::to_vec(meta)?).await?;
            file.flush().await?;
            if durable {
                file.sync_all().await?;
            }
            drop(file);
            tfs::rename(&temp, path).await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = tfs::remove_file(temp).await;
        }
        result
    }
    async fn remove_files(base: &Path) {
        let _ = tfs::remove_file(base.with_extension("bin")).await;
        let _ = tfs::remove_file(base.with_extension("meta")).await;
    }
    // Caller owns mutation lock. Never await disk I/O with the index locked.
    async fn remove_hash(&self, hash: &str) {
        {
            let mut inner = self.inner.lock().await;
            if let Some(entry) = inner.index.remove(hash) {
                inner.total_size = inner.total_size.saturating_sub(entry.meta.size);
            }
        }
        Self::remove_files(&self.hash_path(hash)).await;
    }
    async fn rebuild_index(&self) -> Result<()> {
        let mut stack = vec![self.root.clone()];
        let mut bases = HashSet::new();
        while let Some(dir) = stack.pop() {
            let mut rd = tfs::read_dir(dir).await?;
            while let Some(entry) = rd.next_entry().await? {
                let path = entry.path();
                let ty = entry.file_type().await?;
                if ty.is_dir() {
                    stack.push(path);
                } else if ty.is_file() {
                    match path.extension().and_then(|s| s.to_str()) {
                        Some("meta" | "bin") => {
                            bases.insert(path.with_extension(""));
                        }
                        Some("tmp") => {
                            let _ = tfs::remove_file(path).await;
                        }
                        _ => {}
                    }
                }
            }
        }
        let mut pending = bases.into_iter();
        let mut tasks = tokio::task::JoinSet::new();
        let mut index = HashMap::new();
        let mut total_size = 0u64;
        loop {
            while tasks.len() < 32 {
                let Some(base) = pending.next() else {
                    break;
                };
                tasks.spawn(async move {
                    let result = async {
                        let (bytes, stat) = tokio::join!(
                            tfs::read(base.with_extension("meta")),
                            tfs::metadata(base.with_extension("bin"))
                        );
                        let meta: Meta = serde_json::from_slice(&bytes?)?;
                        let stat = stat?;
                        anyhow::ensure!(
                            stat.is_file() && stat.len() == meta.size && !meta.expired(now_secs()),
                            "invalid cache pair"
                        );
                        let shard = base
                            .parent()
                            .and_then(|p| p.file_name())
                            .and_then(|s| s.to_str())
                            .unwrap_or("");
                        let name = base.file_name().and_then(|s| s.to_str()).unwrap_or("");
                        let hash = format!("{shard}{name}");
                        anyhow::ensure!(
                            shard.len() == 2
                                && name.len() == 62
                                && hash.bytes().all(|b| b.is_ascii_hexdigit()),
                            "invalid cache key"
                        );
                        Ok::<_, anyhow::Error>((hash, meta))
                    }
                    .await;
                    if result.is_err() {
                        Self::remove_files(&base).await;
                    }
                    result.ok()
                });
            }
            let Some(result) = tasks.join_next().await else {
                break;
            };
            if let Some((hash, meta)) = result? {
                total_size = total_size.saturating_add(meta.size);
                index.insert(
                    hash,
                    IndexEntry {
                        access_order: meta.last_access_at,
                        meta,
                        generation: TEMP_COUNTER.fetch_add(1, Ordering::Relaxed),
                        dirty: false,
                    },
                );
            }
        }
        let access_clock = index.values().map(|e| e.access_order).max().unwrap_or(0);
        *self.inner.lock().await = CacheInner {
            index,
            total_size,
            access_clock,
        };
        Ok(())
    }
    pub async fn size_info(&self) -> (u64, u64) {
        let inner = self.inner.lock().await;
        (inner.total_size, inner.index.len() as u64)
    }
    pub async fn get_file(&self, key: &str) -> Result<Option<CacheFileEntry>> {
        let hash = Self::key_hash(key);
        loop {
            let snapshot = { self.inner.lock().await.index.get(&hash).cloned() };
            let Some(snapshot) = snapshot else {
                return Ok(None);
            };
            let path = self.hash_path(&hash).with_extension("bin");
            let valid = tfs::metadata(&path)
                .await
                .is_ok_and(|s| s.is_file() && s.len() == snapshot.meta.size);
            let now = now_secs();
            if !valid || snapshot.meta.expired(now) {
                let _mutation = self.mutations.lock().await;
                let current = self
                    .inner
                    .lock()
                    .await
                    .index
                    .get(&hash)
                    .map(|e| e.generation);
                if current != Some(snapshot.generation) {
                    continue;
                }
                self.remove_hash(&hash).await;
                return Ok(None);
            }
            let mut inner = self.inner.lock().await;
            inner.access_clock = inner.access_clock.saturating_add(1);
            let order = inner.access_clock;
            let Some(entry) = inner.index.get_mut(&hash) else {
                return Ok(None);
            };
            if entry.generation != snapshot.generation {
                continue;
            }
            entry.access_order = order;
            entry.meta.last_access_at = now;
            entry.dirty = true;
            let meta = &entry.meta;
            return Ok(Some(CacheFileEntry {
                path,
                size: meta.size,
                content_type: meta.content_type.clone(),
                is_fresh: now <= meta.expires_at,
                etag: meta.etag.clone(),
                created_at: meta.created_at,
                last_modified: meta.last_modified.clone(),
                generation: entry.generation,
            }));
        }
    }
    async fn flush_touches(&self) {
        let hashes: Vec<_> = self
            .inner
            .lock()
            .await
            .index
            .iter()
            .filter(|(_, e)| e.dirty)
            .map(|(k, _)| k.clone())
            .collect();
        for hash in hashes {
            let _mutation = self.mutations.lock().await;
            let meta = {
                let mut inner = self.inner.lock().await;
                inner.index.get_mut(&hash).map(|e| {
                    e.dirty = false;
                    e.meta.clone()
                })
            };
            if let Some(meta) = meta
                && let Err(e) =
                    Self::write_meta(&self.hash_path(&hash).with_extension("meta"), &meta, false)
                        .await
            {
                if let Some(entry) = self.inner.lock().await.index.get_mut(&hash) {
                    entry.dirty = true;
                }
                debug!(error=?e, "touch persistence failed");
            }
        }
    }
    pub async fn revalidate(
        &self,
        key: &str,
        expected: &CacheFileEntry,
        options: CacheStoreOptions,
    ) -> Result<bool> {
        let _mutation = self.mutations.lock().await;
        let hash = Self::key_hash(key);
        let snapshot = { self.inner.lock().await.index.get(&hash).cloned() };
        let Some(mut entry) = snapshot.filter(|e| e.generation == expected.generation) else {
            return Ok(false);
        };
        let ttl = options.ttl.unwrap_or_else(|| {
            Duration::from_secs(entry.meta.expires_at.saturating_sub(entry.meta.created_at))
        });
        let swr = options.swr.or_else(|| {
            entry
                .meta
                .swr_expires_at
                .map(|end| Duration::from_secs(end.saturating_sub(entry.meta.expires_at)))
        });
        let now = now_secs();
        entry.meta.created_at = now;
        entry.meta.expires_at = now.saturating_add(ttl.as_secs());
        // Explicit zero clears SWR during revalidation; None preserves it.
        entry.meta.swr_expires_at = swr
            .filter(|d| !d.is_zero())
            .map(|d| entry.meta.expires_at.saturating_add(d.as_secs()));
        if options.content_type.is_some() {
            entry.meta.content_type = options.content_type;
        }
        if options.etag.is_some() {
            entry.meta.etag = options.etag;
        }
        if options.last_modified.is_some() {
            entry.meta.last_modified = options.last_modified;
        }
        Self::write_meta(
            &self.hash_path(&hash).with_extension("meta"),
            &entry.meta,
            true,
        )
        .await?;
        let mut inner = self.inner.lock().await;
        if let Some(current) = inner.index.get_mut(&hash) {
            entry.meta.last_access_at = current.meta.last_access_at;
            current.meta = entry.meta;
            current.generation = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        }
        Ok(true)
    }
    pub async fn put_file(
        &self,
        key: &str,
        temp_path: &Path,
        size: u64,
        options: CacheStoreOptions,
    ) -> Result<()> {
        anyhow::ensure!(size <= self.max_size, "entry exceeds cache capacity");
        let now = now_secs();
        let ttl = options.ttl.unwrap_or(self.default_ttl);
        let expires_at = now.saturating_add(ttl.as_secs());
        let meta = Meta {
            expires_at,
            created_at: now,
            size,
            content_type: options.content_type,
            swr_expires_at: options.swr.map(|d| expires_at.saturating_add(d.as_secs())),
            last_access_at: now,
            etag: options.etag,
            last_modified: options.last_modified,
        };
        let hash = Self::key_hash(key);
        let base = self.hash_path(&hash);
        tfs::create_dir_all(base.parent().unwrap()).await?;
        // Prepare durable metadata before entering the serialized commit section.
        let staged = Self::temp_path_for(&base.with_extension("meta"));
        Self::write_meta(&staged, &meta, true).await?;
        let _mutation = self.mutations.lock().await;
        {
            let mut inner = self.inner.lock().await;
            if let Some(old) = inner.index.remove(&hash) {
                inner.total_size = inner.total_size.saturating_sub(old.meta.size);
            }
        }
        let result = async {
            tfs::rename(temp_path, base.with_extension("bin")).await?;
            tfs::rename(&staged, base.with_extension("meta")).await?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(e) = result {
            let _ = tfs::remove_file(staged).await;
            Self::remove_files(&base).await;
            return Err(e);
        }
        {
            let mut inner = self.inner.lock().await;
            inner.access_clock = inner.access_clock.saturating_add(1);
            let access_order = inner.access_clock;
            inner.index.insert(
                hash,
                IndexEntry {
                    meta,
                    generation: TEMP_COUNTER.fetch_add(1, Ordering::Relaxed),
                    access_order,
                    dirty: false,
                },
            );
            inner.total_size = inner.total_size.saturating_add(size);
        }
        let _ = self.evict_tx.try_send(());
        Ok(())
    }
    async fn enforce_size_limit(&self) -> Result<()> {
        let _mutation = self.mutations.lock().await;
        let now = now_secs();
        let (mut candidates, mut total) = {
            let inner = self.inner.lock().await;
            (
                inner
                    .index
                    .iter()
                    .filter(|(_, e)| inner.total_size > self.max_size || e.meta.expired(now))
                    .map(|(key, e)| {
                        (
                            key.clone(),
                            e.meta.size,
                            e.meta.expired(now),
                            e.access_order,
                        )
                    })
                    .collect::<Vec<_>>(),
                inner.total_size,
            )
        };
        candidates.sort_unstable_by_key(|(_, _, expired, access)| (!*expired, *access));
        for (hash, size, expired, _) in candidates {
            if !expired && total <= self.max_size {
                break;
            }
            self.remove_hash(&hash).await;
            total = total.saturating_sub(size);
        }
        Ok(())
    }
}
