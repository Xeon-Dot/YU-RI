use super::*;

async fn fixture() -> DiskCache {
    let root = std::env::temp_dir().join(format!(
        "yu-ri-cache-tests-{}-{}",
        std::process::id(),
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    DiskCache::new(root, 1000, Duration::from_secs(60))
        .await
        .unwrap()
}
fn options() -> CacheStoreOptions {
    CacheStoreOptions {
        content_type: Some("text/plain".into()),
        ttl: None,
        swr: None,
        etag: Some("\"v1\"".into()),
        last_modified: None,
    }
}
async fn store(cache: &DiskCache, key: &str) {
    let path = cache.temp_data_path(key).await.unwrap();
    tfs::write(&path, b"data").await.unwrap();
    cache.put_file(key, &path, 4, options()).await.unwrap();
}
#[tokio::test]
async fn restart_replacement_counts_body_once() {
    let cache = fixture().await;
    store(&cache, "https://example.org/file").await;
    let root = cache.root.clone();
    drop(cache);
    let cache = DiskCache::new(&root, 1000, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(cache.size_info().await, (4, 1));
    store(&cache, "https://example.org/file").await;
    assert_eq!(cache.size_info().await, (4, 1));
    tfs::remove_dir_all(root).await.unwrap();
}
#[tokio::test]
async fn restart_eviction_removes_actual_body() {
    let cache = fixture().await;
    store(&cache, "https://example.org/file").await;
    let path = cache
        .get_file("https://example.org/file")
        .await
        .unwrap()
        .unwrap()
        .path;
    let root = cache.root.clone();
    drop(cache);
    let cache = DiskCache::new(&root, 0, Duration::from_secs(60))
        .await
        .unwrap();
    cache.enforce_size_limit().await.unwrap();
    assert!(!path.exists(), "eviction must not rehash a stored hash");
    tfs::remove_dir_all(root).await.unwrap();
}
#[tokio::test]
async fn hot_entry_survives_eviction_and_touch_is_deduplicated() {
    let cache = fixture().await;
    store(&cache, "hot").await;
    store(&cache, "cold").await;
    let cold = cache.get_file("cold").await.unwrap().unwrap().path;
    for _ in 0..100 {
        cache.get_file("hot").await.unwrap().unwrap();
    }
    cache.flush_touches().await;
    assert!(cache.inner.lock().await.index.values().all(|e| !e.dirty));
    let mut limited = cache.clone();
    limited.max_size = 4;
    limited.enforce_size_limit().await.unwrap();
    assert!(limited.get_file("hot").await.unwrap().is_some());
    assert!(!cold.exists());
    assert_eq!(limited.size_info().await, (4, 1));
    tfs::remove_dir_all(&cache.root).await.unwrap();
}

#[tokio::test]
async fn mutation_lock_does_not_block_cache_hits_or_stats() {
    let cache = fixture().await;
    store(&cache, "key").await;
    let mutation = cache.mutations.lock().await;
    assert!(
        tokio::time::timeout(Duration::from_secs(1), cache.get_file("key"))
            .await
            .unwrap()
            .unwrap()
            .is_some()
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), cache.size_info())
            .await
            .unwrap(),
        (4, 1)
    );
    drop(mutation);
    tfs::remove_dir_all(&cache.root).await.unwrap();
}

#[tokio::test]
async fn stale_revalidation_cannot_renew_replacement() {
    let cache = fixture().await;
    store(&cache, "key").await;
    let old = cache.get_file("key").await.unwrap().unwrap();
    store(&cache, "key").await;
    let mut renewal = options();
    renewal.ttl = Some(Duration::from_secs(999));
    assert!(!cache.revalidate("key", &old, renewal).await.unwrap());
    let meta: Meta =
        serde_json::from_slice(&tfs::read(old.path.with_extension("meta")).await.unwrap()).unwrap();
    assert_eq!(meta.expires_at - meta.created_at, 60);
    tfs::remove_dir_all(&cache.root).await.unwrap();
}

#[tokio::test]
async fn restart_sweeps_expired_orphan_and_temporary_files() {
    let cache = fixture().await;
    store(&cache, "expired").await;
    let path = cache.get_file("expired").await.unwrap().unwrap().path;
    let meta_path = path.with_extension("meta");
    let mut meta: Meta = serde_json::from_slice(&tfs::read(&meta_path).await.unwrap()).unwrap();
    meta.expires_at = 0;
    meta.swr_expires_at = None;
    tfs::write(&meta_path, serde_json::to_vec(&meta).unwrap())
        .await
        .unwrap();
    let orphan = cache.key_path("orphan").with_extension("bin");
    tfs::create_dir_all(orphan.parent().unwrap()).await.unwrap();
    tfs::write(&orphan, b"abandoned").await.unwrap();
    let temp = cache.temp_data_path("temp").await.unwrap();
    tfs::write(&temp, b"partial").await.unwrap();
    let root = cache.root.clone();
    drop(cache);
    let cache = DiskCache::new(&root, 1000, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(cache.size_info().await, (0, 0));
    assert!(!path.exists() && !meta_path.exists() && !orphan.exists() && !temp.exists());
    tfs::remove_dir_all(&root).await.unwrap();
}

#[tokio::test]
async fn invalid_entry_removal_updates_accounting() {
    let cache = fixture().await;
    store(&cache, "key").await;
    let path = cache.get_file("key").await.unwrap().unwrap().path;
    tfs::write(path, b"wrong size").await.unwrap();
    assert!(cache.get_file("key").await.unwrap().is_none());
    assert_eq!(cache.size_info().await, (0, 0));
    tfs::remove_dir_all(&cache.root).await.unwrap();
}
