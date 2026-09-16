use super::*;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static NEXT: AtomicU64 = AtomicU64::new(0);

async fn refresh_fixture(
    status: StatusCode,
    cache_control: &'static str,
    vary: bool,
) -> (
    DiskCache,
    Config,
    String,
    tokio::sync::oneshot::Receiver<HeaderMap>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/file", listener.local_addr().unwrap());
    let root = std::env::temp_dir().join(format!(
        "yu-ri-refresh-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let config = Config {
        listen_addr: "127.0.0.1:0".into(),
        log_level: "off".into(),
        upstream_base: url.clone(),
        upstream_subs: vec![],
        cache_dir: root.to_string_lossy().into_owned(),
        max_cache_size_bytes: 1024,
        default_ttl: Duration::from_secs(60),
        max_body_bytes: None,
    };
    let cache = DiskCache::new(&root, 1024, config.default_ttl)
        .await
        .unwrap();
    let temp = cache.temp_data_path(&url).await.unwrap();
    tfs::write(&temp, b"original").await.unwrap();
    cache
        .put_file(
            &url,
            &temp,
            8,
            CacheStoreOptions {
                content_type: Some("text/plain".into()),
                ttl: Some(Duration::from_secs(60)),
                swr: Some(Duration::from_secs(60)),
                etag: Some("\"v1\"".into()),
                last_modified: Some("Wed, 21 Oct 2015 07:28:00 GMT".into()),
            },
        )
        .await
        .unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = std::sync::Arc::new(std::sync::Mutex::new(Some(tx)));
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let service = service_fn(move |req: Request<hyper::body::Incoming>| {
            if let Some(tx) = tx.lock().unwrap().take() {
                let _ = tx.send(req.headers().clone());
            }
            async move {
                let mut response = http::Response::builder()
                    .status(status)
                    .header(header::CACHE_CONTROL, cache_control);
                if vary {
                    response = response.header(header::VARY, "*");
                }
                let body = if status == StatusCode::NOT_MODIFIED {
                    Bytes::new()
                } else {
                    Bytes::from_static(b"replacement")
                };
                Ok::<_, std::convert::Infallible>(response.body(Full::new(body)).unwrap())
            }
        });
        let _ = hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(stream), service)
            .await;
    });
    (cache, config, url, rx, task)
}

fn client() -> HttpClient {
    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    Client::builder(TokioExecutor::new()).build(connector)
}

#[tokio::test]
async fn refresh_304_sends_validators_and_preserves_body() {
    let (cache, config, url, received, server) =
        refresh_fixture(StatusCode::NOT_MODIFIED, "max-age=120", false).await;
    let before = cache.get_file(&url).await.unwrap().unwrap();
    #[cfg(unix)]
    let inode = {
        use std::os::unix::fs::MetadataExt;
        tfs::metadata(&before.path).await.unwrap().ino()
    };
    background_refresh(url.clone(), &config, &cache, &client())
        .await
        .unwrap();
    let headers = received.await.unwrap();
    assert_eq!(
        headers
            .get(header::IF_NONE_MATCH)
            .and_then(|v| v.to_str().ok()),
        Some("\"v1\"")
    );
    let after = cache.get_file(&url).await.unwrap().unwrap();
    assert!(after.is_fresh);
    assert_eq!(after.etag, before.etag);
    assert_eq!(tfs::read(&after.path).await.unwrap(), b"original");
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(tfs::metadata(&after.path).await.unwrap().ino(), inode);
    }
    server.abort();
    let _ = server.await;
    tfs::remove_dir_all(&config.cache_dir).await.unwrap();
}

#[tokio::test]
async fn refresh_304_extends_persisted_ttl() {
    let (cache, config, url, _received, server) =
        refresh_fixture(StatusCode::NOT_MODIFIED, "max-age=120", false).await;
    let before = cache.get_file(&url).await.unwrap().unwrap();
    background_refresh(url.clone(), &config, &cache, &client())
        .await
        .unwrap();
    let meta: serde_json::Value =
        serde_json::from_slice(&tfs::read(before.path.with_extension("meta")).await.unwrap())
            .unwrap();
    server.abort();
    let _ = server.await;
    tfs::remove_dir_all(&config.cache_dir).await.unwrap();
    assert!(
        meta["expires_at"].as_u64().unwrap() >= before.created_at + 120,
        "304 must extend persisted freshness"
    );
}

#[tokio::test]
async fn refresh_304_replaces_swr_policy() {
    let (cache, config, url, _received, server) =
        refresh_fixture(StatusCode::NOT_MODIFIED, "max-age=120", false).await;
    let before = cache.get_file(&url).await.unwrap().unwrap();
    background_refresh(url, &config, &cache, &client())
        .await
        .unwrap();
    let meta: serde_json::Value =
        serde_json::from_slice(&tfs::read(before.path.with_extension("meta")).await.unwrap())
            .unwrap();
    server.abort();
    let _ = server.await;
    tfs::remove_dir_all(&config.cache_dir).await.unwrap();
    assert!(
        meta["swr_expires_at"].is_null(),
        "replacement policy without SWR must clear old permission"
    );
}

#[tokio::test]
async fn refresh_304_vary_star_must_not_extend_ttl() {
    let (cache, config, url, _received, server) =
        refresh_fixture(StatusCode::NOT_MODIFIED, "max-age=120", true).await;
    let before = cache.get_file(&url).await.unwrap().unwrap();
    let meta_path = before.path.with_extension("meta");
    let old: serde_json::Value =
        serde_json::from_slice(&tfs::read(&meta_path).await.unwrap()).unwrap();
    background_refresh(url, &config, &cache, &client())
        .await
        .unwrap();
    let new: serde_json::Value =
        serde_json::from_slice(&tfs::read(meta_path).await.unwrap()).unwrap();
    server.abort();
    let _ = server.await;
    tfs::remove_dir_all(&config.cache_dir).await.unwrap();
    assert_eq!(old["expires_at"], new["expires_at"]);
}

#[tokio::test]
async fn refresh_vary_star_does_not_replace_cached_body() {
    let (cache, config, url, _received, server) =
        refresh_fixture(StatusCode::OK, "max-age=120", true).await;
    background_refresh(url.clone(), &config, &cache, &client())
        .await
        .unwrap();
    let entry = cache.get_file(&url).await.unwrap().unwrap();
    assert_eq!(tfs::read(&entry.path).await.unwrap(), b"original");
    server.abort();
    let _ = server.await;
    tfs::remove_dir_all(&config.cache_dir).await.unwrap();
}
