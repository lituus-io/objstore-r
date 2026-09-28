// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
// Hostile inputs: traversal and injection through keys, symlink escapes from a
// local root, and (feature `gcs`) a scripted HTTP server that redirects,
// lies about sizes, fails transiently and loses responses.

use futures::executor::block_on;
use objstore_r::{Bytes, Cond, Error, LocalDir, Mem, ObjectStore};

const HOSTILE: [&str; 12] = [
    "../escape",
    "a/../../escape",
    "/abs",
    "a//b",
    ".",
    "..",
    "C:\\x",
    "a\\..\\b",
    "a\0b",
    "%2e%2e/x",
    "a/.hidden",
    "NUL",
];

#[test]
fn hostile_keys_never_reach_storage() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store");
    let local = LocalDir::open(&root).unwrap();
    let mem = Mem::default();
    for k in HOSTILE {
        assert!(
            matches!(
                block_on(local.put(k, Bytes::new(), Cond::None)),
                Err(Error::InvalidKey { .. })
            ),
            "{k:?}"
        );
        assert!(matches!(block_on(local.get(k)), Err(Error::InvalidKey { .. })), "{k:?}");
        assert!(
            matches!(block_on(mem.put(k, Bytes::new(), Cond::None)), Err(Error::InvalidKey { .. })),
            "{k:?}"
        );
    }
    let outside: Vec<_> =
        std::fs::read_dir(dir.path()).unwrap().map(|e| e.unwrap().file_name()).collect();
    assert_eq!(outside, ["store"], "nothing was written outside the root");
}

#[cfg(unix)]
#[test]
fn a_symlink_inside_the_root_cannot_redirect_io() {
    let dir = tempfile::tempdir().unwrap();
    let outside = dir.path().join("outside");
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(outside.join("secret"), b"do not read").unwrap();
    let root = dir.path().join("store");
    let s = LocalDir::open(&root).unwrap();
    std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();
    std::os::unix::fs::symlink(outside.join("secret"), root.join("filelink")).unwrap();
    assert!(matches!(
        block_on(s.put("link/x", Bytes::from_static(b"x"), Cond::None)),
        Err(Error::InvalidKey { .. })
    ));
    assert!(matches!(block_on(s.get("link/secret")), Err(Error::InvalidKey { .. })));
    assert!(!outside.join("x").exists(), "the write did not escape");
    assert!(block_on(s.get("filelink")).unwrap().is_none(), "a symlinked object is never followed");
    assert!(block_on(s.list("")).unwrap().is_empty(), "symlinks are not listed");
}

#[test]
fn deleting_is_idempotent_and_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let s = LocalDir::open(dir.path()).unwrap();
    block_on(s.put("keep/a", Bytes::from_static(b"1"), Cond::None)).unwrap();
    block_on(s.delete("keep")).unwrap(); // a directory, not an object: nothing happens
    block_on(s.delete("missing/x")).unwrap();
    assert!(block_on(s.get("keep/a")).unwrap().is_some());
}

#[cfg(feature = "gcs")]
mod gcs {
    use objstore_r::token::{AccessToken, Static};
    use objstore_r::{Bytes, Cond, Error, Gcs, GcsConfig, ObjectStore};
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// One scripted HTTP/1.1 response per connection, in order; records requests.
    async fn server(script: Vec<String>) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        tokio::spawn(async move {
            for resp in script {
                let Ok((mut sock, _)) = listener.accept().await else { return };
                let mut buf = vec![0u8; 64 * 1024];
                let mut n = 0;
                // Read the head and any content-length body.
                loop {
                    let r = sock.read(&mut buf[n..]).await.unwrap_or(0);
                    if r == 0 {
                        break;
                    }
                    n += r;
                    let text = String::from_utf8_lossy(&buf[..n]).to_string();
                    if let Some(end) = text.find("\r\n\r\n") {
                        let len = text
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                            })
                            .unwrap_or(0);
                        if n >= end + 4 + len {
                            break;
                        }
                    }
                }
                log.lock().unwrap().push(
                    String::from_utf8_lossy(&buf[..n]).lines().next().unwrap_or("").to_owned(),
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            }
        });
        (format!("http://127.0.0.1:{}", addr.port()), seen)
    }

    fn resp(status: &str, headers: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n{headers}\r\n{body}",
            body.len()
        )
    }

    fn store(endpoint: String) -> Gcs<Static> {
        let cfg =
            GcsConfig { endpoint, max_retries: 3, max_object_bytes: 1024, ..GcsConfig::default() };
        Gcs::new("bucket", Static::new("tok"), cfg).unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn transient_failures_are_retried() {
        let ok = r#"{"name":"k","size":"1","generation":"42"}"#;
        let (ep, seen) = server(vec![
            resp("503 Unavailable", "", "busy"),
            resp("429 Too Many", "", ""),
            resp("200 OK", "", ok),
        ])
        .await;
        let g = store(ep).put("k", Bytes::from_static(b"x"), Cond::CreateOnly).await.unwrap();
        assert_eq!(g, 42);
        assert_eq!(seen.lock().unwrap().len(), 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_lost_response_on_a_conditional_write_is_recovered() {
        // First attempt "fails" (503 after it landed); the retry sees 412; our
        // bytes are there with a new generation, so it is our write.
        let (ep, _) = server(vec![
            resp("503 Unavailable", "", ""),
            resp("412 Precondition Failed", "", ""),
            resp("200 OK", "x-goog-generation: 77\r\n", "payload"),
        ])
        .await;
        let g = store(ep).put("k", Bytes::from_static(b"payload"), Cond::CreateOnly).await.unwrap();
        assert_eq!(g, 77);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_real_conflict_is_reported() {
        let (ep, _) = server(vec![
            resp("412 Precondition Failed", "", ""),
            resp("200 OK", "x-goog-generation: 5\r\n", "someone else"),
        ])
        .await;
        assert!(matches!(
            store(ep).put("k", Bytes::from_static(b"mine"), Cond::CreateOnly).await,
            Err(Error::Conflict)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn redirects_are_not_followed_and_oversized_bodies_refused() {
        let (ep, _) =
            server(vec![resp("302 Found", "location: http://169.254.169.254/\r\n", "")]).await;
        assert!(matches!(store(ep).get("k").await, Err(Error::Protocol { .. })));
        let big = "x".repeat(2048);
        let (ep, _) = server(vec![resp("200 OK", "x-goog-generation: 1\r\n", &big)]).await;
        assert!(matches!(store(ep).get("k").await, Err(Error::TooLarge { limit: 1024 })));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn auth_failures_are_typed_and_carry_no_token() {
        let (ep, _) =
            server(vec![resp("403 Forbidden", "", "caller lacks storage.objects.get")]).await;
        let err = store(ep).get("k").await.unwrap_err();
        assert!(matches!(err, Error::Auth { .. }));
        assert!(!err.to_string().contains("tok"), "{err}");
        assert!(!format!("{:?}", AccessToken::new("ya29.abc", 1)).contains("ya29"));
    }
}

#[test]
fn nesting_collisions_are_typed_and_leave_the_store_usable() {
    let dir = tempfile::tempdir().unwrap();
    let s = LocalDir::open(dir.path()).unwrap();
    block_on(s.put("a", Bytes::from_static(b"1"), Cond::None)).unwrap();
    assert!(matches!(
        block_on(s.put("a/b", Bytes::new(), Cond::None)),
        Err(Error::InvalidKey { .. })
    ));
    assert!(block_on(s.get("a/b")).unwrap().is_none(), "a path through an object is absent");
    assert!(matches!(block_on(s.get_range("a/b", 0..1)), Err(Error::NotFound)));
    block_on(s.delete("a")).unwrap();
    block_on(s.put("a/b", Bytes::from_static(b"2"), Cond::None)).unwrap();
    assert!(matches!(
        block_on(s.put("a", Bytes::new(), Cond::None)),
        Err(Error::InvalidKey { .. })
    ));
    block_on(s.delete("a/b")).unwrap();
    block_on(s.put("a", Bytes::from_static(b"3"), Cond::CreateOnly))
        .expect("emptied prefixes are pruned");
}
