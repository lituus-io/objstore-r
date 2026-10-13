// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
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
    use std::future::Future;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Drive a scripted server and a client operation together on the current
    /// thread (no spawn, no shared state): the server answers one connection per
    /// scripted response, in order, and returns each request line it saw.
    async fn scripted<F: Future>(
        script: &[String],
        client: impl FnOnce(String) -> F,
    ) -> (Vec<String>, F::Output) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ep = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let run = async { tokio::join!(serve(&listener, script), client(ep)) };
        tokio::time::timeout(Duration::from_secs(20), run)
            .await
            .expect("script and client disagree")
    }

    async fn serve(listener: &TcpListener, script: &[String]) -> Vec<String> {
        let mut seen = Vec::with_capacity(script.len());
        let mut buf = vec![0u8; 64 * 1024];
        for resp in script {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut n = 0;
            // Read the head and any content-length body.
            loop {
                let r = sock.read(&mut buf[n..]).await.unwrap_or(0);
                if r == 0 {
                    break;
                }
                n += r;
                let text = String::from_utf8_lossy(&buf[..n]);
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
            seen.push(String::from_utf8_lossy(&buf[..n]).lines().next().unwrap_or("").to_owned());
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.shutdown().await;
        }
        seen
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
        let script = [
            resp("503 Unavailable", "", "busy"),
            resp("429 Too Many", "", ""),
            resp("200 OK", "", ok),
        ];
        let (seen, g) = scripted(&script, |ep| async move {
            store(ep).put("k", Bytes::from_static(b"x"), Cond::CreateOnly).await
        })
        .await;
        assert_eq!(g.unwrap(), 42);
        assert_eq!(seen.len(), 3);
        assert!(seen.iter().all(|l| l.contains("ifGenerationMatch=0")), "{seen:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_lost_response_on_a_conditional_write_is_recovered() {
        // First attempt "fails" (503 after it landed); the retry sees 412; our
        // bytes are there with a new generation, so it is our write.
        let script = [
            resp("503 Unavailable", "", ""),
            resp("412 Precondition Failed", "", ""),
            resp("200 OK", "x-goog-generation: 77\r\n", "payload"),
        ];
        let (_, g) = scripted(&script, |ep| async move {
            store(ep).put("k", Bytes::from_static(b"payload"), Cond::CreateOnly).await
        })
        .await;
        assert_eq!(g.unwrap(), 77);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_real_conflict_is_reported() {
        let script = [
            resp("412 Precondition Failed", "", ""),
            resp("200 OK", "x-goog-generation: 5\r\n", "someone else"),
        ];
        let (_, r) = scripted(&script, |ep| async move {
            store(ep).put("k", Bytes::from_static(b"mine"), Cond::CreateOnly).await
        })
        .await;
        assert!(matches!(r, Err(Error::Conflict)), "{r:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn redirects_are_not_followed_and_oversized_bodies_refused() {
        let script = [resp("302 Found", "location: http://169.254.169.254/\r\n", "")];
        let (_, r) = scripted(&script, |ep| async move { store(ep).get("k").await }).await;
        assert!(matches!(r, Err(Error::Protocol { .. })), "{r:?}");
        let big = "x".repeat(2048);
        let script = [resp("200 OK", "x-goog-generation: 1\r\n", &big)];
        let (_, r) = scripted(&script, |ep| async move { store(ep).get("k").await }).await;
        assert!(matches!(r, Err(Error::TooLarge { limit: 1024 })), "{r:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn auth_failures_are_typed_and_carry_no_token() {
        let script = [resp("403 Forbidden", "", "caller lacks storage.objects.get")];
        let (_, r) = scripted(&script, |ep| async move { store(ep).get("k").await }).await;
        let err = r.unwrap_err();
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
