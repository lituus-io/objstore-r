// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Opt-in content encoding (feature `gzip`) on every backend: the shared
// contract, decompression bombs, damaged streams, the local sidecar protocol
// under concurrent writers, the GCS wire (a scripted server and, when
// `OBJSTORE_FAKE_GCS` is set, fake-gcs-server), and the mirror publishing
// compressed bases while its log stays plain.

use futures::executor::block_on;
use objstore_r::testing::conformance_encoded;
use objstore_r::{Bytes, Cond, ContentEncoding, Error, LocalDir, Mem, ObjectStore};

const GZIP: ContentEncoding = ContentEncoding::Gzip;

fn text(n: usize) -> Bytes {
    let line = b"2026-10-14T03:00:00Z INFO request served path=/v1/items status=200 ms=12\n";
    line.iter().copied().cycle().take(n).collect::<Vec<u8>>().into()
}

fn gzip(body: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(body).unwrap();
    e.finish().unwrap()
}

// ---- the shared contract -------------------------------------------------

#[test]
fn mem_conforms_encoded() {
    block_on(conformance_encoded(&Mem::default(), "enc")).unwrap();
}

#[test]
fn local_dir_conforms_encoded() {
    let dir = tempfile::tempdir().unwrap();
    let s = LocalDir::open(dir.path()).unwrap();
    block_on(conformance_encoded(&s, "enc")).unwrap();
    // The plain contract still holds on a store that now has a sidecar tree.
    block_on(objstore_r::testing::conformance(&s, "suite")).unwrap();
}

#[test]
fn the_default_trait_method_refuses_and_writes_nothing() {
    /// A store that wraps Mem without overriding the encoding methods.
    struct Bare(Mem);
    impl ObjectStore for Bare {
        type Get<'a> = <Mem as ObjectStore>::Get<'a>;
        type GetRange<'a> = <Mem as ObjectStore>::GetRange<'a>;
        type Head<'a> = <Mem as ObjectStore>::Head<'a>;
        type Put<'a> = <Mem as ObjectStore>::Put<'a>;
        type List<'a> = <Mem as ObjectStore>::List<'a>;
        type Delete<'a> = <Mem as ObjectStore>::Delete<'a>;
        fn get<'a>(&'a self, key: &'a str) -> Self::Get<'a> {
            self.0.get(key)
        }
        fn get_range<'a>(&'a self, key: &'a str, r: std::ops::Range<u64>) -> Self::GetRange<'a> {
            self.0.get_range(key, r)
        }
        fn head<'a>(&'a self, key: &'a str) -> Self::Head<'a> {
            self.0.head(key)
        }
        fn put<'a>(&'a self, key: &'a str, body: Bytes, cond: Cond) -> Self::Put<'a> {
            self.0.put(key, body, cond)
        }
        fn list<'a>(&'a self, prefix: &'a str) -> Self::List<'a> {
            self.0.list(prefix)
        }
        fn delete<'a>(&'a self, key: &'a str) -> Self::Delete<'a> {
            self.0.delete(key)
        }
    }
    let s = Bare(Mem::default());
    let r = block_on(s.put_encoded("k", text(100), Cond::None, GZIP));
    assert!(matches!(r, Err(Error::UnsupportedEncoding { ref encoding }) if encoding == "gzip"));
    assert!(s.0.keys().is_empty(), "nothing was written");
    block_on(s.put("k", text(10), Cond::None)).unwrap();
    let st = block_on(s.stat("k")).unwrap().unwrap();
    assert_eq!((st.content_encoding, st.meta.size), (None, 10));
    assert_eq!(block_on(s.list_stat("")).unwrap().len(), 1);
}

// ---- decompression bombs and damaged streams -----------------------------

#[test]
fn a_decompression_bomb_is_refused_at_the_bound() {
    // 16 MiB of zeros stores as ~16 KiB: past a 1 MiB bound it is refused,
    // within the default bound it decodes.
    let zeros: Bytes = vec![0u8; 16 << 20].into();
    let dir = tempfile::tempdir().unwrap();
    let local = LocalDir::open(dir.path()).unwrap();
    block_on(local.put_encoded("bomb", zeros.clone(), Cond::None, GZIP)).unwrap();
    let stored = std::fs::metadata(dir.path().join("bomb")).unwrap().len();
    assert!(stored < 64 << 10, "the bomb is small on disk: {stored}");
    assert_eq!(block_on(local.get("bomb")).unwrap().unwrap().bytes, zeros);
    let bounded = LocalDir::open(dir.path()).unwrap().with_max_inflated(1 << 20);
    assert!(
        matches!(block_on(bounded.get("bomb")), Err(Error::TooLarge { limit }) if limit == 1 << 20)
    );

    let mem = Mem::default().with_max_inflated(1 << 20);
    block_on(mem.put_encoded("bomb", zeros.clone(), Cond::None, GZIP)).unwrap();
    assert!(matches!(block_on(mem.get("bomb")), Err(Error::TooLarge { .. })));
    // Exactly at the bound is allowed.
    let mem = Mem::default().with_max_inflated(16 << 20);
    block_on(mem.put_encoded("edge", zeros.clone(), Cond::None, GZIP)).unwrap();
    assert_eq!(block_on(mem.get("edge")).unwrap().unwrap().bytes.len(), 16 << 20);
    assert_eq!(objstore_r::MAX_INFLATED_BYTES, 1 << 30, "the documented default");
}

#[test]
fn a_damaged_stream_is_a_typed_error_never_a_panic() {
    let dir = tempfile::tempdir().unwrap();
    let s = LocalDir::open(dir.path()).unwrap();
    let body = text(50_000);
    block_on(s.put_encoded("k", body.clone(), Cond::None, GZIP)).unwrap();
    let path = dir.path().join("k");
    let good = std::fs::read(&path).unwrap();
    let damage: [(&str, Vec<u8>); 4] = [
        ("truncated", good[..good.len() / 2].to_vec()),
        ("no trailer", good[..good.len() - 4].to_vec()),
        ("flipped", {
            let mut v = good.clone();
            let mid = v.len() / 2;
            v[mid] ^= 0xA5;
            v
        }),
        ("trailing garbage", {
            let mut v = good.clone();
            v.extend_from_slice(b"not gzip");
            v
        }),
    ];
    for (what, bytes) in damage {
        // Rewrite in place, keeping the generation the sidecar names.
        let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
        std::fs::write(&path, &bytes).unwrap();
        std::fs::File::options().write(true).open(&path).unwrap().set_modified(mtime).unwrap();
        let r = block_on(s.get("k"));
        assert!(matches!(r, Err(Error::Corrupt { .. })), "{what}: {r:?}");
    }
}

// ---- the local sidecar protocol ------------------------------------------

#[test]
fn local_layout_stores_the_compressed_bytes_and_hides_the_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let s = LocalDir::open(dir.path()).unwrap();
    // Plain writes create no sidecar tree at all.
    block_on(s.put("a/plain", text(1000), Cond::None)).unwrap();
    assert!(!dir.path().join(".objstore-encoding").exists());
    assert_eq!(std::fs::read(dir.path().join("a/plain")).unwrap(), text(1000));

    let body = text(100_000);
    let g = block_on(s.put_encoded("a/enc", body.clone(), Cond::None, GZIP)).unwrap();
    let on_disk = std::fs::read(dir.path().join("a/enc")).unwrap();
    assert_eq!(&on_disk[..2], &[0x1f, 0x8b], "the file holds the gzip stream");
    assert_eq!(on_disk, gzip(&body), "exactly the stream a stock encoder makes");
    let side = std::fs::read_to_string(dir.path().join(".objstore-encoding/a/enc")).unwrap();
    assert_eq!(side, format!("{g} gzip\n"));
    let keys: Vec<String> = block_on(s.list("")).unwrap().into_iter().map(|m| m.key).collect();
    assert_eq!(keys, ["a/enc", "a/plain"], "the sidecar tree is not listed");

    // Deleting prunes the sidecar and its emptied directories.
    block_on(s.delete("a/enc")).unwrap();
    assert!(!dir.path().join(".objstore-encoding/a").exists());
    assert!(dir.path().join(".objstore-encoding").is_dir(), "the tree root stays");
}

#[test]
fn a_crash_between_sidecar_and_body_leaves_reads_correct() {
    let dir = tempfile::tempdir().unwrap();
    let s = LocalDir::open(dir.path()).unwrap();
    let body = text(10_000);
    let g = block_on(s.put_encoded("k", body.clone(), Cond::None, GZIP)).unwrap();
    // A later encoded write recorded its line, then died before its rename.
    let side = dir.path().join(".objstore-encoding/k");
    std::fs::write(&side, format!("{} gzip\n{g} gzip\n", g + 1_000)).unwrap();
    assert_eq!(block_on(s.get("k")).unwrap().unwrap().bytes, body);
    // The same crash over a plain body: its generation has no line, so plain.
    let gp = block_on(s.put("p", text(10), Cond::None)).unwrap();
    std::fs::create_dir_all(dir.path().join(".objstore-encoding")).unwrap();
    std::fs::write(dir.path().join(".objstore-encoding/p"), format!("{} gzip\n", gp + 7)).unwrap();
    assert_eq!(block_on(s.get("p")).unwrap().unwrap().bytes, text(10));
    assert_eq!(block_on(s.get_range("p", 0..4)).unwrap(), text(4));
    // A damaged sidecar fails loudly rather than guessing.
    std::fs::write(&side, "not a sidecar").unwrap();
    assert!(matches!(block_on(s.get("k")), Err(Error::Corrupt { .. })));
    std::fs::write(&side, "x".repeat(4096)).unwrap();
    assert!(matches!(block_on(s.get("k")), Err(Error::Corrupt { .. })));
}

#[cfg(unix)]
#[test]
fn a_symlinked_sidecar_is_never_followed() {
    let dir = tempfile::tempdir().unwrap();
    let outside = dir.path().join("outside");
    std::fs::create_dir(&outside).unwrap();
    let root = dir.path().join("store");
    let s = LocalDir::open(&root).unwrap();
    let gp = block_on(s.put("k", text(100), Cond::None)).unwrap();
    std::fs::write(outside.join("forged"), format!("{gp} gzip\n")).unwrap();
    std::fs::create_dir_all(root.join(".objstore-encoding")).unwrap();
    std::os::unix::fs::symlink(outside.join("forged"), root.join(".objstore-encoding/k")).unwrap();
    assert_eq!(block_on(s.get("k")).unwrap().unwrap().bytes, text(100), "forged line ignored");
    // A symlinked sidecar directory is refused outright.
    std::fs::create_dir(outside.join("d")).unwrap();
    std::os::unix::fs::symlink(outside.join("d"), root.join(".objstore-encoding/d")).unwrap();
    let r = block_on(s.put_encoded("d/x", text(100), Cond::None, GZIP));
    assert!(matches!(r, Err(Error::InvalidKey { .. })), "{r:?}");
    assert!(std::fs::read_dir(outside.join("d")).unwrap().next().is_none(), "nothing escaped");
}

/// A writer flips one key between encoded and plain bodies while readers
/// read it lock-free: every read is exactly one of the bodies written —
/// never compressed bytes, never a mix.
#[test]
fn concurrent_readers_never_see_compressed_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let s = LocalDir::open(dir.path()).unwrap();
    let bodies: Vec<Bytes> = (0..4u8).map(|i| text(20_000 + usize::from(i) * 7)).collect();
    block_on(s.put("k", bodies[0].clone(), Cond::None)).unwrap();
    let done = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|t| {
        t.spawn(|| {
            for i in 0..300usize {
                let b = bodies[i % bodies.len()].clone();
                if i % 2 == 0 {
                    block_on(s.put_encoded("k", b, Cond::None, GZIP)).unwrap();
                } else {
                    block_on(s.put("k", b, Cond::None)).unwrap();
                }
            }
            done.store(true, std::sync::atomic::Ordering::Release);
        });
        for _ in 0..3 {
            t.spawn(|| {
                let mut reads = 0u32;
                while !done.load(std::sync::atomic::Ordering::Acquire) || reads < 50 {
                    let o = block_on(s.get("k")).unwrap().unwrap();
                    assert!(bodies.contains(&o.bytes), "a read returned bytes never written");
                    match block_on(s.get_range("k", 0..8)) {
                        Ok(r) => assert_eq!(r, bodies[0].slice(0..8)),
                        Err(Error::EncodedRange { .. }) => {}
                        Err(e) => panic!("range: {e}"),
                    }
                    reads += 1;
                }
            });
        }
    });
}

// ---- the GCS wire, against a scripted server -----------------------------

#[cfg(feature = "gcs")]
mod gcs_wire {
    use super::{gzip, text, GZIP};
    use objstore_r::token::Static;
    use objstore_r::{Bytes, Cond, Error, Gcs, GcsConfig, ObjectStore};
    use std::future::Future;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// One connection per scripted response; returns every full request
    /// (head and body) seen. Server and client share one task.
    async fn scripted<F: Future>(
        script: &[Vec<u8>],
        client: impl FnOnce(String) -> F,
    ) -> (Vec<Vec<u8>>, F::Output) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ep = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let run = async { tokio::join!(serve(&listener, script), client(ep)) };
        tokio::time::timeout(Duration::from_secs(20), run)
            .await
            .expect("script and client disagree")
    }

    async fn serve(listener: &TcpListener, script: &[Vec<u8>]) -> Vec<Vec<u8>> {
        let mut seen = Vec::with_capacity(script.len());
        for resp in script {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = vec![0u8; 64 * 1024];
            loop {
                let r = sock.read(&mut chunk).await.unwrap_or(0);
                if r == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..r]);
                if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
                    let len = head
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .map_or(0, |v| v.trim().parse::<usize>().unwrap_or(0));
                    if buf.len() >= end + 4 + len {
                        break;
                    }
                }
            }
            seen.push(buf);
            let _ = sock.write_all(resp).await;
            let _ = sock.shutdown().await;
        }
        seen
    }

    fn resp(status: &str, headers: &str, body: &[u8]) -> Vec<u8> {
        let mut r = format!(
            "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n{headers}\r\n",
            body.len()
        )
        .into_bytes();
        r.extend_from_slice(body);
        r
    }

    fn head(req: &[u8]) -> String {
        let end = req.windows(4).position(|w| w == b"\r\n\r\n").unwrap_or(req.len());
        String::from_utf8_lossy(&req[..end]).to_ascii_lowercase()
    }

    fn payload(req: &[u8]) -> &[u8] {
        let end = req.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
        &req[end + 4..]
    }

    fn store(endpoint: String) -> Gcs<Static> {
        let cfg = GcsConfig { endpoint, max_retries: 0, ..GcsConfig::default() };
        Gcs::new("bucket", Static::new("tok"), cfg).unwrap()
    }

    /// `x-goog-hash` for `bytes`, as Cloud Storage sends it.
    fn goog_hash(bytes: &[u8]) -> String {
        let c = crc32c(bytes).to_be_bytes();
        let n = u32::from_be_bytes(c);
        const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let v = u64::from(n) << 4;
        let s: String = (0..6).rev().map(|i| A[((v >> (6 * i)) & 63) as usize] as char).collect();
        format!("x-goog-hash: crc32c={s}==,md5=AAAAAAAAAAAAAAAAAAAAAA==\r\n")
    }

    /// Bitwise reference CRC-32C (independent of the crate's table form).
    fn crc32c(data: &[u8]) -> u32 {
        let mut c = !0u32;
        for &b in data {
            c ^= u32::from(b);
            for _ in 0..8 {
                c = if c & 1 == 1 { (c >> 1) ^ 0x82F6_3B78 } else { c >> 1 };
            }
        }
        !c
    }

    const OK: &str = r#"{"name":"k","size":"1","generation":"42"}"#;

    #[tokio::test(flavor = "current_thread")]
    async fn an_encoded_upload_carries_the_encoding_and_the_compressed_body() {
        let body = text(50_000);
        let script = [resp("200 OK", "", OK.as_bytes())];
        let (seen, g) = scripted(&script, |ep| async move {
            store(ep).put_encoded("k", body.clone(), Cond::CreateOnly, GZIP).await
        })
        .await;
        assert_eq!(g.unwrap(), 42);
        let h = head(&seen[0]);
        assert!(h.contains("uploadtype=media") && h.contains("contentencoding=gzip"), "{h}");
        assert!(h.contains("ifgenerationmatch=0"), "{h}");
        assert_eq!(payload(&seen[0]), gzip(&text(50_000)), "the body is the gzip stream");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_resumable_encoded_upload_declares_the_encoding_at_session_start() {
        // Incompressible enough that the stored bytes pass the threshold.
        let body: Bytes = (0..50_000u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect::<Vec<u8>>()
            .into();
        let stored = gzip(&body);
        // The scripted session URL is on another host, so the store stops
        // after the session start, which is what this test inspects.
        let script =
            [resp("200 OK", "location: http://127.0.0.1/upload/storage/v1/b/bucket/s1\r\n", b"")];
        let (seen, g) = scripted(&script, |ep| async move {
            let cfg = GcsConfig {
                endpoint: ep,
                max_retries: 0,
                resumable_threshold: 1024,
                ..GcsConfig::default()
            };
            let s = Gcs::new("bucket", Static::new("tok"), cfg).unwrap();
            s.put_encoded("k", body, Cond::None, GZIP).await
        })
        .await;
        assert!(matches!(g, Err(Error::Protocol { .. })), "{g:?}");
        let h = head(&seen[0]);
        assert!(h.contains("uploadtype=resumable") && h.contains("contentencoding=gzip"), "{h}");
        assert!(h.contains(&format!("x-upload-content-length: {}", stored.len())), "{h}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_plain_upload_is_unchanged() {
        let script = [resp("200 OK", "", OK.as_bytes())];
        let (seen, g) = scripted(&script, |ep| async move {
            store(ep).put("k", Bytes::from_static(b"plain"), Cond::None).await
        })
        .await;
        assert_eq!(g.unwrap(), 42);
        assert!(!head(&seen[0]).contains("contentencoding"), "{}", head(&seen[0]));
        assert_eq!(payload(&seen[0]), b"plain");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_download_asks_for_the_stored_bytes_verifies_then_decodes() {
        let body = text(30_000);
        let stored = gzip(&body);
        let headers = format!(
            "x-goog-generation: 9\r\ncontent-encoding: gzip\r\n\
             x-goog-stored-content-encoding: gzip\r\n{}",
            goog_hash(&stored)
        );
        let script = [resp("200 OK", &headers, &stored)];
        let (seen, o) = scripted(&script, |ep| async move { store(ep).get("k").await }).await;
        let o = o.unwrap().unwrap();
        assert_eq!((o.bytes, o.generation), (body, 9));
        assert!(head(&seen[0]).contains("accept-encoding: gzip"), "{}", head(&seen[0]));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_checksum_mismatch_is_caught_before_decoding() {
        let stored = gzip(&text(30_000));
        let mut damaged = stored.clone();
        let mid = damaged.len() / 2;
        damaged[mid] ^= 1;
        // The hash names the good bytes; the body is damaged. Only the
        // checksum can report this as a checksum failure — a decoder would
        // say something about the stream.
        let headers = format!(
            "x-goog-generation: 9\r\ncontent-encoding: gzip\r\n\
             x-goog-stored-content-encoding: gzip\r\n{}",
            goog_hash(&stored)
        );
        let script = [resp("200 OK", &headers, &damaged)];
        let (_, r) = scripted(&script, |ep| async move { store(ep).get("k").await }).await;
        match r {
            Err(Error::Corrupt { message }) => assert!(message.contains("CRC-32C"), "{message}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn without_a_hash_the_gzip_trailer_still_catches_damage() {
        let mut damaged = gzip(&text(30_000));
        let n = damaged.len();
        damaged[n - 6] ^= 1; // inside the trailer's CRC-32
        let headers = "x-goog-generation: 9\r\ncontent-encoding: gzip\r\n";
        let script = [resp("200 OK", headers, &damaged)];
        let (_, r) = scripted(&script, |ep| async move { store(ep).get("k").await }).await;
        assert!(matches!(r, Err(Error::Corrupt { .. })), "{r:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_served_bomb_is_refused_at_the_bound() {
        let stored = gzip(&vec![0u8; 8 << 20]);
        let headers =
            format!("x-goog-generation: 3\r\ncontent-encoding: gzip\r\n{}", goog_hash(&stored));
        let script = [resp("200 OK", &headers, &stored)];
        let (_, r) =
            scripted(
                &script,
                |ep| async move { store(ep).with_max_inflated(1 << 20).get("k").await },
            )
            .await;
        assert!(matches!(r, Err(Error::TooLarge { limit }) if limit == 1 << 20), "{r:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_unknown_served_encoding_is_typed() {
        let headers = "x-goog-generation: 3\r\ncontent-encoding: br\r\n";
        let script = [resp("200 OK", headers, b"\x0b\x02\x80")];
        let (_, r) = scripted(&script, |ep| async move { store(ep).get("k").await }).await;
        assert!(matches!(r, Err(Error::UnsupportedEncoding { ref encoding }) if encoding == "br"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_ranged_read_of_an_encoded_object_refuses_from_the_headers() {
        let headers = "content-encoding: gzip\r\nx-goog-stored-content-encoding: gzip\r\n\
                       content-range: bytes 0-3/29\r\n";
        let script = [resp("206 Partial Content", headers, b"\x1f\x8b\x08\x00")];
        let (seen, r) =
            scripted(&script, |ep| async move { store(ep).get_range("k", 0..4).await }).await;
        assert!(matches!(r, Err(Error::EncodedRange { ref encoding }) if encoding == "gzip"));
        let h = head(&seen[0]);
        assert!(h.contains("range: bytes=0-3") && h.contains("accept-encoding: gzip"), "{h}");
        // A server that transcoded anyway (200, whole decoded body) is
        // refused the same way, from its stored-encoding header.
        let script = [resp("200 OK", "x-goog-stored-content-encoding: gzip\r\n", b"hello")];
        let (_, r) =
            scripted(&script, |ep| async move { store(ep).get_range("k", 0..4).await }).await;
        assert!(matches!(r, Err(Error::EncodedRange { .. })), "{r:?}");
        // An empty range asks for the metadata, and refuses the same way.
        let meta = br#"{"name":"k","size":"29","generation":"5","contentEncoding":"gzip"}"#;
        let script = [resp("200 OK", "", meta)];
        let (seen, r) =
            scripted(&script, |ep| async move { store(ep).get_range("k", 4..4).await }).await;
        assert!(matches!(r, Err(Error::EncodedRange { .. })), "{r:?}");
        assert!(head(&seen[0]).contains("contentencoding"), "{}", head(&seen[0]));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn plain_reads_are_unchanged() {
        let script = [resp(
            "206 Partial Content",
            "content-range: bytes 2-4/10\r\nx-goog-stored-content-encoding: identity\r\n",
            b"234",
        )];
        let (_, r) =
            scripted(&script, |ep| async move { store(ep).get_range("k", 2..5).await }).await;
        assert_eq!(r.unwrap(), &b"234"[..]);
        let script =
            [resp("200 OK", "x-goog-generation: 4\r\nx-goog-hash: crc32c=AAAAAA==\r\n", b"raw")];
        let (_, r) = scripted(&script, |ep| async move { store(ep).get("k").await }).await;
        // A plain body is returned as served; its hash is not this change's
        // to enforce (the default path is byte-for-byte what it was).
        assert_eq!(r.unwrap().unwrap().bytes, &b"raw"[..]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stat_and_list_stat_read_the_encoding() {
        let meta = br#"{"name":"k","size":"29","generation":"5","contentEncoding":"gzip"}"#;
        let script = [resp("200 OK", "", meta)];
        let (seen, st) = scripted(&script, |ep| async move { store(ep).stat("k").await }).await;
        let st = st.unwrap().unwrap();
        assert_eq!((st.content_encoding, st.meta.size, st.meta.generation), (Some(GZIP), 29, 5));
        assert!(head(&seen[0]).contains("fields=name,size,generation,contentencoding"));
        let page = br#"{"items":[{"name":"a","size":"3","generation":"1"},
                                 {"name":"b","size":"9","generation":"2","contentEncoding":"gzip"}]}"#;
        let script = [resp("200 OK", "", page)];
        let (_, l) = scripted(&script, |ep| async move { store(ep).list_stat("").await }).await;
        let l = l.unwrap();
        assert_eq!(l.iter().map(|s| s.content_encoding).collect::<Vec<_>>(), [None, Some(GZIP)]);
    }
}

// ---- GCS against fake-gcs-server -----------------------------------------

#[cfg(feature = "gcs")]
#[tokio::test(flavor = "current_thread")]
async fn gcs_conforms_encoded_on_the_emulator() {
    use objstore_r::testing::emulator_bucket;
    for threshold in [8 << 20, 1024] {
        let Some(s) = emulator_bucket(threshold).await else {
            eprintln!("skipped: set OBJSTORE_FAKE_GCS to run against fake-gcs-server");
            return;
        };
        conformance_encoded(&s, "enc")
            .await
            .unwrap_or_else(|e| panic!("threshold {threshold}: {e}"));
    }
}

#[cfg(feature = "gcs")]
#[tokio::test(flavor = "current_thread")]
async fn gcs_conforms_encoded_live() {
    use objstore_r::testing::{live_bucket, unique};
    let Some(s) = live_bucket() else {
        eprintln!("skipped: set OBJSTORE_LIVE_BUCKET to run against a real bucket");
        return;
    };
    conformance_encoded(&s, &unique("objstore-r-encoded")).await.unwrap();
}

// ---- the mirror: compressed bases, plain log ------------------------------

#[cfg(feature = "mirror")]
mod mirror {
    use super::GZIP;
    use futures::executor::block_on;
    use graph_r::prelude::*;
    use objstore_r::mirror::Mirror;
    use objstore_r::{LocalDir, Mem, ObjectStore};

    /// A realistic knowledge store: pages with titles, snippets, segments and
    /// links, compacted into a base, then a few more commits in the log.
    pub(super) fn realistic(dir: &std::path::Path, docs: u64) -> Store {
        let store = Store::create(dir, Config::default()).unwrap();
        store.set_compact_inline(false);
        let commit = |from: u64, to: u64| {
            let mut w = store.writer().unwrap();
            for i in from..to {
                let url = format!("https://docs.example.com/guide/section-{}/page-{i}", i % 40);
                let title = format!("Configuring component {} for production use", i % 97);
                let snippet = format!(
                    "How to configure, deploy and monitor component {} with retries, \
                     timeouts and structured logging; see also page {}.",
                    i % 97,
                    (i * 7) % docs.max(1)
                );
                w.upsert_node(&DocRecord {
                    url: &url,
                    url_key: UrlKey::of(&url),
                    content_hash: i.wrapping_mul(0x9E37_79B9_7F4A_7C15),
                    fetched_at_ms: 1_700_000_000_000 + i,
                    title: Some(&title),
                    snippet: Some(&snippet),
                    etag: Some("\"v1\""),
                    pinned: i % 50 == 0,
                })
                .unwrap();
                let segs = [
                    SegmentRecord {
                        label: "Overview",
                        byte_range: Some((0, 900)),
                        depth: 1,
                        importance: 40_000,
                    },
                    SegmentRecord {
                        label: "Configuration > Options",
                        byte_range: Some((900, 4_000)),
                        depth: 2,
                        importance: 55_000,
                    },
                ];
                w.set_segments(UrlKey::of(&url), &segs).unwrap();
                let next = format!(
                    "https://docs.example.com/guide/section-{}/page-{}",
                    (i + 1) % 40,
                    (i + 1) % docs.max(1)
                );
                w.set_edges(UrlKey::of(&url), &[(UrlKey::of(&next), EdgeType::Link, 65_535)]);
            }
            w.commit().unwrap();
        };
        commit(0, docs);
        store.compact().unwrap();
        commit(docs, docs + 25);
        store
    }

    /// Everything a reader sees of the pages, plus segment and edge counts.
    fn dump(s: &Store) -> String {
        use std::fmt::Write as _;
        let snap = s.snapshot();
        let mut o = format!("len {}\n", snap.len());
        for i in 0..2000u64 {
            let url = format!("https://docs.example.com/guide/section-{}/page-{i}", i % 40);
            let k = UrlKey::of(&url);
            if let Some(n) = snap.node(k) {
                let segs = snap.segments(k).len();
                writeln!(
                    o,
                    "{url} {:?} {:?} {:x} {} {} segs={segs}",
                    n.title,
                    n.snippet,
                    n.content_hash,
                    n.pinned(),
                    n.is_tombstone()
                )
                .unwrap();
            }
        }
        o
    }

    fn published_bytes<O: ObjectStore>(o: &O) -> (u64, u64) {
        let all = block_on(o.list_stat("kb/")).unwrap();
        let base = all.iter().filter(|s| s.meta.key.ends_with(".base")).map(|s| s.meta.size).sum();
        (base, all.iter().map(|s| s.meta.size).sum())
    }

    #[test]
    fn bases_are_encoded_the_log_is_not_and_replicas_open_them() {
        let wdir = tempfile::tempdir().unwrap();
        let store = realistic(wdir.path(), 1500);
        let remote = tempfile::tempdir().unwrap();
        let objects = LocalDir::open(remote.path()).unwrap();
        let m = Mirror::new(&objects, "kb").unwrap().encode_bases(GZIP);
        let p = block_on(m.publish(&store)).unwrap();
        assert!(p.new_base);

        for st in block_on(objects.list_stat("kb/")).unwrap() {
            let want = st.meta.key.ends_with(".base").then_some(GZIP);
            assert_eq!(st.content_encoding, want, "{}", st.meta.key);
        }

        // A replica pulls (decoding the base once into the file) and graph-r
        // opens it unchanged.
        let rdir = tempfile::tempdir().unwrap();
        let r = Mirror::new(&objects, "kb").unwrap();
        block_on(r.pull_into(rdir.path())).unwrap().unwrap();
        assert_eq!(
            std::fs::read(rdir.path().join("graph.base")).unwrap(),
            std::fs::read(wdir.path().join("graph.base")).unwrap(),
            "the pulled base is the writer's base, byte for byte"
        );
        let replica = Store::open_read_only(rdir.path()).unwrap();
        assert_eq!(dump(&replica), dump(&store));
        assert_eq!(replica.len(), store.len());

        // A log-only publish stays plain; a compaction ships a new encoded
        // base; the replica follows both.
        {
            let mut w = store.writer().unwrap();
            w.remove(UrlKey::of("https://docs.example.com/guide/section-0/page-0")).unwrap();
            w.commit().unwrap();
        }
        assert!(!block_on(m.publish(&store)).unwrap().new_base);
        // Checked here, before a compaction retires the segments: an
        // incrementally appended log segment is never encoded.
        let segs: Vec<_> = block_on(objects.list_stat("kb/"))
            .unwrap()
            .into_iter()
            .filter(|s| s.meta.key.ends_with(".seg"))
            .collect();
        assert!(segs.len() >= 2, "the log-only publish added a segment: {segs:?}");
        assert!(segs.iter().all(|s| s.content_encoding.is_none()), "{segs:?}");
        assert!(block_on(r.refresh_into(&replica)).unwrap());
        assert_eq!(dump(&replica), dump(&store));
        store.compact().unwrap();
        assert!(block_on(m.publish(&store)).unwrap().new_base);
        assert!(block_on(r.refresh_into(&replica)).unwrap());
        assert_eq!(dump(&replica), dump(&store));
        let stats = block_on(objects.list_stat("kb/")).unwrap();
        assert!(stats.iter().any(|s| s.meta.key.ends_with(".seg")), "a log is published");
        for st in &stats {
            let base = st.meta.key.ends_with(".base");
            assert_eq!(st.content_encoding.is_some(), base, "{}", st.meta.key);
        }

        // A memory-only replica decodes into its image.
        let im = Mirror::new(&objects, "kb").unwrap();
        let image = block_on(im.load_image()).unwrap().unwrap();
        assert_eq!(dump(&image), dump(&store));
    }

    #[test]
    fn a_retried_encoded_publish_is_recognised() {
        let wdir = tempfile::tempdir().unwrap();
        let store = realistic(wdir.path(), 200);
        // Lose the reply of each operation of a publish in turn; the one that
        // matters is the encoded base's create-only write, which lands while
        // its writer hears a failure. The retry must accept those bytes.
        let mut base_reply_lost = false;
        for skip in 0..6 {
            let mem = Mem::default();
            let m = Mirror::new(&mem, "kb").unwrap().encode_bases(GZIP);
            mem.inject_after(
                skip,
                objstore_r::mem::Fault::LostResponse(objstore_r::Error::Unavailable {
                    message: "lost".into(),
                    retriable: true,
                }),
            );
            if block_on(m.publish(&store)).is_ok() {
                continue;
            }
            let landed = block_on(mem.list_stat("kb/")).unwrap();
            base_reply_lost |= landed.len() == 1 && landed[0].meta.key.ends_with(".base");
            block_on(m.publish(&store)).unwrap();
            let rdir = tempfile::tempdir().unwrap();
            block_on(Mirror::new(&mem, "kb").unwrap().pull_into(rdir.path())).unwrap().unwrap();
            assert_eq!(dump(&Store::open_read_only(rdir.path()).unwrap()), dump(&store));
        }
        assert!(base_reply_lost, "no step lost the encoded base's reply");
    }

    /// Bytes published for one realistic store, plain vs encoded bases.
    #[test]
    fn encoded_bases_publish_fewer_bytes() {
        let wdir = tempfile::tempdir().unwrap();
        let store = realistic(wdir.path(), 1500);
        let (plain, enc) = (Mem::default(), Mem::default());
        block_on(Mirror::new(&plain, "kb").unwrap().publish(&store)).unwrap();
        block_on(Mirror::new(&enc, "kb").unwrap().encode_bases(GZIP).publish(&store)).unwrap();
        let (pb, pt) = published_bytes(&plain);
        let (eb, et) = published_bytes(&enc);
        eprintln!(
            "published: plain {pt} B (base {pb} B), encoded bases {et} B (base {eb} B); \
             base {:.1}% of plain, total {:.1}% of plain",
            eb as f64 * 100.0 / pb as f64,
            et as f64 * 100.0 / pt as f64
        );
        assert!(eb * 2 < pb, "the base compresses at least 2x: {eb} vs {pb}");
        assert_eq!(pt - pb, et - eb, "log and HEAD bytes are identical either way");
    }
}

#[cfg(all(feature = "gcs", feature = "mirror"))]
#[tokio::test(flavor = "current_thread")]
async fn mirror_encoded_bases_on_the_emulator() {
    use graph_r::prelude::*;
    use objstore_r::mirror::Mirror;
    use objstore_r::testing::{clean, emulator_bucket};
    let Some(objects) = emulator_bucket(8 << 20).await else {
        eprintln!("skipped: set OBJSTORE_FAKE_GCS to run against fake-gcs-server");
        return;
    };
    let wdir = tempfile::tempdir().unwrap();
    let store = mirror::realistic(wdir.path(), 400);
    let w = Mirror::new(&objects, "kb").unwrap().encode_bases(GZIP);
    w.publish(&store).await.unwrap();
    for st in objects.list_stat("kb/").await.unwrap() {
        assert_eq!(st.content_encoding.is_some(), st.meta.key.ends_with(".base"), "{st:?}");
    }
    let rdir = tempfile::tempdir().unwrap();
    Mirror::new(&objects, "kb").unwrap().pull_into(rdir.path()).await.unwrap().unwrap();
    assert_eq!(
        std::fs::read(rdir.path().join("graph.base")).unwrap(),
        std::fs::read(wdir.path().join("graph.base")).unwrap()
    );
    assert_eq!(Store::open_read_only(rdir.path()).unwrap().len(), store.len());
    clean(&objects, "kb").await.unwrap();
}

// ---- fuzz: arbitrary and damaged streams never panic ---------------------

mod fuzz {
    use super::{gzip, text};
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn arbitrary_bytes_decode_or_fail_typed(bytes in proptest::collection::vec(any::<u8>(), 0..4096)) {
            objstore_r::encoding::fuzz(&bytes);
        }

        #[test]
        fn damaged_gzip_streams_decode_or_fail_typed(
            len in 0usize..20_000,
            cut in any::<prop::sample::Index>(),
            flips in proptest::collection::vec((any::<prop::sample::Index>(), 1u8..=255), 0..4),
            tail in proptest::collection::vec(any::<u8>(), 0..16),
        ) {
            let mut s = gzip(&text(len));
            for (at, x) in flips {
                let i = at.index(s.len());
                s[i] ^= x;
            }
            s.truncate(cut.index(s.len() + 1));
            s.extend_from_slice(&tail);
            objstore_r::encoding::fuzz(&s);
        }
    }
}
