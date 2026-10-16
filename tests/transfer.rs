// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// How GCS transfers behave on a real network, against a local double of the
// JSON API (tests/support/gcs_mock.rs) with scaled-down timeouts: throttled
// links finish, stalls fail in about the idle timeout, dropped uploads and
// downloads resume from where they stopped, an object replaced mid-read is
// never spliced, an expired session restarts once, hostile range headers are
// typed errors, and the number of requests is exactly what the design says.

#![cfg(feature = "gcs")]

#[path = "support/gcs_mock.rs"]
mod gcs_mock;

use gcs_mock::{bytes, Faults, Mock};
use objstore_r::token::{AccessToken, Static, TokenProvider};
use objstore_r::{Bytes, Cond, Error, Gcs, GcsConfig, ObjectStore};
use std::cell::Cell;
use std::time::{Duration, Instant};

const KIB: usize = 1 << 10;
const CHUNK: usize = 256 * KIB;

/// Scaled-down transfer settings: 256 KiB chunks, everything at or above
/// one chunk resumable, sub-second bounds.
fn cfg(endpoint: &str) -> GcsConfig {
    GcsConfig {
        endpoint: endpoint.to_owned(),
        connect_timeout: Duration::from_secs(1),
        request_timeout: Duration::from_secs(1),
        idle_timeout: Duration::from_secs(1),
        min_throughput: 128 * KIB as u64,
        max_retries: 3,
        resumable_threshold: CHUNK,
        upload_chunk: CHUNK,
        ..GcsConfig::default()
    }
}

fn store(cfg: GcsConfig) -> Gcs<Static> {
    Gcs::new("bucket", Static::new("tok"), cfg).unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn a_throttled_link_finishes_where_one_short_total_timeout_fails() {
    // 2 MiB each way at 512 KiB/s: about 4 s per direction, four times the
    // 1 s bound that stands in for v0.2.4's 60 s total timeout. Each 1 MiB
    // chunk takes 2 s, so it is the `min_throughput` term of its bound
    // (1 s + 1 MiB / 128 KiB/s = 9 s) that lets it finish.
    let mock = Mock::new(Faults { rate: Some(512 * KIB as u64), ..Faults::default() }).await;
    let body = Bytes::from(bytes(2 << 20, 7));
    let s = store(GcsConfig { upload_chunk: 4 * CHUNK, ..cfg(&mock.endpoint) });
    let t = Instant::now();
    let g = mock.run(s.put("big", body.clone(), Cond::CreateOnly)).await.unwrap();
    let up = t.elapsed();
    let t = Instant::now();
    let got = mock.run(s.get("big")).await.unwrap().unwrap();
    let down = t.elapsed();
    assert!(got.bytes == body && got.generation == g);
    assert!(up > Duration::from_secs(3) && down > Duration::from_secs(3), "{up:?} {down:?}");
    // A slow link that keeps moving needs no resume at all.
    let c = mock.counts();
    assert_eq!((c.session_starts, c.chunk_puts, c.status_queries), (1, 2, 0), "{c:?}");
    assert_eq!((c.media_gets, c.resumed_gets.len()), (1, 0), "{c:?}");

    // The old regime on the same link: the whole body in one request under a
    // 1 s bound (one chunk, and a floor so high it adds nothing).
    let old = GcsConfig {
        upload_chunk: 2 << 20,
        min_throughput: u64::MAX,
        max_retries: 0,
        ..cfg(&mock.endpoint)
    };
    let r = mock.run(store(old).put("old", body, Cond::CreateOnly)).await;
    assert!(matches!(&r, Err(Error::Unavailable { retriable: true, .. })), "{r:?}");
}

#[tokio::test(flavor = "current_thread")]
async fn a_dropped_upload_resumes_from_the_committed_offset() {
    // The fourth chunk (at 768 KiB) is cut after 100 000 of its bytes.
    let cut = 100_000;
    let faults =
        Faults { drop_upload: Cell::new(Some((3 * CHUNK as u64, cut))), ..Faults::default() };
    let mock = Mock::new(faults).await;
    let body = Bytes::from(bytes((2 << 20) + 123, 11));
    let g = mock.run(store(cfg(&mock.endpoint)).put("up", body.clone(), Cond::None)).await.unwrap();
    assert_eq!(mock.object("up"), Some((body.to_vec(), g)));
    let c = mock.counts();
    // Nine chunks plus the cut one sent again; one status query; and no
    // byte sent twice except the cut chunk's received prefix.
    assert_eq!((c.session_starts, c.chunk_puts, c.status_queries), (1, 10, 1), "{c:?}");
    assert_eq!(c.upload_bytes, body.len() as u64 + cut as u64, "{c:?}");
}

#[tokio::test(flavor = "current_thread")]
async fn a_dropped_download_resumes_pinned_to_its_generation_and_is_verified() {
    let data = bytes((2 << 20) + 5, 13);
    let cut = 700_000;
    let faults = Faults { drop_download: Cell::new(Some(cut)), ..Faults::default() };
    let mock = Mock::new(faults).await;
    let g = mock.insert("down", data.clone());
    let got = mock.run(store(cfg(&mock.endpoint)).get("down")).await.unwrap().unwrap();
    assert!(got.bytes == data && got.generation == g);
    let c = mock.counts();
    assert_eq!(c.resumed_gets, [(format!("bytes={cut}-"), Some(g))], "{c:?}");
    assert_eq!(c.download_bytes, data.len() as u64, "each byte sent once: {c:?}");

    // The same resume with a damaged tail: the whole-object CRC-32C refuses it.
    let faults =
        Faults { drop_download: Cell::new(Some(cut)), corrupt_resume: true, ..Faults::default() };
    let mock = Mock::new(faults).await;
    mock.insert("down", data);
    let r = mock.run(store(cfg(&mock.endpoint)).get("down")).await;
    assert!(matches!(r, Err(Error::Corrupt { .. })), "{:?}", r.map(|o| o.map(|o| o.generation)));
}

#[tokio::test(flavor = "current_thread")]
async fn an_object_replaced_between_attempts_is_never_spliced() {
    let faults = Faults {
        drop_download: Cell::new(Some(500_000)),
        replace_on_drop: true,
        ..Faults::default()
    };
    let mock = Mock::new(faults).await;
    let g = mock.insert("moving", bytes(1 << 20, 17));
    let r = mock.run(store(cfg(&mock.endpoint)).get("moving")).await;
    assert!(matches!(r, Err(Error::Changed)), "{:?}", r.map(|o| o.map(|o| o.generation)));
    assert_eq!(mock.counts().resumed_gets, [("bytes=500000-".to_owned(), Some(g))]);
}

#[tokio::test(flavor = "current_thread")]
async fn a_stall_fails_in_about_the_idle_timeout() {
    let idle = Duration::from_millis(500);
    let stalled = |retries| GcsConfig { idle_timeout: idle, max_retries: retries, ..cfg("") };

    // A download that goes silent: one idle timeout, not a hang.
    let mock = Mock::new(Faults { stall_download: Some(1000), ..Faults::default() }).await;
    mock.insert("quiet", bytes(1 << 20, 19));
    let t = Instant::now();
    let r = mock
        .run(store(GcsConfig { endpoint: mock.endpoint.clone(), ..stalled(0) }).get("quiet"))
        .await;
    let took = t.elapsed();
    assert!(matches!(&r, Err(Error::Unavailable { retriable: true, .. })), "{r:?}");
    assert!(took >= idle && took < idle * 3, "{took:?}");
    // With retries, each resume stalls too: bounded by (1 + retries) idle
    // timeouts plus backoff.
    let t = Instant::now();
    let r = mock
        .run(store(GcsConfig { endpoint: mock.endpoint.clone(), ..stalled(2) }).get("quiet"))
        .await;
    let took = t.elapsed();
    assert!(r.is_err() && took >= idle * 3 && took < idle * 3 + Duration::from_secs(2), "{took:?}");

    // An upload chunk that is never read: request_timeout + chunk / floor.
    let mock = Mock::new(Faults { stall_upload: true, ..Faults::default() }).await;
    let up = GcsConfig {
        endpoint: mock.endpoint.clone(),
        request_timeout: Duration::from_millis(300),
        min_throughput: 1 << 20, // 256 KiB at 1 MiB/s: 250 ms
        ..stalled(0)
    };
    let t = Instant::now();
    let r = mock.run(store(up).put("quiet", Bytes::from(bytes(CHUNK, 3)), Cond::None)).await;
    let took = t.elapsed();
    assert!(matches!(&r, Err(Error::Unavailable { retriable: true, .. })), "{r:?}");
    assert!(took >= Duration::from_millis(550) && took < Duration::from_millis(1500), "{took:?}");
}

#[tokio::test(flavor = "current_thread")]
async fn an_expired_session_restarts_once_then_fails_typed() {
    let body = Bytes::from(bytes(3 * CHUNK, 23));
    let mock = Mock::new(Faults { gone: Cell::new(1), ..Faults::default() }).await;
    let g = mock.run(store(cfg(&mock.endpoint)).put("s", body.clone(), Cond::CreateOnly)).await;
    assert_eq!(mock.object("s"), Some((body.to_vec(), g.unwrap())));
    // Session 1: chunk 1, chunk 2 (410). Session 2: three chunks.
    let c = mock.counts();
    assert_eq!((c.session_starts, c.chunk_puts), (2, 5), "{c:?}");

    let mock = Mock::new(Faults { gone: Cell::new(2), ..Faults::default() }).await;
    let r = mock.run(store(cfg(&mock.endpoint)).put("s", body, Cond::CreateOnly)).await;
    assert!(matches!(&r, Err(Error::Unavailable { retriable: true, .. })), "{r:?}");
    assert_eq!(mock.counts().session_starts, 2, "exactly one restart");
    assert!(mock.object("s").is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn uploads_send_exactly_one_put_per_chunk() {
    // N = 5 chunks + 1 byte: ceil(N / chunk) = 6 chunk PUTs, 1 session start,
    // no status query.
    let mock = Mock::new(Faults::default()).await;
    let body = Bytes::from(bytes(5 * CHUNK + 1, 29));
    mock.run(store(cfg(&mock.endpoint)).put("n", body.clone(), Cond::None)).await.unwrap();
    let c = mock.counts();
    assert_eq!((c.session_starts, c.chunk_puts, c.status_queries, c.media_uploads), (1, 6, 0, 0));
    assert_eq!(c.upload_bytes, body.len() as u64);
    // Exactly one chunk: one PUT.
    let mock = Mock::new(Faults::default()).await;
    mock.run(store(cfg(&mock.endpoint)).put("n", Bytes::from(bytes(CHUNK, 1)), Cond::None))
        .await
        .unwrap();
    assert_eq!((mock.counts().session_starts, mock.counts().chunk_puts), (1, 1));
    // Below the threshold: one media upload and nothing else.
    let mock = Mock::new(Faults::default()).await;
    mock.run(store(cfg(&mock.endpoint)).put("n", Bytes::from(bytes(CHUNK - 1, 1)), Cond::None))
        .await
        .unwrap();
    let c = mock.counts();
    assert_eq!((c.media_uploads, c.session_starts, c.chunk_puts), (1, 0, 0));
}

/// A token source that hands out a new token on every call, as a provider
/// does when its cached token is about to expire.
#[derive(Default)]
struct Rotating(std::sync::atomic::AtomicU32);

impl TokenProvider for Rotating {
    type Token<'a> = std::future::Ready<objstore_r::Result<AccessToken>>;
    fn token(&self) -> Self::Token<'_> {
        let n = self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::future::ready(Ok(AccessToken::new(format!("t{n}"), u64::MAX)))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn every_chunk_asks_for_a_current_token() {
    let mock = Mock::new(Faults::default()).await;
    let s = Gcs::new("bucket", Rotating::default(), cfg(&mock.endpoint)).unwrap();
    mock.run(s.put("tok", Bytes::from(bytes(4 * CHUNK, 31)), Cond::None)).await.unwrap();
    let tokens = mock.counts().chunk_tokens;
    // t0 started the session; each chunk carried the next one.
    assert_eq!(tokens, ["Bearer t1", "Bearer t2", "Bearer t3", "Bearer t4"]);
}

#[test]
fn chunk_sizes_and_bounds_are_validated() {
    let ep = "http://127.0.0.1:1";
    for bad in [
        GcsConfig { upload_chunk: 0, ..cfg(ep) },
        GcsConfig { upload_chunk: CHUNK + 1, ..cfg(ep) },
        GcsConfig { upload_chunk: 100 * KIB, ..cfg(ep) },
        GcsConfig { min_throughput: 0, ..cfg(ep) },
        GcsConfig { idle_timeout: Duration::ZERO, ..cfg(ep) },
        GcsConfig { request_timeout: Duration::ZERO, ..cfg(ep) },
    ] {
        assert!(matches!(Gcs::new("bucket", Static::none(), bad), Err(Error::Protocol { .. })));
    }
    assert!(Gcs::new("bucket", Static::none(), GcsConfig { upload_chunk: 3 * CHUNK, ..cfg(ep) })
        .is_ok());
}

// Security: what a hostile or broken server says about ranges and sessions.

#[tokio::test(flavor = "current_thread")]
async fn a_resumed_session_never_leaves_the_upload_origin() {
    // The session expires; the restart's Location names another origin.
    let faults = Faults { gone: Cell::new(1), foreign_location: Some(2), ..Faults::default() };
    let mock = Mock::new(faults).await;
    let r = mock
        .run(store(cfg(&mock.endpoint)).put("x", Bytes::from(bytes(3 * CHUNK, 37)), Cond::None))
        .await;
    assert!(matches!(&r, Err(Error::Protocol { .. })), "{r:?}");
    let c = mock.counts();
    assert_eq!((c.session_starts, c.chunk_puts), (2, 2), "nothing sent to it: {c:?}");
}

#[tokio::test(flavor = "current_thread")]
async fn hostile_resume_offsets_are_typed_errors_not_loops() {
    for (nth, lie) in [
        (1, "bytes=0-99999999"), // beyond the total
        (1, "bytes=0-zz"),       // garbage
        (1, "bytes=5-9"),        // not from zero
        (2, "bytes=0-0"),        // backwards from the 256 KiB already confirmed
    ] {
        let mock = Mock::new(Faults { lie_range: Some((nth, lie)), ..Faults::default() }).await;
        let r = mock
            .run(store(cfg(&mock.endpoint)).put("x", Bytes::from(bytes(3 * CHUNK, 41)), Cond::None))
            .await;
        assert!(matches!(&r, Err(Error::Protocol { .. })), "{lie}: {r:?}");
        assert_eq!(mock.counts().chunk_puts, nth, "{lie}: stopped at once");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn hostile_resumed_download_ranges_are_typed_errors() {
    for cr in ["bytes 5-9/10", "bytes 0-1048575/1048576", "bytes 100-50/1048576", "garbage"] {
        let faults = Faults {
            drop_download: Cell::new(Some(1000)),
            content_range: Some(cr),
            ..Faults::default()
        };
        let mock = Mock::new(faults).await;
        mock.insert("r", bytes(1 << 20, 43));
        let r = mock.run(store(cfg(&mock.endpoint)).get("r")).await;
        assert!(
            matches!(&r, Err(Error::Protocol { .. })),
            "{cr}: {:?}",
            r.map(|o| o.map(|o| o.generation))
        );
        assert_eq!(mock.counts().media_gets, 2, "{cr}: no further attempt");
    }
}
