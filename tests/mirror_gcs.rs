// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! The mirror protocol against Google Cloud Storage's JSON API, via
//! fake-gcs-server (`OBJSTORE_FAKE_GCS=http://127.0.0.1:4443`). Skipped when
//! the variable is unset. Never touches a real bucket.

#![cfg(feature = "gcs")]

#[path = "support/golden.rs"]
mod golden;

use graph_r::prelude::*;
use objstore_r::mirror::{Error, Mirror, COALESCE_SEGMENTS};
use objstore_r::testing::{clean, emulator_bucket, live_bucket, unique};
use objstore_r::Gcs;

#[tokio::test(flavor = "current_thread")]
async fn mirror_round_trip_on_the_emulator() {
    let Some(objects) = emulator_bucket(2048).await else {
        eprintln!("OBJSTORE_FAKE_GCS unset: skipped");
        return;
    };
    round_trip(&objects, &unique("mirror")).await;
    fencing_and_coalescing(&objects, &unique("mirror-fence")).await;
}

/// A real bucket through Application Default Credentials, under unique
/// `objstore-r-live/…` prefixes that are deleted afterwards.
#[tokio::test(flavor = "current_thread")]
async fn mirror_round_trip_on_a_live_bucket() {
    let Some(objects) = live_bucket() else {
        eprintln!("OBJSTORE_LIVE_BUCKET unset: skipped");
        return;
    };
    round_trip(&objects, &unique("objstore-r-live/mirror")).await;
    fencing_and_coalescing(&objects, &unique("objstore-r-live/mirror-fence")).await;
}

/// Two writers from one published state: exactly one wins, the other is
/// fenced. Then a long run of small publishes coalesces its log, and a
/// replica pulled afterwards matches the writer.
async fn fencing_and_coalescing<T: objstore_r::token::TokenProvider>(
    objects: &Gcs<T>,
    prefix: &str,
) {
    use objstore_r::ObjectStore;
    let wdir = tempfile::tempdir().unwrap();
    let store = Store::create(wdir.path(), Config::default()).unwrap();
    golden::seed(&store);
    let a = Mirror::new(objects, prefix).unwrap();
    a.publish(&store).await.unwrap();
    let odir = tempfile::tempdir().unwrap();
    let b = Mirror::new(objects, prefix).unwrap();
    b.pull_into(odir.path()).await.unwrap().unwrap();
    let other = Store::open(odir.path()).unwrap();
    // Different changes from the same state (identical bytes would be one
    // publish retried, which the protocol accepts).
    golden::churn(&store);
    {
        let mut w = other.writer().unwrap();
        let url = "https://example.com/only-in-b";
        w.upsert_node(&DocRecord {
            url,
            url_key: UrlKey::of(url),
            content_hash: 7,
            fetched_at_ms: 1,
            title: None,
            snippet: None,
            etag: None,
            pinned: false,
        })
        .unwrap();
        w.commit().unwrap();
    }
    let first = a.publish(&store).await;
    let second = b.publish(&other).await;
    assert!(first.is_ok(), "{first:?}");
    assert!(matches!(second, Err(Error::Fenced { .. })), "{second:?}");

    for i in 0..(COALESCE_SEGMENTS as u64 + 2) {
        let mut w = store.writer().unwrap();
        let url = format!("https://example.com/coalesce/{i}");
        w.upsert_node(&DocRecord {
            url: &url,
            url_key: UrlKey::of(&url),
            content_hash: i,
            fetched_at_ms: 1,
            title: None,
            snippet: None,
            etag: None,
            pinned: false,
        })
        .unwrap();
        w.commit().unwrap();
        drop(w);
        a.publish(&store).await.unwrap();
    }
    let segs = objects.list(&format!("{prefix}/")).await.unwrap();
    let segs = segs.iter().filter(|m| m.key.ends_with(".seg")).count();
    assert!(segs <= 6, "the log was coalesced: {segs} segments");
    let rdir = tempfile::tempdir().unwrap();
    Mirror::new(objects, prefix).unwrap().pull_into(rdir.path()).await.unwrap().unwrap();
    let replica = Store::open_read_only(rdir.path()).unwrap();
    assert_eq!(golden::dump(&replica), golden::dump(&store));
    clean(objects, prefix).await.unwrap();
}

async fn round_trip<T: objstore_r::token::TokenProvider>(objects: &Gcs<T>, prefix: &str) {
    let prefix = prefix.to_owned();
    let wdir = tempfile::tempdir().unwrap();
    let store = Store::create(wdir.path(), Config::default()).unwrap();
    golden::seed(&store);
    let w = Mirror::new(objects, &prefix).unwrap();
    w.acquire_lease("writer-1", 60_000).await.unwrap();
    w.publish(&store).await.unwrap();

    let rdir = tempfile::tempdir().unwrap();
    let r = Mirror::new(objects, &prefix).unwrap();
    r.pull_into(rdir.path()).await.unwrap().unwrap();
    let replica = Store::open_read_only(rdir.path()).unwrap();
    assert_eq!(golden::dump(&replica), golden::dump(&store));

    golden::churn(&store);
    w.publish(&store).await.unwrap();
    assert!(r.refresh_into(&replica).await.unwrap());
    store.compact().unwrap();
    w.publish(&store).await.unwrap();
    assert!(r.refresh_into(&replica).await.unwrap());
    assert!(!r.refresh_into(&replica).await.unwrap());

    assert!(matches!(r.acquire_lease("writer-2", 60_000).await, Err(Error::Leased { .. })));

    // A memory-only replica on the same bucket.
    let im = Mirror::new(objects, &prefix).unwrap();
    let image = im.load_image().await.unwrap().unwrap();
    assert_eq!(golden::dump(&image), golden::dump(&store));
    golden::churn(&store);
    w.publish(&store).await.unwrap();
    assert!(im.refresh_image(&image).await.unwrap());
    assert_eq!(golden::dump(&image), golden::dump(&store));
    w.release_lease().await.unwrap();
    assert!(r.refresh_into(&replica).await.unwrap());
    assert_eq!(golden::dump(&replica), golden::dump(&store));
    clean(objects, &prefix).await.unwrap();
}
