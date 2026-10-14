// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! A conformance suite for [`ObjectStore`] implementations, and the harness
//! every live test of the stack shares (feature `testing`).
//!
//! Every backend in this crate passes it, and crates wrapping other storage in
//! the trait (for example an application's own GCS client) can run it too:
//!
//! ```
//! # futures::executor::block_on(async {
//! objstore_r::testing::conformance(&objstore_r::Mem::default(), "t").await.unwrap();
//! # });
//! ```

use crate::{Bytes, Cond, Error, ObjectStore};

/// Run the suite under `prefix` (a valid key; everything is written below it
/// and deleted afterwards). Returns the first violated expectation.
#[allow(clippy::too_many_lines)] // one linear script: splitting it would hide the order
pub async fn conformance<S: ObjectStore>(s: &S, prefix: &str) -> Result<(), String> {
    macro_rules! ensure {
        ($c:expr, $($t:tt)*) => { if !$c { return Err(format!($($t)*)); } };
    }
    let k = |name: &str| format!("{prefix}/{name}");
    let e = |r: Error| format!("unexpected error: {r}");

    // Missing objects.
    ensure!(s.get(&k("missing")).await.map_err(e)?.is_none(), "get of a missing key must be None");
    ensure!(
        s.head(&k("missing")).await.map_err(e)?.is_none(),
        "head of a missing key must be None"
    );
    ensure!(
        matches!(s.get_range(&k("missing"), 0..4).await, Err(Error::NotFound)),
        "range of a missing key must be NotFound"
    );
    s.delete(&k("missing")).await.map_err(e)?;

    // Create-only, conflict, exact bytes.
    let body = Bytes::from_static(b"hello, object store");
    let g1 = s.put(&k("a"), body.clone(), Cond::CreateOnly).await.map_err(e)?;
    ensure!(g1 != 0, "generations are never zero");
    ensure!(
        matches!(
            s.put(&k("a"), Bytes::from_static(b"x"), Cond::CreateOnly).await,
            Err(Error::Conflict)
        ),
        "create-only over an existing key must be Conflict"
    );
    let got = s.get(&k("a")).await.map_err(e)?.ok_or("object vanished")?;
    ensure!(
        got.bytes == body && got.generation == g1,
        "get must return the bytes and generation written"
    );
    let h = s.head(&k("a")).await.map_err(e)?.ok_or("head of an existing key")?;
    ensure!(
        h.size == body.len() as u64 && h.generation == g1 && h.key == k("a"),
        "head must report size, generation and key"
    );

    // Generation CAS.
    ensure!(
        matches!(
            s.put(&k("a"), Bytes::from_static(b"y"), Cond::IfGeneration(g1 + 1)).await,
            Err(Error::PreconditionFailed)
        ),
        "a stale generation must fail the precondition"
    );
    let g2 = s.put(&k("a"), Bytes::from_static(b"v2"), Cond::IfGeneration(g1)).await.map_err(e)?;
    ensure!(g2 != g1, "a write must change the generation");
    ensure!(
        matches!(
            s.put(&k("a"), Bytes::from_static(b"v3"), Cond::IfGeneration(g1)).await,
            Err(Error::PreconditionFailed)
        ),
        "the old generation must no longer match"
    );
    ensure!(
        matches!(
            s.put(&k("nope"), Bytes::from_static(b"v"), Cond::IfGeneration(g1)).await,
            Err(Error::PreconditionFailed)
        ),
        "IfGeneration on a missing key must fail"
    );
    let g3 = s.put(&k("a"), Bytes::from_static(b"v3"), Cond::None).await.map_err(e)?;
    ensure!(g3 != g2, "an unconditional write changes the generation");

    // Ranges.
    s.put(&k("r"), Bytes::from_static(b"0123456789"), Cond::None).await.map_err(e)?;
    let cases: [(u64, u64, &[u8]); 5] =
        [(0, 4, b"0123"), (6, 10, b"6789"), (8, 100, b"89"), (10, 12, b""), (50, 60, b"")];
    for (a, b, want) in cases {
        let got = s.get_range(&k("r"), a..b).await.map_err(e)?;
        ensure!(got.as_ref() == want, "range {a}..{b} returned {:?}", got);
    }

    // Empty and large bodies.
    s.put(&k("empty"), Bytes::new(), Cond::CreateOnly).await.map_err(e)?;
    ensure!(
        s.get(&k("empty")).await.map_err(e)?.is_some_and(|o| o.bytes.is_empty()),
        "empty objects round-trip"
    );
    let big: Bytes = (0..(1u32 << 20))
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8)
        .collect::<Vec<u8>>()
        .into();
    s.put(&k("big"), big.clone(), Cond::CreateOnly).await.map_err(e)?;
    ensure!(
        s.get(&k("big")).await.map_err(e)?.is_some_and(|o| o.bytes == big),
        "a 1 MiB object round-trips exactly"
    );
    ensure!(
        s.get_range(&k("big"), 1000..1010).await.map_err(e)? == big.slice(1000..1010),
        "ranges of a large object"
    );

    // Above the resumable threshold (8 MiB by default): chunked uploads, and
    // ranges that cross a chunk boundary.
    let huge: Bytes = (0..(9u32 << 20))
        .map(|i| (i.wrapping_mul(2_246_822_519) >> 25) as u8)
        .collect::<Vec<u8>>()
        .into();
    s.put(&k("huge"), huge.clone(), Cond::CreateOnly).await.map_err(e)?;
    ensure!(
        s.head(&k("huge")).await.map_err(e)?.is_some_and(|m| m.size == huge.len() as u64),
        "a 9 MiB object reports its size"
    );
    let edge = (8u64 << 20) - 5;
    ensure!(
        s.get_range(&k("huge"), edge..edge + 10).await.map_err(e)?
            == huge.slice(edge as usize..edge as usize + 10),
        "a range across the 8 MiB boundary"
    );
    ensure!(
        s.get(&k("huge")).await.map_err(e)?.is_some_and(|o| o.bytes == huge),
        "a 9 MiB object round-trips exactly"
    );

    // Listing: sorted, prefix-scoped, nested keys.
    s.put(&k("dir/x"), Bytes::from_static(b"1"), Cond::None).await.map_err(e)?;
    s.put(&k("dir/y/z"), Bytes::from_static(b"22"), Cond::None).await.map_err(e)?;
    s.put(&k("dirt"), Bytes::from_static(b"3"), Cond::None).await.map_err(e)?;
    let listed: Vec<String> =
        s.list(&k("dir/")).await.map_err(e)?.into_iter().map(|m| m.key).collect();
    ensure!(listed == [k("dir/x"), k("dir/y/z")], "list(\"dir/\") returned {listed:?}");
    let listed: Vec<String> =
        s.list(&k("dir")).await.map_err(e)?.into_iter().map(|m| m.key).collect();
    ensure!(listed == [k("dir/x"), k("dir/y/z"), k("dirt")], "list(\"dir\") returned {listed:?}");
    let all = s.list(&format!("{prefix}/")).await.map_err(e)?;
    ensure!(all.windows(2).all(|w| w[0].key < w[1].key), "list output must be sorted");
    ensure!(all.iter().any(|m| m.key == k("dir/y/z") && m.size == 2), "list reports sizes");

    // Invalid keys never reach storage.
    for bad in ["", "../x", "a//b", ".hidden", "sp ace"] {
        ensure!(
            matches!(s.put(bad, Bytes::new(), Cond::None).await, Err(Error::InvalidKey { .. })),
            "key {bad:?} must be rejected"
        );
    }

    // Delete, then recreate with a new generation.
    s.delete(&k("a")).await.map_err(e)?;
    ensure!(s.get(&k("a")).await.map_err(e)?.is_none(), "deleted objects are gone");
    let g4 = s.put(&k("a"), Bytes::from_static(b"again"), Cond::CreateOnly).await.map_err(e)?;
    ensure!(g4 != g3, "a recreated object gets a new generation");

    // Clean up.
    for m in s.list(&format!("{prefix}/")).await.map_err(e)? {
        s.delete(&m.key).await.map_err(e)?;
    }
    ensure!(
        s.list(&format!("{prefix}/")).await.map_err(e)?.is_empty(),
        "cleanup left objects behind"
    );
    Ok(())
}

/// The content-encoding contract ([`ObjectStore::put_encoded`]) under
/// `prefix`, for stores that support `gzip` (every backend here, with feature
/// `gzip`): encoded and plain objects round-trip, `stat`/`list_stat` report
/// the encoding, ranged reads of encoded objects refuse, conditions behave as
/// for `put`, and plain ⇄ encoded rewrites of one key read correctly.
#[allow(clippy::too_many_lines)] // one linear script, like `conformance`
pub async fn conformance_encoded<S: ObjectStore>(s: &S, prefix: &str) -> Result<(), String> {
    use crate::ContentEncoding::Gzip;
    macro_rules! ensure {
        ($c:expr, $($t:tt)*) => { if !$c { return Err(format!($($t)*)); } };
    }
    let k = |name: &str| format!("{prefix}/{name}");
    let e = |r: Error| format!("unexpected error: {r}");
    let text: Bytes = b"a line of text that compresses, ".repeat(4096).into();

    // Round trip, and what is stored is smaller.
    let g1 = s.put_encoded(&k("enc"), text.clone(), Cond::CreateOnly, Gzip).await.map_err(e)?;
    let got = s.get(&k("enc")).await.map_err(e)?.ok_or("encoded object vanished")?;
    ensure!(got.bytes == text && got.generation == g1, "get must decode to the bytes written");
    let st = s.stat(&k("enc")).await.map_err(e)?.ok_or("stat of an encoded object")?;
    ensure!(st.content_encoding == Some(Gzip), "stat must report gzip, got {st:?}");
    ensure!(
        st.meta.generation == g1 && st.meta.size < text.len() as u64 / 4,
        "stat reports the generation and the stored (compressed) size: {st:?}"
    );
    let head = s.head(&k("enc")).await.map_err(e)?.ok_or("head of an encoded object")?;
    ensure!(head == st.meta, "head and stat agree: {head:?} vs {st:?}");

    // Ranges refuse, whatever the range; a missing key is still NotFound.
    for r in [0..4, 0..u64::MAX, 3..3, 1_000_000..1_000_010] {
        ensure!(
            matches!(s.get_range(&k("enc"), r.clone()).await, Err(Error::EncodedRange { .. })),
            "a range {r:?} of an encoded object must be EncodedRange"
        );
    }

    // Plain objects beside it are unchanged.
    s.put(&k("plain"), Bytes::from_static(b"0123456789"), Cond::CreateOnly).await.map_err(e)?;
    ensure!(
        s.get_range(&k("plain"), 2..5).await.map_err(e)? == b"234"[..],
        "plain ranges are unchanged"
    );
    let pst = s.stat(&k("plain")).await.map_err(e)?.ok_or("stat of a plain object")?;
    ensure!(pst.content_encoding.is_none() && pst.meta.size == 10, "plain stat: {pst:?}");

    // Conditions are exactly put's. (Different bytes: a store may recognise
    // its own earlier write, identical bytes, as a lost response.)
    let other: Bytes = b"other text that also compresses, ".repeat(512).into();
    ensure!(
        matches!(
            s.put_encoded(&k("enc"), other.clone(), Cond::CreateOnly, Gzip).await,
            Err(Error::Conflict)
        ),
        "create-only over an encoded object must be Conflict"
    );
    ensure!(
        matches!(
            s.put_encoded(&k("enc"), other.clone(), Cond::IfGeneration(g1 + 1), Gzip).await,
            Err(Error::PreconditionFailed)
        ),
        "a stale generation must fail the precondition"
    );
    ensure!(
        matches!(
            s.put_encoded(&k("none"), other.clone(), Cond::IfGeneration(g1), Gzip).await,
            Err(Error::PreconditionFailed)
        ),
        "IfGeneration on a missing key must fail"
    );

    // Encoded -> plain -> encoded on one key, each read as written.
    let g2 = s.put(&k("enc"), Bytes::from_static(b"now plain"), Cond::IfGeneration(g1)).await;
    let g2 = g2.map_err(e)?;
    ensure!(g2 != g1, "a rewrite changes the generation");
    ensure!(
        s.get(&k("enc")).await.map_err(e)?.is_some_and(|o| o.bytes == b"now plain"[..]),
        "a plain rewrite of an encoded key reads plain"
    );
    ensure!(
        s.get_range(&k("enc"), 0..3).await.map_err(e)? == b"now"[..],
        "and its ranges work again"
    );
    let small: Bytes = Bytes::from_static(b"tiny");
    let g3 = s.put_encoded(&k("enc"), small.clone(), Cond::IfGeneration(g2), Gzip).await;
    let g3 = g3.map_err(e)?;
    ensure!(
        s.get(&k("enc")).await.map_err(e)?.is_some_and(|o| o.bytes == small && o.generation == g3),
        "an encoded rewrite of a plain key decodes"
    );
    let empty = s.put_encoded(&k("empty"), Bytes::new(), Cond::None, Gzip).await.map_err(e)?;
    ensure!(
        s.get(&k("empty"))
            .await
            .map_err(e)?
            .is_some_and(|o| o.bytes.is_empty() && o.generation == empty),
        "an encoded empty body round-trips"
    );

    // Listing reports each object's encoding, sorted.
    let listed = s.list_stat(&format!("{prefix}/")).await.map_err(e)?;
    let seen: Vec<(String, Option<crate::ContentEncoding>)> =
        listed.iter().map(|st| (st.meta.key.clone(), st.content_encoding)).collect();
    ensure!(
        seen == [(k("empty"), Some(Gzip)), (k("enc"), Some(Gzip)), (k("plain"), None)],
        "list_stat returned {seen:?}"
    );
    let plain_list: Vec<_> = s.list(&format!("{prefix}/")).await.map_err(e)?;
    ensure!(
        plain_list == listed.iter().map(|st| st.meta.clone()).collect::<Vec<_>>(),
        "list and list_stat agree"
    );

    // Invalid keys never reach storage.
    ensure!(
        matches!(
            s.put_encoded("../x", Bytes::new(), Cond::None, Gzip).await,
            Err(Error::InvalidKey { .. })
        ),
        "an invalid key is refused before encoding"
    );

    // Delete removes the encoding with the object.
    s.delete(&k("enc")).await.map_err(e)?;
    ensure!(s.stat(&k("enc")).await.map_err(e)?.is_none(), "deleted objects have no stat");
    s.put(&k("enc"), Bytes::from_static(b"fresh"), Cond::CreateOnly).await.map_err(e)?;
    ensure!(
        s.stat(&k("enc")).await.map_err(e)?.is_some_and(|st| st.content_encoding.is_none()),
        "a key recreated plain carries no stale encoding"
    );
    clean(s, prefix).await
}

/// A unique key prefix for one test run: `tag-pid-nanos`.
#[must_use]
pub fn unique(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("{tag}-{}-{nanos}", std::process::id())
}

/// Delete everything under `prefix/` and confirm nothing is left.
pub async fn clean<S: ObjectStore>(s: &S, prefix: &str) -> Result<(), String> {
    let dir = format!("{}/", prefix.trim_end_matches('/'));
    for m in s.list(&dir).await.map_err(|e| e.to_string())? {
        s.delete(&m.key).await.map_err(|e| e.to_string())?;
    }
    if s.list(&dir).await.map_err(|e| e.to_string())?.is_empty() {
        Ok(())
    } else {
        Err(format!("{dir} not empty after cleanup"))
    }
}

/// The live bucket named by `OBJSTORE_LIVE_BUCKET`, through Application
/// Default Credentials; `None` (skip) when unset.
#[cfg(feature = "gcs")]
#[must_use]
pub fn live_bucket() -> Option<crate::Gcs<crate::token::Adc>> {
    let bucket = std::env::var("OBJSTORE_LIVE_BUCKET").ok()?;
    let adc = crate::token::Adc::discover().expect("credentials for the live bucket");
    Some(crate::Gcs::new(&bucket, adc, crate::GcsConfig::default()).expect("live bucket name"))
}

/// A fresh bucket on the fake-gcs-server named by `OBJSTORE_FAKE_GCS`, with
/// uploads above `resumable_threshold` bytes resumable; `None` (skip) when
/// unset.
#[cfg(feature = "gcs")]
pub async fn emulator_bucket(
    resumable_threshold: usize,
) -> Option<crate::Gcs<crate::token::Static>> {
    let endpoint = std::env::var("OBJSTORE_FAKE_GCS").ok()?;
    let bucket = unique("objstore-r").replace('_', "-");
    let resp = reqwest::Client::new()
        .post(format!("{endpoint}/storage/v1/b?project=test"))
        .header("content-type", "application/json")
        .body(format!(r#"{{"name":"{bucket}"}}"#))
        .send()
        .await
        .expect("fake-gcs-server reachable");
    assert!(resp.status().is_success(), "bucket creation: {}", resp.status());
    let cfg = crate::GcsConfig { endpoint, resumable_threshold, ..crate::GcsConfig::default() };
    Some(crate::Gcs::new(&bucket, crate::token::Static::none(), cfg).expect("emulator bucket"))
}
