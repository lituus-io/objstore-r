// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! A conformance suite for [`ObjectStore`] implementations (feature `testing`).
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
