// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Content encoding: a 10 MB text object written and read plain vs gzip on
//! the in-memory and local-directory backends (time, plus the stored bytes
//! printed once), and the bytes a mirror publishes for one store with and
//! without encoded bases.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::executor::block_on;
use objstore_r::{Bytes, Cond, ContentEncoding, LocalDir, Mem, ObjectStore};

const SIZE: usize = 10_000_000;

/// Log-like text with varying fields: compresses like real text, not like a
/// repeated constant.
fn text(n: usize) -> Bytes {
    let mut out = Vec::with_capacity(n + 128);
    let mut i = 0u64;
    while out.len() < n {
        let line = format!(
            "2026-10-14T03:{:02}:{:02}.{:03}Z INFO request served path=/v1/items/{} status={} ms={} \
             user=u{} region=r{}\n",
            (i / 60_000) % 60,
            (i / 1000) % 60,
            i % 1000,
            i.wrapping_mul(2_654_435_761) % 100_000,
            if i.is_multiple_of(17) { 404 } else { 200 },
            (i * 7) % 250,
            i % 5_000,
            i % 7
        );
        out.extend_from_slice(line.as_bytes());
        i += 1;
    }
    out.truncate(n);
    out.into()
}

fn object(c: &mut Criterion) {
    let body = text(SIZE);
    let mut g = c.benchmark_group("encoding_10mb");
    g.throughput(Throughput::Bytes(SIZE as u64));
    g.sample_size(10);

    let mem = Mem::default();
    block_on(mem.put("plain", body.clone(), Cond::None)).unwrap();
    block_on(mem.put_encoded("gzip", body.clone(), Cond::None, ContentEncoding::Gzip)).unwrap();
    let stored = block_on(mem.head("gzip")).unwrap().unwrap().size;
    eprintln!(
        "10 MB text: plain {SIZE} B, gzip {stored} B stored ({:.1}%)",
        stored as f64 * 100.0 / SIZE as f64
    );
    g.bench_function(BenchmarkId::new("mem_put", "plain"), |b| {
        b.iter(|| block_on(mem.put("w", body.clone(), Cond::None)).unwrap());
    });
    g.bench_function(BenchmarkId::new("mem_put", "gzip"), |b| {
        b.iter(|| {
            block_on(mem.put_encoded("w", body.clone(), Cond::None, ContentEncoding::Gzip)).unwrap()
        });
    });
    g.bench_function(BenchmarkId::new("mem_get", "plain"), |b| {
        b.iter(|| block_on(mem.get("plain")).unwrap());
    });
    g.bench_function(BenchmarkId::new("mem_get", "gzip"), |b| {
        b.iter(|| block_on(mem.get("gzip")).unwrap());
    });

    let dir = tempfile::tempdir().unwrap();
    let local = LocalDir::open(dir.path()).unwrap();
    block_on(local.put("plain", body.clone(), Cond::None)).unwrap();
    block_on(local.put_encoded("gzip", body.clone(), Cond::None, ContentEncoding::Gzip)).unwrap();
    g.bench_function(BenchmarkId::new("local_get", "plain"), |b| {
        b.iter(|| block_on(local.get("plain")).unwrap());
    });
    g.bench_function(BenchmarkId::new("local_get", "gzip"), |b| {
        b.iter(|| block_on(local.get("gzip")).unwrap());
    });
    g.finish();

    // The default path on a store that holds encoded objects: one plain
    // 4 KiB read, with and without a sidecar tree present.
    let mut g = c.benchmark_group("local_plain_get_4k");
    let small = text(4096);
    let clean = tempfile::tempdir().unwrap();
    let clean_store = LocalDir::open(clean.path()).unwrap();
    block_on(clean_store.put("k", small.clone(), Cond::None)).unwrap();
    g.bench_function("no_encoded_objects", |b| {
        b.iter(|| block_on(clean_store.get("k")).unwrap());
    });
    block_on(local.put("k", small, Cond::None)).unwrap();
    g.bench_function("beside_encoded_objects", |b| {
        b.iter(|| block_on(local.get("k")).unwrap());
    });
    g.finish();
}

fn mirror_bytes(c: &mut Criterion) {
    use graph_r::prelude::*;
    use objstore_r::mirror::Mirror;
    let dir = tempfile::tempdir().unwrap();
    let store =
        Store::create(dir.path(), Config { durability: Durability::Never, ..Config::default() })
            .unwrap();
    store.set_compact_inline(false);
    let mut w = store.writer().unwrap();
    for i in 0..5_000u64 {
        let url = format!("https://docs.example.com/guide/section-{}/page-{i}", i % 40);
        let title = format!("Configuring component {} for production use", i % 97);
        let snippet = format!(
            "How to configure, deploy and monitor component {} with retries, timeouts and \
             structured logging; see also page {}.",
            i % 97,
            (i * 7) % 5_000
        );
        w.upsert_node(&DocRecord {
            url: &url,
            url_key: UrlKey::of(&url),
            content_hash: i.wrapping_mul(0x9E37_79B9_7F4A_7C15),
            fetched_at_ms: 1_700_000_000_000 + i,
            title: Some(&title),
            snippet: Some(&snippet),
            etag: None,
            pinned: false,
        })
        .unwrap();
    }
    w.commit().unwrap();
    drop(w);
    store.compact().unwrap();
    let total =
        |m: &Mem| -> u64 { block_on(m.list("kb/")).unwrap().iter().map(|m| m.size).sum::<u64>() };
    let (plain, enc) = (Mem::default(), Mem::default());
    block_on(Mirror::new(&plain, "kb").unwrap().publish(&store)).unwrap();
    block_on(Mirror::new(&enc, "kb").unwrap().encode_bases(ContentEncoding::Gzip).publish(&store))
        .unwrap();
    let (p, e) = (total(&plain), total(&enc));
    eprintln!(
        "mirror, 5000 docs: published {p} B plain, {e} B with encoded bases ({:.1}%)",
        e as f64 * 100.0 / p as f64
    );
    let mut g = c.benchmark_group("mirror_5000_docs");
    g.sample_size(10);
    g.bench_function("publish_full_plain", |b| {
        b.iter(|| block_on(Mirror::new(&Mem::default(), "kb").unwrap().publish(&store)).unwrap());
    });
    g.bench_function("publish_full_gzip_base", |b| {
        b.iter(|| {
            let m = Mem::default();
            block_on(
                Mirror::new(&m, "kb").unwrap().encode_bases(ContentEncoding::Gzip).publish(&store),
            )
            .unwrap()
        });
    });
    g.finish();
}

criterion_group!(benches, object, mirror_bytes);
criterion_main!(benches);
