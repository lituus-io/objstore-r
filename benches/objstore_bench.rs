// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::executor::block_on;
use objstore_r::{Bytes, Cond, LocalDir, Mem, ObjectStore};

fn body(n: usize) -> Bytes {
    (0..n).map(|i| (i % 251) as u8).collect::<Vec<u8>>().into()
}

fn bench_mem(c: &mut Criterion) {
    let mut g = c.benchmark_group("mem");
    for size in [1 << 10, 1 << 20] {
        let s = Mem::default();
        let b = body(size);
        block_on(s.put("obj", b.clone(), Cond::None)).unwrap();
        g.throughput(Throughput::Bytes(size as u64));
        g.bench_with_input(BenchmarkId::new("get", size), &size, |bn, _| {
            bn.iter(|| block_on(s.get("obj")).unwrap())
        });
        g.bench_with_input(BenchmarkId::new("put_cas", size), &size, |bn, _| {
            bn.iter(|| {
                let gen = block_on(s.head("obj")).unwrap().unwrap().generation;
                block_on(s.put("obj", b.clone(), Cond::IfGeneration(gen))).unwrap()
            })
        });
    }
    g.finish();
}

fn bench_local(c: &mut Criterion) {
    let mut g = c.benchmark_group("local_dir");
    let dir = tempfile::tempdir().unwrap();
    let s = LocalDir::open(dir.path()).unwrap();
    for size in [1 << 10, 1 << 20] {
        let b = body(size);
        block_on(s.put("obj", b.clone(), Cond::None)).unwrap();
        g.throughput(Throughput::Bytes(size as u64));
        g.bench_with_input(BenchmarkId::new("get", size), &size, |bn, _| {
            bn.iter(|| block_on(s.get("obj")).unwrap())
        });
        g.bench_with_input(BenchmarkId::new("range_4k", size), &size, |bn, _| {
            bn.iter(|| block_on(s.get_range("obj", 0..4096)).unwrap())
        });
        g.bench_with_input(BenchmarkId::new("put_fsync", size), &size, |bn, _| {
            bn.iter(|| block_on(s.put("obj", b.clone(), Cond::None)).unwrap())
        });
    }
    let n = 200;
    for i in 0..n {
        block_on(s.put(&format!("list/{i:04}"), Bytes::from_static(b"x"), Cond::None)).unwrap();
    }
    g.bench_function("list_200", |bn| bn.iter(|| block_on(s.list("list/")).unwrap()));
    g.finish();
}

criterion_group!(benches, bench_mem, bench_local);
criterion_main!(benches);
