// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// The per-request cost of a request policy: an allow-list check on a parsed
// URL (hit and miss), and, for scale, the URL parse the client does anyway.
// Budget: under one microsecond per check.

use criterion::{criterion_group, criterion_main, Criterion};
use objstore_r::policy::{AllowList, RequestPolicy, Rule, Url};
use objstore_r::transport::Method;
use std::hint::black_box;

const BQ: &str = "bigquery.googleapis.com";
const RULES: &[Rule<'static>] = &[
    Rule::new(Method::Get, BQ, "/bigquery/v2/projects/{project}/datasets/{dataset}"),
    Rule::new(Method::Get, BQ, "/bigquery/v2/projects/{project}/datasets/{dataset}/tables/{table}"),
    Rule::new(Method::Post, BQ, "/bigquery/v2/projects/{project}/queries"),
    Rule::new(Method::Get, BQ, "/bigquery/v2/projects/{project}/queries/{job}"),
    Rule::new(
        Method::Get,
        "datalineage.googleapis.com",
        "/v1/projects/{p}/locations/{l}:searchLinks",
    ),
    Rule::new(Method::Get, "storage.googleapis.com", "/storage/v1/b/{bucket}/o/{object}"),
];

fn bench_policy(c: &mut Criterion) {
    let mut g = c.benchmark_group("policy");
    let list = AllowList::new(RULES).unwrap();
    let hit = Url::parse("https://bigquery.googleapis.com/bigquery/v2/projects/my-project/datasets/analytics/tables/events?view=BASIC").unwrap();
    let miss = Url::parse("https://bigquery.googleapis.com/bigquery/v2/projects/my-project/datasets/analytics/tables/events/data").unwrap();
    g.bench_function("allow_list_hit", |b| {
        b.iter(|| list.check(black_box(Method::Get), black_box(&hit)))
    });
    g.bench_function("allow_list_miss", |b| {
        b.iter(|| list.check(black_box(Method::Get), black_box(&miss)))
    });
    g.bench_function("url_parse", |b| b.iter(|| Url::parse(black_box(hit.as_str()))));
    g.finish();
}

criterion_group!(benches, bench_policy);
criterion_main!(benches);
