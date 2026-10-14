# Changelog

## 0.2.3

- Opt-in content encoding (feature `gzip`, flate2 on the pure-Rust
  miniz_oxide backend): `ObjectStore::put_encoded` stores a body
  gzip-compressed and records the encoding — on GCS as the object's
  `contentEncoding`, on `LocalDir` in a sidecar tree no key can name, in `Mem`
  beside the bytes. `get` decodes once into the returned `Bytes`; plain writes
  are byte-for-byte unchanged.
- GCS reads ask for the stored bytes (`Accept-Encoding: gzip`, only in a build
  that can decode them), verify them against the object's CRC-32C from
  `x-goog-hash` before decoding, then inflate them once.
- `get_range` of an encoded object is `Error::EncodedRange`, never compressed
  bytes. An unknown encoding is `Error::UnsupportedEncoding`.
- Decompression bombs: decoding stops at `MAX_INFLATED_BYTES` (1 GiB),
  adjustable per store with `with_max_inflated` (`Error::TooLarge`); a
  truncated or damaged stream is `Error::Corrupt`.
- `ObjectStore::stat` / `list_stat` return `Stat { meta, content_encoding }`;
  defaulted trait methods, so other implementations keep compiling.
- `mirror`: `Mirror::encode_bases` publishes immutable bases compressed; log
  segments and `HEAD` stay plain, and replicas need no setting. A realistic
  5,000-page store published 19.4% of the bytes.
- `fenced::put_immutable_encoded`.
- Python: `put(..., content_encoding="gzip")`, `Meta.content_encoding`,
  `EncodedRangeError`, `UnsupportedEncodingError`, `MAX_INFLATED_BYTES`, and a
  `max_inflated=` keyword on `Store.memory`, `Store.local` and `Store.gcs`.
- New fuzz target `fuzz_encoding`; the GCS wire fuzz target also covers the
  encoding fields and `x-goog-hash`.

## 0.2.2

- Release: the semver check compares against the published crate on
  crates.io; 0.2.1's check could not resolve the graph-r path dependency in
  its git baseline, so 0.2.1 reached crates.io only. 0.2.2 is the same code
  and lockfiles, released to every channel.

## 0.2.1

- Security: every lockfile (crate, Python binding, fuzz) refreshed to
  patched releases, among them rustls 0.23.46 (RUSTSEC-2026-0346, -0347,
  -0348, -0349) and the earlier rustls and quinn-proto fixes
  (RUSTSEC-2026-0285, -0338 to -0342). The PyPI wheels are built from these.
- The Security workflow audits all three lockfiles, not only the crate's.
- Builds against uu-graph-r 0.4.1.

## 0.2.0

- Published as **uu-objstore-r** on crates.io (the library is still
  `objstore_r`).
- Feature `mirror` (`objstore_r::mirror`, formerly the graph-r-mirror crate):
  sync a graph-r store with an object store.
- Builds on graph-r's core (atomic files, locks, clock, xxh3).
- `fenced`: the single-writer publish protocol (`put_immutable`, `cas`,
  `fetch_verified`, sealed records), shared by graph-r-mirror and
  link-r-objstore; `Error::{Fenced, Corrupt}`.
- `transport::ApiError` and `Response::api_error`: Google error envelopes
  (reason, message, code) for every client of the stack.
- `sse::SseDecoder` (from crab-llm), `join::{join_all, spin_on}` (from
  crab-core), and a public `net` policy (`is_retriable_status`, `backoff`,
  `retry_after` in both forms).
- `testing`: `unique`, `clean`, `live_bucket`, `emulator_bucket`, and a 9 MiB
  resumable-upload case in the conformance suite.

- `transport::Transport` / `BodyStream`: a dependency-free GAT seam for
  authorised HTTPS calls to Google REST APIs beyond Cloud Storage (Vertex AI,
  BigQuery). `send` returns the whole bounded body; `open` streams chunks, and
  dropping the stream cancels the request. Per-request `Retry` policy
  (`Transient`, `Throttled` for non-idempotent calls, `Never`);
  `Response::error_for_status` maps statuses to the crate's typed errors.
- `http::Client<T: TokenProvider>` (new feature `http`): the reqwest/rustls
  implementation — HTTPS only (loopback `http` for tests), redirects refused,
  bounded bodies (zero-copy for single-chunk responses), bearer and
  quota-project headers, jittered backoff honouring `Retry-After` (capped at
  30 s). Caller-supplied `authorization` headers are refused.
- Feature `gcs` now implies `http`; the `token` module is available with
  `http` alone. GCS behaviour is unchanged: its retry, backoff and error
  mapping moved to a shared internal module used by both clients.
- `key::encode_component` (also re-exported from `transport`) is public and
  no longer feature-gated.
- `transport::Method::Patch` and `Request::patch_json` (retried only when the
  server provably did not start the work; pair with an `If-Match` header).
- `http::block_on` (new feature `blocking`): drive the transport from a
  synchronous `main` on a single-threaded runtime, so command-line tools need
  no async runtime dependency of their own.

## 0.1.0 — unreleased

First release.

- `ObjectStore` GAT trait with generation preconditions (`Cond::None`,
  `CreateOnly`, `IfGeneration`), zero-copy `Bytes` bodies, ranged reads,
  sorted listing.
- Portable key grammar (`Key::parse`), validated before any I/O.
- Backends: `Mem` (fault injection), `LocalDir` (atomic rename, fsync,
  mtime generations, cross-process CAS, symlink containment), `Gcs` (feature
  `gcs`: JSON API, resumable uploads, retries with jittered backoff, lost-
  response resolution, size caps, no redirects, TLS-only outside loopback).
- Token providers: `Static`, `Metadata`, `GcloudUser`, `Adc::discover`.
- `testing::conformance` suite (feature `testing`).
- `Mem::inject_after` / `Fault::Pass` / `clear_faults` for step-targeted
  crash tests.
- Python bindings: **uu-objstore-r** (`import objstore_r`, abi3 wheel for
  Python ≥ 3.12). One `Store` class over memory, local-directory and GCS
  backends (ADC, token or anonymous), compare-and-swap writes, ranged and
  concurrent reads, zero-copy `Blob` buffers, typed exceptions, GIL
  released on every call; tested on memory, local directory, fake-gcs-server
  and a live bucket. Release workflow builds and publishes the wheels.
