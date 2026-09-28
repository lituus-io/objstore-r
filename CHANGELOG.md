# Changelog

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
