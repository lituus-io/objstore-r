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
