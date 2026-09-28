# objstore-r

A small object-store abstraction shared by [graph-r](../graph-r) and
[link-r](../link-r): one GAT trait, three backends, generation preconditions
everywhere. It exists so both libraries can keep their on-disk formats
byte-for-byte and still live on Google Cloud Storage: an object holds exactly
the bytes of a local file, and moving a store between disk and a bucket is a
plain copy.

```toml
objstore-r = { path = "../objstore-r" }                     # LocalDir + Mem, no HTTP stack
objstore-r = { path = "../objstore-r", features = ["gcs"] } # + Google Cloud Storage
```

```rust
use objstore_r::{Bytes, Cond, Mem, ObjectStore};

# futures::executor::block_on(async {
let s = Mem::default();
let g1 = s.put("HEAD", Bytes::from_static(b"v1"), Cond::CreateOnly).await?;
// Compare-and-swap: only succeeds if nobody wrote since we read g1.
let g2 = s.put("HEAD", Bytes::from_static(b"v2"), Cond::IfGeneration(g1)).await?;
assert!(s.put("HEAD", Bytes::from_static(b"v3"), Cond::IfGeneration(g1)).await.is_err());
# assert_ne!(g1, g2);
# Ok::<(), objstore_r::Error>(()) }).unwrap();
```

## The trait

| Method | Returns | Notes |
|---|---|---|
| `get(key)` | `Option<Object{bytes, generation}>` | body and generation from one consistent read |
| `get_range(key, a..b)` | `Bytes` | clipped to the object; a missing key is `NotFound` |
| `head(key)` | `Option<Meta{key, size, generation}>` | metadata only |
| `put(key, body, cond)` | new generation | `Cond::None`, `CreateOnly`, `IfGeneration(g)` |
| `list(prefix)` | `Vec<Meta>` sorted by key | `"dir/"` lists under a directory, `"dir"` also matches `dir…` |
| `delete(key)` | `()` | idempotent |

Futures are named associated types (GAT + TAIT), never boxed; the trait has no
`Send` bound, and a backend's futures are `Send` whenever its state is, so both
single-threaded and work-stealing executors work. Bodies are `bytes::Bytes`
end to end: no copies between the network and the caller.

**Keys** are `/`-separated segments of `[A-Za-z0-9._-]`, at most 1024 bytes,
no empty segments, no segment starting or ending with a dot (dot-names are
reserved for backend internals), no Windows device names. Every key is
therefore a safe relative path on every OS and needs no escaping beyond
percent-encoding for URLs. Anything else is `Error::InvalidKey` before any I/O.

## Backends

- **`Mem`** — deterministic and in-process, with scripted fault injection
  (`Fault::Fail`, `Fault::LostResponse`: the write lands but the caller sees an
  error), for crash and retry tests of code built on top.
- **`LocalDir`** — a directory. Writes go to a unique temporary file, are
  fsynced and renamed into place, and the directory is fsynced. The generation
  is the file's mtime in nanoseconds, set explicitly and stepped by the
  filesystem's probed timestamp precision, so every write is distinguishable
  even on coarse-timestamp filesystems. Conditional writes are serialized by an
  exclusive `File::lock`, so CAS is atomic across threads and processes on one
  host. Paths never escape the root: symlinks inside the root are refused for
  directories and never followed for objects. A filesystem cannot hold both `k`
  and `k/…`; such writes fail with `InvalidKey`.
- **`Gcs<T: TokenProvider>`** (feature `gcs`) — the JSON API over
  reqwest/rustls with HTTP/2 connection reuse. `ifGenerationMatch` for
  conditions; media GETs with `Range`; single-request uploads up to 8 MiB and
  resumable uploads above; paginated listing. Retries 408/429/5xx and transport
  errors with capped, jittered exponential backoff — never 404 or 412. A lost
  response on a conditional write (it landed, the reply did not) is resolved by
  reading back and comparing bytes, so a retried CAS never reports a spurious
  conflict. Responses are size-capped, redirects are refused, and plain `http`
  is accepted only for a loopback emulator endpoint.

### Credentials (feature `gcs`)

`token::Adc::discover()` follows Application Default Credentials for the cases
that need no private-key signing: `GOOGLE_APPLICATION_CREDENTIALS` or gcloud's
well-known file (user credentials from `gcloud auth application-default
login`), then the metadata server (Cloud Run, GCE, GKE workload identity).
Service-account key files are unsupported on purpose. Applications with their
own credential stack implement the `TokenProvider` GAT trait. Tokens are cached
until a minute before expiry; their `Debug` output is redacted, and no error
message ever contains one.

## Python

`pip install uu-objstore-r` → `import objstore_r`: the same stores and
guarantees from Python (see `bindings/python/README.md`).

```python
import objstore_r
store = objstore_r.Store.gcs("my-bucket")            # ADC; .local(path) / .memory() alike
g = store.put("state/HEAD", b"v1", create_only=True)
store.put("state/HEAD", b"v2", if_generation=g)      # compare-and-swap
view = memoryview(store.get("state/HEAD"))           # zero-copy
```

## Testing

| Layer | Where |
|---|---|
| Unit | every module (`cargo test`) |
| Conformance | `objstore_r::testing::conformance` (feature `testing`) — one suite every backend must pass: `tests/conformance.rs` runs it on `Mem`, `LocalDir`, fake-gcs-server (both upload paths) and, opt-in, a live bucket |
| Property | `tests/proptest_model.rs` — random operation sequences on each backend against a reference model |
| Security | `tests/security.rs` — hostile keys, symlink escapes, nesting collisions, and a scripted HTTP server that redirects, over-sends, fails transiently, loses responses and refuses auth |
| Fuzz | `fuzz/` — key grammar and GCS response parsing (`cargo fuzz run fuzz_key`) |
| Benchmarks | `benches/objstore_bench.rs` (criterion) |
| Supply chain | `cargo deny`, `cargo audit` |

`scripts/ci-local.sh` runs the whole CI locally (it starts fake-gcs-server in
Docker when available). The live test runs only when you export
`OBJSTORE_LIVE_BUCKET` yourself; it writes under a unique prefix and removes it.

## Licence

See `LICENSE`.
