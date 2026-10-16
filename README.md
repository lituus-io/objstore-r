# objstore-r

A small object-store abstraction shared by
[graph-r](https://github.com/lituus-io/graph-r) and
[link-r](https://github.com/lituus-io/link-r): one GAT trait, three backends, generation preconditions
everywhere. It exists so both libraries can keep their on-disk formats
byte-for-byte and still live on Google Cloud Storage: an object holds exactly
the bytes of a local file, and moving a store between disk and a bucket is a
plain copy.

```toml
uu-objstore-r = "0.2"                                      # LocalDir + Mem, no HTTP stack
uu-objstore-r = { version = "0.2", features = ["http"] }   # + authorised HTTPS transport
uu-objstore-r = { version = "0.2", features = ["gcs"] }    # + Google Cloud Storage (implies http)
uu-objstore-r = { version = "0.2", features = ["mirror"] } # + graph-r store mirroring
uu-objstore-r = { version = "0.2", features = ["gzip"] }   # + per-object Content-Encoding: gzip
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
| `put_encoded(key, body, cond, enc)` | new generation | stored compressed (feature `gzip`); `get` decodes, `get_range` is `EncodedRange` |
| `stat(key)` / `list_stat(prefix)` | `Stat{meta, content_encoding}` | `head` / `list` plus the encoding |

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
  conditions; media GETs with `Range`; single-request uploads below 8 MiB and
  chunked resumable uploads at or above it; downloads that resume where a
  dropped connection left them; paginated listing (see [Networks](#networks)).
  Retries 408/429/5xx and transport
  errors with capped, jittered exponential backoff — never 404 or 412. A lost
  response on a conditional write (it landed, the reply did not) is resolved by
  reading back and comparing bytes, so a retried CAS never reports a spurious
  conflict. Responses are size-capped, redirects are refused, and plain `http`
  is accepted only for a loopback emulator endpoint.

### Networks

A large object over a slow or unreliable link is the normal case, not an
edge case: a 200 MiB snapshot over a 20 Mbit/s uplink takes about 80 s, and
corporate proxies and laptop Wi-Fi drop connections. So the GCS store (and
the `http` transport) bound *stalls*, never sizes: no timeout covers a whole
transfer, and every timeout measures time without progress.

| Knob (`GcsConfig`) | Default | Bounds |
|---|---|---|
| `connect_timeout` | 10 s | TCP, TLS and any proxy `CONNECT` |
| `request_timeout` | 30 s | a whole small request (metadata, listing page, delete, session start, session status query); the wait for a download's response headers; the fixed part of an upload request's bound |
| `idle_timeout` | 60 s | the longest a download may receive no bytes |
| `min_throughput` | 64 KiB/s | the slowest upload tolerated: an upload request of `n` bytes may take `request_timeout + n / min_throughput` (8 MiB: 158 s) |
| `max_retries` | 5 | retries of a transient failure; for a resumable transfer, consecutive attempts that make no progress |
| `resumable_threshold` | 8 MiB | bodies at or above it upload through a resumable session |
| `upload_chunk` | 8 MiB | bytes per resumable `PUT`; a multiple of 256 KiB |
| `max_object_bytes` | 4 GiB | the largest body read, across every resumed attempt |

Why idle and not total: a total timeout fails every transfer larger than
`timeout × link speed` whatever the network's health (v0.2.4's 60 s failed a
100 MiB object at 1 MiB/s on every attempt), and retrying it restarts from
byte 0 and fails the same way. An idle timeout fails a dead connection just
as fast and never a slow, healthy one. Uploads cannot observe their own
progress mid-request, so each upload request is bounded by a floor rate
instead, which is why uploads go in chunks.

**Uploads.** Below `resumable_threshold`: one request. At or above it: a
resumable session (preconditions ride on the session start), then one `PUT`
per `upload_chunk` with `Content-Range: bytes a-b/total`, slicing the body
without copying it. A 308 reports the committed length. After a transient
failure (timeout, dropped connection, 408/429/5xx) the store asks the session
for its committed length (`Content-Range: bytes */total`) and sends only the
remainder. A session that has expired (404/410) is restarted once from byte 0;
a second expiry is `Error::Unavailable`. A session URL off the upload
endpoint's origin, or a committed length past the total, backwards or
malformed, is `Error::Protocol`, never a loop. Without failures an upload of
`n` bytes is exactly one session start plus `ceil(n / upload_chunk)` `PUT`s;
each failure adds one status query.

**Downloads.** A whole-object read records the object's generation. If the
body breaks off, the rest is requested with `Range: bytes=<received>-`
pinned by `ifGenerationMatch`; an object replaced in between is
`Error::Changed` (two generations are never spliced). The object's CRC-32C
(`x-goog-hash`) is verified over the assembled bytes before they are
returned. A body served with a `Content-Encoding` cannot resume by range (a
byte range of a gzip stream means nothing): it restarts from byte 0, within
the same `max_retries` budget. `get_range` stays one request, under the idle
timeout.

**Tokens and proxies.** Every request, each chunk included, takes its token
from the provider, which renews it a minute before it expires, so a long
upload never carries a stale one. `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY`
from the environment apply (reqwest's default; the build reads no system
proxy settings beyond the environment).

### Content encoding (feature `gzip`)

Opt in per object: `put_encoded(key, body, cond, ContentEncoding::Gzip)`
stores the body compressed and records the encoding — on GCS as the object's
`contentEncoding` (the `contentEncoding` upload parameter), on `LocalDir` in a
sidecar tree no key can name, in `Mem` beside the bytes. `get` decodes once
into the returned `Bytes`, which is shared by reference count from there.
On GCS every read asks for the stored bytes (`Accept-Encoding: gzip`, so the
server never transcodes) and checks them against the object's CRC-32C
*before* decoding. A ranged read of an encoded object is refused
(`Error::EncodedRange`): a byte range of a gzip stream means nothing. Decoding
stops at `MAX_INFLATED_BYTES` (1 GiB; per store with `with_max_inflated`), so
a small object cannot inflate without bound (`Error::TooLarge`); a damaged or
truncated stream is `Error::Corrupt`. Plain writes are unchanged, byte for
byte. Encode only objects that are read whole.

### Fenced publishing and graph-r mirroring

`fenced` is the single-writer publish protocol every snapshot publisher
shares: create-only immutable objects, a sealed `HEAD` record swapped by
compare-and-swap (a lost response is recognised by reading back; a racing
writer gets `Error::Fenced`), and verified fetches. Feature `mirror` builds
graph-r store mirroring on it (`mirror::Mirror`): byte-identical base and
log files, incremental log shipping, leases, replicas refreshed with one
metadata request when nothing changed. `Mirror::encode_bases(Gzip)` stores
each immutable base compressed (log segments and `HEAD` stay plain); replicas
decode it once into the file graph-r maps, and need no setting.

### Other Google APIs (feature `http`)

`http::Client<T: TokenProvider>` implements the dependency-free
`transport::Transport` trait: authorised HTTPS with the same TLS stack,
tokens, retries and size caps as the GCS backend, for Vertex AI, BigQuery or
any Google REST API. `send` returns a bounded body; `open` streams chunks
(server-sent events) and cancels the request when the stream is dropped.
`HttpConfig::timeout` (60 s) and `stream_timeout` (300 s) bound the wait for
the response headers of `send` and `open` (plus `body / min_throughput` for a
large request body); the body itself, whole or streamed, is bounded by
`idle_timeout` (60 s) between chunks, never by a total.

### Credentials (features `http` / `gcs`)

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
| Transfers | `tests/transfer.rs` — a local double of the JSON API (`tests/support/gcs_mock.rs`) that throttles, drops and stalls connections, expires sessions, replaces objects mid-read and sends hostile range headers; request counts are asserted exactly |
| Live transfers | `tests/transfer_live.rs` — 64 and 300 MiB round trips, 100 MiB each way through a 1 MiB/s CONNECT proxy (`tests/support/throttle_proxy.rs`, chained through `HTTPS_PROXY` when set), and a tunnel killed mid-transfer each way; runs only with `OBJSTORE_GCS_TEST_BUCKET` |
| Fuzz | `fuzz/` — key grammar and GCS response parsing (`cargo fuzz run fuzz_key`) |
| Benchmarks | `benches/objstore_bench.rs` (criterion) |
| Supply chain | `cargo deny`, `cargo audit` |

`scripts/ci-local.sh` runs the whole CI locally (it starts fake-gcs-server in
Docker when available). The live tests run only when you export
`OBJSTORE_LIVE_BUCKET` or `OBJSTORE_GCS_TEST_BUCKET` yourself; they write
under a unique prefix and remove it.

## Licence

See `LICENSE`.
