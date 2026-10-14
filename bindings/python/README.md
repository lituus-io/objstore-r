# objstore-r

`pip install uu-objstore-r` → `import objstore_r` (Python ≥ 3.12, one `abi3`
wheel per platform).

One small object-store API over **Google Cloud Storage**, a **local
directory** and **memory**, with the primitive that makes object storage
usable as a database: **generation preconditions** (compare-and-swap).

```python
import objstore_r

store = objstore_r.Store.gcs("my-bucket")        # Application Default Credentials
# store = objstore_r.Store.local("/var/data")    # same API on disk
# store = objstore_r.Store.memory()              # same API in memory (tests)

gen = store.put("state/HEAD", b"v1", create_only=True)
blob = store.get("state/HEAD")                   # Blob, zero-copy
view = memoryview(blob)                          # no copy of the downloaded bytes
store.put("state/HEAD", b"v2", if_generation=blob.generation)   # CAS
try:
    store.put("state/HEAD", b"v3", if_generation=gen)           # stale
except objstore_r.PreconditionFailedError:
    ...
```

- **Compare-and-swap everywhere.** Every write returns the object's new
  generation; `if_generation=` and `create_only=` make writes conditional —
  on GCS (`ifGenerationMatch`), on disk (atomic across threads and
  processes), and in memory.
- **Zero-copy reads.** `Blob` exports the downloaded buffer through the
  buffer protocol; pass a `Blob` back to `put` and it is not copied either.
- **Ranged and concurrent reads.** `get_range(key, start, end)`;
  `get_many(keys)` fetches concurrently.
- **Opt-in compression.** `put(key, data, content_encoding="gzip")` stores an
  object compressed (on GCS as its `contentEncoding`); `get` decodes it once,
  in Rust, and the `Blob` is zero-copy from there. `Meta.content_encoding`
  reports it from `head` and `list`. A ranged read of an encoded object raises
  `EncodedRangeError`; decoding stops at `max_inflated` bytes (1 GiB by
  default, a keyword on every constructor) with `TooLargeError`.
- **Safe keys.** `/`-separated segments of `[A-Za-z0-9._-]`, validated before
  any I/O: no traversal, no hidden files, portable to every OS
  (`InvalidKeyError` otherwise).
- **Production GCS client.** Resumable uploads above 8 MiB, ranged
  downloads, retries with jittered backoff on 408/429/5xx (never on
  conflicts), lost-response detection on conditional writes, response size
  limits, no redirects, TLS required except for a loopback emulator. Tokens
  are cached and never appear in errors or reprs.
- **Credentials.** Application Default Credentials by default (gcloud user
  credentials or the metadata server on Cloud Run / GCE / GKE; service-account
  key files are deliberately unsupported), `token=` for a fixed bearer token,
  `anonymous=True` for fake-gcs-server.
- **Thread-safe.** Every call releases the GIL; use a thread pool or
  `asyncio.to_thread` for concurrency.

## Errors

All derive from `ObjstoreError`: `NotFoundError` (ranged read of a missing
object; `get`/`head` return `None` instead), `ConflictError`,
`PreconditionFailedError`, `InvalidKeyError`, `UnavailableError`,
`AuthError`, `TooLargeError`, `ProtocolError`, `EncodedRangeError` (ranged
read of an encoded object), `UnsupportedEncodingError` (an object stored with
an encoding this build cannot decode). An unknown `content_encoding=`
argument is a `ValueError` before any I/O.

## Testing your code

`Store.memory().inject_failure(after=n, lost_response=True)` makes the n-th
next operation fail — before applying, or after applying with the reply lost
— to exercise retry paths.

## Licence

AGPL-3.0-or-later, or a commercial licence — see the repository.
