// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Python bindings for `objstore-r` (PyO3 + maturin): `pip install
//! uu-objstore-r`, `import objstore_r`.
//!
//! - One [`PyStore`] class over an enum of backends (memory, local directory,
//!   Google Cloud Storage) — static dispatch, no trait objects.
//! - Every call detaches from the interpreter; the class is usable from any
//!   thread (`asyncio.to_thread`, thread pools).
//! - Reads are zero-copy: [`PyBlob`] exports the downloaded buffer through
//!   the buffer protocol, so `memoryview(blob)` sees the very bytes the
//!   backend produced, and a `Blob` passed back to `put` is not copied.
//! - GCS requests run on a small private runtime; memory and local-directory
//!   operations complete synchronously without one.
//! - `put(..., content_encoding="gzip")` stores an object compressed; `get`
//!   decodes it once, in Rust, into the buffer the `Blob` then shares, so the
//!   zero-copy view is of the original bytes.
//!
//! The only `unsafe` is the buffer export (`__getbuffer__`), documented at
//! the call.

#![deny(unsafe_op_in_unsafe_fn)]

use objstore::token::{Adc, Static};
use objstore::{
    Bytes, Cond, ContentEncoding, Gcs, GcsConfig, LocalDir, Mem, ObjectStore, Stat,
    MAX_INFLATED_BYTES,
};
use pyo3::buffer::PyBuffer;
use pyo3::create_exception;
use pyo3::exceptions::{PyBufferError, PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::future::Future;
use std::os::raw::{c_int, c_void};
use std::time::Duration;

create_exception!(objstore_r, ObjstoreError, PyException, "Base class of every objstore_r error.");
create_exception!(objstore_r, NotFoundError, ObjstoreError, "A ranged read of a missing object.");
create_exception!(
    objstore_r,
    ConflictError,
    ObjstoreError,
    "`create_only` found an existing object."
);
create_exception!(
    objstore_r,
    PreconditionFailedError,
    ObjstoreError,
    "`if_generation` found another generation (or no object)."
);
create_exception!(objstore_r, InvalidKeyError, ObjstoreError, "The key or prefix is not valid.");
create_exception!(objstore_r, UnavailableError, ObjstoreError, "The store could not be reached.");
create_exception!(objstore_r, AuthError, ObjstoreError, "Authentication or authorization failed.");
create_exception!(
    objstore_r,
    TooLargeError,
    ObjstoreError,
    "The object exceeds the configured size limit."
);
create_exception!(objstore_r, ProtocolError, ObjstoreError, "The store answered unexpectedly.");
create_exception!(
    objstore_r,
    EncodedRangeError,
    ObjstoreError,
    "A ranged read of an object stored with a content encoding: read it whole with `get`."
);
create_exception!(
    objstore_r,
    UnsupportedEncodingError,
    ObjstoreError,
    "An object is stored with a content encoding this build cannot decode."
);

fn map_err(e: objstore::Error) -> PyErr {
    use objstore::Error as E;
    let msg = e.to_string();
    match e {
        E::NotFound => NotFoundError::new_err(msg),
        E::Conflict => ConflictError::new_err(msg),
        E::PreconditionFailed => PreconditionFailedError::new_err(msg),
        E::InvalidKey { .. } => InvalidKeyError::new_err(msg),
        E::Unavailable { .. } => UnavailableError::new_err(msg),
        E::Auth { .. } => AuthError::new_err(msg),
        E::TooLarge { .. } => TooLargeError::new_err(msg),
        E::Protocol { .. } => ProtocolError::new_err(msg),
        E::EncodedRange { .. } => EncodedRangeError::new_err(msg),
        E::UnsupportedEncoding { .. } => UnsupportedEncodingError::new_err(msg),
        _ => ObjstoreError::new_err(msg),
    }
}

/// Object bytes, shared with the backend buffer — never copied on the way to
/// Python. Read it with `memoryview(blob)` (zero-copy), `bytes(blob)` (one
/// copy), `len(blob)`, or compare it with any bytes-like value.
#[pyclass(name = "Blob", frozen, module = "objstore_r")]
struct PyBlob {
    bytes: Bytes,
    /// The object's generation (`None` for ranged reads).
    #[pyo3(get)]
    generation: Option<u64>,
}

#[pymethods]
impl PyBlob {
    fn __len__(&self) -> usize {
        self.bytes.len()
    }

    fn __bytes__<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.bytes)
    }

    /// The contents as `bytes` (one copy).
    fn to_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.bytes)
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        if let Ok(b) = other.cast::<PyBlob>() {
            return self.bytes == b.get().bytes;
        }
        PyBuffer::<u8>::get(other).is_ok_and(|buf| {
            buf.len_bytes() == self.bytes.len()
                && buf.to_vec(other.py()).is_ok_and(|v| v == self.bytes.as_ref())
        })
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.bytes.hash(&mut h);
        h.finish()
    }

    fn __repr__(&self) -> String {
        match self.generation {
            Some(g) => format!("Blob(len={}, generation={g})", self.bytes.len()),
            None => format!("Blob(len={})", self.bytes.len()),
        }
    }

    /// Export the bytes read-only through the buffer protocol.
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut pyo3::ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if flags & pyo3::ffi::PyBUF_WRITABLE != 0 {
            return Err(PyBufferError::new_err("Blob is read-only"));
        }
        let bytes = &slf.get().bytes;
        let len = pyo3::ffi::Py_ssize_t::try_from(bytes.len())
            .map_err(|_| PyBufferError::new_err("too large"))?;
        // SAFETY: `view` is the caller-provided buffer struct. The exported
        // pointer is into `Bytes` owned by this frozen object, which is never
        // mutated and cannot be dropped while the view exists:
        // PyBuffer_FillInfo stores a new strong reference to `slf` in
        // `view.obj`, released by the interpreter with the view. The buffer
        // is exported read-only (`readonly = 1`).
        let rc = unsafe {
            pyo3::ffi::PyBuffer_FillInfo(
                view,
                slf.as_ptr(),
                bytes.as_ptr() as *mut c_void,
                len,
                1,
                flags,
            )
        };
        if rc == -1 {
            return Err(PyErr::fetch(slf.py()));
        }
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut pyo3::ffi::Py_buffer) {}
}

/// An object's metadata.
#[pyclass(name = "Meta", frozen, eq, module = "objstore_r")]
#[derive(PartialEq, Eq)]
struct PyMeta {
    /// The full key.
    #[pyo3(get)]
    key: String,
    /// Size in bytes.
    #[pyo3(get)]
    size: u64,
    /// Current generation (never zero; changes on every write).
    #[pyo3(get)]
    generation: u64,
    /// How the stored bytes are encoded (`"gzip"`), or `None` as written.
    /// For an encoded object `size` counts the stored, encoded bytes.
    #[pyo3(get)]
    content_encoding: Option<&'static str>,
}

#[pymethods]
impl PyMeta {
    fn __repr__(&self) -> String {
        let enc = self.content_encoding.map(|e| format!(", content_encoding={e:?}"));
        format!(
            "Meta(key={:?}, size={}, generation={}{})",
            self.key,
            self.size,
            self.generation,
            enc.unwrap_or_default()
        )
    }
}

impl From<Stat> for PyMeta {
    fn from(s: Stat) -> Self {
        let m = s.meta;
        Self {
            key: m.key,
            size: m.size,
            generation: m.generation,
            content_encoding: s.content_encoding.map(ContentEncoding::as_str),
        }
    }
}

enum Backend {
    Mem(Mem),
    Local(LocalDir),
    // Boxed only to keep the enum small; still static dispatch.
    Gcs(Box<Gcs<Adc>>),
}

/// Run one operation on whichever backend, with the backend bound as `$s`.
macro_rules! on {
    ($self:expr, $s:ident => $op:expr) => {
        match &$self.backend {
            Backend::Mem($s) => $self.block($op),
            Backend::Local($s) => $self.block($op),
            Backend::Gcs(g) => {
                let $s: &Gcs<Adc> = g;
                $self.block($op)
            }
        }
    };
}

/// An object store: `Store.memory()`, `Store.local(path)` or `Store.gcs(bucket)`.
///
/// Keys are `/`-separated segments of `[A-Za-z0-9._-]` (no empty segment, no
/// segment starting or ending with a dot, at most 1024 bytes); anything else
/// raises `InvalidKeyError` before any I/O. Every write returns the object's
/// new generation; pass it back as `if_generation` for compare-and-swap.
#[pyclass(name = "Store", frozen, module = "objstore_r")]
struct PyStore {
    backend: Backend,
    rt: Option<tokio::runtime::Runtime>,
    desc: String,
}

impl PyStore {
    fn block<F: Future>(&self, f: F) -> F::Output {
        match &self.rt {
            Some(rt) => rt.block_on(f),
            None => futures_executor::block_on(f),
        }
    }
}

/// Bytes to upload: a `Blob` is shared as is; any other bytes-like value is
/// copied once (it may be mutable and must outlive the upload).
fn body(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Ok(b) = data.cast::<PyBlob>() {
        return Ok(b.get().bytes.clone());
    }
    let buf = PyBuffer::<u8>::get(data)
        .map_err(|_| PyValueError::new_err("data must be bytes, bytearray, memoryview or Blob"))?;
    Ok(Bytes::from(buf.to_vec(py)?))
}

/// The `content_encoding=` argument: `None`, `"gzip"`, or `"identity"` (the
/// same as `None`). Anything else is a `ValueError` before any I/O.
fn encoding(token: Option<&str>) -> PyResult<Option<ContentEncoding>> {
    match token {
        None => Ok(None),
        Some(t) => ContentEncoding::parse(t).map_err(|_| {
            let shown: String = t.chars().take(64).collect();
            PyValueError::new_err(format!(
                "unsupported content_encoding {shown:?}: use None or \"gzip\""
            ))
        }),
    }
}

fn cond(if_generation: Option<u64>, create_only: bool) -> PyResult<Cond> {
    match (if_generation, create_only) {
        (Some(_), true) => {
            Err(PyValueError::new_err("pass at most one of if_generation and create_only"))
        }
        (Some(0), false) => {
            Err(PyValueError::new_err("generations are never 0; use create_only=True"))
        }
        (Some(g), false) => Ok(Cond::IfGeneration(g)),
        (None, true) => Ok(Cond::CreateOnly),
        (None, false) => Ok(Cond::None),
    }
}

#[pymethods]
impl PyStore {
    /// An in-memory store (tests, caches). Generations increase across the
    /// whole store, like GCS.
    /// `max_inflated` bounds the decoded size of an encoded object (a
    /// decompression bomb raises `TooLargeError`), on every backend.
    #[staticmethod]
    #[pyo3(signature = (*, max_inflated=MAX_INFLATED_BYTES))]
    fn memory(max_inflated: u64) -> Self {
        let mem = Mem::default().with_max_inflated(max_inflated);
        Self { backend: Backend::Mem(mem), rt: None, desc: "Store.memory()".into() }
    }

    /// A store in a local directory (created if missing). Writes are atomic
    /// (temporary + fsync + rename); compare-and-swap is atomic across threads
    /// and processes on the host; paths never escape the directory.
    #[staticmethod]
    #[pyo3(signature = (root, *, max_inflated=MAX_INFLATED_BYTES))]
    fn local(py: Python<'_>, root: std::path::PathBuf, max_inflated: u64) -> PyResult<Self> {
        let desc = format!("Store.local({:?})", root.display().to_string());
        let dir = py.detach(|| LocalDir::open(&root)).map_err(map_err)?;
        let dir = dir.with_max_inflated(max_inflated);
        Ok(Self { backend: Backend::Local(dir), rt: None, desc })
    }

    /// A Google Cloud Storage bucket. Credentials: Application Default
    /// Credentials by default (`gcloud auth application-default login`, or
    /// the metadata server on Cloud Run / GCE / GKE); `token=` for a fixed
    /// bearer token; `anonymous=True` for an emulator. `endpoint` must be
    /// https except on loopback (e.g. fake-gcs-server).
    #[staticmethod]
    #[pyo3(signature = (bucket, *, token=None, anonymous=false, endpoint=None, timeout=60.0, max_retries=5, max_object_bytes=4 << 30, resumable_threshold=8 << 20, max_inflated=MAX_INFLATED_BYTES))]
    #[allow(clippy::too_many_arguments)]
    fn gcs(
        py: Python<'_>,
        bucket: &str,
        token: Option<String>,
        anonymous: bool,
        endpoint: Option<String>,
        timeout: f64,
        max_retries: u32,
        max_object_bytes: u64,
        resumable_threshold: usize,
        max_inflated: u64,
    ) -> PyResult<Self> {
        if token.is_some() && anonymous {
            return Err(PyValueError::new_err("pass at most one of token and anonymous"));
        }
        if !(timeout.is_finite() && timeout > 0.0) {
            return Err(PyValueError::new_err("timeout must be a positive number of seconds"));
        }
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("objstore-r")
            .enable_all()
            .build()
            .map_err(|e| ObjstoreError::new_err(format!("runtime: {e}")))?;
        let mut cfg = GcsConfig::default();
        if let Some(e) = endpoint {
            cfg.endpoint = e;
        }
        cfg.timeout = Duration::from_secs_f64(timeout);
        cfg.max_retries = max_retries;
        cfg.max_object_bytes = max_object_bytes;
        cfg.resumable_threshold = resumable_threshold;
        let desc = format!("Store.gcs({bucket:?})");
        let gcs = py
            .detach(|| {
                let tokens = match (token, anonymous) {
                    (Some(t), _) => Adc::Static(Static::new(t)),
                    (None, true) => Adc::Static(Static::none()),
                    (None, false) => {
                        let _guard = rt.enter(); // the metadata client may need a reactor
                        Adc::discover()?
                    }
                };
                let _guard = rt.enter();
                Gcs::new(bucket, tokens, cfg).map(|g| g.with_max_inflated(max_inflated))
            })
            .map_err(map_err)?;
        Ok(Self { backend: Backend::Gcs(Box::new(gcs)), rt: Some(rt), desc })
    }

    /// `"memory"`, `"local"` or `"gcs"`.
    #[getter]
    fn kind(&self) -> &'static str {
        match self.backend {
            Backend::Mem(_) => "memory",
            Backend::Local(_) => "local",
            Backend::Gcs(_) => "gcs",
        }
    }

    fn __repr__(&self) -> String {
        self.desc.clone()
    }

    /// The object and its generation, or `None` when it does not exist. An
    /// encoded object is decoded once, into the buffer the `Blob` shares.
    fn get(&self, py: Python<'_>, key: &str) -> PyResult<Option<PyBlob>> {
        let got = py.detach(|| on!(self, s => s.get(key))).map_err(map_err)?;
        Ok(got.map(|o| PyBlob { bytes: o.bytes, generation: Some(o.generation) }))
    }

    /// Bytes `[start, end)` of an object, clipped to its size (an empty Blob
    /// past the end). Raises `NotFoundError` for a missing object and
    /// `EncodedRangeError` for an encoded one (read those whole).
    fn get_range(&self, py: Python<'_>, key: &str, start: u64, end: u64) -> PyResult<PyBlob> {
        if end < start {
            return Err(PyValueError::new_err("end must be >= start"));
        }
        let bytes = py.detach(|| on!(self, s => s.get_range(key, start..end))).map_err(map_err)?;
        Ok(PyBlob { bytes, generation: None })
    }

    /// Several objects fetched concurrently, in order (`None` for missing).
    fn get_many(&self, py: Python<'_>, keys: Vec<String>) -> PyResult<Vec<Option<PyBlob>>> {
        let got = py
            .detach(|| {
                on!(self, s => futures_util::future::try_join_all(keys.iter().map(|k| s.get(k))))
            })
            .map_err(map_err)?;
        Ok(got
            .into_iter()
            .map(|o| o.map(|o| PyBlob { bytes: o.bytes, generation: Some(o.generation) }))
            .collect())
    }

    /// Metadata only, or `None` when the object does not exist.
    fn head(&self, py: Python<'_>, key: &str) -> PyResult<Option<PyMeta>> {
        Ok(py.detach(|| on!(self, s => s.stat(key))).map_err(map_err)?.map(PyMeta::from))
    }

    /// Write an object; returns its new generation. `create_only=True` fails
    /// with `ConflictError` if it exists; `if_generation=g` fails with
    /// `PreconditionFailedError` unless the current generation is `g`.
    /// `content_encoding="gzip"` stores the bytes compressed (for objects read
    /// whole: `get` decodes, `get_range` refuses); `None` stores them as given.
    #[pyo3(signature = (key, data, *, if_generation=None, create_only=false, content_encoding=None))]
    fn put(
        &self,
        py: Python<'_>,
        key: &str,
        data: &Bound<'_, PyAny>,
        if_generation: Option<u64>,
        create_only: bool,
        content_encoding: Option<&str>,
    ) -> PyResult<u64> {
        let cond = cond(if_generation, create_only)?;
        let encoding = encoding(content_encoding)?;
        let body = body(py, data)?;
        py.detach(|| match encoding {
            None => on!(self, s => s.put(key, body.clone(), cond)),
            Some(e) => on!(self, s => s.put_encoded(key, body.clone(), cond, e)),
        })
        .map_err(map_err)
    }

    /// Objects whose key starts with `prefix`, sorted by key. `"dir/"` lists
    /// under a directory; `""` lists everything.
    #[pyo3(signature = (prefix=""))]
    fn list(&self, py: Python<'_>, prefix: &str) -> PyResult<Vec<PyMeta>> {
        let metas = py.detach(|| on!(self, s => s.list_stat(prefix))).map_err(map_err)?;
        Ok(metas.into_iter().map(PyMeta::from).collect())
    }

    /// Delete an object (a missing one is fine).
    fn delete(&self, py: Python<'_>, key: &str) -> PyResult<()> {
        py.detach(|| on!(self, s => s.delete(key))).map_err(map_err)
    }

    /// Memory stores only: make the operation `after` calls from now fail —
    /// before applying (`lost_response=False`) or after applying with the
    /// reply lost (`lost_response=True`). For testing retry logic.
    #[pyo3(signature = (*, after=0, lost_response=false))]
    fn inject_failure(&self, after: u64, lost_response: bool) -> PyResult<()> {
        let Backend::Mem(m) = &self.backend else {
            return Err(PyValueError::new_err(
                "fault injection is only available on Store.memory()",
            ));
        };
        let e =
            objstore::Error::Unavailable { message: "injected failure".into(), retriable: true };
        m.inject_after(
            after,
            if lost_response {
                objstore::mem::Fault::LostResponse(e)
            } else {
                objstore::mem::Fault::Fail(e)
            },
        );
        Ok(())
    }
}

/// Validate a key without touching any store (raises `InvalidKeyError`).
#[pyfunction]
fn check_key(key: &str) -> PyResult<()> {
    objstore::Key::parse(key).map(|_| ()).map_err(map_err)
}

#[pymodule]
fn objstore_r(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<PyStore>()?;
    m.add_class::<PyBlob>()?;
    m.add_class::<PyMeta>()?;
    m.add_function(wrap_pyfunction!(check_key, m)?)?;
    m.add("ObjstoreError", py.get_type::<ObjstoreError>())?;
    m.add("NotFoundError", py.get_type::<NotFoundError>())?;
    m.add("ConflictError", py.get_type::<ConflictError>())?;
    m.add("PreconditionFailedError", py.get_type::<PreconditionFailedError>())?;
    m.add("InvalidKeyError", py.get_type::<InvalidKeyError>())?;
    m.add("UnavailableError", py.get_type::<UnavailableError>())?;
    m.add("AuthError", py.get_type::<AuthError>())?;
    m.add("TooLargeError", py.get_type::<TooLargeError>())?;
    m.add("ProtocolError", py.get_type::<ProtocolError>())?;
    m.add("EncodedRangeError", py.get_type::<EncodedRangeError>())?;
    m.add("UnsupportedEncodingError", py.get_type::<UnsupportedEncodingError>())?;
    m.add("MAX_INFLATED_BYTES", MAX_INFLATED_BYTES)?;
    Ok(())
}
