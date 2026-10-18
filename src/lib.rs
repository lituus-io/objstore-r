// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! # objstore-r
//!
//! Object stores with **generation preconditions**, for data that is published
//! as immutable snapshots plus append-only segments (graph-r, link-r).
//!
//! Every object carries a *generation* (a `u64`, never zero) that changes on
//! each write. Writes take a [`Cond`]:
//!
//! - [`Cond::CreateOnly`]: succeed only if the key does not exist. Immutable
//!   objects (snapshots, log segments) are written this way, so a retry after a
//!   lost response is harmless and a second writer can never clobber the first.
//! - [`Cond::IfGeneration`]: succeed only if the object is still at that
//!   generation. A small `HEAD` object updated this way gives single-writer
//!   compare-and-swap across machines, with no lock service.
//!
//! ## Backends
//!
//! | Backend | Feature | Use |
//! |---|---|---|
//! | [`LocalDir`] | always | a directory; atomic rename, fsync, `File::lock` for conditions |
//! | [`Mem`] | always | tests; deterministic, with scripted fault injection |
//! | `Gcs` | `gcs` | Google Cloud Storage (JSON API), ADC tokens, retries, ranged reads |
//!
//! ## Design tenets
//!
//! - **Named futures, no boxing.** [`ObjectStore`] methods return associated
//!   future types (GATs), so callers compose them without `Box<dyn Future>`
//!   and without `Send`/`'static` bounds imposed by this crate. Futures are
//!   `Send` whenever the backend is, so multi-threaded runtimes work too.
//! - **Enums over `dyn`.** Choose a backend at compile time, or wrap several in
//!   your own enum; nothing here needs a trait object.
//! - **Zero-copy bodies.** Objects are [`bytes::Bytes`]; a body is read or
//!   written once and shared by reference count from there on.
//! - **No unsafe code.**
//!
//! ## Content encoding
//!
//! Opt in per object with [`ObjectStore::put_encoded`] (feature `gzip`): the
//! body is stored compressed and [`get`](ObjectStore::get) decodes it once
//! into the returned buffer, bounded against decompression bombs; a ranged
//! read of an encoded object is refused ([`Error::EncodedRange`]).
//! [`ObjectStore::stat`] and [`ObjectStore::list_stat`] report the encoding.
//! See [`encoding`].
//!
//! ## Example
//!
//! ```
//! use objstore_r::{Cond, Mem, ObjectStore};
//! # futures::executor::block_on(async {
//! let store = Mem::default();
//! let g1 = store.put("snap/0001", "v1".into(), Cond::CreateOnly).await?;
//! assert!(store.put("snap/0001", "again".into(), Cond::CreateOnly).await.is_err());
//! let head = store.put("HEAD", "0001".into(), Cond::None).await?;
//! store.put("HEAD", "0002".into(), Cond::IfGeneration(head)).await?;       // CAS
//! assert_eq!(store.get("snap/0001").await?.unwrap().bytes, "v1");
//! # let _ = g1;
//! # Ok::<(), objstore_r::Error>(()) }).unwrap();
//! ```

#![cfg_attr(feature = "http", feature(impl_trait_in_assoc_type))]
#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(rust_2018_idioms)]
#![deny(unreachable_pub)]
#![warn(clippy::all, clippy::pedantic, clippy::cargo)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::multiple_crate_versions,
    clippy::cast_possible_truncation
)]

pub mod encoding;
pub mod error;
pub mod fenced;
pub mod join;
pub mod key;
pub mod local;
pub mod mem;
#[cfg(feature = "mirror")]
pub mod mirror;
pub mod net;
pub mod sse;
pub mod transport;

#[cfg(feature = "gcs")]
pub mod gcs;
#[cfg(feature = "http")]
pub mod http;
#[cfg(feature = "http")]
pub mod policy;
#[cfg(feature = "http")]
pub mod token;

#[cfg(feature = "testing")]
pub mod testing;

use std::future::Future;
use std::ops::Range;

pub use bytes::Bytes;
pub use encoding::{ContentEncoding, MAX_INFLATED_BYTES};
pub use error::{Error, PolicyError, Result};
pub use key::Key;
pub use local::LocalDir;
pub use mem::{Fault, Mem};

#[cfg(feature = "gcs")]
pub use gcs::{Gcs, GcsConfig};

/// An object's content and generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Object {
    /// The whole body (shared, zero-copy).
    pub bytes: Bytes,
    /// The generation it was read at (never zero).
    pub generation: u64,
}

/// A listed object.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Meta {
    /// The full key.
    pub key: String,
    /// Body size in bytes.
    pub size: u64,
    /// Current generation (never zero).
    pub generation: u64,
}

/// [`Meta`] and the object's content encoding, from [`ObjectStore::stat`]
/// and [`ObjectStore::list_stat`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Stat {
    /// Key, generation, and the **stored** size (for an encoded object, the
    /// encoded bytes).
    pub meta: Meta,
    /// How the stored bytes are encoded (`None`: as written).
    pub content_encoding: Option<ContentEncoding>,
}

/// A write precondition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Cond {
    /// Write unconditionally (replace whatever is there).
    #[default]
    None,
    /// Write only if no object exists under the key ([`Error::Conflict`] otherwise).
    CreateOnly,
    /// Write only if the object is at exactly this generation
    /// ([`Error::PreconditionFailed`] otherwise, including when it does not exist).
    IfGeneration(u64),
}

/// An object store with generation preconditions.
///
/// Keys are validated by [`Key::parse`]: `/`-separated segments of
/// `[A-Za-z0-9._-]`, no empty segment, none starting with `.`, at most 1024 bytes.
///
/// Implementations must be linearizable per key: a successful conditional
/// write is observed by every later read.
pub trait ObjectStore {
    /// Future of [`get`](ObjectStore::get).
    type Get<'a>: Future<Output = Result<Option<Object>>> + 'a
    where
        Self: 'a;
    /// Future of [`get_range`](ObjectStore::get_range).
    type GetRange<'a>: Future<Output = Result<Bytes>> + 'a
    where
        Self: 'a;
    /// Future of [`head`](ObjectStore::head).
    type Head<'a>: Future<Output = Result<Option<Meta>>> + 'a
    where
        Self: 'a;
    /// Future of [`put`](ObjectStore::put).
    type Put<'a>: Future<Output = Result<u64>> + 'a
    where
        Self: 'a;
    /// Future of [`list`](ObjectStore::list).
    type List<'a>: Future<Output = Result<Vec<Meta>>> + 'a
    where
        Self: 'a;
    /// Future of [`delete`](ObjectStore::delete).
    type Delete<'a>: Future<Output = Result<()>> + 'a
    where
        Self: 'a;

    /// The whole object, or `None` if it does not exist.
    fn get<'a>(&'a self, key: &'a str) -> Self::Get<'a>;

    /// Bytes `range` of the object. A range running past the end is clipped;
    /// a range starting at or past the end is an empty body. A missing object
    /// is [`Error::NotFound`].
    fn get_range<'a>(&'a self, key: &'a str, range: Range<u64>) -> Self::GetRange<'a>;

    /// Size and generation, without the body.
    fn head<'a>(&'a self, key: &'a str) -> Self::Head<'a>;

    /// Write `body` under `key` if `cond` holds; returns the new generation.
    fn put<'a>(&'a self, key: &'a str, body: Bytes, cond: Cond) -> Self::Put<'a>;

    /// Every object whose key starts with `prefix`, sorted by key. An empty
    /// prefix lists everything; a non-empty prefix is validated like a key
    /// (a trailing `/` is allowed).
    fn list<'a>(&'a self, prefix: &'a str) -> Self::List<'a>;

    /// Remove the object. Deleting a missing key succeeds.
    fn delete<'a>(&'a self, key: &'a str) -> Self::Delete<'a>;

    /// Like [`put`](ObjectStore::put), storing `body` compressed with
    /// `encoding` and recording the encoding, so [`get`](ObjectStore::get)
    /// returns `body` again and [`get_range`](ObjectStore::get_range) refuses
    /// with [`Error::EncodedRange`]. Conditions behave exactly as for `put`.
    ///
    /// Every backend in this crate implements it (feature `gzip`). The
    /// default, for stores that cannot record an encoding, refuses with
    /// [`Error::UnsupportedEncoding`] and writes nothing.
    fn put_encoded<'a>(
        &'a self,
        key: &'a str,
        body: Bytes,
        cond: Cond,
        encoding: ContentEncoding,
    ) -> impl Future<Output = Result<u64>> + 'a {
        let _ = (key, body, cond);
        std::future::ready(Err(Error::UnsupportedEncoding { encoding: encoding.as_str().into() }))
    }

    /// [`head`](ObjectStore::head) plus the content encoding. The default
    /// reports none, which is exact for a store that cannot record one.
    fn stat<'a>(&'a self, key: &'a str) -> impl Future<Output = Result<Option<Stat>>> + 'a {
        let head = self.head(key);
        async move { Ok(head.await?.map(|meta| Stat { meta, content_encoding: None })) }
    }

    /// [`list`](ObjectStore::list) plus each object's content encoding. The
    /// default reports none, which is exact for a store that cannot record
    /// one.
    fn list_stat<'a>(&'a self, prefix: &'a str) -> impl Future<Output = Result<Vec<Stat>>> + 'a {
        let list = self.list(prefix);
        async move {
            Ok(list.await?.into_iter().map(|meta| Stat { meta, content_encoding: None }).collect())
        }
    }
}

/// Clip `range` to an object of `len` bytes, as `get_range` specifies.
pub(crate) fn clip(range: &Range<u64>, len: u64) -> Range<usize> {
    let start = range.start.min(len);
    let end = range.end.clamp(start, len);
    start as usize..end as usize
}
