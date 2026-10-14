// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! An in-memory store with scripted fault injection, for tests.
//!
//! Faults are queued with [`Mem::inject`] and consumed one per operation, in
//! order. [`Fault::LostResponse`] applies the operation and then reports it as
//! failed: the classic ambiguity a real network adds, which crash-consistent
//! callers must survive.
//!
//! Encoded objects ([`ObjectStore::put_encoded`]) keep their encoding beside
//! the stored bytes and are decoded on `get`, outside the lock.

use crate::encoding::{self, ContentEncoding, MAX_INFLATED_BYTES};
use crate::key::check_prefix;
use crate::{clip, Bytes, Cond, Error, Key, Meta, Object, ObjectStore, Result, Stat};
use std::collections::{BTreeMap, VecDeque};
use std::future::{ready, Ready};
use std::ops::Range;
use std::sync::Mutex;

/// A scripted outcome for one future operation.
#[derive(Debug)]
#[non_exhaustive]
pub enum Fault {
    /// Let the operation through untouched (a placeholder that lets a real
    /// fault target the n-th operation; see [`Mem::inject_after`]).
    Pass,
    /// Fail without applying the operation.
    Fail(Error),
    /// Apply the operation, then report this error.
    LostResponse(Error),
}

/// The stored bytes and how they are encoded.
#[derive(Debug, Clone)]
struct Stored {
    obj: Object,
    encoding: Option<ContentEncoding>,
}

impl Stored {
    fn meta(&self, key: String) -> Meta {
        Meta { key, size: self.obj.bytes.len() as u64, generation: self.obj.generation }
    }
}

#[derive(Debug, Default)]
struct Inner {
    objects: BTreeMap<String, Stored>,
    next_generation: u64,
    faults: VecDeque<Fault>,
    ops: u64,
}

/// An in-memory [`ObjectStore`]. Generations increase monotonically across
/// the whole store, like GCS.
#[derive(Debug)]
pub struct Mem {
    inner: Mutex<Inner>,
    /// Bound on a decoded body.
    max_inflated: u64,
}

impl Default for Mem {
    fn default() -> Self {
        Self { inner: Mutex::default(), max_inflated: MAX_INFLATED_BYTES }
    }
}

impl Mem {
    /// Refuse to decode an encoded object past `limit` bytes
    /// ([`Error::TooLarge`]); the default is [`MAX_INFLATED_BYTES`].
    #[must_use]
    pub fn with_max_inflated(mut self, limit: u64) -> Self {
        self.max_inflated = limit;
        self
    }

    /// Queue a fault for a future operation (consumed in order, one per call).
    pub fn inject(&self, fault: Fault) {
        self.lock().faults.push_back(fault);
    }

    /// Let `skip` operations through, then apply `fault` — for crash tests
    /// that fail each step of a multi-operation protocol in turn.
    pub fn inject_after(&self, skip: u64, fault: Fault) {
        let mut g = self.lock();
        for _ in 0..skip {
            g.faults.push_back(Fault::Pass);
        }
        g.faults.push_back(fault);
    }

    /// Drop every queued fault.
    pub fn clear_faults(&self) {
        self.lock().faults.clear();
    }

    /// Operations served so far (every call, failed or not).
    #[must_use]
    pub fn ops(&self) -> u64 {
        self.lock().ops
    }

    /// Every key, sorted.
    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        self.lock().objects.keys().cloned().collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A panic while holding the lock can only come from a bug in this
        // module; the data is still consistent (every mutation is one insert
        // or remove), so poisoning is ignored.
        self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn put_stored(
        &self,
        key: &str,
        body: Bytes,
        cond: Cond,
        encoding: Option<ContentEncoding>,
    ) -> Result<u64> {
        let k = Key::parse(key)?;
        self.run(|g| {
            let current = g.objects.get(k.as_str()).map(|o| o.obj.generation);
            match (cond, current) {
                (Cond::CreateOnly, Some(_)) => return Err(Error::Conflict),
                (Cond::IfGeneration(want), cur) if cur != Some(want) => {
                    return Err(Error::PreconditionFailed)
                }
                _ => {}
            }
            g.next_generation += 1;
            let generation = g.next_generation;
            let obj = Object { bytes: body, generation };
            g.objects.insert(k.as_str().to_owned(), Stored { obj, encoding });
            Ok(generation)
        })
    }

    fn run<T>(&self, op: impl FnOnce(&mut Inner) -> Result<T>) -> Result<T> {
        let mut g = self.lock();
        g.ops += 1;
        match g.faults.pop_front() {
            Some(Fault::Fail(e)) => Err(e),
            Some(Fault::LostResponse(e)) => {
                let _ = op(&mut g);
                Err(e)
            }
            Some(Fault::Pass) | None => op(&mut g),
        }
    }
}

impl ObjectStore for Mem {
    type Get<'a> = Ready<Result<Option<Object>>>;
    type GetRange<'a> = Ready<Result<Bytes>>;
    type Head<'a> = Ready<Result<Option<Meta>>>;
    type Put<'a> = Ready<Result<u64>>;
    type List<'a> = Ready<Result<Vec<Meta>>>;
    type Delete<'a> = Ready<Result<()>>;

    fn get<'a>(&'a self, key: &'a str) -> Self::Get<'a> {
        let stored =
            Key::parse(key).and_then(|k| self.run(|g| Ok(g.objects.get(k.as_str()).cloned())));
        ready(stored.and_then(|s| {
            s.map(|Stored { obj, encoding }| match encoding {
                None => Ok(obj),
                Some(e) => Ok(Object {
                    bytes: encoding::decode(e, &obj.bytes, self.max_inflated)?,
                    generation: obj.generation,
                }),
            })
            .transpose()
        }))
    }

    fn get_range<'a>(&'a self, key: &'a str, range: Range<u64>) -> Self::GetRange<'a> {
        ready(Key::parse(key).and_then(|k| {
            self.run(|g| {
                let o = g.objects.get(k.as_str()).ok_or(Error::NotFound)?;
                if let Some(e) = o.encoding {
                    return Err(Error::EncodedRange { encoding: e.as_str().into() });
                }
                Ok(o.obj.bytes.slice(clip(&range, o.obj.bytes.len() as u64)))
            })
        }))
    }

    fn head<'a>(&'a self, key: &'a str) -> Self::Head<'a> {
        ready(Key::parse(key).and_then(|k| {
            self.run(|g| Ok(g.objects.get(k.as_str()).map(|o| o.meta(k.as_str().to_owned()))))
        }))
    }

    fn put<'a>(&'a self, key: &'a str, body: Bytes, cond: Cond) -> Self::Put<'a> {
        ready(self.put_stored(key, body, cond, None))
    }

    fn list<'a>(&'a self, prefix: &'a str) -> Self::List<'a> {
        ready(check_prefix(prefix).and_then(|()| {
            self.run(|g| {
                Ok(g.objects
                    .range(prefix.to_owned()..)
                    .take_while(|(k, _)| k.starts_with(prefix))
                    .map(|(k, o)| o.meta(k.clone()))
                    .collect())
            })
        }))
    }

    fn delete<'a>(&'a self, key: &'a str) -> Self::Delete<'a> {
        ready(Key::parse(key).and_then(|k| {
            self.run(|g| {
                g.objects.remove(k.as_str());
                Ok(())
            })
        }))
    }

    fn put_encoded<'a>(
        &'a self,
        key: &'a str,
        body: Bytes,
        cond: Cond,
        encoding: ContentEncoding,
    ) -> impl std::future::Future<Output = Result<u64>> + 'a {
        // Validate before compressing; compress outside the lock.
        ready(Key::parse(key).and_then(|_| {
            let stored = encoding::encode(encoding, &body)?;
            self.put_stored(key, stored, cond, Some(encoding))
        }))
    }

    fn stat<'a>(
        &'a self,
        key: &'a str,
    ) -> impl std::future::Future<Output = Result<Option<Stat>>> + 'a {
        ready(Key::parse(key).and_then(|k| {
            self.run(|g| {
                Ok(g.objects.get(k.as_str()).map(|o| Stat {
                    meta: o.meta(k.as_str().to_owned()),
                    content_encoding: o.encoding,
                }))
            })
        }))
    }

    fn list_stat<'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl std::future::Future<Output = Result<Vec<Stat>>> + 'a {
        ready(check_prefix(prefix).and_then(|()| {
            self.run(|g| {
                Ok(g.objects
                    .range(prefix.to_owned()..)
                    .take_while(|(k, _)| k.starts_with(prefix))
                    .map(|(k, o)| Stat { meta: o.meta(k.clone()), content_encoding: o.encoding })
                    .collect())
            })
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inject_after_targets_the_nth_operation() {
        let m = Mem::default();
        m.inject_after(2, Fault::Fail(Error::unavailable("boom", true)));
        let put = |k| futures::executor::block_on(m.put(k, Bytes::new(), Cond::None));
        assert!(put("a").is_ok());
        assert!(put("b").is_ok());
        assert!(put("c").is_err());
        assert!(put("d").is_ok());
    }
    use futures::executor::block_on;

    #[test]
    fn faults_apply_in_order() {
        let m = Mem::default();
        m.inject(Fault::LostResponse(Error::unavailable("lost", true)));
        m.inject(Fault::Fail(Error::unavailable("down", true)));
        assert!(block_on(m.put("a", Bytes::from_static(b"1"), Cond::CreateOnly)).is_err());
        assert_eq!(block_on(m.get("a")).unwrap_err().to_string(), "store unavailable: down");
        assert!(block_on(m.get("a")).unwrap().is_some(), "the lost-response write was applied");
        assert!(matches!(
            block_on(m.put("a", Bytes::new(), Cond::CreateOnly)),
            Err(Error::Conflict)
        ));
        assert_eq!(m.ops(), 4);
    }
}
