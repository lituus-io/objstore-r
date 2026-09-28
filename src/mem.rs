// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! An in-memory store with scripted fault injection, for tests.
//!
//! Faults are queued with [`Mem::inject`] and consumed one per operation, in
//! order. [`Fault::LostResponse`] applies the operation and then reports it as
//! failed: the classic ambiguity a real network adds, which crash-consistent
//! callers must survive.

use crate::key::check_prefix;
use crate::{clip, Bytes, Cond, Error, Key, Meta, Object, ObjectStore, Result};
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

#[derive(Debug, Default)]
struct Inner {
    objects: BTreeMap<String, Object>,
    next_generation: u64,
    faults: VecDeque<Fault>,
    ops: u64,
}

/// An in-memory [`ObjectStore`]. Generations increase monotonically across
/// the whole store, like GCS.
#[derive(Debug, Default)]
pub struct Mem {
    inner: Mutex<Inner>,
}

impl Mem {
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
        ready(Key::parse(key).and_then(|k| self.run(|g| Ok(g.objects.get(k.as_str()).cloned()))))
    }

    fn get_range<'a>(&'a self, key: &'a str, range: Range<u64>) -> Self::GetRange<'a> {
        ready(Key::parse(key).and_then(|k| {
            self.run(|g| {
                let o = g.objects.get(k.as_str()).ok_or(Error::NotFound)?;
                Ok(o.bytes.slice(clip(&range, o.bytes.len() as u64)))
            })
        }))
    }

    fn head<'a>(&'a self, key: &'a str) -> Self::Head<'a> {
        ready(Key::parse(key).and_then(|k| {
            self.run(|g| {
                Ok(g.objects.get(k.as_str()).map(|o| Meta {
                    key: k.as_str().to_owned(),
                    size: o.bytes.len() as u64,
                    generation: o.generation,
                }))
            })
        }))
    }

    fn put<'a>(&'a self, key: &'a str, body: Bytes, cond: Cond) -> Self::Put<'a> {
        ready(Key::parse(key).and_then(|k| {
            self.run(|g| {
                let current = g.objects.get(k.as_str()).map(|o| o.generation);
                match (cond, current) {
                    (Cond::CreateOnly, Some(_)) => return Err(Error::Conflict),
                    (Cond::IfGeneration(want), cur) if cur != Some(want) => {
                        return Err(Error::PreconditionFailed)
                    }
                    _ => {}
                }
                g.next_generation += 1;
                let generation = g.next_generation;
                g.objects.insert(k.as_str().to_owned(), Object { bytes: body, generation });
                Ok(generation)
            })
        }))
    }

    fn list<'a>(&'a self, prefix: &'a str) -> Self::List<'a> {
        ready(check_prefix(prefix).and_then(|()| {
            self.run(|g| {
                Ok(g.objects
                    .range(prefix.to_owned()..)
                    .take_while(|(k, _)| k.starts_with(prefix))
                    .map(|(k, o)| Meta {
                        key: k.clone(),
                        size: o.bytes.len() as u64,
                        generation: o.generation,
                    })
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
