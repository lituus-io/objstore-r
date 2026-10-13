// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! A directory as an object store.
//!
//! Each object is a file at `root/<key>` holding exactly the object's bytes,
//! so a store can be copied to or from a bucket byte-for-byte.
//!
//! - **Atomic writes.** A body is written to a uniquely named temporary file in
//!   the target directory, fsynced, then renamed over the key (and the
//!   directory fsynced on unix). Readers see the old or the new body, never a
//!   mix.
//! - **Generations.** An object's generation is its modification time in
//!   nanoseconds, *set explicitly* by the writer. Writes allocate generations
//!   that strictly increase per key, in steps of the filesystem's timestamp
//!   precision (probed at [`LocalDir::open`]), so coarse-timestamp filesystems
//!   still distinguish every write.
//! - **Conditions.** Writes and deletes hold an exclusive `File::lock` on
//!   `root/.objstore.lock` while they check and apply, so conditional writes are
//!   atomic across threads and processes on the same host. Reads take no lock.
//! - **Nesting.** A filesystem cannot hold an object `k` and objects under
//!   `k/` at once; such a write fails with [`Error::InvalidKey`]. Layouts that
//!   keep objects at the leaves (the usual case) are unaffected.
//! - **Blocking I/O.** Operations complete synchronously inside the returned
//!   futures (bodies here are snapshots and log segments, and local disk is
//!   fast); keep very large stores off latency-critical executor threads.

use crate::key::check_prefix;
use crate::{clip, Bytes, Cond, Error, Key, Meta, Object, ObjectStore, Result};
use graph_r::fsio;
use std::fs::{self, File};
use std::future::{ready, Ready};
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};

use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const LOCK_FILE: &str = ".objstore.lock";
const PROBE_FILE: &str = ".objstore.probe";

/// A directory-backed [`ObjectStore`].
#[derive(Debug)]
pub struct LocalDir {
    root: PathBuf,
    /// `root`, canonicalized: every object path must resolve inside it.
    canon: PathBuf,
    /// Timestamp precision of the filesystem, in nanoseconds.
    precision: u64,
    /// Highest generation this handle has issued (keeps delete-then-recreate
    /// from reusing a generation within the process).
    last: Mutex<u64>,
}

impl LocalDir {
    /// Open (creating if needed) a store rooted at `root`.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let canon = root.canonicalize()?;
        let precision = probe_precision(&root)?;
        Ok(Self { root, canon, precision, last: Mutex::new(0) })
    }

    /// The root directory.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The probed timestamp precision in nanoseconds (1 on most modern filesystems).
    #[must_use]
    pub fn precision_ns(&self) -> u64 {
        self.precision
    }

    fn path(&self, key: Key<'_>) -> PathBuf {
        let mut p = self.root.clone();
        p.extend(key.as_str().split('/'));
        p
    }

    /// Refuse paths whose existing directories resolve outside the root (a
    /// symlink planted inside the store must not redirect reads or writes).
    fn contained(&self, dir: &Path) -> Result<()> {
        let mut probe = dir;
        // The nearest existing ancestor decides (missing parts are created
        // below it, so they cannot escape).
        loop {
            match probe.canonicalize() {
                Ok(c) if c.starts_with(&self.canon) => return Ok(()),
                Ok(_) => {
                    return Err(Error::InvalidKey { reason: "resolves outside the store root" })
                }
                Err(e) if absent(&e) => match probe.parent() {
                    Some(p) => probe = p,
                    None => {
                        return Err(Error::InvalidKey { reason: "resolves outside the store root" })
                    }
                },
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn lock(&self) -> Result<File> {
        Ok(fsio::lock(&self.root.join(LOCK_FILE))?)
    }

    fn next_generation(&self, current: Option<u64>) -> u64 {
        let p = self.precision;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
        let mut last = self.last.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let floor = current.unwrap_or(0).max(*last).saturating_add(p);
        let g = round_up(now.max(floor), p).max(p);
        *last = g;
        g
    }

    fn put_sync(&self, key: &str, body: &Bytes, cond: Cond) -> Result<u64> {
        let key = Key::parse(key)?;
        let path = self.path(key);
        self.contained(path.parent().unwrap_or(&self.root))?;
        let _lock = self.lock()?;
        let current = generation_of(&path)?;
        match (cond, current) {
            (Cond::CreateOnly, Some(_)) => return Err(Error::Conflict),
            (Cond::IfGeneration(want), cur) if cur != Some(want) => {
                return Err(Error::PreconditionFailed)
            }
            _ => {}
        }
        let dir = path.parent().unwrap_or(&self.root);
        self.contained(dir)?;
        fs::create_dir_all(dir).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotADirectory | std::io::ErrorKind::AlreadyExists => COLLISION,
            _ => e.into(),
        })?;
        self.contained(dir)?;
        let generation = self.next_generation(current);
        // The generation is the file's modification time, stamped before the
        // rename makes the object visible.
        let stamp = UNIX_EPOCH + Duration::from_nanos(generation);
        fsio::write_atomic_with(&path, &[body], |f| f.set_modified(stamp))?;
        // Read back what the filesystem recorded: the generation callers will see.
        Ok(generation_of(&path)?.unwrap_or(generation))
    }

    fn list_sync(&self, prefix: &str) -> Result<Vec<Meta>> {
        check_prefix(prefix)?;
        // Walk only the deepest directory the prefix pins down.
        let (dir_part, _) = prefix.rsplit_once('/').unwrap_or(("", prefix));
        let mut start = self.root.clone();
        if !dir_part.is_empty() {
            start.extend(dir_part.split('/'));
        }
        let mut out = Vec::new();
        let mut stack = vec![(start, dir_part.to_owned())];
        while let Some((dir, rel)) = stack.pop() {
            let entries = match fs::read_dir(&dir) {
                Ok(e) => e,
                Err(e) if absent(&e) => continue,
                Err(e) => return Err(e.into()),
            };
            for entry in entries {
                let entry = entry?;
                let Some(name) = entry.file_name().to_str().map(str::to_owned) else { continue };
                let key = if rel.is_empty() { name.clone() } else { format!("{rel}/{name}") };
                if Key::parse(&key).is_err() {
                    continue; // temporaries, lock/probe files, foreign names
                }
                let ft = entry.file_type()?; // symlinks are neither dirs nor files here: skipped
                if ft.is_dir() {
                    if prefix.starts_with(&format!("{key}/"))
                        || key.starts_with(prefix)
                        || prefix.is_empty()
                    {
                        stack.push((entry.path(), key));
                    }
                } else if ft.is_file() && key.starts_with(prefix) {
                    let md = entry.metadata()?;
                    out.push(Meta { key, size: md.len(), generation: mtime_generation(&md) });
                }
            }
        }
        out.sort();
        Ok(out)
    }
}

impl ObjectStore for LocalDir {
    type Get<'a> = Ready<Result<Option<Object>>>;
    type GetRange<'a> = Ready<Result<Bytes>>;
    type Head<'a> = Ready<Result<Option<Meta>>>;
    type Put<'a> = Ready<Result<u64>>;
    type List<'a> = Ready<Result<Vec<Meta>>>;
    type Delete<'a> = Ready<Result<()>>;

    fn get<'a>(&'a self, key: &'a str) -> Self::Get<'a> {
        ready((|| {
            let path = self.path(Key::parse(key)?);
            self.contained(path.parent().unwrap_or(&self.root))?;
            let Some(mut f) = open_existing(&path)? else { return Ok(None) };
            // Metadata and body from one handle: consistent even if a writer
            // renames a new version over the path meanwhile.
            let md = f.metadata()?;
            let mut buf = Vec::with_capacity(usize::try_from(md.len()).unwrap_or(0));
            f.read_to_end(&mut buf)?;
            Ok(Some(Object { bytes: Bytes::from(buf), generation: mtime_generation(&md) }))
        })())
    }

    fn get_range<'a>(&'a self, key: &'a str, range: Range<u64>) -> Self::GetRange<'a> {
        ready((|| {
            let path = self.path(Key::parse(key)?);
            self.contained(path.parent().unwrap_or(&self.root))?;
            let Some(mut f) = open_existing(&path)? else { return Err(Error::NotFound) };
            let r = clip(&range, f.metadata()?.len());
            let mut buf = vec![0; r.len()];
            f.seek(SeekFrom::Start(r.start as u64))?;
            f.read_exact(&mut buf)?;
            Ok(Bytes::from(buf))
        })())
    }

    fn head<'a>(&'a self, key: &'a str) -> Self::Head<'a> {
        ready((|| {
            let k = Key::parse(key)?;
            let path = self.path(k);
            self.contained(path.parent().unwrap_or(&self.root))?;
            match fs::symlink_metadata(&path) {
                Ok(md) if md.is_file() => Ok(Some(Meta {
                    key: k.as_str().to_owned(),
                    size: md.len(),
                    generation: mtime_generation(&md),
                })),
                Ok(_) => Ok(None),
                Err(e) if absent(&e) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })())
    }

    fn put<'a>(&'a self, key: &'a str, body: Bytes, cond: Cond) -> Self::Put<'a> {
        ready(self.put_sync(key, &body, cond))
    }

    fn list<'a>(&'a self, prefix: &'a str) -> Self::List<'a> {
        ready(self.list_sync(prefix))
    }

    fn delete<'a>(&'a self, key: &'a str) -> Self::Delete<'a> {
        ready((|| {
            let path = self.path(Key::parse(key)?);
            self.contained(path.parent().unwrap_or(&self.root))?;
            let _lock = self.lock()?;
            // Only a regular file is an object: directories and symlinks stay.
            match fs::symlink_metadata(&path) {
                Ok(md) if md.is_file() => {}
                Ok(_) => return Ok(()),
                Err(e) if absent(&e) => return Ok(()),
                Err(e) => return Err(e.into()),
            }
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if absent(&e) => return Ok(()),
                Err(e) => return Err(e.into()),
            }
            // Prune directories the delete emptied, so the key space frees up
            // (`k` is writable again once nothing is left under `k/`).
            let mut dir = path.parent();
            while let Some(d) = dir.filter(|d| *d != self.root) {
                if fs::remove_dir(d).is_err() {
                    break; // not empty (or raced): stop
                }
                dir = d.parent();
            }
            fsio::sync_dir(dir.unwrap_or(&self.root));
            Ok(())
        })())
    }
}

/// A filesystem cannot hold both `a` and `a/b`; object stores can.
const COLLISION: Error = Error::InvalidKey {
    reason: "an object and a prefix collide (LocalDir cannot hold both `k` and `k/...`)",
};

/// Missing, or a path through an object as if it were a directory.
fn absent(e: &std::io::Error) -> bool {
    matches!(e.kind(), std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory)
}

fn open_existing(path: &Path) -> Result<Option<File>> {
    // An object is a regular file; a symlink at an object path is never followed.
    match fs::symlink_metadata(path) {
        Ok(md) if !md.file_type().is_file() => return Ok(None),
        Ok(_) => {}
        Err(e) if absent(&e) => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    match File::open(path) {
        Ok(f) if f.metadata()?.is_file() => Ok(Some(f)),
        Ok(_) => Ok(None),
        Err(e) if absent(&e) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn generation_of(path: &Path) -> Result<Option<u64>> {
    match fs::symlink_metadata(path) {
        Ok(md) if md.is_file() => Ok(Some(mtime_generation(&md))),
        Ok(md) if md.is_dir() => Err(COLLISION),
        Ok(_) => Err(Error::Protocol {
            message: "a non-file entry exists where an object key is".into(),
        }),
        Err(e) if absent(&e) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn mtime_generation(md: &fs::Metadata) -> u64 {
    md.modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map_or(1, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX).max(1))
}

fn round_up(v: u64, step: u64) -> u64 {
    v.div_ceil(step).saturating_mul(step)
}

/// The filesystem's timestamp precision: set a known nanosecond mtime and see
/// what survives.
fn probe_precision(root: &Path) -> Result<u64> {
    const PROBE_NS: u64 = 1_700_000_000_123_456_789;
    let path = root.join(PROBE_FILE);
    let f = File::create(&path)?;
    f.set_modified(UNIX_EPOCH + Duration::from_nanos(PROBE_NS))?;
    drop(f);
    let got = mtime_generation(&fs::metadata(&path)?);
    let _ = fs::remove_file(&path);
    for step in [
        1,
        10,
        100,
        1_000,
        10_000,
        100_000,
        1_000_000,
        10_000_000,
        100_000_000,
        1_000_000_000,
        2_000_000_000,
    ] {
        if got == PROBE_NS / step * step {
            return Ok(step);
        }
    }
    Ok(2_000_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    #[test]
    fn generations_increase_per_key_even_when_written_fast() {
        let dir = tempfile::tempdir().unwrap();
        let s = LocalDir::open(dir.path()).unwrap();
        let mut last = 0;
        for i in 0..50u8 {
            let g = block_on(s.put("k", Bytes::from(vec![i]), Cond::None)).unwrap();
            assert!(g > last, "generation {g} after {last}");
            last = g;
        }
        assert_eq!(block_on(s.head("k")).unwrap().unwrap().generation, last);
    }

    #[test]
    fn no_temporaries_left_and_bytes_are_exact() {
        let dir = tempfile::tempdir().unwrap();
        let s = LocalDir::open(dir.path()).unwrap();
        block_on(s.put("a/b/c.bin", Bytes::from_static(b"\x00\x01payload"), Cond::CreateOnly))
            .unwrap();
        assert_eq!(fs::read(dir.path().join("a/b/c.bin")).unwrap(), b"\x00\x01payload");
        let names: Vec<_> =
            fs::read_dir(dir.path().join("a/b")).unwrap().map(|e| e.unwrap().file_name()).collect();
        assert_eq!(names, ["c.bin"]);
    }
}
