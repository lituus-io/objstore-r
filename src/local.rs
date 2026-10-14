// Author: spicyzhug@gmail.com
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
//! - **Content encoding.** An encoded object ([`ObjectStore::put_encoded`])
//!   is still one file holding exactly its stored (compressed) bytes; the
//!   encoding lives in a sidecar under `root/.objstore-encoding/<key>`, a
//!   tree no key can name. Each sidecar line binds an encoding to one
//!   *generation*, and is written before the body it describes is renamed
//!   into place, so a lock-free reader always knows how to read the body it
//!   holds: a line for its generation means encoded; no line, with the body
//!   still at that generation, means plain; otherwise the body moved on and
//!   the read is retried (the last attempt under the store lock). A store
//!   that never held an encoded object has no sidecar tree, and its reads
//!   pay one `stat` for that.
//! - **Blocking I/O.** Operations complete synchronously inside the returned
//!   futures (bodies here are snapshots and log segments, and local disk is
//!   fast); keep very large stores off latency-critical executor threads.

use crate::encoding::{self, ContentEncoding, MAX_INFLATED_BYTES};
use crate::key::check_prefix;
use crate::{clip, Bytes, Cond, Error, Key, Meta, Object, ObjectStore, Result, Stat};
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
/// Root of the encoding sidecars (a leading `.`: never a key segment).
const ENC_DIR: &str = ".objstore-encoding";
/// A sidecar holds at most two short lines; anything longer is not ours.
const MAX_SIDECAR: u64 = 256;
/// Lock-free attempts at a read whose body moves under it; the last one
/// holds the store lock.
const READ_ATTEMPTS: u32 = 3;

/// How the body at one generation is encoded, as the sidecar says.
enum Seen {
    Plain,
    Encoded(ContentEncoding),
    /// The body is no longer at the generation read: read again.
    Moved,
}

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
    /// Bound on a decoded body.
    max_inflated: u64,
}

impl LocalDir {
    /// Open (creating if needed) a store rooted at `root`.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let canon = root.canonicalize()?;
        let precision = probe_precision(&root)?;
        Ok(Self { root, canon, precision, last: Mutex::new(0), max_inflated: MAX_INFLATED_BYTES })
    }

    /// Refuse to decode an encoded object past `limit` bytes
    /// ([`Error::TooLarge`]); the default is [`MAX_INFLATED_BYTES`].
    #[must_use]
    pub fn with_max_inflated(mut self, limit: u64) -> Self {
        self.max_inflated = limit;
        self
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

    fn sidecar(&self, key: Key<'_>) -> PathBuf {
        let mut p = self.root.join(ENC_DIR);
        p.extend(key.as_str().split('/'));
        p
    }

    /// How the body read at `generation` is encoded (see the module docs).
    fn seen(&self, key: Key<'_>, path: &Path, generation: u64) -> Result<Seen> {
        // The sidecar tree is created before the first encoded body and never
        // removed: without it, nothing here was ever encoded.
        match fs::symlink_metadata(self.root.join(ENC_DIR)) {
            Err(e) if absent(&e) => return Ok(Seen::Plain),
            Err(e) => return Err(e.into()),
            Ok(_) => {}
        }
        let side = self.sidecar(key);
        self.contained(side.parent().unwrap_or(&self.root))?;
        if let Some(token) = sidecar_entry(&side, generation)? {
            return match ContentEncoding::parse(&token)? {
                Some(e) => Ok(Seen::Encoded(e)),
                None => Err(Error::corrupt("encoding sidecar names no encoding")),
            };
        }
        // No line for it: a line is only ever dropped after its body was
        // replaced, so a body still at `generation` was written plain.
        Ok(match fs::symlink_metadata(path) {
            Ok(md) if md.is_file() && mtime_generation(&md) == generation => Seen::Plain,
            Ok(_) => Seen::Moved,
            Err(e) if absent(&e) => Seen::Moved,
            Err(e) => return Err(e.into()),
        })
    }

    /// Read `key` whole with its generation and encoding, consistently
    /// against concurrent writers.
    fn read_whole(&self, key: Key<'_>) -> Result<Option<(Bytes, u64, Option<ContentEncoding>)>> {
        let path = self.path(key);
        self.contained(path.parent().unwrap_or(&self.root))?;
        for attempt in 1..=READ_ATTEMPTS {
            let _quiesce = if attempt == READ_ATTEMPTS { Some(self.lock()?) } else { None };
            let Some(mut f) = open_existing(&path)? else { return Ok(None) };
            // Metadata and body from one handle: consistent even if a writer
            // renames a new version over the path meanwhile.
            let md = f.metadata()?;
            let generation = mtime_generation(&md);
            let mut buf = Vec::with_capacity(usize::try_from(md.len()).unwrap_or(0));
            f.read_to_end(&mut buf)?;
            match self.seen(key, &path, generation)? {
                Seen::Plain => return Ok(Some((Bytes::from(buf), generation, None))),
                Seen::Encoded(e) => return Ok(Some((Bytes::from(buf), generation, Some(e)))),
                Seen::Moved => {}
            }
        }
        Err(Error::protocol("an object changed while the store lock was held"))
    }

    fn stat_sync(&self, key: Key<'_>) -> Result<Option<Stat>> {
        let path = self.path(key);
        self.contained(path.parent().unwrap_or(&self.root))?;
        for attempt in 1..=READ_ATTEMPTS {
            let _quiesce = if attempt == READ_ATTEMPTS { Some(self.lock()?) } else { None };
            let md = match fs::symlink_metadata(&path) {
                Ok(md) if md.is_file() => md,
                Ok(_) => return Ok(None),
                Err(e) if absent(&e) => return Ok(None),
                Err(e) => return Err(e.into()),
            };
            let meta = Meta {
                key: key.as_str().to_owned(),
                size: md.len(),
                generation: mtime_generation(&md),
            };
            let content_encoding = match self.seen(key, &path, meta.generation)? {
                Seen::Plain => None,
                Seen::Encoded(e) => Some(e),
                Seen::Moved => continue,
            };
            return Ok(Some(Stat { meta, content_encoding }));
        }
        Err(Error::protocol("an object changed while the store lock was held"))
    }

    fn list_stat_sync(&self, prefix: &str) -> Result<Vec<Stat>> {
        let metas = self.list_sync(prefix)?;
        let mut out = Vec::with_capacity(metas.len());
        for meta in metas {
            let key = Key::parse(&meta.key)?;
            let content_encoding = match self.seen(key, &self.path(key), meta.generation)? {
                Seen::Plain => None,
                Seen::Encoded(e) => Some(e),
                // Rewritten since it was listed: report it as it is now.
                Seen::Moved => {
                    out.extend(self.stat_sync(key)?);
                    continue;
                }
            };
            out.push(Stat { meta, content_encoding });
        }
        Ok(out)
    }

    /// Remove `key`'s sidecar and the directories that leaves empty.
    fn drop_sidecar(&self, key: Key<'_>) -> Result<()> {
        let enc_root = self.root.join(ENC_DIR);
        if !enc_root.is_dir() {
            return Ok(());
        }
        let side = self.sidecar(key);
        self.contained(side.parent().unwrap_or(&self.root))?;
        match fs::remove_file(&side) {
            Ok(()) => {}
            Err(e) if absent(&e) => return Ok(()),
            Err(e) => return Err(e.into()),
        }
        let mut dir = side.parent();
        while let Some(d) = dir.filter(|d| *d != enc_root) {
            if fs::remove_dir(d).is_err() {
                break;
            }
            dir = d.parent();
        }
        Ok(())
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

    fn put_sync(
        &self,
        key: &str,
        body: &Bytes,
        cond: Cond,
        encoding: Option<ContentEncoding>,
    ) -> Result<u64> {
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
        // An encoded write records its line before the body becomes visible,
        // keeping the line of the body it replaces (a reader may hold it).
        let side = match encoding {
            None => None,
            Some(e) => {
                let side = self.sidecar(key);
                let sdir = side.parent().unwrap_or(&self.root);
                self.contained(sdir)?;
                fs::create_dir_all(sdir).map_err(|e| match e.kind() {
                    std::io::ErrorKind::NotADirectory | std::io::ErrorKind::AlreadyExists => {
                        COLLISION
                    }
                    _ => e.into(),
                })?;
                self.contained(sdir)?;
                let kept = match current {
                    Some(c) => sidecar_entry(&side, c)?.map(|t| format!("{c} {t}\n")),
                    None => None,
                };
                Some((side, e, kept.unwrap_or_default()))
            }
        };
        let generation = self.next_generation(current);
        // The generation is the file's modification time, stamped before the
        // rename makes the object visible.
        let stamp = UNIX_EPOCH + Duration::from_nanos(generation);
        let mut recorded = generation;
        fsio::write_atomic_with(&path, &[body], |f| {
            f.set_modified(stamp)?;
            if let Some((side, e, kept)) = &side {
                recorded = mtime_generation(&f.metadata()?);
                fsio::write_atomic(side, &[format!("{recorded} {e}\n{kept}").as_bytes()])?;
            }
            Ok(())
        })?;
        // Read back what the filesystem recorded: the generation callers will see.
        let visible = generation_of(&path)?.unwrap_or(generation);
        if let Some((side, e, kept)) = &side {
            if visible != recorded {
                // The filesystem moved the stamp at rename: rebind the line.
                fsio::write_atomic(side, &[format!("{visible} {e}\n{kept}").as_bytes()])?;
            }
        }
        Ok(visible)
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
            let Some((stored, generation, encoding)) = self.read_whole(Key::parse(key)?)? else {
                return Ok(None);
            };
            let bytes = match encoding {
                None => stored,
                Some(e) => encoding::decode(e, &stored, self.max_inflated)?,
            };
            Ok(Some(Object { bytes, generation }))
        })())
    }

    fn get_range<'a>(&'a self, key: &'a str, range: Range<u64>) -> Self::GetRange<'a> {
        ready((|| {
            let k = Key::parse(key)?;
            let path = self.path(k);
            self.contained(path.parent().unwrap_or(&self.root))?;
            for attempt in 1..=READ_ATTEMPTS {
                let _quiesce = if attempt == READ_ATTEMPTS { Some(self.lock()?) } else { None };
                let Some(mut f) = open_existing(&path)? else { return Err(Error::NotFound) };
                let md = f.metadata()?;
                match self.seen(k, &path, mtime_generation(&md))? {
                    Seen::Encoded(e) => {
                        return Err(Error::EncodedRange { encoding: e.as_str().into() })
                    }
                    Seen::Moved => continue,
                    Seen::Plain => {}
                }
                let r = clip(&range, md.len());
                let mut buf = vec![0; r.len()];
                f.seek(SeekFrom::Start(r.start as u64))?;
                f.read_exact(&mut buf)?;
                return Ok(Bytes::from(buf));
            }
            Err(Error::protocol("an object changed while the store lock was held"))
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
        ready(self.put_sync(key, &body, cond, None))
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
            // After the body: a reader holding it still finds its line.
            self.drop_sidecar(Key::parse(key)?)?;
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

    fn put_encoded<'a>(
        &'a self,
        key: &'a str,
        body: Bytes,
        cond: Cond,
        encoding: ContentEncoding,
    ) -> impl std::future::Future<Output = Result<u64>> + 'a {
        // Validate before compressing; compress before taking the lock.
        ready(Key::parse(key).and_then(|_| {
            let stored = encoding::encode(encoding, &body)?;
            self.put_sync(key, &stored, cond, Some(encoding))
        }))
    }

    fn stat<'a>(
        &'a self,
        key: &'a str,
    ) -> impl std::future::Future<Output = Result<Option<Stat>>> + 'a {
        ready(Key::parse(key).and_then(|k| self.stat_sync(k)))
    }

    fn list_stat<'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl std::future::Future<Output = Result<Vec<Stat>>> + 'a {
        ready(self.list_stat_sync(prefix))
    }
}

/// The encoding token a sidecar binds to `generation`, if any. Lines are
/// `{generation} {token}`, newest first; at most two.
fn sidecar_entry(side: &Path, generation: u64) -> Result<Option<String>> {
    // Never follow a symlink planted at a sidecar path, and never read more
    // than a sidecar can hold.
    let Some(f) = open_existing(side)? else { return Ok(None) };
    let mut text = Vec::with_capacity(64);
    f.take(MAX_SIDECAR + 1).read_to_end(&mut text)?;
    if text.len() as u64 > MAX_SIDECAR {
        return Err(Error::corrupt("encoding sidecar too long"));
    }
    let text =
        std::str::from_utf8(&text).map_err(|_| Error::corrupt("encoding sidecar is not text"))?;
    for line in text.lines() {
        let (g, token) =
            line.split_once(' ').ok_or_else(|| Error::corrupt("malformed encoding sidecar"))?;
        let g: u64 = g.parse().map_err(|_| Error::corrupt("malformed encoding sidecar"))?;
        if g == generation {
            return Ok(Some(token.to_owned()));
        }
    }
    Ok(None)
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
