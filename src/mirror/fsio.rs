// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Local file plumbing over graph-r's shared primitives: the store
//! directory's writer lock (the very lock a writable `Store` takes, so the
//! two can never both write one directory), atomic multi-part writes, and
//! durable appends.

use super::{Error, Result};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

/// Take the store directory's writer lock for as long as the returned handle
/// lives. Never blocks.
pub(crate) fn lock_dir(dir: &Path) -> Result<File> {
    std::fs::create_dir_all(dir)?;
    graph_r::fsio::try_lock(&dir.join(graph_r::store::LOCK_FILE))?
        .ok_or(Error::Graph(graph_r::Error::Locked))
}

/// Write the concatenation of `parts` to `path` atomically.
pub(crate) fn write_atomic(path: &Path, parts: &[&[u8]]) -> Result<()> {
    Ok(graph_r::fsio::write_atomic(path, parts)?)
}

/// Append `parts` to an existing file and fsync.
pub(crate) fn append(path: &Path, parts: &[&[u8]]) -> Result<()> {
    let mut f = OpenOptions::new().append(true).open(path)?;
    for p in parts {
        f.write_all(p)?;
    }
    f.sync_all()?;
    Ok(())
}
