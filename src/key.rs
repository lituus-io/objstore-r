// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Object keys.
//!
//! One key grammar for every backend, so a store can move between a local
//! directory and a bucket with a plain copy: `/`-separated segments of
//! `[A-Za-z0-9._-]`, no empty segment, no segment starting with `.`, at most
//! [`MAX_KEY`] bytes. The grammar also makes local paths safe by construction
//! (no traversal, no absolute paths, no device names with separators).

use crate::{Error, Result};

/// Longest key, in bytes (GCS allows 1024).
pub const MAX_KEY: usize = 1024;

/// A validated key (borrowed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key<'a>(&'a str);

impl<'a> Key<'a> {
    /// Validate a key.
    ///
    /// ```
    /// use objstore_r::Key;
    /// assert!(Key::parse("graph/base/0000000000000001.base").is_ok());
    /// assert!(Key::parse("../etc/passwd").is_err());
    /// assert!(Key::parse("a//b").is_err());
    /// ```
    pub fn parse(s: &'a str) -> Result<Self> {
        check(s)?;
        Ok(Key(s))
    }

    /// The key text.
    #[must_use]
    pub fn as_str(&self) -> &'a str {
        self.0
    }
}

/// Validate a list prefix: empty, or a key optionally ending in `/`.
pub(crate) fn check_prefix(s: &str) -> Result<()> {
    if s.is_empty() {
        return Ok(());
    }
    check(s.strip_suffix('/').unwrap_or(s))
}

fn check(s: &str) -> Result<()> {
    let bad = |reason| Err(Error::InvalidKey { reason });
    if s.is_empty() {
        return bad("empty");
    }
    if s.len() > MAX_KEY {
        return bad("longer than 1024 bytes");
    }
    for seg in s.split('/') {
        if seg.is_empty() {
            return bad("empty segment");
        }
        if seg.starts_with('.') {
            // Covers `.` and `..`, and reserves dot-names for store internals
            // (temporary files, lock files) so they can never collide with keys.
            return bad("segment starts with a dot");
        }
        if !seg.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-')) {
            return bad("characters outside [A-Za-z0-9._-/]");
        }
        if seg.ends_with('.') {
            return bad("segment ends with a dot");
        }
        if is_device_name(seg) {
            return bad("reserved device name");
        }
    }
    Ok(())
}

/// Windows device names (with any extension) cannot be files there; rejecting
/// them everywhere keeps every key portable to every backend.
fn is_device_name(seg: &str) -> bool {
    let stem = seg.split('.').next().unwrap_or(seg);
    let upper = stem.to_ascii_uppercase();
    matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (upper.len() == 4
            && (upper.starts_with("COM") || upper.starts_with("LPT"))
            && upper.as_bytes()[3].is_ascii_digit())
}

/// Percent-encode `s` as one URL path or query component (RFC 3986
/// unreserved kept; `/` encoded, as the GCS JSON API requires for object
/// names), appending to `out`.
pub fn encode_component(s: &str, out: &mut String) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push('%');
            out.push(char::from(HEX[usize::from(b >> 4)]));
            out.push(char::from(HEX[usize::from(b & 0xF)]));
        }
    }
}

/// Fuzzing entry point: arbitrary input must be classified without panicking,
/// and everything accepted must be a path that stays inside a root.
#[doc(hidden)]
pub fn fuzz(input: &[u8]) {
    let Ok(s) = std::str::from_utf8(input) else { return };
    let prefix_ok = check_prefix(s).is_ok();
    if let Ok(k) = Key::parse(s) {
        assert!(prefix_ok, "every key is a valid prefix");
        let k = k.as_str();
        assert!(!k.starts_with('/') && !k.ends_with('/') && !k.contains("//"));
        assert!(k.split('/').all(|seg| seg != "." && seg != ".." && !seg.starts_with('.')));
        assert!(k.bytes().all(|b| b.is_ascii_graphic() && b != b'\\'));
        {
            let mut enc = String::new();
            encode_component(k, &mut enc);
            assert!(enc.bytes().all(
                |b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~' | b'%')
            ));
            assert!(!enc.contains('/'));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grammar() {
        for ok in [
            "a",
            "a/b",
            "HEAD",
            "bases/00000001.base",
            "wal/0001/000000000001.seg",
            "x-y_z.1",
            "console",
            "com",
            "COMX",
        ] {
            assert!(Key::parse(ok).is_ok(), "{ok}");
        }
        for bad in [
            "", "/a", "a/", "a//b", ".", "..", "a/../b", "a/./b", ".hidden", "a/.tmp", "a b",
            "a\\b", "ä", "a\0b", "C:/x", "a?b", "a.", "x/NUL", "con.txt", "com1", "LPT9.log",
        ] {
            assert!(Key::parse(bad).is_err(), "{bad:?}");
        }
        assert!(Key::parse(&"a".repeat(MAX_KEY)).is_ok());
        assert!(Key::parse(&"a".repeat(MAX_KEY + 1)).is_err());
        assert!(
            check_prefix("").is_ok()
                && check_prefix("wal/").is_ok()
                && check_prefix("wal/000").is_ok()
        );
        assert!(check_prefix("/").is_err() && check_prefix("../").is_err());
    }

    #[test]
    fn encoding() {
        let mut s = String::new();
        encode_component("wal/0001/a-b_c.seg", &mut s);
        assert_eq!(s, "wal%2F0001%2Fa-b_c.seg");
    }
}
