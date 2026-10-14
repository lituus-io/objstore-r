// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Per-object content encoding (`Content-Encoding`), opt in per write.
//!
//! [`ObjectStore::put_encoded`](crate::ObjectStore::put_encoded) compresses a
//! body before it is stored and records the encoding beside it (GCS: the
//! object's `contentEncoding`; [`LocalDir`](crate::LocalDir) and
//! [`Mem`](crate::Mem): the backend's own metadata). Reads decode it:
//!
//! - **Decoded once.** [`get`](crate::ObjectStore::get) inflates into one
//!   fresh buffer and returns it as [`Bytes`]; from there the body is shared
//!   by reference count like any other, so zero-copy consumers (an mmap'd
//!   file written from it, a Python buffer view) are unaffected.
//! - **No ranges.** A byte range of a gzip stream is meaningless, and handing
//!   back compressed bytes would be silent corruption, so
//!   [`get_range`](crate::ObjectStore::get_range) of an encoded object is
//!   [`Error::EncodedRange`]. Encode only objects that are read whole.
//! - **Bounded.** A small object can inflate without bound (a decompression
//!   bomb). Decoding stops at the store's inflated-size limit —
//!   [`MAX_INFLATED_BYTES`] unless the store was built `with_max_inflated` —
//!   with [`Error::TooLarge`]; a truncated or damaged stream is
//!   [`Error::Corrupt`]. Neither ever panics.
//!
//! The codec is feature `gzip` (flate2 on the pure-Rust `miniz_oxide` backend).
//! Without it the type still exists, and every encoded write or read is
//! [`Error::UnsupportedEncoding`] rather than raw compressed bytes.

use crate::{Bytes, Error, Result};

/// Default bound on a decoded body: 1 GiB. A gzip stream inflates at most
/// ~1032:1, so the bound, not the stored size, is what caps the memory one
/// read can claim. Raise or lower it per store with `with_max_inflated`.
pub const MAX_INFLATED_BYTES: u64 = 1 << 30;

/// How an object's stored bytes are encoded. No encoding (`identity`) is
/// `Option::<ContentEncoding>::None` throughout the API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum ContentEncoding {
    /// RFC 1952 gzip (`Content-Encoding: gzip`).
    Gzip,
}

impl ContentEncoding {
    /// The `Content-Encoding` token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            ContentEncoding::Gzip => "gzip",
        }
    }

    /// Parse a `Content-Encoding` token (ASCII case-insensitive): `gzip`, or
    /// `None` for an empty value or `identity`. Anything else is
    /// [`Error::UnsupportedEncoding`].
    pub fn parse(token: &str) -> Result<Option<Self>> {
        let t = token.trim();
        if t.is_empty() || t.eq_ignore_ascii_case("identity") {
            Ok(None)
        } else if t.eq_ignore_ascii_case("gzip") {
            Ok(Some(ContentEncoding::Gzip))
        } else {
            Err(unsupported(t))
        }
    }
}

impl std::fmt::Display for ContentEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

fn unsupported(token: &str) -> Error {
    // Bounded: the token may come from a server or a file.
    Error::UnsupportedEncoding { encoding: token.chars().take(64).collect() }
}

/// Compress `body` for storage.
#[cfg_attr(not(feature = "gzip"), allow(clippy::unnecessary_wraps))]
pub(crate) fn encode(encoding: ContentEncoding, body: &[u8]) -> Result<Bytes> {
    #[cfg(feature = "gzip")]
    {
        use std::io::Write;
        let ContentEncoding::Gzip = encoding;
        // Header mtime 0 and a fixed OS byte (flate2's defaults): the same
        // body always encodes to the same bytes, so a retried create-only
        // write of an immutable object stores identical bytes.
        let mut enc = flate2::write::GzEncoder::new(
            Vec::with_capacity(body.len() / 4 + 64),
            flate2::Compression::default(),
        );
        enc.write_all(body)?;
        Ok(Bytes::from(enc.finish()?))
    }
    #[cfg(not(feature = "gzip"))]
    {
        let _ = body;
        Err(unsupported(encoding.as_str()))
    }
}

/// Decode stored bytes into one fresh buffer, refusing to inflate past
/// `limit` bytes ([`Error::TooLarge`]) and any malformed, truncated or
/// trailing-garbage stream ([`Error::Corrupt`]).
pub(crate) fn decode(encoding: ContentEncoding, stored: &[u8], limit: u64) -> Result<Bytes> {
    #[cfg(feature = "gzip")]
    {
        use std::io::Read;
        let ContentEncoding::Gzip = encoding;
        // Capacity from the trailer's ISIZE (the size mod 2^32), trusted only
        // as far as the stored length could possibly inflate and the limit
        // allows: a lying trailer cannot make this allocate.
        let isize_hint = stored
            .len()
            .checked_sub(4)
            .and_then(|at| stored.get(at..))
            .map_or(0, |t| u64::from(u32::from_le_bytes([t[0], t[1], t[2], t[3]])));
        let ceiling = (stored.len() as u64).saturating_mul(1032);
        let hint = isize_hint.min(ceiling).min(limit);
        let mut out = Vec::with_capacity(usize::try_from(hint).unwrap_or(0));
        // Concatenated members are valid gzip (RFC 1952 §2.2): decode all.
        // One byte past the limit proves the stream is larger than allowed
        // without inflating any further.
        let mut dec = flate2::read::MultiGzDecoder::new(stored).take(limit.saturating_add(1));
        dec.read_to_end(&mut out).map_err(|e| Error::corrupt(format!("gzip stream: {e}")))?;
        if out.len() as u64 > limit {
            return Err(Error::TooLarge { limit });
        }
        Ok(Bytes::from(out))
    }
    #[cfg(not(feature = "gzip"))]
    {
        let _ = (stored, limit);
        Err(unsupported(encoding.as_str()))
    }
}

/// Fuzzing entry point: decode anything under a small bound; a typed error or
/// a body within the bound, never a panic.
#[doc(hidden)]
pub fn fuzz(stored: &[u8]) {
    const LIMIT: u64 = 1 << 20;
    if let Ok(b) = decode(ContentEncoding::Gzip, stored, LIMIT) {
        assert!(b.len() as u64 <= LIMIT);
        if let Ok(again) = encode(ContentEncoding::Gzip, &b) {
            assert_eq!(decode(ContentEncoding::Gzip, &again, LIMIT).ok().as_ref(), Some(&b));
        }
    }
    let _ = ContentEncoding::parse(&String::from_utf8_lossy(stored));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_parse_and_print() {
        assert_eq!(ContentEncoding::parse("gzip").unwrap(), Some(ContentEncoding::Gzip));
        assert_eq!(ContentEncoding::parse(" GZip ").unwrap(), Some(ContentEncoding::Gzip));
        assert_eq!(ContentEncoding::parse("identity").unwrap(), None);
        assert_eq!(ContentEncoding::parse("").unwrap(), None);
        for bad in ["br", "deflate", "gzip, br", "x-gzip"] {
            assert!(
                matches!(ContentEncoding::parse(bad), Err(Error::UnsupportedEncoding { .. })),
                "{bad}"
            );
        }
        assert_eq!(ContentEncoding::Gzip.to_string(), "gzip");
        let long = "z".repeat(10_000);
        let Err(Error::UnsupportedEncoding { encoding }) = ContentEncoding::parse(&long) else {
            panic!("unknown token accepted")
        };
        assert_eq!(encoding.len(), 64, "a hostile token is bounded in the error");
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn round_trip_bounds_and_damage() {
        let body: Vec<u8> = b"the same line, again and again\n".repeat(1000);
        let gz = encode(ContentEncoding::Gzip, &body).unwrap();
        assert!(gz.len() * 10 < body.len(), "{} -> {}", body.len(), gz.len());
        assert_eq!(gz, encode(ContentEncoding::Gzip, &body).unwrap(), "deterministic");
        assert_eq!(decode(ContentEncoding::Gzip, &gz, body.len() as u64).unwrap(), body);
        assert!(matches!(
            decode(ContentEncoding::Gzip, &gz, body.len() as u64 - 1),
            Err(Error::TooLarge { .. })
        ));
        assert!(matches!(
            decode(ContentEncoding::Gzip, &gz[..gz.len() - 3], u64::MAX),
            Err(Error::Corrupt { .. })
        ));
        let mut bad = gz.to_vec();
        let mid = bad.len() / 2;
        bad[mid] ^= 0x55;
        assert!(decode(ContentEncoding::Gzip, &bad, u64::MAX).is_err());
        assert!(matches!(decode(ContentEncoding::Gzip, b"", 10), Err(Error::Corrupt { .. })));
        assert!(matches!(
            decode(ContentEncoding::Gzip, b"plain text", 10),
            Err(Error::Corrupt { .. })
        ));
        // Two members concatenated decode to both bodies.
        let mut two = encode(ContentEncoding::Gzip, b"ab").unwrap().to_vec();
        two.extend_from_slice(&encode(ContentEncoding::Gzip, b"cd").unwrap());
        assert_eq!(decode(ContentEncoding::Gzip, &two, 10).unwrap(), &b"abcd"[..]);
        // The empty body is a valid stream.
        let empty = encode(ContentEncoding::Gzip, b"").unwrap();
        assert!(decode(ContentEncoding::Gzip, &empty, 0).unwrap().is_empty());
    }

    #[cfg(not(feature = "gzip"))]
    #[test]
    fn without_the_codec_encoded_io_is_refused() {
        assert!(matches!(
            encode(ContentEncoding::Gzip, b"x"),
            Err(Error::UnsupportedEncoding { .. })
        ));
        assert!(matches!(
            decode(ContentEncoding::Gzip, b"x", 1),
            Err(Error::UnsupportedEncoding { .. })
        ));
    }
}
