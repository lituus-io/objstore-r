// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! The two byte-range headers a resumed transfer trusts: the committed
//! offset of a resumable upload (`Range` on a 308) and the span of a resumed
//! download (`Content-Range` on a 206). Both come from the network, so both
//! are parsed strictly: a value that does not fit the transfer is an error,
//! never a guess, and never a reason to loop.

/// The committed length a 308 reports: `Range: bytes=0-<last>` is `last + 1`
/// bytes, and no header is none. `None` for anything else, or for a length
/// past `total`.
pub(crate) fn committed(range: Option<&str>, total: u64) -> Option<u64> {
    let Some(v) = range else { return Some(0) };
    let last = number(v.trim().strip_prefix("bytes=0-")?)?;
    last.checked_add(1).filter(|n| *n <= total)
}

/// A 206's `Content-Range: bytes <first>-<last>/<total>` as
/// `(first, last, total)`, with `first <= last < total`. `None` for an
/// unknown total (`*`) or anything malformed.
pub(crate) fn content_range(v: &str) -> Option<(u64, u64, u64)> {
    let (span, total) = v.trim().strip_prefix("bytes ")?.split_once('/')?;
    let (first, last) = span.split_once('-')?;
    let (first, last, total) = (number(first)?, number(last)?, number(total)?);
    (first <= last && last < total).then_some((first, last, total))
}

/// ASCII digits only (no sign, no space, no `+`), fitting a `u64`.
fn number(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn committed_lengths() {
        assert_eq!(committed(None, 10), Some(0));
        assert_eq!(committed(Some("bytes=0-0"), 10), Some(1));
        assert_eq!(committed(Some(" bytes=0-9 "), 10), Some(10));
        for bad in [
            "bytes=0-10", // past the total
            "bytes=1-5",  // not from zero
            "bytes=0-",
            "bytes=0--1",
            "bytes=0-+3",
            "bytes=0-3,5-6",
            "bytes=0-18446744073709551615", // overflows on +1
            "items=0-3",
            "",
            "garbage",
        ] {
            assert_eq!(committed(Some(bad), 10), None, "{bad}");
        }
    }

    #[test]
    fn content_ranges() {
        assert_eq!(content_range("bytes 5-9/10"), Some((5, 9, 10)));
        assert_eq!(content_range("bytes 0-0/1"), Some((0, 0, 1)));
        for bad in [
            "bytes 5-9/*",
            "bytes */10",
            "bytes 9-5/10",
            "bytes 5-10/10",
            "bytes 5-9",
            "bytes -1-9/10",
            "5-9/10",
            "bytes 5 - 9/10",
            "",
        ] {
            assert_eq!(content_range(bad), None, "{bad}");
        }
    }
}
