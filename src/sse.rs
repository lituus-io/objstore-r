// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Incremental server-sent-events decoder, for streaming responses read
//! through [`crate::transport::BodyStream`].
//!
//! Chunks arrive split at arbitrary byte positions (even inside a UTF-8
//! sequence or a `\r\n`). [`SseDecoder`] buffers only the unconsumed tail,
//! compacts lazily, and hands out each event's `data` payload by reference to
//! its internal buffer — no allocation per event once warm. The buffered
//! event size is capped so a hostile stream cannot grow memory unboundedly.

use crate::{Error, Result};

/// Largest single event accepted, bytes.
pub const MAX_EVENT_BYTES: usize = 8 << 20;

/// Incremental SSE decoder. See the module docs.
#[derive(Debug, Default)]
pub struct SseDecoder {
    buf: Vec<u8>,
    start: usize,
    scanned: usize,
    data: String,
}

impl SseDecoder {
    /// A fresh decoder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a received chunk.
    pub fn push(&mut self, chunk: &[u8]) -> Result<()> {
        if self.start > 0 && self.start * 2 >= self.buf.len() {
            self.buf.drain(..self.start);
            self.scanned -= self.start;
            self.start = 0;
        }
        if self.buf.len() - self.start + chunk.len() > MAX_EVENT_BYTES {
            return Err(Error::TooLarge { limit: MAX_EVENT_BYTES as u64 });
        }
        self.buf.extend_from_slice(chunk);
        Ok(())
    }

    /// The next complete event's `data` payload (multi-line data joined with
    /// `\n`), or `None` until more input arrives. Events without data
    /// (comments, keep-alives, `event:`/`id:`-only) are skipped.
    pub fn next_data(&mut self) -> Result<Option<&str>> {
        loop {
            let Some((end, next)) = self.find_boundary() else { return Ok(None) };
            let event = &self.buf[self.start..end];
            self.start = next;
            self.scanned = next;
            self.data.clear();
            let mut has_data = false;
            for line in event.split(|&b| b == b'\n') {
                let line = line.strip_suffix(b"\r").unwrap_or(line);
                let Some(rest) = line.strip_prefix(b"data:") else { continue };
                let rest = rest.strip_prefix(b" ").unwrap_or(rest);
                if has_data {
                    self.data.push('\n');
                }
                has_data = true;
                let text = std::str::from_utf8(rest)
                    .map_err(|_| Error::Protocol { message: "sse data is not utf-8".into() })?;
                self.data.push_str(text);
            }
            if has_data {
                return Ok(Some(&self.data));
            }
        }
    }

    /// Unconsumed bytes remain (a truncated final event).
    #[must_use]
    pub fn has_partial(&self) -> bool {
        self.buf[self.start..].iter().any(|b| !b.is_ascii_whitespace())
    }

    /// Find the end of the first complete event: a blank line (`\n\n`,
    /// `\r\n\r\n`, or mixed). Returns (event end, next event start).
    fn find_boundary(&mut self) -> Option<(usize, usize)> {
        let b = &self.buf;
        let mut i = self.scanned.max(self.start);
        while i < b.len() {
            if b[i] == b'\n' {
                // Look for the next line being empty: "\n\n" or "\n\r\n".
                if b.get(i + 1) == Some(&b'\n') {
                    return Some((i, i + 2));
                }
                if b.get(i + 1) == Some(&b'\r') && b.get(i + 2) == Some(&b'\n') {
                    return Some((i, i + 3));
                }
                if i + 2 >= b.len() {
                    // Might complete with the next chunk: rescan from here.
                    self.scanned = i;
                    return None;
                }
            }
            i += 1;
        }
        self.scanned = b.len();
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(chunks: &[&[u8]]) -> Vec<String> {
        let mut d = SseDecoder::new();
        let mut out = Vec::new();
        for c in chunks {
            d.push(c).unwrap();
            while let Some(data) = d.next_data().unwrap() {
                out.push(data.to_owned());
            }
        }
        out
    }

    #[test]
    fn whole_events() {
        assert_eq!(collect(&[b"data: a\n\ndata: b\n\n"]), ["a", "b"]);
    }

    #[test]
    fn events_split_at_every_byte() {
        let src =
            b"data: {\"x\":\"\xc3\xa9\"}\r\n\r\n: keep-alive\n\nevent: e\ndata: l1\ndata: l2\n\n";
        let chunks: Vec<&[u8]> = src.chunks(1).collect();
        assert_eq!(collect(&chunks), ["{\"x\":\"\u{e9}\"}", "l1\nl2"]);
    }

    #[test]
    fn no_data_events_are_skipped() {
        assert_eq!(collect(&[b": comment\n\nid: 1\n\n"]), Vec::<String>::new());
    }

    #[test]
    fn partial_tail_is_reported() {
        let mut d = SseDecoder::new();
        d.push(b"data: a\n\ndata: trunc").unwrap();
        assert_eq!(d.next_data().unwrap(), Some("a"));
        assert_eq!(d.next_data().unwrap(), None);
        assert!(d.has_partial());
    }

    #[test]
    fn oversize_events_are_refused() {
        let mut d = SseDecoder::new();
        let big = vec![b'x'; MAX_EVENT_BYTES];
        d.push(&big).unwrap();
        assert!(d.push(b"y").is_err());
    }

    #[test]
    fn buffer_compacts_under_steady_streaming() {
        let mut d = SseDecoder::new();
        for _ in 0..10_000 {
            d.push(b"data: 0123456789\n\n").unwrap();
            assert_eq!(d.next_data().unwrap(), Some("0123456789"));
        }
        assert!(d.buf.capacity() < 4096, "consumed events do not accumulate");
    }
}
