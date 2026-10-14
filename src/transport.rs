// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! A minimal authorised-HTTP seam for Google REST APIs beyond Cloud Storage
//! (Vertex AI, `BigQuery`), so dependants reuse this crate's TLS stack, token
//! providers, retry policy and size caps instead of carrying their own HTTP
//! client.
//!
//! The trait is dependency-free and always compiled; the reqwest-backed
//! implementation is [`crate::http::Client`] (feature `http`). Futures are
//! named associated types, never boxed, with no `Send`/`'static` bounds
//! imposed here — the same rules as [`crate::ObjectStore`].
//!
//! Two ways to call:
//!
//! - [`Transport::send`]: one request, whole body back (bounded).
//! - [`Transport::open`]: status now, body as a [`BodyStream`] of chunks
//!   (server-sent events, long generations). **Dropping the stream cancels
//!   the request**, which is how a caller stops paying for output it no
//!   longer wants.

pub use crate::key::encode_component;
use crate::{Bytes, Result};
use std::future::Future;

/// HTTP method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    /// `GET`
    Get,
    /// `POST`
    Post,
    /// `PUT`
    Put,
    /// `DELETE`
    Delete,
    /// `PATCH`
    Patch,
}

/// When a failed attempt may be repeated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Retry {
    /// Retry on transport errors and 408/429/5xx. Use for reads, and for
    /// writes the server de-duplicates (e.g. a `BigQuery` `requestId`).
    #[default]
    Transient,
    /// Retry only on 429 and on connect failures — cases where the server
    /// provably did not start the work. Use for non-idempotent calls.
    Throttled,
    /// Never retry.
    Never,
}

/// One request. Everything is borrowed except the body, which is a
/// reference-counted [`Bytes`] so retries resend it without copying.
#[derive(Debug, Clone)]
pub struct Request<'a> {
    /// Method.
    pub method: Method,
    /// Absolute `https://` URL (plain `http://` only to loopback).
    pub url: &'a str,
    /// Body (empty for none).
    pub body: Bytes,
    /// `content-type` of the body, when there is one.
    pub content_type: Option<&'a str>,
    /// Extra headers. `authorization` is set by the transport and must not
    /// be passed here.
    pub headers: &'a [(&'a str, &'a str)],
    /// Retry policy.
    pub retry: Retry,
}

impl<'a> Request<'a> {
    /// `GET url`.
    #[must_use]
    pub fn get(url: &'a str) -> Self {
        Self {
            method: Method::Get,
            url,
            body: Bytes::new(),
            content_type: None,
            headers: &[],
            retry: Retry::Transient,
        }
    }

    /// `POST url` with a JSON body.
    #[must_use]
    pub fn post_json(url: &'a str, body: Bytes) -> Self {
        Self {
            method: Method::Post,
            url,
            body,
            content_type: Some("application/json"),
            headers: &[],
            retry: Retry::Transient,
        }
    }

    /// `PATCH url` with a JSON body. Retried only when the server provably
    /// did not start the work ([`Retry::Throttled`]); pair it with an
    /// `If-Match` header for optimistic concurrency.
    #[must_use]
    pub fn patch_json(url: &'a str, body: Bytes) -> Self {
        Self {
            method: Method::Patch,
            url,
            body,
            content_type: Some("application/json"),
            headers: &[],
            retry: Retry::Throttled,
        }
    }

    /// Replace the retry policy.
    #[must_use]
    pub fn retry(mut self, retry: Retry) -> Self {
        self.retry = retry;
        self
    }

    /// Replace the extra headers.
    #[must_use]
    pub fn headers(mut self, headers: &'a [(&'a str, &'a str)]) -> Self {
        self.headers = headers;
        self
    }
}

/// A complete response.
#[derive(Debug, Clone)]
pub struct Response {
    /// HTTP status.
    pub status: u16,
    /// Body (bounded by the transport's limit).
    pub body: Bytes,
}

impl Response {
    /// True for 2xx.
    #[must_use]
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// The error envelope of this (non-2xx) response.
    #[must_use]
    pub fn api_error(&self) -> ApiError {
        ApiError::parse(self.status, &self.body)
    }

    /// `Ok(self)` for 2xx, else the status mapped to a typed [`crate::Error`]
    /// (401/403 → `Auth`, 408/429/5xx → retriable `Unavailable`, others →
    /// `Protocol`) carrying a short, control-character-free excerpt of the
    /// server's message — never the request or a token.
    pub fn error_for_status(self) -> Result<Self> {
        if self.is_success() {
            Ok(self)
        } else {
            Err(crate::net::status_error(self.status, &self.body))
        }
    }
}

/// A Google API error envelope (`{"error": {"code", "message", "errors":
/// [{"reason"}], "status"}}`), reduced to what callers branch on. Text is
/// control-character-free and bounded; a body that is not an envelope keeps
/// a short excerpt as its message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    /// HTTP status; for an error inside a 2xx body (a streamed error event,
    /// parsed with status 0), the envelope's numeric `code`.
    pub status: u16,
    /// The first `reason` (for example `accessDenied`), or empty.
    pub reason: String,
    /// The server's message.
    pub message: String,
}

impl ApiError {
    /// Longest message kept.
    pub const MAX_MESSAGE: usize = 500;

    /// Parse an error body. Never fails: anything unparseable becomes the
    /// message excerpt.
    #[must_use]
    pub fn parse(status: u16, body: &[u8]) -> Self {
        let (mut message, mut reason, mut code) = (None, None, None);
        json_members(body, |key, value, is_str| {
            let slot = match (key, is_str) {
                ("message", true) => &mut message,
                ("reason", true) => &mut reason,
                ("code", false) => {
                    code = code.or_else(|| value.parse::<u16>().ok());
                    return;
                }
                _ => return,
            };
            if slot.is_none() {
                *slot = Some(clean(value));
            }
        });
        let status = if status == 0 { code.unwrap_or(0) } else { status };
        let message = message.unwrap_or_else(|| {
            clean(&String::from_utf8_lossy(&body[..body.len().min(Self::MAX_MESSAGE)]))
        });
        Self { status, reason: reason.unwrap_or_default(), message }
    }
}

fn clean(s: &str) -> String {
    s.chars().filter(|c| !c.is_control()).take(ApiError::MAX_MESSAGE).collect()
}

/// Call `f(key, value, is_string)` for every scalar member of a JSON text
/// (strings decoded, other scalars raw), at any depth, in document order;
/// stops quietly at malformed input.
fn json_members(b: &[u8], mut f: impl FnMut(&str, &str, bool)) {
    let mut i = 0;
    let mut key: Option<String> = None;
    let mut after_colon = false;
    while i < b.len() {
        match b[i] {
            b'"' => {
                let Some((s, next)) = json_string(b, i + 1) else { return };
                i = next;
                if after_colon {
                    if let Some(k) = key.take() {
                        f(&k, &s, true);
                    }
                    after_colon = false;
                } else {
                    key = Some(s);
                }
            }
            b':' => {
                after_colon = key.is_some();
                i += 1;
            }
            b',' | b'{' | b'}' | b'[' | b']' => {
                key = None;
                after_colon = false;
                i += 1;
            }
            c if after_colon && !c.is_ascii_whitespace() => {
                let end = b[i..]
                    .iter()
                    .position(|c| matches!(c, b',' | b'}' | b']') || c.is_ascii_whitespace())
                    .map_or(b.len(), |n| i + n);
                if let (Some(k), Ok(v)) = (key.take(), std::str::from_utf8(&b[i..end])) {
                    f(&k, v, false);
                }
                after_colon = false;
                i = end;
            }
            _ => i += 1,
        }
    }
}

/// Decode the JSON string starting after its opening quote at `i`; returns
/// it and the index after its closing quote.
fn json_string(b: &[u8], mut i: usize) -> Option<(String, usize)> {
    let mut out = Vec::new();
    while i < b.len() {
        match b[i] {
            b'"' => return Some((String::from_utf8_lossy(&out).into_owned(), i + 1)),
            b'\\' => {
                let e = *b.get(i + 1)?;
                i += 2;
                let c = match e {
                    b'n' => '\n',
                    b't' => '\t',
                    b'r' => '\r',
                    b'b' => '\u{8}',
                    b'f' => '\u{c}',
                    b'u' => {
                        let hex = |at: usize| {
                            std::str::from_utf8(b.get(at..at + 4)?)
                                .ok()
                                .and_then(|h| u32::from_str_radix(h, 16).ok())
                        };
                        let hi = hex(i)?;
                        i += 4;
                        if (0xD800..0xDC00).contains(&hi) && b.get(i..i + 2) == Some(b"\\u") {
                            let lo = hex(i + 2)?;
                            i += 6;
                            char::from_u32(
                                0x10000 + ((hi - 0xD800) << 10) + (lo.wrapping_sub(0xDC00) & 0x3FF),
                            )
                            .unwrap_or('\u{FFFD}')
                        } else {
                            char::from_u32(hi).unwrap_or('\u{FFFD}')
                        }
                    }
                    other => char::from(other),
                };
                let mut tmp = [0; 4];
                out.extend_from_slice(c.encode_utf8(&mut tmp).as_bytes());
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    None
}

/// A response body delivered in chunks as they arrive.
pub trait BodyStream {
    /// Future of [`next_chunk`](BodyStream::next_chunk).
    type Next<'a>: Future<Output = Result<Option<Bytes>>> + 'a
    where
        Self: 'a;

    /// The next chunk, or `None` at the end of the body. The total is bounded
    /// by the transport's limit.
    fn next_chunk(&mut self) -> Self::Next<'_>;
}

/// An authorised HTTPS client.
pub trait Transport {
    /// Future of [`send`](Transport::send).
    type Send<'a>: Future<Output = Result<Response>> + 'a
    where
        Self: 'a;
    /// The chunked body of an [`open`](Transport::open)ed response.
    type Body<'a>: BodyStream + 'a
    where
        Self: 'a;
    /// Future of [`open`](Transport::open).
    type Open<'a>: Future<Output = Result<(u16, Self::Body<'a>)>> + 'a
    where
        Self: 'a;

    /// Send `req` and read the whole body. Non-2xx statuses are returned, not
    /// raised (see [`Response::error_for_status`]); only transport failures
    /// and limit violations are errors.
    fn send<'a>(&'a self, req: Request<'a>) -> Self::Send<'a>;

    /// Send `req` and return the status with a chunked body. Retries apply
    /// until response headers arrive, never after the body has started.
    fn open<'a>(&'a self, req: Request<'a>) -> Self::Open<'a>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_envelopes_parse_to_reason_and_message() {
        // `U+` stands for a JSON `\u` escape.
        let esc = |t: &str| t.replace("U+", "\\u");
        let body =
            esc(r#"{"error": {"code": 400, "message": "Unrecognized name: \"x\" at [1:8]U+00e9",
            "errors": [{"message": "inner", "domain": "global", "reason": "invalidQuery"}],
            "status": "INVALID_ARGUMENT"}}"#);
        let e = ApiError::parse(400, body.as_bytes());
        assert_eq!(e.reason, "invalidQuery");
        assert_eq!(e.message, "Unrecognized name: \"x\" at [1:8]\u{e9}");
        let raw = ApiError::parse(502, b"<html>bad\x1b[31m gateway</html>");
        assert_eq!(
            (raw.reason.as_str(), raw.message.as_str()),
            ("", "<html>bad[31m gateway</html>")
        );
        let pair =
            ApiError::parse(500, esc(r#"{"message": "U+d83dU+de00 x", "reason": 7}"#).as_bytes());
        assert_eq!(pair.message, "\u{1F600} x");
        for cut in 0..body.len() {
            let _ = ApiError::parse(400, &body.as_bytes()[..cut]);
        }
        assert!(ApiError::parse(400, &[b'x'; 2000]).message.len() <= ApiError::MAX_MESSAGE);
        // An error event inside a 2xx stream carries its own code; arrays wrap.
        let ev = ApiError::parse(0, br#"[{"error":{"code":429,"message":"slow down"}}]"#);
        assert_eq!((ev.status, ev.message.as_str()), (429, "slow down"));
        assert_eq!(ApiError::parse(503, br#"{"code": 400}"#).status, 503);
    }
}
