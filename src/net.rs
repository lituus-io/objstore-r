// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Network policy shared by the GCS store and the generic HTTP client: which
//! statuses are transient, backoff timing, endpoint rules and error mapping.
//! One implementation, so both clients behave identically.

// Only `status_error` is used without the HTTP stack (by `transport`).
#![cfg_attr(not(feature = "http"), allow(dead_code))]

use crate::Error;
use std::time::Duration;

/// 408/429/5xx: worth another attempt.
#[must_use]
pub(crate) fn is_retriable_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

/// Plain `http://` is accepted only for a loopback emulator or test server.
#[must_use]
pub(crate) fn is_loopback_http(url: &str) -> bool {
    ["http://127.0.0.1:", "http://127.0.0.1/", "http://localhost:", "http://[::1]:"]
        .iter()
        .any(|p| url.starts_with(p))
}

/// `https://`, or loopback `http://`.
#[must_use]
pub(crate) fn is_allowed_endpoint(url: &str) -> bool {
    url.starts_with("https://") || is_loopback_http(url)
}

/// 100 ms × 2^(n-1), capped at 5 s, with jitter over its upper half.
#[must_use]
pub(crate) fn backoff(attempt: u32) -> Duration {
    let base = 100u64.saturating_mul(1 << attempt.saturating_sub(1).min(6)).min(5_000);
    Duration::from_millis(base / 2 + jitter() % (base / 2 + 1))
}

/// A cheap, well-mixed pseudo-random number (xorshift over a time-seeded counter);
/// only used to de-synchronize retries, never for anything security-relevant.
fn jitter() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static STATE: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let mut x = STATE.fetch_add(u64::from(nanos) | 1, Ordering::Relaxed);
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

/// Map a non-2xx status and body to a typed error with a short, sanitised
/// excerpt of the server's message (never our request or token).
#[must_use]
pub(crate) fn status_error(status: u16, body: &[u8]) -> Error {
    let excerpt: String = String::from_utf8_lossy(&body[..body.len().min(200)])
        .chars()
        .filter(|c| !c.is_control())
        .collect();
    match status {
        401 | 403 => Error::Auth { message: format!("{status}: {excerpt}") },
        s if is_retriable_status(s) => Error::unavailable(format!("{s}: {excerpt}"), true),
        _ => Error::protocol(format!("{status}: {excerpt}")),
    }
}

/// Map a reqwest failure to a typed error (no URL, no credentials).
#[cfg(feature = "http")]
#[must_use]
pub(crate) fn transport_error(e: &reqwest::Error) -> Error {
    let kind = if e.is_timeout() {
        "timed out"
    } else if e.is_connect() {
        "connection failed"
    } else {
        "request failed"
    };
    Error::unavailable(kind, e.is_timeout() || e.is_connect() || e.is_request())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_bounded() {
        for n in 1..20 {
            let d = backoff(n);
            assert!(d <= Duration::from_millis(5_000) && d >= Duration::from_millis(25), "{d:?}");
        }
    }

    #[test]
    fn endpoints() {
        assert!(is_allowed_endpoint("https://aiplatform.googleapis.com"));
        assert!(is_allowed_endpoint("http://127.0.0.1:8080/x"));
        assert!(!is_allowed_endpoint("http://example.com"));
        assert!(!is_allowed_endpoint("ftp://127.0.0.1:1"));
    }

    #[test]
    fn status_mapping_is_typed_and_sanitised() {
        assert!(matches!(status_error(403, b"denied"), Error::Auth { .. }));
        assert!(status_error(503, b"busy").is_retriable());
        let e = status_error(400, "bad\u{1b}[31m request".as_bytes());
        assert!(matches!(&e, Error::Protocol { message } if !message.contains('\u{1b}')));
    }
}
