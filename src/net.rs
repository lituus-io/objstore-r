// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Network policy shared by every HTTP client of the stack (the GCS store,
//! the authorised transport, link-r's crawler): which statuses are
//! transient, backoff timing, `Retry-After`, endpoint rules and error
//! mapping. One implementation, so all clients behave identically.

// Only `status_error` is used without the HTTP stack (by `transport`).
#![cfg_attr(not(feature = "http"), allow(dead_code))]

use crate::Error;
use std::time::Duration;

/// 408/429/5xx: worth another attempt.
#[must_use]
pub fn is_retriable_status(status: u16) -> bool {
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

/// The wait before retry `attempt` (1-based): 100 ms × 2^(n-1), capped at
/// 5 s, with jitter over its upper half so clients de-synchronize.
#[must_use]
pub fn backoff(attempt: u32) -> Duration {
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

/// A `Retry-After` value in either RFC 9110 form — delay-seconds or an
/// IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`) — as the wait from
/// `now_ms`; `None` when it is neither. Callers clamp it.
#[must_use]
pub fn retry_after(value: &str, now_ms: u64) -> Option<Duration> {
    let v = value.trim();
    let ms = match v.parse::<u64>() {
        Ok(secs) => secs.saturating_mul(1000),
        Err(_) => imf_fixdate_ms(v)?.saturating_sub(now_ms),
    };
    Some(Duration::from_millis(ms))
}

/// An IMF-fixdate as Unix milliseconds.
fn imf_fixdate_ms(s: &str) -> Option<u64> {
    const MONTHS: [&str; 12] =
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let rest = s.split_once(", ").map_or(s, |(_, r)| r);
    let mut parts = rest.split_ascii_whitespace();
    let day: i64 = parts.next()?.parse().ok()?;
    let month = parts.next()?;
    let mon = i64::try_from(MONTHS.iter().position(|m| *m == month)?).ok()? + 1;
    let year: i64 = parts.next()?.parse().ok()?;
    let mut hms = parts.next()?.split(':');
    let h: u64 = hms.next()?.parse().ok()?;
    let m: u64 = hms.next()?.parse().ok()?;
    let sec: u64 = hms.next()?.parse().ok()?;
    if parts.next() != Some("GMT") || !(1..=31).contains(&day) || h > 23 || m > 59 || sec > 60 {
        return None;
    }
    // Days from civil (proleptic Gregorian); day 0 is 1970-01-01.
    let y = year - i64::from(mon <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (mon + if mon > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = u64::try_from(era * 146_097 + doe - 719_468).ok()?;
    Some((days * 86_400 + h * 3600 + m * 60 + sec) * 1000)
}

/// Map a non-2xx status and body to a typed error with a short, sanitised
/// excerpt of the server's message (never our request or token).
#[must_use]
pub(crate) fn status_error(status: u16, body: &[u8]) -> Error {
    let excerpt = crate::transport::ApiError::parse(status, body).message;
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
    fn retry_after_takes_both_forms() {
        assert_eq!(retry_after(" 7 ", 0), Some(Duration::from_secs(7)));
        assert_eq!(
            retry_after("Thu, 01 Jan 1970 00:00:10 GMT", 4_000),
            Some(Duration::from_secs(6))
        );
        let rfc = imf_fixdate_ms("Sun, 06 Nov 1994 08:49:37 GMT");
        assert_eq!(rfc, Some(784_111_777_000));
        assert_eq!(retry_after("Sun, 06 Nov 1994 08:49:37 GMT", u64::MAX), Some(Duration::ZERO));
        assert_eq!(imf_fixdate_ms("06 Nov 1994 08:49:37 GMT"), rfc, "the weekday is optional");
        assert_eq!(imf_fixdate_ms("Thu, 01 Jan 1970 00:00:00 GMT"), Some(0));
        for bad in [
            "",
            "soon",
            "Sun, 32 Nov 1994 08:49:37 GMT",
            "Sun, 06 Nov 1994 25:00:00 GMT",
            "Sun, 06 Nov 1994 08:49:37 PST",
            "Sun, 06 Foo 1994 08:49:37 GMT",
        ] {
            assert_eq!(retry_after(bad, 0), None, "{bad}");
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
