// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
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
