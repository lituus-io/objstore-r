// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! [`Client`]: the reqwest-backed [`Transport`] (feature `http`).
//!
//! It applies the same policy as the GCS store: HTTPS only (loopback `http`
//! for emulators and tests), redirects refused, bounded bodies, bearer tokens
//! from any [`TokenProvider`] (plus the quota-project header for user
//! credentials), and jittered exponential backoff that honours `Retry-After`.
//! One client holds one connection pool; with HTTP/2 many concurrent calls
//! share a connection.
//!
//! ```no_run
//! # async fn demo() -> objstore_r::Result<()> {
//! use objstore_r::transport::{Request, Transport};
//! use objstore_r::{http::Client, token::Adc, Bytes};
//!
//! let client = Client::new(Adc::discover()?, Default::default())?;
//! let url = "https://us-central1-aiplatform.googleapis.com/v1/projects/p/locations/us-central1/publishers/google/models/m:generateContent";
//! let resp = client.send(Request::post_json(url, Bytes::from_static(b"{}"))).await?.error_for_status()?;
//! # let _ = resp; Ok(()) }
//! ```

use crate::net::{backoff, is_allowed_endpoint, is_retriable_status, transport_error};
use crate::token::TokenProvider;
use crate::transport::{BodyStream, Method, Request, Response, Retry, Transport};
use crate::{Bytes, Error, Result};
use std::future::Future;
use std::time::Duration;

/// Longest server-requested `Retry-After` honoured.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);

/// Settings.
#[derive(Debug, Clone)]
pub struct HttpConfig {
    /// Whole-request timeout for [`Transport::send`].
    pub timeout: Duration,
    /// Whole-request timeout for [`Transport::open`] (long streams).
    pub stream_timeout: Duration,
    /// Connection-establishment timeout.
    pub connect_timeout: Duration,
    /// Retries after the first attempt (per the request's [`Retry`] policy).
    pub max_retries: u32,
    /// Largest body accepted, whole or streamed.
    pub max_response_bytes: u64,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            stream_timeout: Duration::from_secs(300),
            connect_timeout: Duration::from_secs(10),
            max_retries: 4,
            max_response_bytes: 64 << 20,
        }
    }
}

/// An authorised HTTPS client. See the module docs.
#[derive(Debug)]
pub struct Client<T: TokenProvider> {
    http: reqwest::Client,
    tokens: T,
    cfg: HttpConfig,
}

impl<T: TokenProvider> Client<T> {
    /// A client authenticating with `tokens`.
    pub fn new(tokens: T, cfg: HttpConfig) -> Result<Self> {
        let http = reqwest::Client::builder()
            .connect_timeout(cfg.connect_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .pool_idle_timeout(Duration::from_secs(90))
            .user_agent(concat!("objstore-r/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| Error::unavailable(format!("http client: {e}"), false))?;
        Ok(Self { http, tokens, cfg })
    }

    /// The token provider.
    pub fn tokens(&self) -> &T {
        &self.tokens
    }

    /// The settings.
    pub fn config(&self) -> &HttpConfig {
        &self.cfg
    }

    fn check(req: &Request<'_>) -> Result<()> {
        if !is_allowed_endpoint(req.url) {
            return Err(Error::protocol("the URL must be https:// (http:// only to loopback)"));
        }
        if req.headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("authorization")) {
            return Err(Error::protocol("authorization is set by the transport"));
        }
        Ok(())
    }

    async fn build(&self, req: &Request<'_>, timeout: Duration) -> Result<reqwest::RequestBuilder> {
        let method = match req.method {
            Method::Get => reqwest::Method::GET,
            Method::Post => reqwest::Method::POST,
            Method::Put => reqwest::Method::PUT,
            Method::Delete => reqwest::Method::DELETE,
            Method::Patch => reqwest::Method::PATCH,
        };
        let mut rb = self.http.request(method, req.url).timeout(timeout);
        let t = self.tokens.token().await?;
        if !t.secret().is_empty() {
            rb = rb.bearer_auth(t.secret());
        }
        if let Some(q) = self.tokens.quota_project() {
            rb = rb.header("x-goog-user-project", q);
        }
        if let Some(ct) = req.content_type {
            rb = rb.header("content-type", ct);
        }
        for &(k, v) in req.headers {
            rb = rb.header(k, v);
        }
        if !req.body.is_empty() || matches!(req.method, Method::Post | Method::Put | Method::Patch)
        {
            rb = rb.body(req.body.clone());
        }
        Ok(rb)
    }

    /// Send with the request's retry policy; returns once headers arrive.
    async fn exchange(&self, req: &Request<'_>, timeout: Duration) -> Result<reqwest::Response> {
        Self::check(req)?;
        let mut attempt = 0;
        loop {
            let outcome = self.build(req, timeout).await?.send().await;
            let retry = match (&outcome, req.retry) {
                (_, Retry::Never) => false,
                (Ok(r), Retry::Transient) => is_retriable_status(r.status().as_u16()),
                (Ok(r), Retry::Throttled) => r.status().as_u16() == 429,
                (Err(e), Retry::Transient) => e.is_timeout() || e.is_connect() || e.is_request(),
                (Err(e), Retry::Throttled) => e.is_connect(),
            };
            if !retry || attempt >= self.cfg.max_retries {
                return outcome.map_err(|e| transport_error(&e));
            }
            attempt += 1;
            let wait = outcome
                .as_ref()
                .ok()
                .and_then(retry_after)
                .map_or(backoff(attempt), |ra| ra.max(backoff(attempt)));
            tokio::time::sleep(wait).await;
        }
    }

    async fn read_all(&self, resp: reqwest::Response) -> Result<Bytes> {
        let limit = self.cfg.max_response_bytes;
        if resp.content_length().is_some_and(|n| n > limit) {
            return Err(Error::TooLarge { limit });
        }
        let mut body = Chunks { resp, left: limit, limit };
        // Most responses arrive in one chunk: return it without copying.
        let Some(first) = body.next().await? else { return Ok(Bytes::new()) };
        let Some(second) = body.next().await? else { return Ok(first) };
        let mut buf = bytes::BytesMut::with_capacity(first.len() + second.len());
        buf.extend_from_slice(&first);
        buf.extend_from_slice(&second);
        while let Some(chunk) = body.next().await? {
            buf.extend_from_slice(&chunk);
        }
        Ok(buf.freeze())
    }
}

/// The response's `Retry-After` (either form), capped.
fn retry_after(resp: &reqwest::Response) -> Option<Duration> {
    let v = resp.headers().get("retry-after")?.to_str().ok()?;
    Some(crate::net::retry_after(v, graph_r::fsio::now_ms())?.min(MAX_RETRY_AFTER))
}

/// A bounded chunked body. Dropping it aborts the transfer.
#[derive(Debug)]
pub struct Chunks {
    resp: reqwest::Response,
    left: u64,
    limit: u64,
}

impl Chunks {
    async fn next(&mut self) -> Result<Option<Bytes>> {
        let chunk = self.resp.chunk().await.map_err(|e| transport_error(&e))?;
        if let Some(c) = &chunk {
            let n = c.len() as u64;
            if n > self.left {
                return Err(Error::TooLarge { limit: self.limit });
            }
            self.left -= n;
        }
        Ok(chunk)
    }
}

impl BodyStream for Chunks {
    type Next<'a>
        = impl Future<Output = Result<Option<Bytes>>> + 'a
    where
        Self: 'a;

    fn next_chunk(&mut self) -> Self::Next<'_> {
        self.next()
    }
}

impl<T: TokenProvider> Transport for Client<T> {
    type Send<'a>
        = impl Future<Output = Result<Response>> + 'a
    where
        Self: 'a;
    type Body<'a>
        = Chunks
    where
        Self: 'a;
    type Open<'a>
        = impl Future<Output = Result<(u16, Chunks)>> + 'a
    where
        Self: 'a;

    fn send<'a>(&'a self, req: Request<'a>) -> Self::Send<'a> {
        async move {
            let resp = self.exchange(&req, self.cfg.timeout).await?;
            let status = resp.status().as_u16();
            let body = self.read_all(resp).await?;
            Ok(Response { status, body })
        }
    }

    fn open<'a>(&'a self, req: Request<'a>) -> Self::Open<'a> {
        async move {
            let resp = self.exchange(&req, self.cfg.stream_timeout).await?;
            let limit = self.cfg.max_response_bytes;
            if resp.content_length().is_some_and(|n| n > limit) {
                return Err(Error::TooLarge { limit });
            }
            Ok((resp.status().as_u16(), Chunks { resp, left: limit, limit }))
        }
    }
}

/// Run `fut` to completion on a single-threaded runtime driving this
/// transport's I/O and timers — for command-line tools with a synchronous
/// `main`. Libraries should stay runtime-agnostic and leave this to the
/// binary. Fails only if the runtime cannot start.
#[cfg(feature = "blocking")]
pub fn block_on<F: std::future::Future>(fut: F) -> std::io::Result<F::Output> {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    Ok(rt.block_on(fut))
}
