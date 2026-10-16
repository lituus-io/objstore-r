// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Google Cloud Storage over the JSON API (feature `gcs`).
//!
//! - **Preconditions** map to `ifGenerationMatch` (`0` for create-only).
//!   Status 412 becomes [`Error::Conflict`] or [`Error::PreconditionFailed`].
//! - **Retries** with jittered exponential backoff on 408/429/5xx and on
//!   transport errors. A conditional write whose first attempt succeeded but
//!   whose response was lost comes back as 412 on retry; the store then reads
//!   the object back and, if it holds exactly our bytes, reports success.
//! - **Uploads** are single requests below [`GcsConfig::resumable_threshold`]
//!   and chunked resumable sessions at or above it: each
//!   [`GcsConfig::upload_chunk`] is one `PUT` with `Content-Range`, and a
//!   failed chunk resumes from the offset the server reports as committed,
//!   so only the remainder is sent again.
//! - **Downloads** resume: a whole-object read that drops mid-body asks for
//!   the rest with `Range`, pinned to the generation it started on, and
//!   verifies the object's CRC-32C over the assembled bytes.
//! - **Timeouts** bound stalls, not sizes (see [`GcsConfig`]): a transfer may
//!   take as long as it needs while bytes keep moving, and fails within
//!   [`GcsConfig::idle_timeout`] once they stop.
//! - **Ranged reads** send `Range`, so a caller can fetch a header or one
//!   section of a large snapshot without downloading all of it.
//! - **Safety:** the endpoint must be `https://` (plain `http://` is accepted
//!   only for a loopback emulator); redirects are refused; response bodies are
//!   bounded by [`GcsConfig::max_object_bytes`]; error messages never include
//!   tokens or request bodies.
//! - **Content encoding** ([`ObjectStore::put_encoded`]): the compressed body
//!   is uploaded with the `contentEncoding` insert parameter (documented for
//!   `uploadType=media`, and accepted by the resumable session start).
//!   Every read sends `Accept-Encoding: gzip`, so Cloud Storage serves the
//!   stored bytes as they are instead of decompressing them server-side
//!   (decompressive transcoding, which also ignores `Range`). A body served
//!   with `Content-Encoding` is checked against the object's CRC-32C from
//!   `x-goog-hash` — which covers the stored, compressed bytes — *before* it
//!   is decoded, then inflated once under [`Gcs::with_max_inflated`]'s bound.
//!   A ranged read of an encoded object is refused from the response headers,
//!   before any body is read.

mod crc32c;
mod ranges;
mod wire;

#[doc(hidden)]
pub use wire::fuzz as fuzz_wire;

use crate::encoding::{self, ContentEncoding, MAX_INFLATED_BYTES};
use crate::key::{check_prefix, encode_component};
use crate::net::{backoff, is_allowed_endpoint, is_retriable_status, transport_error};
use crate::token::TokenProvider;
use crate::{Bytes, Cond, Error, Key, Meta, Object, ObjectStore, Result, Stat};
use std::future::Future;
use std::ops::Range;
use std::time::Duration;

/// One listing page: its items and the next page token.
type Page<M> = (Vec<M>, Option<String>);

/// Granularity of a resumable upload chunk (Cloud Storage's rule).
const CHUNK_UNIT: usize = 256 << 10;

/// Settings.
///
/// No timeout covers a whole transfer: a 300 MiB object over a slow link
/// takes as long as it takes. What is bounded is time without progress — a
/// response that does not start, a body that stops arriving, an upload chunk
/// that does not finish at a floor rate — so a dead connection fails in
/// about [`idle_timeout`](Self::idle_timeout) whatever the object's size.
#[derive(Debug, Clone)]
pub struct GcsConfig {
    /// API endpoint (`https://storage.googleapis.com`, or an emulator).
    pub endpoint: String,
    /// Connection establishment (TCP, TLS and any proxy `CONNECT`).
    pub connect_timeout: Duration,
    /// A whole small request: metadata, listing pages, deletes, starting a
    /// resumable session and asking it for its committed offset. Also the
    /// wait for a download's response headers, and the fixed part of an
    /// upload's bound (see [`min_throughput`](Self::min_throughput)).
    pub request_timeout: Duration,
    /// The longest a download may receive no bytes before the attempt fails
    /// (and resumes from where it stopped).
    pub idle_timeout: Duration,
    /// The slowest upload rate tolerated, in bytes per second: one upload
    /// request of `n` bytes (a chunk, or a whole small object) fails after
    /// `request_timeout + n / min_throughput`. Must not be zero.
    pub min_throughput: u64,
    /// Retries after the first attempt for transient failures; for a
    /// resumable transfer, consecutive attempts that make no progress.
    pub max_retries: u32,
    /// Largest body accepted from a read, across every resumed attempt.
    pub max_object_bytes: u64,
    /// Bodies at or above this size use a resumable upload session.
    pub resumable_threshold: usize,
    /// Bytes per `PUT` of a resumable upload: a non-zero multiple of 256 KiB.
    /// Smaller chunks lose less to a failure and cost a round trip each.
    pub upload_chunk: usize,
}

impl Default for GcsConfig {
    fn default() -> Self {
        Self {
            endpoint: "https://storage.googleapis.com".into(),
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(60),
            min_throughput: 64 << 10,
            max_retries: 5,
            max_object_bytes: 4 << 30,
            resumable_threshold: 8 << 20,
            upload_chunk: 8 << 20,
        }
    }
}

impl GcsConfig {
    fn check(&self) -> Result<()> {
        let bad = |what| Err(Error::protocol(format!("invalid configuration: {what}")));
        if [self.connect_timeout, self.request_timeout, self.idle_timeout].contains(&Duration::ZERO)
        {
            return bad("timeouts must be positive");
        }
        if self.min_throughput == 0 {
            return bad("min_throughput must be positive");
        }
        if self.upload_chunk == 0 || !self.upload_chunk.is_multiple_of(CHUNK_UNIT) {
            return bad("upload_chunk must be a positive multiple of 256 KiB");
        }
        Ok(())
    }

    /// The bound on one upload request carrying `len` bytes.
    fn upload_bound(&self, len: usize) -> Duration {
        let ms = (len as u128 * 1000) / u128::from(self.min_throughput);
        self.request_timeout
            .saturating_add(Duration::from_millis(u64::try_from(ms).unwrap_or(u64::MAX)))
    }
}

/// How long one request may take.
#[derive(Debug, Clone, Copy)]
enum Bound {
    /// The whole exchange, body included (small requests, upload requests).
    Whole(Duration),
    /// Until the response headers; the body is then read under the idle
    /// timeout, chunk by chunk (downloads).
    Headers(Duration),
}

/// How a resumable session ended.
enum Session {
    /// The final response: the object, or a status for the caller to judge.
    Done(u16, reqwest::Response),
    /// 404/410: the session expired.
    Gone,
}

/// A bucket as an [`ObjectStore`].
#[derive(Debug)]
pub struct Gcs<T: TokenProvider> {
    http: reqwest::Client,
    tokens: T,
    cfg: GcsConfig,
    /// `{endpoint}/storage/v1/b/{bucket}`
    api: String,
    /// `{endpoint}/upload/storage/v1/b/{bucket}`
    upload: String,
    /// Bound on a decoded body.
    max_inflated: u64,
}

impl<T: TokenProvider> Gcs<T> {
    /// A store for `bucket`, authenticated by `tokens`.
    pub fn new(bucket: &str, tokens: T, cfg: GcsConfig) -> Result<Self> {
        if !is_valid_bucket(bucket) {
            return Err(Error::InvalidKey { reason: "invalid bucket name" });
        }
        let endpoint = cfg.endpoint.trim_end_matches('/').to_owned();
        if !is_allowed_endpoint(&endpoint) {
            return Err(Error::protocol(
                "the endpoint must be https:// (http:// only for a loopback emulator)",
            ));
        }
        cfg.check()?;
        // No client-wide timeout: each request carries its own bound (see
        // `Bound`). reqwest's `read_timeout` is not used either: its timer
        // runs from the send to the response headers without being reset by
        // request-body progress, so on an upload it is a total timeout again.
        // The system proxy (`HTTPS_PROXY`, `NO_PROXY`) applies as reqwest's
        // default.
        let http = reqwest::Client::builder()
            .connect_timeout(cfg.connect_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .pool_idle_timeout(Duration::from_secs(90))
            .user_agent(concat!("objstore-r/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| Error::unavailable(format!("http client: {e}"), false))?;
        Ok(Self {
            http,
            tokens,
            api: format!("{endpoint}/storage/v1/b/{bucket}"),
            upload: format!("{endpoint}/upload/storage/v1/b/{bucket}"),
            cfg,
            max_inflated: MAX_INFLATED_BYTES,
        })
    }

    /// Refuse to decode an encoded object past `limit` bytes
    /// ([`Error::TooLarge`]); the default is [`MAX_INFLATED_BYTES`]. The
    /// stored bytes stay bounded by [`GcsConfig::max_object_bytes`].
    #[must_use]
    pub fn with_max_inflated(mut self, limit: u64) -> Self {
        self.max_inflated = limit;
        self
    }

    /// The token provider.
    pub fn tokens(&self) -> &T {
        &self.tokens
    }

    fn object_url(&self, key: &str) -> String {
        let mut u = String::with_capacity(self.api.len() + key.len() * 3 + 4);
        u.push_str(&self.api);
        u.push_str("/o/");
        encode_component(key, &mut u);
        u
    }

    async fn authed(&self, rb: reqwest::RequestBuilder) -> Result<reqwest::RequestBuilder> {
        let t = self.tokens.token().await?;
        let mut rb = rb;
        if !t.secret().is_empty() {
            rb = rb.bearer_auth(t.secret());
        }
        if let Some(q) = self.tokens.quota_project() {
            rb = rb.header("x-goog-user-project", q);
        }
        Ok(rb)
    }

    /// A small request's bound.
    fn small(&self) -> Bound {
        Bound::Whole(self.cfg.request_timeout)
    }

    /// A download's bound: headers within `request_timeout`, then the body
    /// under `idle_timeout`.
    fn download(&self) -> Bound {
        Bound::Headers(self.cfg.request_timeout)
    }

    /// One attempt of an authorised request under `bound`.
    async fn dispatch(rb: reqwest::RequestBuilder, bound: Bound) -> Result<reqwest::Response> {
        match bound {
            Bound::Whole(d) => rb.timeout(d).send().await.map_err(|e| transport_error(&e)),
            Bound::Headers(d) => match tokio::time::timeout(d, rb.send()).await {
                Ok(r) => r.map_err(|e| transport_error(&e)),
                Err(_) => Err(Error::unavailable("timed out", true)),
            },
        }
    }

    /// Send with retries. `build` makes a fresh request per attempt.
    async fn send(
        &self,
        bound: Bound,
        build: impl Fn() -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response> {
        let mut attempt = 0;
        loop {
            // A fresh token per attempt (the provider caches it and renews it
            // before expiry); failing to get one is reported at once.
            let rb = self.authed(build()).await?;
            let outcome = Self::dispatch(rb, bound).await;
            let retry = match &outcome {
                Ok(r) => is_retriable_status(r.status().as_u16()),
                Err(e) => e.is_retriable(),
            };
            if !retry || attempt >= self.cfg.max_retries {
                return outcome;
            }
            attempt += 1;
            tokio::time::sleep(backoff(attempt)).await;
        }
    }

    /// Read `resp`'s body onto `buf`, each chunk within the idle timeout.
    /// `Ok` once the body ends; a stall or a dropped connection is a
    /// retriable [`Error::Unavailable`] with `buf` holding what arrived.
    async fn read_into(
        &self,
        resp: &mut reqwest::Response,
        buf: &mut bytes::BytesMut,
    ) -> Result<()> {
        let limit = self.cfg.max_object_bytes;
        loop {
            let chunk = match tokio::time::timeout(self.cfg.idle_timeout, resp.chunk()).await {
                Err(_) => {
                    return Err(Error::unavailable(
                        "stalled: no data within the idle timeout",
                        true,
                    ))
                }
                Ok(Err(_)) => return Err(Error::unavailable("connection lost mid-body", true)),
                Ok(Ok(None)) => return Ok(()),
                Ok(Ok(Some(c))) => c,
            };
            if (buf.len() + chunk.len()) as u64 > limit {
                return Err(Error::TooLarge { limit });
            }
            buf.extend_from_slice(&chunk);
        }
    }

    /// A buffer for a body of `len` bytes (when known and allowed).
    fn buffer(&self, len: Option<u64>) -> Result<bytes::BytesMut> {
        let limit = self.cfg.max_object_bytes;
        match len {
            Some(n) if n > limit => Err(Error::TooLarge { limit }),
            Some(n) => Ok(bytes::BytesMut::with_capacity(usize::try_from(n).unwrap_or(0))),
            None => Ok(bytes::BytesMut::new()),
        }
    }

    async fn body(&self, resp: reqwest::Response) -> Result<Bytes> {
        let mut buf = self.buffer(resp.content_length())?;
        let mut resp = resp;
        self.read_into(&mut resp, &mut buf).await?;
        Ok(buf.freeze())
    }

    async fn head_inner(&self, key: &str) -> Result<Option<Meta>> {
        let url = format!("{}?fields=name,size,generation", self.object_url(key));
        let resp = self.send(self.small(), || self.http.get(&url)).await?;
        match resp.status().as_u16() {
            404 => Ok(None),
            200 => {
                let body = self.body(resp).await?;
                wire::parse_meta(&body).map(Some)
            }
            _ => Err(status_error(resp).await),
        }
    }

    async fn stat_inner(&self, key: &str) -> Result<Option<Stat>> {
        let url = format!("{}?fields=name,size,generation,contentEncoding", self.object_url(key));
        let resp = self.send(self.small(), || self.http.get(&url)).await?;
        match resp.status().as_u16() {
            404 => Ok(None),
            200 => {
                let body = self.body(resp).await?;
                wire::parse_stat(&body).map(Some)
            }
            _ => Err(status_error(resp).await),
        }
    }

    async fn get_inner(&self, key: &str) -> Result<Option<Object>> {
        let url = format!("{}?alt=media", self.object_url(key));
        let resp = self.send(self.download(), || stored_bytes(self.http.get(&url))).await?;
        match resp.status().as_u16() {
            404 => Ok(None),
            200 => {
                let header_gen = generation_header(&resp);
                let served = served_encoding(&resp)?;
                let mut bytes = self.read_object(&url, resp, header_gen, served.is_some()).await?;
                if let Some(e) = served {
                    bytes = encoding::decode(e, &bytes, self.max_inflated)?;
                }
                let generation = match header_gen {
                    Some(g) => g,
                    // Emulators may omit the header: ask for the metadata.
                    None => self.head_inner(key).await?.map_or(1, |m| m.generation),
                };
                Ok(Some(Object { bytes, generation }))
            }
            _ => Err(status_error(resp).await),
        }
    }

    /// The whole body of a 200 media response, resuming when it breaks off,
    /// verified against the stored CRC-32C (which covers the bytes as served
    /// here: plain bytes, or an encoded body before it is decoded).
    ///
    /// A plain object of known length and generation resumes with
    /// `Range: bytes=<received>-` pinned by `ifGenerationMatch`, so the parts
    /// are always one generation. An encoded body cannot resume (a byte range
    /// of a compressed stream is meaningless) and neither can a body of
    /// unknown length or generation: those restart from byte 0. Either way
    /// at most `max_retries` consecutive attempts may make no progress.
    async fn read_object(
        &self,
        url: &str,
        first: reqwest::Response,
        generation: Option<u64>,
        encoded: bool,
    ) -> Result<Bytes> {
        let total = first.content_length();
        let ranged = !encoded && generation.is_some() && total.is_some();
        let mut sum = stored_crc32c(&first);
        let mut buf = self.buffer(total)?;
        let mut resp = first;
        // Progress is a body longer than any before it: a server that drops
        // every attempt, or ignores `Range` and restarts, runs out of tries.
        let mut best = 0;
        let mut stalls = 0;
        loop {
            let failure = match self.read_into(&mut resp, &mut buf).await {
                Ok(()) if total.is_none_or(|t| buf.len() as u64 == t) => break,
                // Ended short of its Content-Length: a dropped body by
                // another name.
                Ok(()) => Error::unavailable("body ended early", true),
                Err(e) if e.is_retriable() => e,
                Err(e) => return Err(e),
            };
            stalls = if ranged && buf.len() > best { 0 } else { stalls + 1 };
            best = best.max(buf.len());
            if stalls > self.cfg.max_retries {
                return Err(failure);
            }
            tokio::time::sleep(backoff(stalls.max(1))).await;
            let from = if ranged { buf.len() as u64 } else { 0 };
            resp = self.resume(url, generation, from, total).await?;
            if resp.status().as_u16() == 200 {
                // Whole again (a restart, or a server that ignored Range).
                buf.clear();
                sum = stored_crc32c(&resp);
            }
        }
        if sum.is_some_and(|want| crc32c::crc32c(&buf) != want) {
            return Err(Error::corrupt("stored bytes fail their CRC-32C"));
        }
        Ok(buf.freeze())
    }

    /// Ask again for an object read that broke off: from byte `from` (or
    /// whole when `from == 0`), pinned to `generation` when known. Returns a
    /// 200 or a 206 that continues exactly at `from`; a changed or deleted
    /// object is [`Error::Changed`].
    async fn resume(
        &self,
        url: &str,
        generation: Option<u64>,
        from: u64,
        total: Option<u64>,
    ) -> Result<reqwest::Response> {
        let pinned = match generation {
            Some(g) => format!("{url}&ifGenerationMatch={g}"),
            None => url.to_owned(),
        };
        let range = format!("bytes={from}-");
        let resp = self
            .send(self.download(), || {
                let rb = stored_bytes(self.http.get(&pinned));
                if from == 0 {
                    rb
                } else {
                    rb.header("range", &range)
                }
            })
            .await?;
        let status = resp.status().as_u16();
        let same = generation.is_none_or(|g| generation_header(&resp).is_none_or(|h| h == g));
        match status {
            412 | 404 => Err(Error::Changed),
            200 | 206 if !same => Err(Error::Changed),
            200 => Ok(resp),
            206 => {
                let span = resp
                    .headers()
                    .get("content-range")
                    .and_then(|v| v.to_str().ok())
                    .and_then(ranges::content_range);
                match span {
                    Some((first, _, size)) if first == from && total.is_none_or(|t| t == size) => {
                        Ok(resp)
                    }
                    _ => Err(Error::protocol("a resumed read answered with another range")),
                }
            }
            _ => Err(status_error(resp).await),
        }
    }

    async fn range_inner(&self, key: &str, range: Range<u64>) -> Result<Bytes> {
        if range.start >= range.end {
            return match self.stat_inner(key).await? {
                Some(Stat { content_encoding: Some(e), .. }) => {
                    Err(Error::EncodedRange { encoding: e.as_str().into() })
                }
                Some(_) => Ok(Bytes::new()),
                None => Err(Error::NotFound),
            };
        }
        let url = format!("{}?alt=media", self.object_url(key));
        let header = format!("bytes={}-{}", range.start, range.end - 1);
        // `Accept-Encoding: gzip` keeps the server from decompressing an
        // encoded object (and ignoring the range) only to have it refused.
        let resp = self
            .send(self.download(), || stored_bytes(self.http.get(&url).header("range", &header)))
            .await?;
        if matches!(resp.status().as_u16(), 200 | 206 | 416) {
            if let Some(token) = stored_encoding(&resp) {
                return Err(Error::EncodedRange { encoding: token });
            }
        }
        match resp.status().as_u16() {
            404 => Err(Error::NotFound),
            // A range past the end names no encoding in its headers: only
            // an encoded object costs the metadata read that settles it.
            416 => match self.stat_inner(key).await? {
                Some(Stat { content_encoding: Some(e), .. }) => {
                    Err(Error::EncodedRange { encoding: e.as_str().into() })
                }
                _ => Ok(Bytes::new()),
            },
            206 => self.body(resp).await,
            // A server may ignore Range and send everything: slice it here.
            200 => {
                let all = self.body(resp).await?;
                Ok(all.slice(crate::clip(&range, all.len() as u64)))
            }
            _ => Err(status_error(resp).await),
        }
    }

    /// Upload `stored` (the bytes as kept: `body` itself, or `body` encoded
    /// with `encoding`).
    async fn put_inner(
        &self,
        key: &str,
        body: Bytes,
        stored: Bytes,
        cond: Cond,
        encoding: Option<ContentEncoding>,
    ) -> Result<u64> {
        let mut query = String::from("name=");
        encode_component(key, &mut query);
        if let Some(e) = encoding {
            query.push_str("&contentEncoding=");
            query.push_str(e.as_str());
        }
        match cond {
            Cond::None => {}
            Cond::CreateOnly => query.push_str("&ifGenerationMatch=0"),
            Cond::IfGeneration(g) => {
                use std::fmt::Write;
                let _ = write!(query, "&ifGenerationMatch={g}");
            }
        }
        let (status, resp) = if stored.len() >= self.cfg.resumable_threshold {
            self.put_resumable(&query, &stored).await?
        } else {
            let url = format!("{}/o?uploadType=media&{query}", self.upload);
            let bound = Bound::Whole(self.cfg.upload_bound(stored.len()));
            let resp = self
                .send(bound, || {
                    self.http
                        .post(&url)
                        .header("content-type", "application/octet-stream")
                        .body(stored.clone())
                })
                .await?;
            (resp.status().as_u16(), resp)
        };
        match status {
            200 | 201 => {
                let b = self.body(resp).await?;
                wire::parse_meta(&b).map(|m| m.generation)
            }
            412 if cond != Cond::None => {
                // Either a real conflict, or our earlier attempt landed and its
                // response was lost. Our bytes being there settles it (an
                // encoded object reads back decoded, so compare the body).
                if let Some(o) = self.get_inner(key).await? {
                    if o.bytes == body
                        && !matches!(cond, Cond::IfGeneration(g) if g == o.generation)
                    {
                        return Ok(o.generation);
                    }
                }
                Err(if cond == Cond::CreateOnly {
                    Error::Conflict
                } else {
                    Error::PreconditionFailed
                })
            }
            _ => Err(status_error(resp).await),
        }
    }

    /// A resumable upload: start a session (carrying the preconditions),
    /// then send `body` in chunks. A session that expires mid-upload is
    /// restarted once from byte 0. Returns the final status and response.
    async fn put_resumable(&self, query: &str, body: &Bytes) -> Result<(u16, reqwest::Response)> {
        let init = format!("{}/o?uploadType=resumable&{query}", self.upload);
        let total = body.len().to_string();
        let mut restarted = false;
        loop {
            let resp = self
                .send(self.small(), || {
                    self.http
                        .post(&init)
                        .header("content-type", "application/json")
                        .header("x-upload-content-type", "application/octet-stream")
                        .header("x-upload-content-length", &total)
                        .body("{}")
                })
                .await?;
            let status = resp.status().as_u16();
            if status != 200 && status != 201 {
                return Ok((status, resp));
            }
            let session = resp
                .headers()
                .get("location")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| Error::protocol("resumable upload without a session URL"))?;
            if !self.is_upload_url(session) {
                return Err(Error::protocol("resumable session URL on an unexpected host"));
            }
            match self.upload_chunks(session, body).await? {
                Session::Done(status, resp) => return Ok((status, resp)),
                Session::Gone if !restarted => restarted = true,
                Session::Gone => {
                    return Err(Error::unavailable(
                        "resumable upload session expired again after a restart",
                        true,
                    ))
                }
            }
        }
    }

    /// A session URL must be on the upload endpoint's own origin: the chunks
    /// carry our token and our bytes.
    fn is_upload_url(&self, url: &str) -> bool {
        // `upload` is `{scheme}://{authority}/upload/…`: the origin is
        // everything before the first `/` after the scheme, and the URL must
        // continue it with a `/` (not `.evil`, `@evil` or `:port`).
        let after_scheme = self.upload.find("://").map_or(0, |i| i + 3);
        let origin = self.upload[after_scheme..]
            .find('/')
            .map_or(self.upload.as_str(), |i| &self.upload[..after_scheme + i]);
        url.strip_prefix(origin).is_some_and(|rest| rest.starts_with('/'))
    }

    /// Send `body` through `session` chunk by chunk.
    ///
    /// Each chunk is `PUT` with `Content-Range: bytes a-b/total`; a 308
    /// reports the committed length in `Range`. After a transient failure
    /// (timeout, lost connection, 408/429/5xx) the session is asked for its
    /// committed length (`Content-Range: bytes */total`, empty body) and only
    /// the remainder is sent again. At most `max_retries` consecutive
    /// attempts may make no progress. The body is sliced, never copied.
    async fn upload_chunks(&self, session: &str, body: &Bytes) -> Result<Session> {
        let total = body.len() as u64;
        // Highest committed length the server has confirmed: it never shrinks.
        let mut acked = 0u64;
        let mut stalls = 0u32;
        let mut failed = false;
        loop {
            let resp = if failed {
                // Where did it get to?
                let rb = self
                    .http
                    .put(session)
                    .header("content-range", format!("bytes */{total}"))
                    .body(Bytes::new());
                Self::dispatch(self.authed(rb).await?, self.small()).await
            } else {
                let end = acked.saturating_add(self.cfg.upload_chunk as u64).min(total);
                let piece = body.slice(span(acked, end));
                let range = if piece.is_empty() {
                    format!("bytes */{total}")
                } else {
                    format!("bytes {acked}-{}/{total}", end - 1)
                };
                let bound = Bound::Whole(self.cfg.upload_bound(piece.len()));
                let rb = self.http.put(session).header("content-range", range).body(piece);
                Self::dispatch(self.authed(rb).await?, bound).await
            };
            let error = match resp {
                Ok(r) => match r.status().as_u16() {
                    308 => {
                        let range = r.headers().get("range").map(|v| v.to_str().unwrap_or(""));
                        let now = ranges::committed(range, total)
                            .filter(|n| *n >= acked)
                            .ok_or_else(|| {
                                Error::protocol("resumable session reported an impossible offset")
                            })?;
                        // A chunk that moved nothing counts against the
                        // budget like a failure, so a server that never
                        // advances cannot hold the upload in a loop.
                        let progressed = now > acked;
                        acked = now;
                        if progressed {
                            stalls = 0;
                        }
                        if progressed || failed {
                            failed = false;
                            continue;
                        }
                        Error::unavailable("resumable session made no progress", true)
                    }
                    404 | 410 => return Ok(Session::Gone),
                    s if is_retriable_status(s) => status_error(r).await,
                    s => return Ok(Session::Done(s, r)),
                },
                Err(e) if e.is_retriable() => e,
                Err(e) => return Err(e),
            };
            stalls += 1;
            if stalls > self.cfg.max_retries {
                return Err(error);
            }
            failed = true;
            tokio::time::sleep(backoff(stalls)).await;
        }
    }

    async fn list_inner(&self, prefix: &str) -> Result<Vec<Meta>> {
        self.list_pages(prefix, "items(name,size,generation)", wire::parse_list, |m| &m.key).await
    }

    async fn list_stat_inner(&self, prefix: &str) -> Result<Vec<Stat>> {
        self.list_pages(
            prefix,
            "items(name,size,generation,contentEncoding)",
            wire::parse_list_stat,
            |s| &s.meta.key,
        )
        .await
    }

    /// Every page of a listing of `items` fields, parsed by `parse`.
    async fn list_pages<M: Ord>(
        &self,
        prefix: &str,
        items: &str,
        parse: fn(&[u8]) -> Result<Page<M>>,
        key_of: fn(&M) -> &String,
    ) -> Result<Vec<M>> {
        let mut out = Vec::new();
        let mut page: Option<String> = None;
        loop {
            let mut url = format!("{}/o?fields={items},nextPageToken&maxResults=1000", self.api);
            if !prefix.is_empty() {
                url.push_str("&prefix=");
                encode_component(prefix, &mut url);
            }
            if let Some(p) = &page {
                url.push_str("&pageToken=");
                encode_component(p, &mut url);
            }
            let resp = self.send(self.small(), || self.http.get(&url)).await?;
            if resp.status().as_u16() != 200 {
                return Err(status_error(resp).await);
            }
            let body = self.body(resp).await?;
            let (items, next) = parse(&body)?;
            // Objects written by other tools may not fit the key grammar: skip them.
            out.extend(items.into_iter().filter(|m| {
                let k = key_of(m);
                Key::parse(k).is_ok() && k.starts_with(prefix)
            }));
            match next {
                Some(n) if !n.is_empty() => page = Some(n),
                _ => break,
            }
        }
        out.sort();
        Ok(out)
    }

    async fn delete_inner(&self, key: &str) -> Result<()> {
        let url = self.object_url(key);
        let resp = self.send(self.small(), || self.http.delete(&url)).await?;
        match resp.status().as_u16() {
            200 | 204 | 404 => Ok(()),
            _ => Err(status_error(resp).await),
        }
    }
}

impl<T: TokenProvider> ObjectStore for Gcs<T> {
    type Get<'a>
        = impl Future<Output = Result<Option<Object>>> + 'a
    where
        Self: 'a;
    type GetRange<'a>
        = impl Future<Output = Result<Bytes>> + 'a
    where
        Self: 'a;
    type Head<'a>
        = impl Future<Output = Result<Option<Meta>>> + 'a
    where
        Self: 'a;
    type Put<'a>
        = impl Future<Output = Result<u64>> + 'a
    where
        Self: 'a;
    type List<'a>
        = impl Future<Output = Result<Vec<Meta>>> + 'a
    where
        Self: 'a;
    type Delete<'a>
        = impl Future<Output = Result<()>> + 'a
    where
        Self: 'a;

    fn get<'a>(&'a self, key: &'a str) -> Self::Get<'a> {
        async move { self.get_inner(Key::parse(key)?.as_str()).await }
    }

    fn get_range<'a>(&'a self, key: &'a str, range: Range<u64>) -> Self::GetRange<'a> {
        async move { self.range_inner(Key::parse(key)?.as_str(), range).await }
    }

    fn head<'a>(&'a self, key: &'a str) -> Self::Head<'a> {
        async move { self.head_inner(Key::parse(key)?.as_str()).await }
    }

    fn put<'a>(&'a self, key: &'a str, body: Bytes, cond: Cond) -> Self::Put<'a> {
        async move { self.put_inner(Key::parse(key)?.as_str(), body.clone(), body, cond, None).await }
    }

    fn list<'a>(&'a self, prefix: &'a str) -> Self::List<'a> {
        async move {
            check_prefix(prefix)?;
            self.list_inner(prefix).await
        }
    }

    fn delete<'a>(&'a self, key: &'a str) -> Self::Delete<'a> {
        async move { self.delete_inner(Key::parse(key)?.as_str()).await }
    }

    async fn put_encoded<'a>(
        &'a self,
        key: &'a str,
        body: Bytes,
        cond: Cond,
        encoding: ContentEncoding,
    ) -> Result<u64> {
        let key = Key::parse(key)?.as_str();
        let stored = encoding::encode(encoding, &body)?;
        self.put_inner(key, body, stored, cond, Some(encoding)).await
    }

    async fn stat<'a>(&'a self, key: &'a str) -> Result<Option<Stat>> {
        self.stat_inner(Key::parse(key)?.as_str()).await
    }

    async fn list_stat<'a>(&'a self, prefix: &'a str) -> Result<Vec<Stat>> {
        check_prefix(prefix)?;
        self.list_stat_inner(prefix).await
    }
}

/// `a..b` of a body held in memory (both within it by construction).
fn span(a: u64, b: u64) -> Range<usize> {
    usize::try_from(a).unwrap_or(usize::MAX)..usize::try_from(b).unwrap_or(usize::MAX)
}

/// GCS bucket names: 3–222 characters of `[a-z0-9._-]`, starting and ending
/// with a letter or digit.
pub(crate) fn is_valid_bucket(b: &str) -> bool {
    (3..=222).contains(&b.len())
        && b.bytes().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'.' | b'_' | b'-')
        })
        && b.bytes().next().is_some_and(|c| c.is_ascii_alphanumeric())
        && b.bytes().last().is_some_and(|c| c.is_ascii_alphanumeric())
}

fn generation_header(resp: &reqwest::Response) -> Option<u64> {
    resp.headers()
        .get("x-goog-generation")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
        .filter(|g| *g != 0)
}

/// Ask for an object's stored bytes as they are (`Accept-Encoding: gzip`),
/// so Cloud Storage never decompresses server-side. Only a build that can
/// decode them asks: without feature `gzip`, a gzip object written by another
/// tool keeps arriving decoded by the server, exactly as before.
fn stored_bytes(req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    if cfg!(feature = "gzip") {
        req.header("accept-encoding", "gzip")
    } else {
        req
    }
}

/// The encoding a body was *served* with (`Content-Encoding`); `None` for
/// bytes as written, including bytes the server already decoded.
fn served_encoding(resp: &reqwest::Response) -> Result<Option<ContentEncoding>> {
    match resp.headers().get("content-encoding") {
        None => Ok(None),
        Some(v) => ContentEncoding::parse(
            v.to_str().map_err(|_| Error::protocol("unreadable content-encoding"))?,
        ),
    }
}

/// The encoding an object is *stored* with, from either header (a server may
/// send only one): `None` for identity.
fn stored_encoding(resp: &reqwest::Response) -> Option<String> {
    ["x-goog-stored-content-encoding", "content-encoding"].iter().find_map(|h| {
        let v = resp.headers().get(*h)?.to_str().unwrap_or("unreadable").trim();
        (!v.is_empty() && !v.eq_ignore_ascii_case("identity")).then(|| v.chars().take(64).collect())
    })
}

/// The stored bytes' CRC-32C, when the body is served as stored.
fn stored_crc32c(resp: &reqwest::Response) -> Option<u32> {
    let h = resp.headers();
    let served = h.get("content-encoding").and_then(|v| v.to_str().ok());
    // Absent: assume as stored (Cloud Storage always sends it; a server that
    // does not cannot be transcoding either).
    let stored = h.get("x-goog-stored-content-encoding").and_then(|v| v.to_str().ok());
    if stored.is_some_and(|s| Some(s) != served) {
        return None;
    }
    crc32c::from_goog_hash(h.get_all("x-goog-hash").iter().filter_map(|v| v.to_str().ok()))
}

async fn status_error(resp: reqwest::Response) -> Error {
    let status = resp.status().as_u16();
    let body = resp.bytes().await.unwrap_or_default();
    crate::net::status_error(status, &body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_build_that_decodes_asks_for_stored_bytes() {
        // Without the codec, asking would turn a gzip object another tool
        // wrote (served decoded by default) into an unreadable one.
        let req = stored_bytes(reqwest::Client::new().get("http://127.0.0.1/o")).build().unwrap();
        let asked = req.headers().get("accept-encoding").and_then(|v| v.to_str().ok());
        assert_eq!(asked, cfg!(feature = "gzip").then_some("gzip"));
    }

    #[test]
    fn session_urls_stay_on_the_upload_origin() {
        let g = Gcs::new("bkt", crate::token::Static::none(), GcsConfig::default()).unwrap();
        let ok = "https://storage.googleapis.com/upload/storage/v1/b/bkt/o?uploadType=resumable&upload_id=x";
        assert!(g.is_upload_url(ok));
        for bad in [
            "https://storage.googleapis.com.evil.example/upload/x",
            "https://storage.googleapis.com@evil.example/upload/x",
            "https://storage.googleapis.com:8443/upload/x",
            "http://storage.googleapis.com/upload/x",
            "https://evil.example/upload/storage/v1/b/bkt/o",
            "https://storage.googleapis.com",
            "",
        ] {
            assert!(!g.is_upload_url(bad), "{bad}");
        }
    }

    #[test]
    fn upload_bounds_grow_with_the_body() {
        let c = GcsConfig::default();
        assert_eq!(c.upload_bound(0), Duration::from_secs(30));
        // 8 MiB at 64 KiB/s is 128 s on top of the 30 s.
        assert_eq!(c.upload_bound(8 << 20), Duration::from_secs(158));
        let floor = GcsConfig { min_throughput: 1, ..GcsConfig::default() };
        assert!(floor.upload_bound(usize::MAX) > Duration::from_secs(1 << 40), "saturates");
    }

    #[test]
    fn bucket_names_and_endpoints() {
        assert!(is_valid_bucket("my-project-sayasaya-state") && is_valid_bucket("a.b_c"));
        for bad in ["ab", "-ab", "ab-", "AB", "a/b", &"a".repeat(223)] {
            assert!(!is_valid_bucket(bad), "{bad}");
        }
        let cfg = |e: &str| GcsConfig { endpoint: e.into(), ..GcsConfig::default() };
        let t = crate::token::Static::none;
        assert!(
            Gcs::new("bkt", t(), cfg("http://example.com")).is_err(),
            "plain http to a remote host is refused"
        );
        assert!(Gcs::new("bkt", t(), cfg("http://127.0.0.1:4443")).is_ok());
        assert!(Gcs::new("bkt", t(), cfg("https://storage.googleapis.com/")).is_ok());
    }
}
