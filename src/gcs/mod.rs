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
//! - **Uploads** are single requests up to [`GcsConfig::resumable_threshold`]
//!   and resumable sessions above it.
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

/// Settings.
#[derive(Debug, Clone)]
pub struct GcsConfig {
    /// API endpoint (`https://storage.googleapis.com`, or an emulator).
    pub endpoint: String,
    /// Per-request timeout.
    pub timeout: Duration,
    /// Retries after the first attempt for transient failures.
    pub max_retries: u32,
    /// Largest body accepted from a read.
    pub max_object_bytes: u64,
    /// Bodies at or above this size use a resumable upload session.
    pub resumable_threshold: usize,
}

impl Default for GcsConfig {
    fn default() -> Self {
        Self {
            endpoint: "https://storage.googleapis.com".into(),
            timeout: Duration::from_secs(60),
            max_retries: 5,
            max_object_bytes: 4 << 30,
            resumable_threshold: 8 << 20,
        }
    }
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
        let http = reqwest::Client::builder()
            .timeout(cfg.timeout)
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

    /// Send with retries. `build` makes a fresh request per attempt.
    async fn send(&self, build: impl Fn() -> reqwest::RequestBuilder) -> Result<reqwest::Response> {
        let mut attempt = 0;
        loop {
            let rb = self.authed(build()).await?;
            let outcome = rb.send().await;
            let retry = match &outcome {
                Ok(r) => is_retriable_status(r.status().as_u16()),
                Err(e) => e.is_timeout() || e.is_connect() || e.is_request(),
            };
            if !retry || attempt >= self.cfg.max_retries {
                return outcome.map_err(|e| transport_error(&e));
            }
            attempt += 1;
            tokio::time::sleep(backoff(attempt)).await;
        }
    }

    async fn body(&self, resp: reqwest::Response) -> Result<Bytes> {
        if resp.content_length().is_some_and(|n| n > self.cfg.max_object_bytes) {
            return Err(Error::TooLarge { limit: self.cfg.max_object_bytes });
        }
        let mut resp = resp;
        let mut buf = bytes::BytesMut::new();
        while let Some(chunk) = resp.chunk().await.map_err(|e| transport_error(&e))? {
            if (buf.len() + chunk.len()) as u64 > self.cfg.max_object_bytes {
                return Err(Error::TooLarge { limit: self.cfg.max_object_bytes });
            }
            buf.extend_from_slice(&chunk);
        }
        Ok(buf.freeze())
    }

    async fn head_inner(&self, key: &str) -> Result<Option<Meta>> {
        let url = format!("{}?fields=name,size,generation", self.object_url(key));
        let resp = self.send(|| self.http.get(&url)).await?;
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
        let resp = self.send(|| self.http.get(&url)).await?;
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
        let resp = self.send(|| stored_bytes(self.http.get(&url))).await?;
        match resp.status().as_u16() {
            404 => Ok(None),
            200 => {
                let header_gen = generation_header(&resp);
                let served = served_encoding(&resp)?;
                let sum = served.and_then(|_| stored_crc32c(&resp));
                let mut bytes = self.body(resp).await?;
                if let Some(e) = served {
                    // The checksum covers the stored bytes, which is what an
                    // encoded body is served as: verify, then decode once.
                    if sum.is_some_and(|want| crc32c::crc32c(&bytes) != want) {
                        return Err(Error::corrupt(format!(
                            "{key}: stored bytes fail their CRC-32C"
                        )));
                    }
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
        let resp = self.send(|| stored_bytes(self.http.get(&url).header("range", &header))).await?;
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
            let resp = self
                .send(|| {
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

    async fn put_resumable(&self, query: &str, body: &Bytes) -> Result<(u16, reqwest::Response)> {
        let init = format!("{}/o?uploadType=resumable&{query}", self.upload);
        let resp = self
            .send(|| {
                self.http
                    .post(&init)
                    .header("content-type", "application/json")
                    .header("x-upload-content-type", "application/octet-stream")
                    .header("x-upload-content-length", body.len().to_string())
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
            .map(str::to_owned)
            .ok_or_else(|| Error::protocol("resumable upload without a session URL"))?;
        if !(session.starts_with(&self.upload) || session.starts_with("https://")) {
            return Err(Error::protocol("resumable session URL on an unexpected host"));
        }
        let resp = self.send(|| self.http.put(&session).body(body.clone())).await?;
        Ok((resp.status().as_u16(), resp))
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
            let resp = self.send(|| self.http.get(&url)).await?;
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
        let resp = self.send(|| self.http.delete(&url)).await?;
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
