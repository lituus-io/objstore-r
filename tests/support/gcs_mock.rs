// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// A Cloud Storage JSON-API double over loopback HTTP/1.1 for transfer tests:
// media and chunked resumable uploads, media downloads with `Range` and
// `ifGenerationMatch`, metadata and delete, with scripted faults (throttled
// bodies, dropped and stalled connections, expired sessions, hostile range
// headers) and counters for every request that matters. Connections are
// served concurrently on the caller's task (no spawn, no Arc): `run` races
// the server against the client future.

#![allow(dead_code)] // each test binary uses a different subset

use futures::stream::{FuturesUnordered, StreamExt};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Faults, set before the run. `Cell`s are consumed as they fire.
#[derive(Default)]
pub struct Faults {
    /// Bytes per second for request and response bodies.
    pub rate: Option<u64>,
    /// The first chunk starting at or after `.0` is cut after `.1` body bytes.
    pub drop_upload: Cell<Option<(u64, usize)>>,
    /// Chunk PUTs are never read nor answered.
    pub stall_upload: bool,
    /// The next media GET is cut after this many body bytes.
    pub drop_download: Cell<Option<usize>>,
    /// When a download is cut, the object is rewritten (a new generation).
    pub replace_on_drop: bool,
    /// Every media GET goes silent after this many body bytes (a resumed
    /// one after none).
    pub stall_download: Option<usize>,
    /// A resumed (206) body has its first byte flipped.
    pub corrupt_resume: bool,
    /// A resumed (206) response carries this `Content-Range` instead.
    pub content_range: Option<&'static str>,
    /// This many chunk PUTs after a session's first answer 410 Gone.
    pub gone: Cell<u32>,
    /// Session start number N (1-based) answers a `Location` on another origin.
    pub foreign_location: Option<u32>,
    /// The Nth 308 (1-based) carries this `Range` instead of the truth.
    pub lie_range: Option<(u32, &'static str)>,
}

/// What the server saw.
#[derive(Default, Debug, Clone)]
pub struct Counts {
    pub session_starts: u32,
    pub chunk_puts: u32,
    pub status_queries: u32,
    pub media_uploads: u32,
    /// Request-body bytes read from chunk PUTs, including cut ones.
    pub upload_bytes: u64,
    pub media_gets: u32,
    /// `(range, ifGenerationMatch)` of every media GET that sent either.
    pub resumed_gets: Vec<(String, Option<u64>)>,
    /// Response-body bytes written for media GETs.
    pub download_bytes: u64,
    /// `Authorization` of every chunk PUT.
    pub chunk_tokens: Vec<String>,
}

struct Upload {
    key: String,
    total: u64,
    data: Vec<u8>,
    cond: Option<u64>,
    done: Option<u64>,
}

pub struct Mock {
    listener: TcpListener,
    pub endpoint: String,
    pub faults: Faults,
    pub counts: RefCell<Counts>,
    objects: RefCell<HashMap<String, (Vec<u8>, u64)>>,
    sessions: RefCell<HashMap<u64, Upload>>,
    next_gen: Cell<u64>,
    next_session: Cell<u64>,
    replies_308: Cell<u32>,
}

struct Req {
    method: String,
    path: String,
    query: HashMap<String, String>,
    headers: HashMap<String, String>,
    /// Body bytes that arrived with the head.
    early: Vec<u8>,
}

impl Mock {
    pub async fn new(faults: Faults) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        Self {
            listener,
            endpoint,
            faults,
            counts: RefCell::default(),
            objects: RefCell::default(),
            sessions: RefCell::default(),
            next_gen: Cell::new(1000),
            next_session: Cell::new(1),
            replies_308: Cell::new(0),
        }
    }

    /// Store an object directly; returns its generation.
    pub fn insert(&self, key: &str, data: Vec<u8>) -> u64 {
        let g = self.generation();
        self.objects.borrow_mut().insert(key.to_owned(), (data, g));
        g
    }

    /// An object's bytes and generation.
    pub fn object(&self, key: &str) -> Option<(Vec<u8>, u64)> {
        self.objects.borrow().get(key).cloned()
    }

    pub fn counts(&self) -> Counts {
        self.counts.borrow().clone()
    }

    fn generation(&self) -> u64 {
        let g = self.next_gen.get() + 1;
        self.next_gen.set(g);
        g
    }

    /// Run `client` against the server; the server stops with it.
    pub async fn run<F: Future>(&self, client: F) -> F::Output {
        let run = async {
            tokio::select! {
                biased;
                r = client => r,
                () = self.serve() => unreachable!("the server never returns"),
            }
        };
        tokio::time::timeout(Duration::from_secs(60), run).await.expect("the client hung")
    }

    async fn serve(&self) {
        let mut conns = FuturesUnordered::new();
        loop {
            tokio::select! {
                a = self.listener.accept() => {
                    if let Ok((sock, _)) = a {
                        conns.push(self.conn(sock));
                    }
                }
                Some(()) = conns.next(), if !conns.is_empty() => {}
            }
        }
    }

    async fn conn(&self, mut sock: TcpStream) {
        let Some(req) = read_head(&mut sock).await else { return };
        let is_media = req.query.get("alt").is_some_and(|a| a == "media");
        match (req.method.as_str(), req.query.get("uploadType").map(String::as_str)) {
            ("POST", Some("media")) => self.media_upload(sock, req).await,
            ("POST", Some("resumable")) => self.start_session(sock, req).await,
            ("PUT", Some("resumable")) => self.session_put(sock, req).await,
            ("GET", _) if is_media => self.media_get(sock, req).await,
            ("GET", _) => self.metadata(sock, &req).await,
            ("DELETE", _) => {
                self.objects.borrow_mut().remove(object_key(&req.path));
                reply(&mut sock, "204 No Content", "", b"").await;
            }
            _ => reply(&mut sock, "400 Bad Request", "", b"").await,
        }
    }

    async fn media_upload(&self, mut sock: TcpStream, req: Req) {
        self.counts.borrow_mut().media_uploads += 1;
        let len = content_length(&req);
        let Some(body) = self.read_body(&mut sock, req.early, len, None).await else { return };
        let key = req.query.get("name").cloned().unwrap_or_default();
        let cond = req.query.get("ifGenerationMatch").and_then(|g| g.parse().ok());
        match self.commit(&key, body, cond) {
            Some(json) => reply(&mut sock, "200 OK", "", json.as_bytes()).await,
            None => reply(&mut sock, "412 Precondition Failed", "", b"").await,
        }
    }

    async fn start_session(&self, mut sock: TcpStream, req: Req) {
        let n = {
            let mut c = self.counts.borrow_mut();
            c.session_starts += 1;
            c.session_starts
        };
        let len = content_length(&req);
        let _ = self.read_body(&mut sock, req.early, len, None).await;
        let key = req.query.get("name").cloned().unwrap_or_default();
        let cond: Option<u64> = req.query.get("ifGenerationMatch").and_then(|g| g.parse().ok());
        if !self.precondition_holds(&key, cond) {
            return reply(&mut sock, "412 Precondition Failed", "", b"").await;
        }
        let total = req.headers.get("x-upload-content-length").and_then(|v| v.parse().ok());
        let id = self.next_session.get();
        self.next_session.set(id + 1);
        self.sessions.borrow_mut().insert(
            id,
            Upload { key, total: total.unwrap_or(0), data: Vec::new(), cond, done: None },
        );
        // The same server under another origin: following it would show up
        // in the counters.
        let origin = if self.faults.foreign_location == Some(n) {
            self.endpoint.replace("127.0.0.1", "localhost")
        } else {
            self.endpoint.clone()
        };
        let location = format!(
            "location: {origin}/upload/storage/v1/b/bucket/o?uploadType=resumable&upload_id={id}\r\n"
        );
        reply(&mut sock, "200 OK", &location, b"").await;
    }

    async fn session_put(&self, mut sock: TcpStream, req: Req) {
        let id: u64 = req.query.get("upload_id").and_then(|v| v.parse().ok()).unwrap_or(0);
        let range = req.headers.get("content-range").cloned().unwrap_or_default();
        let len = content_length(&req);
        let Some(spec) = range.strip_prefix("bytes ") else {
            return reply(&mut sock, "400 Bad Request", "", b"").await;
        };
        let (span, _) = spec.split_once('/').unwrap_or((spec, ""));
        if span == "*" {
            self.counts.borrow_mut().status_queries += 1;
            let _ = self.read_body(&mut sock, req.early, len, None).await;
            return self.session_state(&mut sock, id).await;
        }
        let start: u64 = span.split('-').next().and_then(|v| v.parse().ok()).unwrap_or(u64::MAX);
        let first_chunk = {
            let mut c = self.counts.borrow_mut();
            c.chunk_puts += 1;
            c.chunk_tokens.push(req.headers.get("authorization").cloned().unwrap_or_default());
            self.sessions.borrow().get(&id).is_some_and(|u| u.data.is_empty())
        };
        if self.faults.stall_upload {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            return;
        }
        let cut = match self.faults.drop_upload.get() {
            Some((at, after)) if start >= at => {
                self.faults.drop_upload.set(None);
                Some(after)
            }
            _ => None,
        };
        let body = self.read_body(&mut sock, req.early, len, cut).await;
        let body_len = body.as_ref().map_or(cut.unwrap_or(0), Vec::len);
        self.counts.borrow_mut().upload_bytes += body_len as u64;
        let Some(body) = body else { return }; // cut: drop the connection
        if !first_chunk && self.faults.gone.get() > 0 {
            self.faults.gone.set(self.faults.gone.get() - 1);
            self.sessions.borrow_mut().remove(&id);
            return reply(&mut sock, "410 Gone", "", b"").await;
        }
        // Overlap with what is committed is ignored, as Cloud Storage does.
        let refused = self.sessions.borrow_mut().get_mut(&id).map(|u| {
            let have = u.data.len() as u64;
            let ok = start <= have;
            if ok {
                let skip = usize::try_from(have - start).unwrap_or(usize::MAX).min(body.len());
                u.data.extend_from_slice(&body[skip..]);
            }
            !ok
        });
        match refused {
            None => reply(&mut sock, "404 Not Found", "", b"").await,
            Some(true) => reply(&mut sock, "400 Bad Request", "", b"").await,
            Some(false) => self.session_state(&mut sock, id).await,
        }
    }

    /// Answer for a session: 308 with its committed range, or the object.
    async fn session_state(&self, sock: &mut TcpStream, id: u64) {
        let state = self.sessions.borrow().get(&id).map(|u| {
            (u.data.len() as u64 >= u.total, u.data.len() as u64, u.key.clone(), u.cond, u.done)
        });
        let Some((complete, have, key, cond, done)) = state else {
            return reply(sock, "404 Not Found", "", b"").await;
        };
        if let Some(g) = done {
            let json = meta_json(&key, have, g);
            return reply(sock, "200 OK", "", json.as_bytes()).await;
        }
        if complete {
            let data = std::mem::take(&mut self.sessions.borrow_mut().get_mut(&id).unwrap().data);
            let Some(json) = self.commit(&key, data, cond) else {
                return reply(sock, "412 Precondition Failed", "", b"").await;
            };
            let (data, g) = self.object(&key).unwrap();
            if let Some(u) = self.sessions.borrow_mut().get_mut(&id) {
                (u.done, u.data) = (Some(g), data);
            }
            return reply(sock, "200 OK", "", json.as_bytes()).await;
        }
        let n = self.replies_308.get() + 1;
        self.replies_308.set(n);
        let range = match self.faults.lie_range {
            Some((at, lie)) if at == n => format!("range: {lie}\r\n"),
            _ if have == 0 => String::new(),
            _ => format!("range: bytes=0-{}\r\n", have - 1),
        };
        reply(sock, "308 Resume Incomplete", &range, b"").await;
    }

    async fn media_get(&self, mut sock: TcpStream, req: Req) {
        let key = object_key(&req.path).to_owned();
        let pin: Option<u64> = req.query.get("ifGenerationMatch").and_then(|g| g.parse().ok());
        let range = req.headers.get("range").cloned();
        {
            let mut c = self.counts.borrow_mut();
            c.media_gets += 1;
            if pin.is_some() || range.is_some() {
                c.resumed_gets.push((range.clone().unwrap_or_default(), pin));
            }
        }
        let Some((data, g)) = self.object(&key) else {
            return reply(&mut sock, "404 Not Found", "", b"").await;
        };
        if pin.is_some_and(|p| p != g) {
            return reply(&mut sock, "412 Precondition Failed", "", b"").await;
        }
        let total = data.len();
        let from = range
            .as_deref()
            .and_then(|r| r.strip_prefix("bytes="))
            .and_then(|r| r.split('-').next())
            .and_then(|r| r.parse::<usize>().ok());
        let hash =
            format!("x-goog-hash: crc32c={}\r\nx-goog-generation: {g}\r\n", crc32c_b64(&data));
        let (status, headers, mut body) = match from {
            Some(a) if a < total => {
                let cr = self
                    .faults
                    .content_range
                    .map_or_else(|| format!("bytes {a}-{}/{total}", total - 1), str::to_owned);
                (
                    "206 Partial Content",
                    format!("{hash}content-range: {cr}\r\n"),
                    data[a..].to_vec(),
                )
            }
            Some(_) => return reply(&mut sock, "416 Range Not Satisfiable", "", b"").await,
            None => ("200 OK", hash, data),
        };
        if from.is_some() && self.faults.corrupt_resume {
            body[0] ^= 0xFF;
        }
        let head = format!(
            "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n{headers}\r\n",
            body.len()
        );
        if sock.write_all(head.as_bytes()).await.is_err() {
            return;
        }
        let cut = self.faults.drop_download.take();
        // A stalled server sends its few bytes once; resumed reads get none.
        let stall = self.faults.stall_download.map(|n| if from.is_some() { 0 } else { n });
        let limit = cut.or(stall).unwrap_or(body.len()).min(body.len());
        let sent = self.write_body(&mut sock, &body[..limit]).await;
        self.counts.borrow_mut().download_bytes += sent as u64;
        if cut.is_some() {
            if self.faults.replace_on_drop {
                self.insert(&key, b"a different object".to_vec());
            }
            return; // drop the connection mid-body
        }
        if self.faults.stall_download.is_some() {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
        let _ = sock.shutdown().await;
    }

    async fn metadata(&self, mut sock: TcpStream, req: &Req) {
        match self.object(object_key(&req.path)) {
            Some((d, g)) => {
                let json = meta_json(object_key(&req.path), d.len() as u64, g);
                reply(&mut sock, "200 OK", "", json.as_bytes()).await;
            }
            None => reply(&mut sock, "404 Not Found", "", b"").await,
        }
    }

    fn precondition_holds(&self, key: &str, cond: Option<u64>) -> bool {
        match cond {
            None => true,
            Some(0) => !self.objects.borrow().contains_key(key),
            Some(g) => self.objects.borrow().get(key).is_some_and(|(_, have)| *have == g),
        }
    }

    /// Store `data` under `key` if `cond` holds: the object resource JSON.
    fn commit(&self, key: &str, data: Vec<u8>, cond: Option<u64>) -> Option<String> {
        if !self.precondition_holds(key, cond) {
            return None;
        }
        let g = self.generation();
        let json = meta_json(key, data.len() as u64, g);
        self.objects.borrow_mut().insert(key.to_owned(), (data, g));
        Some(json)
    }

    /// Read a `len`-byte body (some of it already in `early`) at the
    /// configured rate; `None` when cut after `cut` bytes.
    async fn read_body(
        &self,
        sock: &mut TcpStream,
        early: Vec<u8>,
        len: usize,
        cut: Option<usize>,
    ) -> Option<Vec<u8>> {
        let mut body = early;
        let want = cut.map_or(len, |c| c.min(len));
        let start = Instant::now();
        let mut buf = vec![0u8; 16 << 10];
        while body.len() < want {
            let step =
                self.faults.rate.map_or(buf.len(), |r| (r as usize / 20).clamp(1, buf.len()));
            let n = sock.read(&mut buf[..step.min(want - body.len())]).await.ok()?;
            if n == 0 {
                return None;
            }
            body.extend_from_slice(&buf[..n]);
            self.pace(start, body.len()).await;
        }
        if cut.is_some() {
            return None;
        }
        body.truncate(len);
        Some(body)
    }

    /// Write `body` at the configured rate; the bytes written.
    async fn write_body(&self, sock: &mut TcpStream, body: &[u8]) -> usize {
        let start = Instant::now();
        let step = self.faults.rate.map_or(64 << 10, |r| (r as usize / 20).max(1));
        let mut sent = 0;
        for piece in body.chunks(step) {
            if sock.write_all(piece).await.is_err() {
                break;
            }
            sent += piece.len();
            self.pace(start, sent).await;
        }
        sent
    }

    /// Sleep until `done` bytes since `start` keep to the rate.
    async fn pace(&self, start: Instant, done: usize) {
        if let Some(rate) = self.faults.rate {
            let due = start + Duration::from_secs_f64(done as f64 / rate as f64);
            tokio::time::sleep_until(due.into()).await;
        }
    }
}

async fn read_head(sock: &mut TcpStream) -> Option<Req> {
    let mut buf = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];
    let end = loop {
        let n = sock.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i;
        }
    };
    let head = String::from_utf8_lossy(&buf[..end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut first = lines.next()?.split(' ');
    let method = first.next()?.to_owned();
    let target = first.next()?;
    let (path, q) = target.split_once('?').unwrap_or((target, ""));
    let query = q
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
    let headers = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    Some(Req { method, path: path.to_owned(), query, headers, early: buf[end + 4..].to_vec() })
}

fn content_length(req: &Req) -> usize {
    req.headers.get("content-length").and_then(|v| v.parse().ok()).unwrap_or(0)
}

/// `/storage/v1/b/bucket/o/<key>` → `<key>` (test keys need no decoding).
fn object_key(path: &str) -> &str {
    path.rsplit_once("/o/").map_or("", |(_, k)| k)
}

fn meta_json(key: &str, size: u64, g: u64) -> String {
    format!(r#"{{"name":"{key}","size":"{size}","generation":"{g}"}}"#)
}

async fn reply(sock: &mut TcpStream, status: &str, headers: &str, body: &[u8]) {
    let head = format!(
        "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n{headers}\r\n",
        body.len()
    );
    let _ = sock.write_all(head.as_bytes()).await;
    let _ = sock.write_all(body).await;
    let _ = sock.shutdown().await;
}

/// CRC-32C, bitwise (the crate's table-driven one is private).
pub fn crc32c(data: &[u8]) -> u32 {
    let mut c = !0u32;
    for &b in data {
        c ^= u32::from(b);
        for _ in 0..8 {
            c = if c & 1 == 1 { (c >> 1) ^ 0x82F6_3B78 } else { c >> 1 };
        }
    }
    !c
}

/// Base64 of the CRC's four big-endian bytes, as `x-goog-hash` carries it.
fn crc32c_b64(data: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let b = crc32c(data).to_be_bytes();
    let n = u32::from(b[0]) << 16 | u32::from(b[1]) << 8 | u32::from(b[2]);
    let m = u32::from(b[3]) << 16;
    let mut s = String::with_capacity(8);
    for i in [18, 12, 6, 0] {
        s.push(A[(n >> i & 63) as usize] as char);
    }
    s.push(A[(m >> 18 & 63) as usize] as char);
    s.push(A[(m >> 12 & 63) as usize] as char);
    s.push_str("==");
    s
}

/// Deterministic, incompressible test bytes.
pub fn bytes(len: usize, seed: u32) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            (x >> 24) as u8
        })
        .collect()
}
