// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// A small HTTP CONNECT proxy for live transfer tests: it relays tunnels at a
// fixed rate per direction, can kill one tunnel once after a set number of
// bytes, and counts tunnels and bytes each way. When the environment already
// names an HTTPS proxy, upstream tunnels are opened through it, so the test
// also works on a network that reaches the internet only via a proxy.
// Served on the caller's task (no spawn, no Arc): `run` races it against the
// client future.

#![allow(dead_code)] // each test binary uses a different subset

use futures::stream::{FuturesUnordered, StreamExt};
use std::cell::Cell;
use std::future::Future;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A relay direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dir {
    /// Client to server (uploads).
    Up,
    /// Server to client (downloads).
    Down,
}

pub struct Proxy {
    listener: TcpListener,
    /// `http://127.0.0.1:<port>`, for `HTTPS_PROXY`.
    pub url: String,
    /// `host:port` of the proxy the environment named, if any.
    upstream: Option<String>,
    /// Bytes per second, each direction (all tunnels share it).
    rate: Option<u64>,
    /// Kill the tunnel that carries direction `.0` past `.1` bytes (once).
    kill_at: Cell<Option<(Dir, u64)>>,
    next_up: Cell<Option<Instant>>,
    next_down: Cell<Option<Instant>>,
    pub tunnels: Cell<u32>,
    pub kills: Cell<u32>,
    pub up_bytes: Cell<u64>,
    pub down_bytes: Cell<u64>,
}

impl Proxy {
    /// Bind; read the environment's HTTPS proxy now, before a test points
    /// the environment at this one.
    pub async fn new(rate: Option<u64>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let upstream = ["HTTPS_PROXY", "https_proxy"]
            .iter()
            .find_map(|k| std::env::var(k).ok().filter(|v| !v.is_empty()))
            .map(|v| {
                let v = v.trim_end_matches('/');
                let v = v.split_once("://").map_or(v, |(_, rest)| rest);
                v.rsplit_once('@').map_or(v, |(_, host)| host).to_owned()
            });
        Self {
            listener,
            url,
            upstream,
            rate,
            kill_at: Cell::new(None),
            next_up: Cell::new(None),
            next_down: Cell::new(None),
            tunnels: Cell::new(0),
            kills: Cell::new(0),
            up_bytes: Cell::new(0),
            down_bytes: Cell::new(0),
        }
    }

    /// Kill the tunnel carrying `dir` once its total passes `bytes` more.
    pub fn kill_after(&self, dir: Dir, bytes: u64) {
        let base = match dir {
            Dir::Up => self.up_bytes.get(),
            Dir::Down => self.down_bytes.get(),
        };
        self.kill_at.set(Some((dir, base + bytes)));
    }

    pub fn chained(&self) -> bool {
        self.upstream.is_some()
    }

    /// Run `client` with the proxy serving beside it.
    pub async fn run<F: Future>(&self, client: F) -> F::Output {
        tokio::select! {
            biased;
            r = client => r,
            () = self.serve() => unreachable!("the proxy never returns"),
        }
    }

    async fn serve(&self) {
        let mut conns = FuturesUnordered::new();
        loop {
            tokio::select! {
                a = self.listener.accept() => {
                    if let Ok((sock, _)) = a {
                        conns.push(self.tunnel(sock));
                    }
                }
                Some(()) = conns.next(), if !conns.is_empty() => {}
            }
        }
    }

    async fn tunnel(&self, mut client: TcpStream) {
        let Some(head) = read_head(&mut client).await else { return };
        let Some(target) = head.strip_prefix("CONNECT ").and_then(|r| r.split(' ').next()) else {
            let _ = client.write_all(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n").await;
            return;
        };
        let target = target.to_owned();
        let Some(mut server) = self.open(&target).await else {
            let _ = client.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
            return;
        };
        if client.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n").await.is_err() {
            return;
        }
        self.tunnels.set(self.tunnels.get() + 1);
        let (cr, cw) = client.split();
        let (sr, sw) = server.split();
        // Either direction ending (or being killed) closes the tunnel.
        tokio::select! {
            () = self.pump(cr, sw, Dir::Up) => {}
            () = self.pump(sr, cw, Dir::Down) => {}
        }
    }

    /// A connection to `target`, through the upstream proxy when there is one.
    async fn open(&self, target: &str) -> Option<TcpStream> {
        let Some(up) = &self.upstream else { return TcpStream::connect(target).await.ok() };
        let mut s = TcpStream::connect(up.as_str()).await.ok()?;
        let req = format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n");
        s.write_all(req.as_bytes()).await.ok()?;
        let head = read_head(&mut s).await?;
        let status = head.split(' ').nth(1)?;
        (status == "200").then_some(s)
    }

    async fn pump<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
        &self,
        mut r: R,
        mut w: W,
        dir: Dir,
    ) {
        let mut buf = vec![0u8; 16 << 10];
        loop {
            let n = match r.read(&mut buf).await {
                Ok(0) | Err(_) => {
                    let _ = w.shutdown().await;
                    return;
                }
                Ok(n) => n,
            };
            self.pace(dir, n).await;
            if w.write_all(&buf[..n]).await.is_err() {
                return;
            }
            let total = match dir {
                Dir::Up => &self.up_bytes,
                Dir::Down => &self.down_bytes,
            };
            total.set(total.get() + n as u64);
            if let Some((d, at)) = self.kill_at.get() {
                if d == dir && total.get() >= at {
                    self.kill_at.set(None);
                    self.kills.set(self.kills.get() + 1);
                    return; // dropping both halves' streams kills the tunnel
                }
            }
        }
    }

    /// Hold `n` bytes in `dir` to the rate, across all tunnels.
    async fn pace(&self, dir: Dir, n: usize) {
        let Some(rate) = self.rate else { return };
        let slot = match dir {
            Dir::Up => &self.next_up,
            Dir::Down => &self.next_down,
        };
        let now = Instant::now();
        let start = slot.get().map_or(now, |t| t.max(now));
        let due = start + Duration::from_secs_f64(n as f64 / rate as f64);
        slot.set(Some(due));
        tokio::time::sleep_until(due.into()).await;
    }
}

/// A request or response head (through the blank line), byte by byte so
/// nothing after it is consumed.
async fn read_head(s: &mut TcpStream) -> Option<String> {
    let mut head = Vec::with_capacity(512);
    let mut b = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if s.read(&mut b).await.ok()? == 0 || head.len() > 16 << 10 {
            return None;
        }
        head.push(b[0]);
    }
    String::from_utf8(head).ok()
}
