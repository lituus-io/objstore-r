// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// A scripted HTTP/1.1 stub server for policy tests: it answers one
// connection per scripted response, in order, and records every request
// head and body that arrived. Served on the caller's task (no spawn, no
// Arc): `scripted` joins it with the client future.

#![allow(dead_code)] // each test binary uses a different subset

use std::future::Future;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Run `client(endpoint)` against a server that answers `script` in order;
/// return the requests seen and the client's output. Fails if the client
/// sends fewer requests than scripted (the server would wait forever).
pub async fn scripted<F: Future>(
    script: &[String],
    client: impl FnOnce(String) -> F,
) -> (Vec<String>, F::Output) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ep = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let run = async { tokio::join!(serve(&listener, script), client(ep)) };
    tokio::time::timeout(Duration::from_secs(20), run).await.expect("script and client disagree")
}

/// Answer `script.len()` connections; return what each sent.
pub async fn serve(listener: &TcpListener, script: &[String]) -> Vec<String> {
    let mut seen = Vec::with_capacity(script.len());
    let mut buf = vec![0u8; 64 * 1024];
    for resp in script {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut n = 0;
        loop {
            let r = sock.read(&mut buf[n..]).await.unwrap_or(0);
            if r == 0 {
                break;
            }
            n += r;
            let text = String::from_utf8_lossy(&buf[..n]);
            if let Some(end) = text.find("\r\n\r\n") {
                let len = text
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                    })
                    .unwrap_or(0);
                if n >= end + 4 + len {
                    break;
                }
            }
        }
        seen.push(String::from_utf8_lossy(&buf[..n]).into_owned());
        let _ = sock.write_all(resp.as_bytes()).await;
        let _ = sock.shutdown().await;
    }
    seen
}

/// A complete response with `connection: close`.
pub fn resp(status: &str, headers: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n{headers}\r\n{body}",
        body.len()
    )
}
