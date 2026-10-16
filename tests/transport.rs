// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// The generic authorised HTTP transport (feature `http`) against a scripted
// server: retry policies, redirect refusal, size limits, header handling,
// streaming and early cancellation. Same no-spawn, no-Arc harness as the GCS
// security tests: server and client are joined on one task.

#![cfg(feature = "http")]

use objstore_r::http::{Client, HttpConfig};
use objstore_r::token::Static;
use objstore_r::transport::{BodyStream, Request, Retry, Transport};
use objstore_r::{Bytes, Error};
use std::future::Future;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Answer one connection per scripted response, in order; return the full
/// request heads seen.
async fn scripted<F: Future>(
    script: &[String],
    client: impl FnOnce(String) -> F,
) -> (Vec<String>, F::Output) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ep = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let run = async { tokio::join!(serve(&listener, script), client(ep)) };
    tokio::time::timeout(Duration::from_secs(20), run).await.expect("script and client disagree")
}

async fn serve(listener: &TcpListener, script: &[String]) -> Vec<String> {
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

fn resp(status: &str, headers: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n{headers}\r\n{body}",
        body.len()
    )
}

fn client(limit: u64) -> Client<Static> {
    let cfg = HttpConfig { max_retries: 3, max_response_bytes: limit, ..HttpConfig::default() };
    Client::new(Static::new("tok"), cfg).unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn transient_failures_are_retried_and_the_body_returned() {
    let script = [resp("503 Unavailable", "", "busy"), resp("200 OK", "", r#"{"ok":true}"#)];
    let (seen, r) = scripted(&script, |ep| async move {
        let url = format!("{ep}/v1/x:generate");
        client(1024).send(Request::post_json(&url, Bytes::from_static(b"{}"))).await
    })
    .await;
    let r = r.unwrap();
    assert_eq!((r.status, &r.body[..]), (200, &b"{\"ok\":true}"[..]));
    assert_eq!(seen.len(), 2);
    let head = seen[1].to_ascii_lowercase();
    assert!(head.starts_with("post /v1/x:generate"), "{head}");
    assert!(head.contains("authorization: bearer tok"), "bearer token sent");
    assert!(head.contains("content-type: application/json"));
    assert!(seen[1].ends_with("{}"), "the body is resent on retry");
}

#[tokio::test(flavor = "current_thread")]
async fn throttled_policy_retries_429_but_not_503() {
    let script =
        [resp("429 Too Many", "retry-after: 0\r\n", ""), resp("503 Unavailable", "", "down")];
    let (seen, r) = scripted(&script, |ep| async move {
        let url = format!("{ep}/q");
        client(1024).send(Request::post_json(&url, Bytes::new()).retry(Retry::Throttled)).await
    })
    .await;
    assert_eq!(seen.len(), 2);
    let r = r.unwrap();
    assert_eq!(r.status, 503, "503 is returned, not retried, for non-idempotent calls");
    assert!(r.error_for_status().unwrap_err().is_retriable());
}

#[tokio::test(flavor = "current_thread")]
async fn never_policy_makes_one_attempt() {
    let script = [resp("500 Internal", "", "")];
    let (seen, r) = scripted(&script, |ep| async move {
        let url = format!("{ep}/q");
        client(1024).send(Request::get(&url).retry(Retry::Never)).await
    })
    .await;
    assert_eq!(seen.len(), 1);
    assert_eq!(r.unwrap().status, 500);
}

#[tokio::test(flavor = "current_thread")]
async fn redirects_are_not_followed() {
    let script = [resp("302 Found", "location: https://evil.example/steal\r\n", "")];
    let (seen, r) = scripted(&script, |ep| async move {
        let url = format!("{ep}/q");
        client(1024).send(Request::get(&url)).await
    })
    .await;
    assert_eq!(seen.len(), 1);
    assert!(matches!(r.unwrap().error_for_status(), Err(Error::Protocol { .. })));
}

#[tokio::test(flavor = "current_thread")]
async fn oversized_bodies_are_refused() {
    let big = "x".repeat(2048);
    let script = [resp("200 OK", "", &big)];
    let (_, r) = scripted(&script, |ep| async move {
        let url = format!("{ep}/q");
        client(1024).send(Request::get(&url)).await
    })
    .await;
    assert!(matches!(r, Err(Error::TooLarge { limit: 1024 })), "{r:?}");
}

#[tokio::test(flavor = "current_thread")]
async fn auth_errors_are_typed_and_carry_no_token() {
    let script = [resp("403 Forbidden", "", "permission denied on project")];
    let (_, r) = scripted(&script, |ep| async move {
        let url = format!("{ep}/q");
        client(1024).send(Request::get(&url)).await
    })
    .await;
    let e = r.unwrap().error_for_status().unwrap_err();
    assert!(
        matches!(&e, Error::Auth { message } if message.contains("permission denied") && !message.contains("tok"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn streamed_bodies_arrive_in_order_and_can_be_abandoned() {
    let sse = "data: {\"a\":1}\n\ndata: {\"a\":2}\n\n";
    let script = [
        resp("200 OK", "content-type: text/event-stream\r\n", sse),
        resp("200 OK", "", &"y".repeat(4096)),
    ];
    let (_, (whole, abandoned)) = scripted(&script, |ep| async move {
        let c = client(1 << 20);
        let url = format!("{ep}/stream");
        let (status, mut body) =
            c.open(Request::post_json(&url, Bytes::from_static(b"{}"))).await.unwrap();
        assert_eq!(status, 200);
        let mut got = Vec::new();
        while let Some(chunk) = body.next_chunk().await.unwrap() {
            got.extend_from_slice(&chunk);
        }
        // Second stream: read one chunk and drop the rest (cancellation).
        let (_, mut body2) = c.open(Request::get(&url)).await.unwrap();
        let first = body2.next_chunk().await.unwrap();
        drop(body2);
        (got, first.is_some())
    })
    .await;
    assert_eq!(whole, sse.as_bytes());
    assert!(abandoned);
}

/// Answer one request with headers, then `pieces` body pieces `gap` apart;
/// with `stall`, go silent after the second piece instead.
async fn trickle(listener: &TcpListener, pieces: usize, gap: Duration, stall: bool) {
    let (mut sock, _) = listener.accept().await.unwrap();
    let mut buf = [0u8; 4096];
    let _ = sock.read(&mut buf).await;
    let head = format!("HTTP/1.1 200 OK\r\ncontent-length: {pieces}\r\nconnection: close\r\n\r\n");
    let _ = sock.write_all(head.as_bytes()).await;
    for i in 0..pieces {
        if stall && i == 2 {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
        tokio::time::sleep(gap).await;
        let _ = sock.write_all(b"z").await;
    }
}

/// Header waits of 300 ms; bodies under an idle timeout of 300 ms.
fn quick() -> Client<Static> {
    let cfg = HttpConfig {
        timeout: Duration::from_millis(300),
        stream_timeout: Duration::from_millis(300),
        idle_timeout: Duration::from_millis(300),
        max_retries: 0,
        ..HttpConfig::default()
    };
    Client::new(Static::new("tok"), cfg).unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn a_slow_body_that_keeps_moving_is_not_cut_off() {
    // Eight bytes 100 ms apart: 800 ms in all, well past the 300 ms header
    // wait that used to be the whole request's bound.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://127.0.0.1:{}/slow", listener.local_addr().unwrap().port());
    let gap = Duration::from_millis(100);
    let c = quick();
    let (_, r) = tokio::join!(trickle(&listener, 8, gap, false), c.send(Request::get(&url)));
    assert_eq!(&r.unwrap().body[..], b"zzzzzzzz");
    let ((), streamed) = tokio::join!(trickle(&listener, 8, gap, false), async {
        let (_, mut body) = c.open(Request::get(&url)).await?;
        let mut n = 0;
        while let Some(c) = body.next_chunk().await? {
            n += c.len();
        }
        Ok::<_, Error>(n)
    });
    assert_eq!(streamed.unwrap(), 8);
}

#[tokio::test(flavor = "current_thread")]
async fn a_stalled_body_fails_in_about_the_idle_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://127.0.0.1:{}/stall", listener.local_addr().unwrap().port());
    let c = quick();
    let t = std::time::Instant::now();
    let run = async {
        tokio::select! {
            () = trickle(&listener, 8, Duration::from_millis(10), true) => unreachable!(),
            r = c.send(Request::get(&url)) => r,
        }
    };
    let r = tokio::time::timeout(Duration::from_secs(10), run).await.expect("the client hung");
    let took = t.elapsed();
    assert!(matches!(&r, Err(Error::Unavailable { retriable: true, .. })), "{r:?}");
    assert!(took >= Duration::from_millis(300) && took < Duration::from_secs(1), "{took:?}");
}

#[test]
fn zero_bounds_are_refused() {
    for cfg in [
        HttpConfig { idle_timeout: Duration::ZERO, ..HttpConfig::default() },
        HttpConfig { min_throughput: 0, ..HttpConfig::default() },
    ] {
        assert!(matches!(Client::new(Static::new("t"), cfg), Err(Error::Protocol { .. })));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_requests_fail_before_any_io() {
    let c = client(1024);
    let r = c.send(Request::get("http://example.com/plain-http")).await;
    assert!(matches!(r, Err(Error::Protocol { .. })), "remote plain http refused");
    let hdrs = [("Authorization", "Bearer smuggled")];
    let r = c.send(Request::get("https://example.com/").headers(&hdrs)).await;
    assert!(matches!(r, Err(Error::Protocol { .. })), "caller-supplied authorization refused");
}

#[cfg(feature = "blocking")]
#[test]
fn block_on_drives_futures_from_sync_code() {
    let v = objstore_r::http::block_on(async { 40 + 2 }).unwrap();
    assert_eq!(v, 42);
}

#[test]
fn patch_requests_are_not_blindly_retried() {
    use objstore_r::transport::{Method, Request, Retry};
    let r = Request::patch_json("https://example.com/x", objstore_r::Bytes::from_static(b"{}"))
        .headers(&[("If-Match", "etag")]);
    assert_eq!(
        (r.method, r.retry, r.content_type),
        (Method::Patch, Retry::Throttled, Some("application/json"))
    );
    assert_eq!(r.headers, &[("If-Match", "etag")]);
}
