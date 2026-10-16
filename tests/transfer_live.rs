// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Large transfers against a real bucket: `OBJSTORE_GCS_TEST_BUCKET` names it,
// Application Default Credentials reach it. Each test is skipped when the
// variable is unset, writes under a unique prefix and deletes everything it
// wrote. Slow on purpose (minutes): never part of the normal CI run.
//
//   OBJSTORE_GCS_TEST_BUCKET=<bucket> cargo test --all-features \
//     --test transfer_live -- --nocapture --test-threads=1

#![cfg(feature = "gcs")]

#[path = "support/throttle_proxy.rs"]
mod throttle_proxy;

use objstore_r::testing::{clean, unique};
use objstore_r::token::Adc;
use objstore_r::{Bytes, Cond, Gcs, GcsConfig, ObjectStore};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use throttle_proxy::{Dir, Proxy};

const MIB: usize = 1 << 20;

/// Serialises edits of the process environment (the proxy variables are read
/// when a client is built).
static ENV: Mutex<()> = Mutex::new(());

fn bucket() -> Option<String> {
    std::env::var("OBJSTORE_GCS_TEST_BUCKET").ok().filter(|b| !b.is_empty())
}

/// A store on the test bucket; with `proxy`, its connections go through it
/// (via `HTTPS_PROXY`, as any proxy from the environment does). Credentials
/// are discovered first, so token requests keep the environment's own route.
fn store(bucket: &str, proxy: Option<&Proxy>, cfg: GcsConfig) -> Gcs<Adc> {
    let _env = ENV.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let adc = Adc::discover().expect("Application Default Credentials");
    let keys = ["HTTPS_PROXY", "https_proxy"];
    let saved: Vec<Option<String>> = keys.iter().map(|k| std::env::var(k).ok()).collect();
    if let Some(p) = proxy {
        for k in keys {
            std::env::set_var(k, &p.url);
        }
    }
    let gcs = Gcs::new(bucket, adc, cfg);
    for (k, v) in keys.iter().zip(saved) {
        match v {
            Some(v) => std::env::set_var(k, v),
            None => std::env::remove_var(k),
        }
    }
    gcs.expect("store")
}

/// Incompressible bytes, quickly.
fn payload(len: usize, seed: u64) -> Bytes {
    let mut x = seed | 1;
    let mut v = Vec::with_capacity(len);
    while v.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        v.extend_from_slice(&x.to_le_bytes());
    }
    v.truncate(len);
    v.into()
}

fn mibps(bytes: usize, d: Duration) -> f64 {
    bytes as f64 / MIB as f64 / d.as_secs_f64()
}

/// Upload then download `body` at `key`, verified; the two durations.
async fn round_trip(s: &Gcs<Adc>, key: &str, body: &Bytes) -> Result<(Duration, Duration), String> {
    let t = Instant::now();
    let g = s.put(key, body.clone(), Cond::CreateOnly).await.map_err(|e| format!("put: {e}"))?;
    let up = t.elapsed();
    let t = Instant::now();
    let got = s.get(key).await.map_err(|e| format!("get: {e}"))?.ok_or("vanished")?;
    let down = t.elapsed();
    if got.generation != g || got.bytes != *body {
        return Err(format!("{key}: read back differs"));
    }
    Ok((up, down))
}

#[tokio::test(flavor = "current_thread")]
async fn large_objects_round_trip() {
    let Some(b) = bucket() else { return eprintln!("OBJSTORE_GCS_TEST_BUCKET unset: skipped") };
    let s = store(&b, None, GcsConfig::default());
    let prefix = unique("objstore-r-transfer/big");
    let r = async {
        for mib in [64, 300] {
            let body = payload(mib * MIB, mib as u64);
            let (up, down) = round_trip(&s, &format!("{prefix}/{mib}"), &body).await?;
            eprintln!(
                "round trip {mib} MiB: up {up:.1?} ({:.1} MiB/s), down {down:.1?} ({:.1} MiB/s)",
                mibps(body.len(), up),
                mibps(body.len(), down)
            );
        }
        Ok::<_, String>(())
    }
    .await;
    clean(&s, &prefix).await.unwrap();
    r.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn a_slow_link_outlasts_the_old_total_timeout() {
    let Some(b) = bucket() else { return eprintln!("OBJSTORE_GCS_TEST_BUCKET unset: skipped") };
    let rate = MIB as u64; // 1 MiB/s each way
    let proxy = Proxy::new(Some(rate)).await;
    let s = store(&b, Some(&proxy), GcsConfig::default());
    let prefix = unique("objstore-r-transfer/slow");
    let body = payload(100 * MIB, 3);
    let r = proxy.run(round_trip(&s, &format!("{prefix}/100"), &body)).await;
    eprintln!(
        "100 MiB at 1 MiB/s (chained: {}): {r:.1?}; tunnels {}, up {} MiB, down {} MiB",
        proxy.chained(),
        proxy.tunnels.get(),
        proxy.up_bytes.get() >> 20,
        proxy.down_bytes.get() >> 20
    );
    proxy.run(clean(&s, &prefix)).await.unwrap();
    let (up, down) = r.unwrap();
    // Each direction alone takes longer than v0.2.4's 60 s total timeout.
    assert!(up > Duration::from_secs(60) && down > Duration::from_secs(60), "{up:?} {down:?}");
    assert_eq!(proxy.kills.get(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn a_killed_tunnel_resumes_both_ways() {
    let Some(b) = bucket() else { return eprintln!("OBJSTORE_GCS_TEST_BUCKET unset: skipped") };
    let proxy = Proxy::new(Some(8 * MIB as u64)).await;
    let s = store(&b, Some(&proxy), GcsConfig::default());
    let prefix = unique("objstore-r-transfer/kill");
    let key = format!("{prefix}/64");
    let size = 64 * MIB;
    let body = payload(size, 5);
    let chunk = GcsConfig::default().upload_chunk as u64;

    // Upload: the tunnel dies 20 MiB in; only the unfinished chunk is resent.
    proxy.kill_after(Dir::Up, 20 * MIB as u64);
    let t = Instant::now();
    let up = proxy.run(s.put(&key, body.clone(), Cond::CreateOnly)).await;
    let (up_time, up_bytes, tunnels) = (t.elapsed(), proxy.up_bytes.get(), proxy.tunnels.get());

    // Download: the tunnel dies 20 MiB in; the rest is fetched by range.
    let down_before = proxy.down_bytes.get();
    proxy.kill_after(Dir::Down, 20 * MIB as u64);
    let t = Instant::now();
    let down = proxy.run(s.get(&key)).await;
    let (down_time, down_bytes) = (t.elapsed(), proxy.down_bytes.get() - down_before);
    eprintln!(
        "64 MiB with one killed tunnel each way: up {up_time:.1?} sending {:.1} MiB, \
         down {down_time:.1?} receiving {:.1} MiB; tunnels {} then {}, kills {}",
        up_bytes as f64 / MIB as f64,
        down_bytes as f64 / MIB as f64,
        tunnels,
        proxy.tunnels.get(),
        proxy.kills.get()
    );
    proxy.run(clean(&s, &prefix)).await.unwrap();
    let g = up.unwrap();
    let got = down.unwrap().unwrap();
    assert!(got.bytes == body && got.generation == g);
    assert_eq!(proxy.kills.get(), 2, "both kills fired");
    // A restart from byte 0 would send at least size + 20 MiB each way.
    let slack = 2 * MIB as u64; // TLS framing and HTTP heads
    assert!(up_bytes < size as u64 + chunk + slack, "upload resent {up_bytes}");
    assert!(down_bytes < size as u64 + slack, "download refetched {down_bytes}");
}
