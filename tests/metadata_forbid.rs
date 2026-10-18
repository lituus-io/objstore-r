// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// `MetadataPolicy::Forbid` against a decoy metadata server. The decoy stands
// where the metadata server would be (`GCE_METADATA_HOST`); a control run
// under `Allow` proves it is reachable and counted, then `Forbid` must leave
// it at zero hits. Its own test binary: it sets process environment
// variables, which no other test may race.

#![cfg(feature = "http")]

#[path = "support/stub_http.rs"]
mod stub;

use objstore_r::token::{Adc, MetadataPolicy, TokenProvider};
use objstore_r::Error;
use std::time::Duration;
use tokio::net::TcpListener;

/// Count connections to `listener` for `window`, answering each with a token.
async fn count_hits(listener: &TcpListener, window: Duration) -> usize {
    let mut hits = 0;
    let ok = stub::resp("200 OK", "", r#"{"access_token":"ya29.instance","expires_in":3600}"#);
    while let Ok(Ok((mut sock, _))) = tokio::time::timeout(window, listener.accept()).await {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut buf = [0u8; 4096];
        let _ = sock.read(&mut buf).await;
        let _ = sock.write_all(ok.as_bytes()).await;
        let _ = sock.shutdown().await;
        hits += 1;
    }
    hits
}

#[tokio::test(flavor = "current_thread")]
async fn forbid_leaves_the_decoy_metadata_server_untouched() {
    let decoy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let home = tempfile::tempdir().unwrap();
    std::env::set_var("GCE_METADATA_HOST", decoy.local_addr().unwrap().to_string());
    std::env::remove_var("GOOGLE_APPLICATION_CREDENTIALS");
    // No well-known credentials file anywhere gcloud would look.
    for v in ["CLOUDSDK_CONFIG", "HOME", "APPDATA"] {
        std::env::set_var(v, home.path());
    }

    // Control: under Allow the same environment reaches the decoy.
    let allow = Adc::discover_with(MetadataPolicy::Allow).unwrap();
    assert!(matches!(allow, Adc::Metadata(_)));
    let (hits, token) = tokio::join!(count_hits(&decoy, Duration::from_millis(500)), allow.token());
    assert_eq!(token.unwrap().secret(), "ya29.instance");
    assert_eq!(hits, 1, "the decoy is wired where the metadata server is");

    // Forbid: a typed refusal, and the decoy hears nothing.
    let (hits, forbid) = tokio::join!(count_hits(&decoy, Duration::from_millis(500)), async {
        Adc::discover_with(MetadataPolicy::Forbid)
    });
    assert!(matches!(forbid, Err(Error::NoCredentials { .. })), "{forbid:?}");
    assert_eq!(hits, 0, "Forbid must never contact the metadata server");

    // The caller's own token is used as is; still no metadata traffic.
    let (hits, tok) = tokio::join!(count_hits(&decoy, Duration::from_millis(300)), async {
        Adc::from_access_token("ya29.caller").unwrap().token().await
    });
    assert_eq!((tok.unwrap().secret(), hits), ("ya29.caller", 0));
}
