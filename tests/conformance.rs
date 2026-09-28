// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
// Every backend passes the same conformance suite. The GCS backend runs
// against fake-gcs-server when `OBJSTORE_FAKE_GCS` is set (CI starts one),
// and against a real bucket when `OBJSTORE_LIVE_BUCKET` is set.

use objstore_r::testing::conformance;
use objstore_r::{LocalDir, Mem};

#[test]
fn mem_conforms() {
    futures::executor::block_on(conformance(&Mem::default(), "suite")).unwrap();
}

#[test]
fn local_dir_conforms() {
    let dir = tempfile::tempdir().unwrap();
    let s = LocalDir::open(dir.path()).unwrap();
    futures::executor::block_on(conformance(&s, "suite")).unwrap();
}

#[cfg(feature = "gcs")]
mod gcs {
    use objstore_r::testing::conformance;
    use objstore_r::token::{Adc, Static};
    use objstore_r::{Gcs, GcsConfig};

    fn unique(tag: &str) -> String {
        let nanos =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        format!("{tag}-{}-{nanos}", std::process::id())
    }

    async fn fake_bucket(endpoint: &str) -> String {
        let bucket = unique("objstore-r");
        let resp = reqwest::Client::new()
            .post(format!("{endpoint}/storage/v1/b?project=test"))
            .header("content-type", "application/json")
            .body(format!(r#"{{"name":"{bucket}"}}"#))
            .send()
            .await
            .expect("fake-gcs-server reachable");
        assert!(resp.status().is_success(), "bucket creation: {}", resp.status());
        bucket
    }

    /// Against fake-gcs-server (`OBJSTORE_FAKE_GCS=http://127.0.0.1:4443`), with
    /// single-request uploads and again with every upload forced resumable.
    #[tokio::test(flavor = "current_thread")]
    async fn gcs_conforms_on_the_emulator() {
        let Ok(endpoint) = std::env::var("OBJSTORE_FAKE_GCS") else {
            eprintln!("skipped: set OBJSTORE_FAKE_GCS to run against fake-gcs-server");
            return;
        };
        for threshold in [8 << 20, 1024] {
            let bucket = fake_bucket(&endpoint).await;
            let cfg = GcsConfig {
                endpoint: endpoint.clone(),
                resumable_threshold: threshold,
                ..GcsConfig::default()
            };
            let s = Gcs::new(&bucket, Static::none(), cfg).unwrap();
            conformance(&s, "suite").await.unwrap_or_else(|e| panic!("threshold {threshold}: {e}"));
        }
    }

    /// Against a real bucket with Application Default Credentials
    /// (`OBJSTORE_LIVE_BUCKET=<bucket>`). Writes only under a unique prefix and
    /// deletes everything it wrote.
    #[tokio::test(flavor = "current_thread")]
    async fn gcs_conforms_live() {
        let Ok(bucket) = std::env::var("OBJSTORE_LIVE_BUCKET") else {
            eprintln!("skipped: set OBJSTORE_LIVE_BUCKET to run against a real bucket");
            return;
        };
        let s = Gcs::new(&bucket, Adc::discover().unwrap(), GcsConfig::default()).unwrap();
        conformance(&s, &unique("objstore-r-conformance")).await.unwrap();
    }
}
