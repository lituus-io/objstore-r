// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
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
    use objstore_r::testing::{conformance, emulator_bucket, live_bucket, unique};

    /// Against fake-gcs-server (`OBJSTORE_FAKE_GCS=http://127.0.0.1:4443`), with
    /// the default threshold and again with every upload forced resumable.
    #[tokio::test(flavor = "current_thread")]
    async fn gcs_conforms_on_the_emulator() {
        for threshold in [8 << 20, 1024] {
            let Some(s) = emulator_bucket(threshold).await else {
                eprintln!("skipped: set OBJSTORE_FAKE_GCS to run against fake-gcs-server");
                return;
            };
            conformance(&s, "suite").await.unwrap_or_else(|e| panic!("threshold {threshold}: {e}"));
        }
    }

    /// Against a real bucket with Application Default Credentials
    /// (`OBJSTORE_LIVE_BUCKET=<bucket>`). Writes only under a unique prefix and
    /// deletes everything it wrote.
    #[tokio::test(flavor = "current_thread")]
    async fn gcs_conforms_live() {
        let Some(s) = live_bucket() else {
            eprintln!("skipped: set OBJSTORE_LIVE_BUCKET to run against a real bucket");
            return;
        };
        conformance(&s, &unique("objstore-r-conformance")).await.unwrap();
    }
}
