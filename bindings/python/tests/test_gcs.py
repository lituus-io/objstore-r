# Author: support@lituus.io
# Copyright: lituus-io, all rights reserved
"""Google Cloud Storage: fake-gcs-server (OBJSTORE_FAKE_GCS) and a real
bucket via Application Default Credentials (OBJSTORE_LIVE_BUCKET). Each is
skipped unless its variable is set; the live test writes under a unique
prefix and deletes everything it wrote."""

import json
import os
import time
import urllib.request

import pytest

import objstore_r as o


def exercise(store, prefix):
    k = f"{prefix}/head"
    g = store.put(k, b"v1", create_only=True)
    with pytest.raises(o.ConflictError):
        store.put(k, b"x", create_only=True)
    g2 = store.put(k, b"v2", if_generation=g)
    with pytest.raises(o.PreconditionFailedError):
        store.put(k, b"v3", if_generation=g)
    blob = store.get(k)
    assert blob == b"v2" and blob.generation == g2
    big = os.urandom(3 * 1024 * 1024 + 7)
    store.put(f"{prefix}/big", big)  # resumable when above the threshold
    assert store.get(f"{prefix}/big") == big
    assert store.get_range(f"{prefix}/big", 10, 20) == big[10:20]
    assert memoryview(store.get(f"{prefix}/big")).obj is not None
    assert [m.key for m in store.list(f"{prefix}/")] == [f"{prefix}/big", f"{prefix}/head"]
    got = store.get_many([f"{prefix}/head", f"{prefix}/missing"])
    assert got[0] == b"v2" and got[1] is None
    for m in store.list(f"{prefix}/"):
        store.delete(m.key)
    assert store.list(f"{prefix}/") == []


def test_emulator():
    endpoint = os.environ.get("OBJSTORE_FAKE_GCS")
    if not endpoint:
        pytest.skip("OBJSTORE_FAKE_GCS unset")
    bucket = f"py-{os.getpid()}"
    req = urllib.request.Request(
        f"{endpoint}/storage/v1/b?project=test",
        data=json.dumps({"name": bucket}).encode(),
        headers={"content-type": "application/json"},
    )
    try:
        urllib.request.urlopen(req).read()
    except urllib.error.HTTPError as e:
        if e.code != 409:
            raise
    store = o.Store.gcs(bucket, anonymous=True, endpoint=endpoint, resumable_threshold=1 << 20)
    assert store.kind == "gcs" and "py-" in repr(store)
    exercise(store, "suite")


def test_live_bucket():
    bucket = os.environ.get("OBJSTORE_LIVE_BUCKET")
    if not bucket:
        pytest.skip("OBJSTORE_LIVE_BUCKET unset")
    store = o.Store.gcs(bucket, resumable_threshold=1 << 20)
    exercise(store, f"objstore-r-live/py-{os.getpid()}-{time.time_ns()}")


def test_plain_http_is_refused_off_loopback():
    with pytest.raises(o.ObjstoreError):
        o.Store.gcs("bucket", anonymous=True, endpoint="http://example.com")


def test_token_never_in_repr():
    s = o.Store.gcs("bucket", token="ya29.super-secret")
    assert "ya29" not in repr(s)
