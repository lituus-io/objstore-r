# Author: spicyzhug@gmail.com
# Copyright: lituus-io, all rights reserved
"""The Store contract on the memory and local-directory backends."""

import sys

import pytest

import objstore_r as o


@pytest.fixture(params=["memory", "local"])
def store(request, tmp_path):
    return o.Store.memory() if request.param == "memory" else o.Store.local(tmp_path / "store")


def test_round_trip_and_generations(store):
    assert store.get("a/b") is None
    assert store.head("a/b") is None
    g1 = store.put("a/b", b"hello world", create_only=True)
    blob = store.get("a/b")
    assert blob == b"hello world" and bytes(blob) == b"hello world" and len(blob) == 11
    assert blob.generation == g1
    meta = store.head("a/b")
    assert (meta.key, meta.size, meta.generation) == ("a/b", 11, g1)
    g2 = store.put("a/b", bytearray(b"v2"))
    assert g2 != g1
    assert store.get("a/b").to_bytes() == b"v2"


def test_compare_and_swap(store):
    g = store.put("head", b"1", create_only=True)
    with pytest.raises(o.ConflictError):
        store.put("head", b"x", create_only=True)
    g2 = store.put("head", b"2", if_generation=g)
    with pytest.raises(o.PreconditionFailedError):
        store.put("head", b"3", if_generation=g)
    with pytest.raises(o.PreconditionFailedError):
        store.put("missing", b"3", if_generation=g2)
    assert store.get("head") == b"2"
    with pytest.raises(ValueError):
        store.put("head", b"x", if_generation=g2, create_only=True)
    with pytest.raises(ValueError):
        store.put("head", b"x", if_generation=0)
    assert all(issubclass(e, o.ObjstoreError) for e in (o.ConflictError, o.PreconditionFailedError))


def test_ranges(store):
    store.put("r", b"0123456789")
    assert store.get_range("r", 2, 5) == b"234"
    assert store.get_range("r", 8, 100) == b"89"
    assert store.get_range("r", 50, 60) == b""
    assert store.get_range("r", 3, 3) == b""
    with pytest.raises(o.NotFoundError):
        store.get_range("nope", 0, 1)
    with pytest.raises(ValueError):
        store.get_range("r", 5, 2)


def test_list_delete_and_get_many(store):
    for k in ["d/1", "d/2", "d/sub/3", "dx", "e"]:
        store.put(k, k.encode())
    assert [m.key for m in store.list("d/")] == ["d/1", "d/2", "d/sub/3"]
    assert [m.key for m in store.list("d")] == ["d/1", "d/2", "d/sub/3", "dx"]
    assert [m.key for m in store.list()] == ["d/1", "d/2", "d/sub/3", "dx", "e"]
    got = store.get_many(["e", "missing", "d/1"])
    assert [None if b is None else bytes(b) for b in got] == [b"e", None, b"d/1"]
    store.delete("dx")
    store.delete("dx")  # idempotent
    assert store.get("dx") is None


def test_zero_copy_buffers(store):
    payload = bytes(range(256)) * 4096  # 1 MiB
    store.put("big", payload)
    blob = store.get("big")
    view = memoryview(blob)
    assert view.readonly and view.nbytes == len(payload)
    assert view.obj is blob, "exported by the Blob itself: no intermediate copy"
    assert view[1000:1004].tobytes() == payload[1000:1004]
    with pytest.raises(TypeError):
        memoryview(blob)[0:1] = b"x"  # read-only
    # A Blob uploads as is.
    g = store.put("copy", blob)
    assert store.get("copy") == payload and store.get("copy").generation == g
    assert hash(store.get("copy")) == hash(blob)


def test_invalid_keys_never_reach_storage(store):
    for bad in ["", "/abs", "a//b", "../x", "a/../b", ".hidden", "a/.x", "a b", "C:\\x", "NUL", "é"]:
        with pytest.raises(o.InvalidKeyError):
            store.put(bad, b"x")
        with pytest.raises(o.InvalidKeyError):
            o.check_key(bad)
    assert store.list() == []
    o.check_key("ok/key-1.v2_x")


def test_bad_data_types(store):
    with pytest.raises(ValueError):
        store.put("k", "a str is not bytes")
    with pytest.raises(ValueError):
        store.put("k", 42)


def test_repr_and_kind(store):
    assert store.kind in ("memory", "local")
    assert repr(store).startswith("Store.")
    store.put("k", b"abc")
    assert repr(store.get("k")).startswith("Blob(len=3")
    assert "generation" in repr(store.head("k"))


def test_fault_injection_on_memory(tmp_path):
    s = o.Store.memory()
    s.inject_failure(after=1)
    s.put("a", b"1")
    with pytest.raises(o.UnavailableError):
        s.put("b", b"2")
    assert s.get("b") is None, "failed before applying"
    s.inject_failure(lost_response=True)
    with pytest.raises(o.UnavailableError):
        s.put("c", b"3")
    assert s.get("c") == b"3", "applied, reply lost"
    with pytest.raises(ValueError):
        o.Store.local(tmp_path / "x").inject_failure()


@pytest.mark.skipif(sys.platform == "win32", reason="symlinks need privileges on Windows")
def test_local_symlinks_cannot_escape(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret").write_bytes(b"do not read")
    s = o.Store.local(tmp_path / "store")
    (tmp_path / "store" / "link").symlink_to(outside)
    with pytest.raises(o.InvalidKeyError):
        s.put("link/x", b"x")
    with pytest.raises(o.InvalidKeyError):
        s.get("link/secret")
    assert not (outside / "x").exists()


def test_version():
    assert o.__version__ == "0.3.0"
