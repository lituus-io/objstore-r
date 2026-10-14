# Author: spicyzhug@gmail.com
# Copyright: lituus-io, all rights reserved
"""Opt-in content encoding: `put(..., content_encoding="gzip")` on every
backend. Memory and local-directory stores run directly; GCS runs against a
small in-process fake of the JSON API that records what the client sent."""

import gzip
import http.server
import json
import os
import threading
import time
import urllib.parse

import pytest

import objstore_r as o

TEXT = b"a line of lineage text that compresses well, " * 20_000


@pytest.fixture(params=["memory", "local"])
def store(request, tmp_path):
    return o.Store.memory() if request.param == "memory" else o.Store.local(tmp_path / "store")


def test_encoded_round_trip_is_zero_copy_after_one_decode(store):
    g = store.put("a/enc", TEXT, content_encoding="gzip", create_only=True)
    blob = store.get("a/enc")
    assert blob == TEXT and blob.generation == g and len(blob) == len(TEXT)
    view = memoryview(blob)
    assert view.readonly and view.nbytes == len(TEXT) and view.obj is blob
    assert view[:6].tobytes() == TEXT[:6]
    # A Blob handed back to put is shared, and stores plain when not asked.
    store.put("a/copy", blob)
    assert store.head("a/copy").content_encoding is None


def test_head_and_list_report_the_encoding_and_the_stored_size(store):
    store.put("p/enc", TEXT, content_encoding="gzip")
    store.put("p/plain", b"0123456789")
    meta = store.head("p/enc")
    assert meta.content_encoding == "gzip"
    assert meta.size < len(TEXT) // 10, "size counts the stored, compressed bytes"
    assert repr(meta).endswith(', content_encoding="gzip")'), repr(meta)
    plain = store.head("p/plain")
    assert (plain.content_encoding, plain.size) == (None, 10)
    assert "content_encoding" not in repr(plain), "a plain Meta reads as before"
    assert [(m.key, m.content_encoding) for m in store.list("p/")] == [
        ("p/enc", "gzip"),
        ("p/plain", None),
    ]


def test_ranges_of_encoded_objects_refuse_and_plain_ranges_are_unchanged(store):
    store.put("r/enc", TEXT, content_encoding="gzip")
    store.put("r/plain", b"0123456789")
    for start, end in [(0, 4), (5, 5), (10**9, 10**9 + 1)]:
        with pytest.raises(o.EncodedRangeError):
            store.get_range("r/enc", start, end)
    assert issubclass(o.EncodedRangeError, o.ObjstoreError)
    assert store.get_range("r/plain", 2, 5) == b"234"
    with pytest.raises(o.NotFoundError):
        store.get_range("r/missing", 0, 1)


def test_get_many_decodes_each(store):
    store.put("m/1", TEXT, content_encoding="gzip")
    store.put("m/2", b"plain")
    got = store.get_many(["m/1", "m/2", "m/3"])
    assert got[0] == TEXT and got[1] == b"plain" and got[2] is None


def test_compare_and_swap_is_unchanged_by_the_encoding(store):
    g = store.put("c/k", b"v1" * 1000, content_encoding="gzip", create_only=True)
    with pytest.raises(o.ConflictError):
        store.put("c/k", b"other", content_encoding="gzip", create_only=True)
    with pytest.raises(o.PreconditionFailedError):
        store.put("c/k", b"v2", content_encoding="gzip", if_generation=g + 10**6)
    g2 = store.put("c/k", b"plain now", if_generation=g)
    assert store.get("c/k") == b"plain now" and store.head("c/k").content_encoding is None
    g3 = store.put("c/k", b"encoded again", content_encoding="gzip", if_generation=g2)
    blob = store.get("c/k")
    assert blob == b"encoded again" and blob.generation == g3


def test_the_argument_is_checked_before_any_io(store):
    for bad in ["br", "deflate", "x-gzip", "gzip, br"]:
        with pytest.raises(ValueError, match="content_encoding"):
            store.put("v/k", b"x", content_encoding=bad)
    assert store.head("v/k") is None, "nothing was written"
    # identity is the absence of an encoding.
    store.put("v/k", b"x", content_encoding="identity")
    assert store.head("v/k").content_encoding is None
    with pytest.raises(o.InvalidKeyError):
        store.put("../x", b"x", content_encoding="gzip")


def test_empty_bodies_round_trip(store):
    store.put("e/k", b"", content_encoding="gzip")
    assert store.get("e/k") == b"" and store.head("e/k").content_encoding == "gzip"


@pytest.mark.parametrize("kind", ["memory", "local"])
def test_a_decompression_bomb_is_refused_at_the_bound(kind, tmp_path):
    s = (
        o.Store.memory(max_inflated=1 << 20)
        if kind == "memory"
        else o.Store.local(tmp_path / "s", max_inflated=1 << 20)
    )
    s.put("bomb", bytes(8 << 20), content_encoding="gzip")
    assert s.head("bomb").size < 64 * 1024, "eight MiB of zeros stores small"
    with pytest.raises(o.TooLargeError):
        s.get("bomb")
    s.put("fits", bytes(1 << 20), content_encoding="gzip")
    assert len(s.get("fits")) == 1 << 20, "exactly the bound is allowed"
    assert o.MAX_INFLATED_BYTES == 1 << 30


def test_default_writes_are_byte_identical_on_disk(tmp_path):
    s = o.Store.local(tmp_path / "s")
    s.put("k", TEXT)
    assert (tmp_path / "s" / "k").read_bytes() == TEXT
    assert not (tmp_path / "s" / ".objstore-encoding").exists(), "no sidecar tree for plain stores"
    s.put("z", TEXT, content_encoding="gzip")
    on_disk = (tmp_path / "s" / "z").read_bytes()
    assert gzip.decompress(on_disk) == TEXT, "the file holds a standard gzip stream"
    assert [m.key for m in s.list()] == ["k", "z"], "the sidecar tree is never listed"


def test_a_damaged_stored_stream_is_a_typed_error(tmp_path):
    s = o.Store.local(tmp_path / "s")
    s.put("k", TEXT, content_encoding="gzip")
    f = tmp_path / "s" / "k"
    data = bytearray(f.read_bytes())
    data[len(data) // 2] ^= 0xFF
    # Rewrite in place keeping the generation (mtime) the sidecar names.
    st = f.stat()
    f.write_bytes(bytes(data))
    os.utime(f, ns=(st.st_atime_ns, st.st_mtime_ns))
    assert s.head("k").content_encoding == "gzip", "still the encoded generation"
    with pytest.raises(o.ObjstoreError):
        s.get("k")


# ---- GCS: an in-process fake of the JSON API --------------------------------


class FakeGcs(http.server.BaseHTTPRequestHandler):
    """Objects as {name: (stored bytes, generation, encoding)}; serves stored
    bytes only to a client that accepts gzip, decompressing otherwise, like
    Cloud Storage's decompressive transcoding."""

    objects: dict = {}
    seen: list = []
    gen = [0]

    def log_message(self, *args):
        pass

    def _send(self, status, body=b"", headers=()):
        self.send_response(status)
        for k, v in headers:
            self.send_header(k, v)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _resource(self, name):
        stored, gen, enc = self.objects[name]
        r = {"name": name, "size": str(len(stored)), "generation": str(gen)}
        if enc:
            r["contentEncoding"] = enc
        return r

    def do_POST(self):
        url = urllib.parse.urlparse(self.path)
        q = dict(urllib.parse.parse_qsl(url.query))
        body = self.rfile.read(int(self.headers.get("content-length", 0)))
        self.seen.append(("POST", self.path, dict(self.headers.items())))
        name = q["name"]
        if q.get("ifGenerationMatch") == "0" and name in self.objects:
            return self._send(412)
        self.gen[0] += 1
        self.objects[name] = (body, self.gen[0], q.get("contentEncoding"))
        self._send(200, json.dumps(self._resource(name)).encode())

    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        q = dict(urllib.parse.parse_qsl(url.query))
        self.seen.append(("GET", self.path, dict(self.headers.items())))
        parts = url.path.split("/o/", 1)
        if len(parts) == 1:  # a listing
            items = [self._resource(n) for n in sorted(self.objects) if n.startswith(q.get("prefix", ""))]
            return self._send(200, json.dumps({"items": items}).encode())
        name = urllib.parse.unquote(parts[1])
        if name not in self.objects:
            return self._send(404)
        stored, gen, enc = self.objects[name]
        if q.get("alt") != "media":
            return self._send(200, json.dumps(self._resource(name)).encode())
        headers = [("x-goog-generation", str(gen))]
        if enc:
            headers.append(("x-goog-stored-content-encoding", enc))
        accepts = "gzip" in self.headers.get("accept-encoding", "")
        if enc == "gzip" and not accepts:
            return self._send(200, gzip.decompress(stored), headers)  # transcoded
        if enc:
            headers.append(("content-encoding", enc))
        self._send(200, stored, headers)


@pytest.fixture
def fake_gcs():
    FakeGcs.objects, FakeGcs.seen, FakeGcs.gen = {}, [], [0]
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), FakeGcs)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    endpoint = f"http://127.0.0.1:{srv.server_address[1]}"
    try:
        yield o.Store.gcs("bucket", anonymous=True, endpoint=endpoint, max_retries=0)
    finally:
        srv.shutdown()


def test_gcs_upload_carries_the_encoding_and_download_asks_for_stored_bytes(fake_gcs):
    g = fake_gcs.put("k", TEXT, content_encoding="gzip", create_only=True)
    method, path, _ = FakeGcs.seen[-1]
    assert method == "POST" and "uploadType=media" in path and "contentEncoding=gzip" in path
    stored = FakeGcs.objects["k"][0]
    assert gzip.decompress(stored) == TEXT and len(stored) < len(TEXT) // 10
    blob = fake_gcs.get("k")
    assert blob == TEXT and blob.generation == g and memoryview(blob).nbytes == len(TEXT)
    _, _, headers = FakeGcs.seen[-1]
    assert headers.get("Accept-Encoding", headers.get("accept-encoding")) == "gzip"
    assert fake_gcs.head("k").content_encoding == "gzip"
    assert [(m.key, m.content_encoding) for m in fake_gcs.list()] == [("k", "gzip")]
    with pytest.raises(o.EncodedRangeError):
        fake_gcs.get_range("k", 0, 4)
    with pytest.raises(o.ConflictError):
        fake_gcs.put("k", b"other", content_encoding="gzip", create_only=True)


def test_gcs_plain_writes_are_unchanged(fake_gcs):
    fake_gcs.put("p", b"0123456789")
    _, path, _ = FakeGcs.seen[-1]
    assert "contentEncoding" not in path
    assert FakeGcs.objects["p"][0] == b"0123456789"
    assert fake_gcs.get("p") == b"0123456789"
    assert fake_gcs.head("p").content_encoding is None


def test_gcs_an_unknown_stored_encoding_is_typed(fake_gcs):
    FakeGcs.gen[0] += 1
    FakeGcs.objects["b"] = (b"\x0b\x02\x80", FakeGcs.gen[0], "br")
    with pytest.raises(o.UnsupportedEncodingError):
        fake_gcs.get("b")


def test_encoding_does_not_slow_plain_reads(tmp_path):
    # Performance guard: a plain local read pays at most one extra stat for
    # the (absent) sidecar tree; a plain read of 1 MiB stays well under 5 ms.
    s = o.Store.local(tmp_path / "s")
    s.put("k", TEXT)
    t = time.perf_counter()
    for _ in range(50):
        s.get("k")
    per = (time.perf_counter() - t) / 50
    assert per < 0.005, f"{per * 1e3:.2f} ms per plain read"
