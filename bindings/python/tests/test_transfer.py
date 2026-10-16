# Author: spicyzhug@gmail.com
# Copyright: lituus-io, all rights reserved
"""Transfer settings on Store.gcs, and the transfer model end to end against
a small local double of the JSON API: an upload goes in chunks, and a
download dropped mid-body resumes with Range pinned to its generation."""

import base64
import http.server
import json
import os
import threading
import urllib.parse

import pytest

import objstore_r as o

CHUNK = 256 << 10


def test_transfer_keywords_and_their_validation():
    o.Store.gcs(
        "bucket",
        anonymous=True,
        connect_timeout=5,
        request_timeout=10,
        idle_timeout=20,
        min_throughput=1 << 20,
        upload_chunk=4 * CHUNK,
    )
    o.Store.gcs("bucket", anonymous=True, timeout=12.5)  # the 0.2.4 name
    for bad in [
        dict(upload_chunk=CHUNK + 1),
        dict(upload_chunk=0),
        dict(min_throughput=0),
        dict(idle_timeout=0),
        dict(request_timeout=-1),
        dict(connect_timeout=float("nan")),
        dict(timeout=0),
    ]:
        with pytest.raises(ValueError):
            o.Store.gcs("bucket", anonymous=True, **bad)


def _crc32c_b64(data):
    table = []
    for i in range(256):
        c = i
        for _ in range(8):
            c = (c >> 1) ^ 0x82F63B78 if c & 1 else c >> 1
        table.append(c)
    c = 0xFFFFFFFF
    for b in data:
        c = table[(c ^ b) & 0xFF] ^ (c >> 8)
    return base64.b64encode((c ^ 0xFFFFFFFF).to_bytes(4, "big")).decode()


class _Gcs(http.server.BaseHTTPRequestHandler):
    """One bucket, one object at a time; the first media GET is cut short."""

    objects = {}
    upload = bytearray()
    seen = []
    cut_next_get = True

    def log_message(self, *_):
        pass

    def _reply(self, status, headers=(), body=b""):
        self.send_response(status)
        for k, v in headers:
            self.send_header(k, v)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _query(self):
        return dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(self.path).query))

    def _body(self):
        return self.rfile.read(int(self.headers.get("content-length") or 0))

    def do_POST(self):
        q = self._query()
        self._body()
        self.seen.append(("start", q.get("name")))
        type(self).upload = bytearray()
        type(self).pending = (q["name"], int(self.headers["x-upload-content-length"]))
        port = self.server.server_address[1]
        loc = f"http://127.0.0.1:{port}/upload/storage/v1/b/bucket/o?uploadType=resumable&upload_id=1"
        self._reply(200, [("location", loc)])

    def do_PUT(self):
        body = self._body()
        span = self.headers["content-range"]
        self.seen.append(("put", span))
        self.upload.extend(body)
        name, total = self.pending
        if len(self.upload) < total:
            self._reply(308, [("range", f"bytes=0-{len(self.upload) - 1}")])
            return
        self.objects[name] = (bytes(self.upload), 7)
        meta = {"name": name, "size": str(total), "generation": "7"}
        self._reply(200, [("content-type", "application/json")], json.dumps(meta).encode())

    def do_GET(self):
        q = self._query()
        name = urllib.parse.unquote(urllib.parse.urlsplit(self.path).path.rsplit("/o/", 1)[1])
        data, gen = self.objects[name]
        rng = self.headers.get("range")
        self.seen.append(("get", rng, q.get("ifGenerationMatch")))
        hdrs = [("x-goog-generation", str(gen)), ("x-goog-hash", f"crc32c={_crc32c_b64(data)}")]
        if rng:
            start = int(rng.removeprefix("bytes=").split("-")[0])
            hdrs.append(("content-range", f"bytes {start}-{len(data) - 1}/{len(data)}"))
            self._reply(206, hdrs, data[start:])
            return
        if type(self).cut_next_get:
            type(self).cut_next_get = False
            self.send_response(200)
            for k, v in hdrs:
                self.send_header(k, v)
            self.send_header("content-length", str(len(data)))
            self.end_headers()
            self.wfile.write(data[: len(data) // 2])
            self.wfile.flush()
            self.close_connection = True
            return
        self._reply(200, hdrs, data)


def test_chunked_upload_and_resumed_download():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Gcs)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        store = o.Store.gcs(
            "bucket",
            anonymous=True,
            endpoint=f"http://127.0.0.1:{server.server_address[1]}",
            request_timeout=5,
            idle_timeout=5,
            resumable_threshold=CHUNK,
            upload_chunk=CHUNK,
        )
        data = os.urandom(2 * CHUNK + 1000)
        assert store.put("snap", data) == 7
        puts = [s[1] for s in _Gcs.seen if s[0] == "put"]
        total = len(data)
        assert puts == [
            f"bytes 0-{CHUNK - 1}/{total}",
            f"bytes {CHUNK}-{2 * CHUNK - 1}/{total}",
            f"bytes {2 * CHUNK}-{total - 1}/{total}",
        ]
        blob = store.get("snap")
        assert bytes(blob) == data and blob.generation == 7
        gets = [s for s in _Gcs.seen if s[0] == "get"]
        assert gets == [("get", None, None), ("get", f"bytes={total // 2}-", "7")]
    finally:
        server.shutdown()
