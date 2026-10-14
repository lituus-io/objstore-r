# Author: spicyzhug@gmail.com
# Copyright: lituus-io, all rights reserved
"""Concurrency: compare-and-swap counters from many threads never lose an
update, and calls release the GIL."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

import objstore_r as o


@pytest.fixture(params=["memory", "local"])
def store(request, tmp_path):
    return o.Store.memory() if request.param == "memory" else o.Store.local(tmp_path / "s")


def increment(store, key, times):
    done = 0
    while done < times:
        blob = store.get(key)
        value = int(bytes(blob)) + 1
        try:
            store.put(key, str(value).encode(), if_generation=blob.generation)
            done += 1
        except o.PreconditionFailedError:
            continue  # lost the race: re-read and retry
    return done


def test_cas_counter_never_loses_an_update(store):
    store.put("counter", b"0", create_only=True)
    with ThreadPoolExecutor(max_workers=8) as pool:
        total = sum(pool.map(lambda _: increment(store, "counter", 50), range(8)))
    assert total == 400
    assert int(bytes(store.get("counter"))) == 400


def test_create_only_has_exactly_one_winner(store):
    wins = []
    barrier = threading.Barrier(8)

    def race(i):
        barrier.wait()
        try:
            store.put("lock", str(i).encode(), create_only=True)
            wins.append(i)
        except o.ConflictError:
            pass

    threads = [threading.Thread(target=race, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(wins) == 1
    assert bytes(store.get("lock")) == str(wins[0]).encode()


def test_calls_release_the_gil(tmp_path):
    # Local writes fsync; while one thread is inside Rust, another Python
    # thread keeps running.
    store = o.Store.local(tmp_path / "s")
    ticks = 0
    stop = threading.Event()

    def spin():
        nonlocal ticks
        while not stop.is_set():
            ticks += 1

    t = threading.Thread(target=spin)
    t.start()
    start = time.perf_counter()
    for i in range(20):
        store.put(f"k{i}", b"x" * 65536)
    stop.set()
    t.join()
    assert time.perf_counter() - start > 0
    assert ticks > 1000, ticks
