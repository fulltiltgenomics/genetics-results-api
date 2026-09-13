"""The parsed tabix index is compact, its cache is bounded by bytes, and a request's
file fan-out is bounded in flight.

One R14 GWAS `.tbi` is 1.8 MB on disk and was 52.5 MiB once parsed into tuples and
dicts; a PheWAS over a few hundred phenotypes therefore parked 13 GiB in the process
and nothing ever released it. The index is now flat arrays, the cache evicts by
resident bytes, and only a bounded number of files are fetched-and-unfiltered at the
same moment. Every check here is offline: the `.tbi` files are synthesised.
"""

import asyncio
import random
import struct
import zlib
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.services import gcloud_tabix_base
from app.services.gcloud_tabix_base import GCloudTabixBase
from app.services.tabix_query import _reg2bins, parse_tabix_index

_BGZF_EOF = (
    b"\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\xff\x06\x00\x42\x43\x02\x00\x1b\x00"
    b"\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00"
)


def _bgzf(payload: bytes) -> bytes:
    out = b""
    for i in range(0, len(payload), 60000):
        chunk = payload[i : i + 60000]
        c = zlib.compressobj(6, zlib.DEFLATED, -15)
        body = c.compress(chunk) + c.flush()
        bsize = 18 + len(body) + 8 - 1
        out += (
            b"\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\xff"
            + struct.pack("<H", 6)
            + b"BC"
            + struct.pack("<HH", 2, bsize)
            + body
            + struct.pack("<II", zlib.crc32(chunk) & 0xFFFFFFFF, len(chunk))
        )
    return out + _BGZF_EOF


def _tbi(names: list[bytes], refs: list[tuple[dict, list[int]]]) -> bytes:
    """Serialize a (bins, linear) model per reference exactly as htslib writes it,
    bins in the dict's (deliberately unsorted) insertion order."""
    nm = b"".join(n + b"\0" for n in names)
    out = b"TBI\x01" + struct.pack("<8i", len(refs), 0, 1, 2, 2, ord("#"), 0, len(nm)) + nm
    for bins, linear in refs:
        out += struct.pack("<i", len(bins))
        for b, chunks in bins.items():
            out += struct.pack("<Ii", b, len(chunks))
            out += b"".join(struct.pack("<QQ", a, z) for a, z in chunks)
        out += struct.pack("<i", len(linear)) + struct.pack(f"<{len(linear)}Q", *linear)
    return _bgzf(out)


def _random_model(rng: random.Random, n_windows: int) -> tuple[dict, list[int]]:
    voff = 0
    linear = []
    for _ in range(n_windows):
        voff += rng.randrange(1, 3 << 16)
        linear.append(voff)
    bins: dict[int, list[tuple[int, int]]] = {}
    for _ in range(rng.randrange(1, 60)):
        b = rng.choice(_reg2bins(rng.randrange(n_windows << 14), rng.randrange(1, 3 << 14)))
        chunks = []
        for _ in range(rng.randrange(1, 4)):
            a = rng.randrange(voff + (4 << 16))
            chunks.append((a, a + rng.randrange(1, 2 << 16)))
        bins.setdefault(b, []).extend(chunks)
    keys = list(bins)
    rng.shuffle(keys)
    return {k: bins[k] for k in keys}, linear


def _reference_ranges(refs, regions):
    """The pre-array algorithm over the dict model: the oracle the arrays must match."""
    spans = []
    for tid, beg0, end in regions:
        bins, linear = refs[tid]
        li = beg0 >> 14
        min_off = linear[li] if li < len(linear) else 0
        for b in _reg2bins(beg0, end):
            for cbeg, cend in bins.get(b, ()):
                if cend > min_off:
                    spans.append((cbeg >> 16, cend >> 16, cbeg & 0xFFFF))
    if not spans:
        return []
    spans.sort()
    merged = [spans[0]]
    for s, e, u in spans[1:]:
        ls, le, lu = merged[-1]
        if s <= le + 65536:
            if e > le:
                merged[-1] = (ls, e, lu)
        else:
            merged.append((s, e, u))
    return merged


@pytest.mark.parametrize("seed", range(5))
def test_compact_index_matches_dict_model(seed):
    rng = random.Random(seed)
    refs = [_random_model(rng, rng.randrange(1, 40)) for _ in range(rng.randrange(1, 4))]
    idx = parse_tabix_index(_tbi([b"1", b"2", b"X"][: len(refs)], refs))

    for (bins, linear), ref in zip(refs, idx.refs):
        assert list(ref.linear) == linear
        assert list(ref.bin_ids) == sorted(bins)
        for b, chunks in bins.items():
            cb, ce = ref.chunks(b)
            assert list(zip(cb, ce)) == chunks
        absent_b, absent_e = ref.chunks(10**9)
        assert len(absent_b) == 0 and len(absent_e) == 0

    for _ in range(300):
        regions = []
        for _ in range(rng.randrange(1, 6)):
            tid = rng.randrange(len(refs))
            beg = rng.randrange(len(refs[tid][1]) << 14)
            regions.append((tid, beg, beg + rng.choice([1, 1, 500, 40000])))
        assert idx.byte_ranges(regions) == _reference_ranges(refs, regions)


def test_index_nbytes_is_the_array_footprint():
    rng = random.Random(1)
    refs = [_random_model(rng, 30)]
    idx = parse_tabix_index(_tbi([b"1"], refs))
    n_chunks = sum(len(c) for c in refs[0][0].values())
    n_bins = len(refs[0][0])
    expected = 4 * n_bins + 4 * (n_bins + 1) + 8 * n_chunks * 2 + 8 * 30 + 1
    assert idx.nbytes == expected


class _Idx:
    def __init__(self, nbytes):
        self.nbytes = nbytes


@pytest.fixture
def clean_index_cache(monkeypatch):
    monkeypatch.setattr(gcloud_tabix_base, "_index_cache", type(gcloud_tabix_base._index_cache)())
    monkeypatch.setattr(gcloud_tabix_base, "_index_cache_bytes", 0)
    yield


def test_index_cache_evicts_least_recently_used_by_bytes(clean_index_cache, monkeypatch):
    monkeypatch.setattr(gcloud_tabix_base, "_INDEX_CACHE_BYTES", 100)
    put, get = gcloud_tabix_base._index_cache_put, gcloud_tabix_base._index_cache_get
    put("a", _Idx(40))
    put("b", _Idx(40))
    assert get("a") is not None  # a is now the most recently used
    put("c", _Idx(40))  # 120 > 100: evicts b, the least recently used, not a
    assert get("b") is None
    assert get("a") is not None
    assert get("c") is not None
    assert gcloud_tabix_base._index_cache_bytes == 80


def test_index_cache_keeps_one_entry_even_when_it_alone_exceeds_the_budget(
    clean_index_cache, monkeypatch
):
    monkeypatch.setattr(gcloud_tabix_base, "_INDEX_CACHE_BYTES", 10)
    gcloud_tabix_base._index_cache_put("big", _Idx(1000))
    assert gcloud_tabix_base._index_cache_get("big") is not None


def test_index_cache_unbounded_when_budget_is_zero(clean_index_cache, monkeypatch):
    monkeypatch.setattr(gcloud_tabix_base, "_INDEX_CACHE_BYTES", 0)
    for i in range(50):
        gcloud_tabix_base._index_cache_put(str(i), _Idx(10**9))
    assert len(gcloud_tabix_base._index_cache) == 50


class _CountingAccess(GCloudTabixBase):
    """Stands in for any data-access subclass: one index, one byte range per file,
    and a fetch that records how many files are mid-flight at once."""

    def __init__(self, idx, active: set, high: list):
        super().__init__()
        self._idx = idx
        self._active = active
        self._high = high

    async def _get_index(self, file_path):
        return self._idx

    async def _fetch_blocks(self, url, start, last_block_off):
        self._active.add(url)
        self._high[0] = max(self._high[0], len(self._active))
        await asyncio.sleep(0.01)
        self._active.discard(url)
        return b""


def test_files_in_flight_are_capped_across_a_primed_fan_out(monkeypatch):
    idx = parse_tabix_index(_tbi([b"1"], [({4681: [(0, 100 << 16)]}, [0])]))
    active: set = set()
    high = [0]
    pool = ThreadPoolExecutor(2)
    monkeypatch.setattr(gcloud_tabix_base, "_get_filter_pool", lambda: pool)

    async def run():
        monkeypatch.setattr(gcloud_tabix_base, "_files_semaphore", asyncio.Semaphore(3))
        access = _CountingAccess(idx, active, high)
        streams = [
            await access._stream_range(f"gs://b/f{i}.gz", [1], [5], [5], 0) for i in range(20)
        ]

        async def drain(s):
            return [chunk async for chunk in s]

        # prime every file at once, the way start_iterators() does for a merge
        return await asyncio.gather(*(drain(s) for s in streams))

    try:
        results = asyncio.run(run())
    finally:
        pool.shutdown()
    assert len(results) == 20 and all(r == [] for r in results)
    assert high[0] == 3
