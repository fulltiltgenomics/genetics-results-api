"""Protein structure data access: what the routes read out of a loaded release.

Gene-keyed rows come from the release's tabix files through the in-process reader; model
files and PAE objects are read by model file id, and only ever for a `Model` taken from the
loaded index, so no object path is built from request input.

A PAE object is a raw n x n uint8 matrix, row r at bytes [r*n, (r+1)*n). This module only
moves bytes: a slice is whole rows read by byte range, then the wanted columns cut out of
each. Nothing is decoded and no matrix library is involved.

Every object read is checked against the size the index holds for it (consumer check 4 of
the contract): a range response states the object's total size, a whole read its length.
"""

import asyncio
import json
import logging
import math
import os
import zlib
from collections import OrderedDict
from dataclasses import dataclass
from typing import AsyncGenerator, AsyncIterator, Awaitable, Callable, TypeVar

import app.config.protein_structure_contract as contract
from app.core.exceptions import ParseException
from app.services.gcloud_tabix_base import (
    _TABIX_MAX_ATTEMPTS,
    _TABIX_RETRY_BASE_DELAY,
    _get_fetch_semaphore,
)
from app.services.protein_structure import Model, ProteinStructureAccess, Release

logger = logging.getLogger(__name__)

_READ_CHUNK = 64 * 1024

# whole PAE objects kept for slicing, LRU by resident bytes like the tabix index cache. Unlike
# TABIX_INDEX_CACHE_BYTES, 0 here switches the cache off rather than the bound: an unbounded
# cache of matrices is the failure this exists to prevent
_PAE_CACHE_BYTES = int(os.environ.get("PROTEIN_STRUCTURE_PAE_CACHE_BYTES", str(64 * 1024 * 1024)))
# an object over this is sliced by range reads and never cached: one titin-sized window would
# otherwise evict every other entry to serve a handful of rows
_PAE_CACHE_OBJECT_MAX = 4 * 1024 * 1024
_pae_cache: "OrderedDict[str, bytes]" = OrderedDict()
_pae_cache_bytes = 0

# scattered positions are merged into at most this many range reads; the rows between two
# merged runs are read and dropped
_MAX_PAE_RANGE_READS = 16

# PAE slice and rows requests reading or cutting at once; the rest wait holding nothing. Each
# holds at most MAX_RESPONSE_BYTES of full-width rows plus its output, on top of whatever the
# cache holds under its own bound
_PAE_SLICES_IN_FLIGHT = 4
_slice_semaphore: "asyncio.Semaphore | None" = None


def _get_slice_semaphore() -> asyncio.Semaphore:
    global _slice_semaphore
    if _slice_semaphore is None:
        _slice_semaphore = asyncio.Semaphore(_PAE_SLICES_IN_FLIGHT)
    return _slice_semaphore


# JSON bodies being built at once, from the first tabix read to the finished body. Each holds
# the gene's filtered rows as the tabix reader returns them, up to max_bytes of encoded rows,
# and the joined body: under 3 x MAX_RESPONSE_BYTES while the build's per-gene limit holds
_JSON_BUILDS_IN_FLIGHT = 2
_json_semaphore: "asyncio.Semaphore | None" = None
_JSON_BATCH_ROWS = 2000

# no stored model file inflates to anywhere near this; one that does is not a model file
MAX_STRUCTURE_TEXT_BYTES = contract.MAX_RESPONSE_BYTES

_MAX_POSITION_DIGITS = 9


def json_build_slot() -> asyncio.Semaphore:
    global _json_semaphore
    if _json_semaphore is None:
        _json_semaphore = asyncio.Semaphore(_JSON_BUILDS_IN_FLIGHT)
    return _json_semaphore


def _pae_cache_get(path: str) -> bytes | None:
    obj = _pae_cache.get(path)
    if obj is not None:
        _pae_cache.move_to_end(path)
    return obj


def _pae_cache_put(path: str, obj: bytes) -> None:
    global _pae_cache_bytes
    if path in _pae_cache or len(obj) > _PAE_CACHE_BYTES:
        return
    _pae_cache[path] = obj
    _pae_cache_bytes += len(obj)
    while _pae_cache_bytes > _PAE_CACHE_BYTES:
        _, evicted = _pae_cache.popitem(last=False)
        _pae_cache_bytes -= len(evicted)


class ObjectMismatch(RuntimeError):
    """A stored object is missing, or is not the size the release index gives it."""


class ResponseTooLarge(ValueError):
    """A body that would pass the per-response byte limit."""


@dataclass(frozen=True)
class Table:
    file: str
    columns: tuple[tuple[str, type], ...]

    @property
    def names(self) -> list[str]:
        return [c for c, _ in self.columns]

    @property
    def header_schema(self) -> dict[str, type]:
        return dict(self.columns)


RESIDUES = Table(contract.RESIDUES_FILE, tuple(contract.RESIDUE_COLUMNS))
FEATURES = Table(contract.FEATURES_FILE, tuple(contract.FEATURE_COLUMNS))
SCORES = Table(contract.SCORES_FILE, tuple(contract.SCORE_COLUMNS))

_NULL = b"NA"


async def tsv_chunks(table: Table, rows: AsyncIterator[list[bytes]]) -> AsyncGenerator[bytes, None]:
    """Header line, then the rows with the release's empty cells written as NA, as every TSV of this API does."""
    buf = bytearray(("\t".join(table.names) + "\n").encode())
    async for fields in rows:
        buf += b"\t".join(f or _NULL for f in fields) + b"\n"
        if len(buf) >= _READ_CHUNK:
            yield bytes(buf)
            buf.clear()
    if buf:
        yield bytes(buf)


def _finite(v: str) -> float | None:
    # JSON has no NaN or Infinity; a cell holding one is a value this API cannot state
    x = float(v)
    return x if math.isfinite(x) else None


_CASTS: dict[type, Callable[[str], object]] = {
    str: str,
    int: int,
    float: _finite,
    bool: lambda v: {"true": True, "false": False}[v],
}


def _encode_rows(table: Table, batch: list[list[bytes]]) -> list[bytes]:
    names = table.names
    casts = [_CASTS[t] for _, t in table.columns]
    return [
        json.dumps(
            {n: cast(f.decode()) if f else None for n, cast, f in zip(names, casts, fields, strict=True)},
            separators=(",", ":"), allow_nan=False,
        ).encode()
        for fields in batch
    ]


async def json_body(table: Table, rows: AsyncIterator[list[bytes]], max_bytes: int) -> bytes:
    """The rows as a JSON array of objects, refused once it would pass max_bytes.

    Encoded batch by batch in a worker thread: the refusal comes at max_bytes of text, not
    after a list of row dicts several times that size has been built, and the event loop is
    held for no longer than it takes to hand a batch over.
    """
    parts: list[bytes] = []
    size = 2
    batch: list[list[bytes]] = []

    async def flush() -> None:
        nonlocal size
        for encoded in await asyncio.to_thread(_encode_rows, table, batch):
            size += len(encoded) + (1 if parts else 0)
            if size > max_bytes:
                raise ResponseTooLarge(f"the JSON body is over {max_bytes} bytes")
            parts.append(encoded)
        batch.clear()

    async for fields in rows:
        batch.append(fields)
        if len(batch) >= _JSON_BATCH_ROWS:
            await flush()
    await flush()
    return await asyncio.to_thread(lambda: b"[" + b",".join(parts) + b"]")


def parse_positions(text: str, n: int) -> list[int]:
    """'12,15-20,33' as distinct file positions in ascending order, each within 1..n."""
    spans: list[tuple[int, int]] = []
    for part in text.split(","):
        token = part.strip()
        lo, sep, hi = token.partition("-")
        # the length is checked before int(): CPython refuses to convert very long digit strings
        if not all(
            x.isascii() and x.isdigit() and len(x) <= _MAX_POSITION_DIGITS for x in ((lo, hi) if sep else (lo,))
        ):
            raise ParseException(f"invalid position {token[:40]!r}: expected N or N-M")
        first, last = int(lo), int(hi if sep else lo)
        if not 1 <= first <= last <= n:
            raise ParseException(f"position {token[:40]!r} is outside 1..{n}")
        spans.append((first, last))
    # merged before anything is expanded, so overlaps count once and an oversized range is never built
    merged: list[tuple[int, int]] = []
    for first, last in sorted(spans):
        if merged and first <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], last))
        else:
            merged.append((first, last))
    most = contract.pae_rows_per_request(n)
    if sum(last - first + 1 for first, last in merged) > most:
        raise ParseException(f"more than {most} positions for a model of {n} residues")
    return [p for first, last in merged for p in range(first, last + 1)]


def inflate_structure(stored: bytes, max_bytes: int) -> bytes:
    """A stored model file's text; refused when it is not gzip or inflates past max_bytes."""
    out = bytearray()
    data = stored
    try:
        while data:
            member = zlib.decompressobj(47)
            out += member.decompress(data, max_bytes + 1 - len(out))
            if len(out) > max_bytes or not member.eof:
                raise ObjectMismatch(f"a model file inflates past {max_bytes} bytes or ends inside its gzip stream")
            data = member.unused_data
    except zlib.error as e:
        raise ObjectMismatch(f"a model file is not gzip: {e}")
    return bytes(out)


def _runs(sorted_values: list[int]) -> list[tuple[int, int]]:
    """Half-open runs of consecutive values."""
    runs: list[tuple[int, int]] = []
    for v in sorted_values:
        if runs and runs[-1][1] == v:
            runs[-1] = (runs[-1][0], v + 1)
        else:
            runs.append((v, v + 1))
    return runs


def plan_row_reads(rows: list[int], max_reads: int) -> list[tuple[int, int]]:
    """Half-open row ranges covering `rows` in at most max_reads reads, closing the smallest gaps first."""
    runs = _runs(rows)
    excess = len(runs) - max_reads
    if excess <= 0:
        return runs
    gaps = sorted(range(len(runs) - 1), key=lambda i: runs[i + 1][0] - runs[i][1])
    closed = set(gaps[:excess])
    merged = [runs[0]]
    for i in range(1, len(runs)):
        if i - 1 in closed:
            merged[-1] = (merged[-1][0], runs[i][1])
        else:
            merged.append(runs[i])
    return merged


def cut_columns(rows: list[bytes], column_runs: list[tuple[int, int]]) -> bytes:
    return b"".join(row[a:b] for row in rows for a, b in column_runs)


T = TypeVar("T")


class ProteinStructureData(ProteinStructureAccess):
    """The loaded release plus the reads the routes make against it."""

    async def stream_rows(
        self, release: Release, table: Table, genes: list[dict], accession: str | None = None,
        predictors: frozenset[str] | None = None,
    ) -> AsyncGenerator[list[bytes], None]:
        """Rows of the given genes, optionally of one accession, split into fields.

        The files are point-indexed on gene_start, so a lookup returns every gene that starts
        there; the gene_id column picks out the requested ones. A row whose accession is not in
        the loaded index is never yielded: the files hold every accession of the release, the
        index only those this deployment serves. The same holds for a score row and the
        release's predictors, which `predictors` can narrow and never widen.
        """
        wanted = {g["ensg"].encode() for g in genes}
        starts = sorted({(g["chrom"], g["gene_start"]) for g in genes})
        accession_col = table.names.index("accession")
        only = accession.encode() if accession is not None else None
        n_columns = len(table.columns)
        served = release.by_accession
        predictor = None
        if "predictor" in table.names:
            predictor = (
                table.names.index("predictor"),
                frozenset(p.encode() for p in release.predictors or () if predictors is None or p in predictors),
            )
        stream = await self._stream_range(
            release.object_path(table.file), [c for c, _ in starts], [s for _, s in starts], [s for _, s in starts], 0
        )
        pending = b""
        async for chunk in stream:
            *lines, pending = (pending + chunk).split(b"\n")
            for line in lines:
                fields = self._wanted_fields(line, n_columns, wanted, accession_col, only, served, predictor)
                if fields is not None:
                    yield fields
        if pending:
            fields = self._wanted_fields(pending, n_columns, wanted, accession_col, only, served, predictor)
            if fields is not None:
                yield fields

    @staticmethod
    def _wanted_fields(
        line: bytes, n_columns: int, wanted: set[bytes], accession_col: int, only: bytes | None,
        served: dict[str, tuple[str, ...]], predictor: tuple[int, frozenset[bytes]] | None,
    ) -> list[bytes] | None:
        if not line:
            return None
        fields = line.split(b"\t")
        if fields[0].split(b".", 1)[0] not in wanted:
            return None
        if len(fields) != n_columns:
            raise ValueError(f"a row has {len(fields)} fields, the contract has {n_columns}")
        if only is not None and fields[accession_col] != only:
            return None
        if fields[accession_col].decode() not in served:
            return None
        if predictor is not None and fields[predictor[0]] not in predictor[1]:
            return None
        return fields

    async def _retrying(self, op: Callable[[], Awaitable[T]], what: str) -> T:
        last: Exception | None = None
        for attempt in range(_TABIX_MAX_ATTEMPTS):
            try:
                return await op()
            except ObjectMismatch:
                raise
            except Exception as e:
                last = e
                if attempt < _TABIX_MAX_ATTEMPTS - 1:
                    await asyncio.sleep(_TABIX_RETRY_BASE_DELAY * (2**attempt))
        raise RuntimeError(f"failed to read {what}: {last}")

    async def _chunks(
        self, gs_path: str, total: int, byte_range: tuple[int, int] | None = None
    ) -> AsyncGenerator[bytes, None]:
        """The object's bytes, or the half-open byte_range of them, refused unless the object is `total` long.

        The size is checked before the first chunk is yielded, and the count delivered after
        the last, so a caller that has read the generator to its end holds exactly the bytes asked for.
        """
        headers = await self.storage._headers()
        expected = total
        if byte_range is not None:
            headers = {**headers, "Range": f"bytes={byte_range[0]}-{byte_range[1] - 1}"}
            expected = byte_range[1] - byte_range[0]
        async with self.session.get(self._gcs_url(gs_path), headers=headers) as resp:
            if resp.status in (404, 416):
                raise ObjectMismatch(f"{gs_path}: status {resp.status}, the index gives it {total} bytes")
            resp.raise_for_status()
            if byte_range is not None:
                # "bytes a-b/total"; a 200 here would be the whole object, not the range asked for
                stated = resp.headers.get("Content-Range", "").rpartition("/")[2]
                if resp.status != 206 or stated != str(total):
                    raise ObjectMismatch(
                        f"{gs_path}: range answered {resp.status} of {stated or 'unstated'} bytes, "
                        f"the index gives it {total}"
                    )
            elif resp.content_length is not None and resp.content_length != total:
                raise ObjectMismatch(f"{gs_path} is {resp.content_length} bytes, the index gives it {total}")
            received = 0
            async for chunk in resp.content.iter_chunked(_READ_CHUNK):
                received += len(chunk)
                if received > expected:
                    raise ObjectMismatch(f"{gs_path}: more than the {expected} bytes expected")
                yield chunk
            if received != expected:
                raise ObjectMismatch(f"{gs_path}: {received} bytes arrived of {expected}")

    async def _read_object(self, gs_path: str, total: int) -> bytes:
        async def once() -> bytes:
            async with _get_fetch_semaphore():
                buf = bytearray()
                async for chunk in self._chunks(gs_path, total):
                    buf += chunk
                return bytes(buf)

        return await self._retrying(once, gs_path)

    async def structure(self, release: Release, model: Model) -> bytes:
        """The stored model file: the bundle's .pdb.gz bytes, unchanged."""
        return await self._read_object(release.object_path(model.structure_path), model.structure_bytes)

    async def open_pae(self, release: Release, model: Model) -> AsyncGenerator[bytes, None]:
        """The whole PAE object as a stream whose size has already been checked when this returns.

        Not buffered and not cached: the largest objects are larger than the cache's per-object
        limit, and a passthrough needs one chunk of memory whatever the size.
        """
        chunks = self._chunks(release.object_path(model.pae_path), model.pae_bytes)
        try:
            first = await anext(chunks)
        except StopAsyncIteration:
            first = b""
        except BaseException:
            await chunks.aclose()
            raise

        async def body() -> AsyncGenerator[bytes, None]:
            try:
                yield first
                async for chunk in chunks:
                    yield chunk
            finally:
                await chunks.aclose()

        return body()

    async def _rows_by_range(self, gs_path: str, model: Model, lo: int, hi: int, wanted: set[int]) -> list[bytes]:
        """The wanted rows among rows [lo, hi), read as one byte range; the others are dropped as they arrive."""
        n = model.n_residues

        async def once() -> list[bytes]:
            rows: list[bytes] = []
            buf = bytearray()
            row = lo
            async with _get_fetch_semaphore():
                async for chunk in self._chunks(gs_path, model.pae_bytes, (lo * n, hi * n)):
                    buf += chunk
                    whole = len(buf) // n
                    for i in range(whole):
                        if row + i in wanted:
                            rows.append(bytes(buf[i * n : (i + 1) * n]))
                    row += whole
                    del buf[: whole * n]
            return rows

        return await self._retrying(once, gs_path)

    async def _full_rows(self, release: Release, model: Model, positions: list[int]) -> list:
        """The full-width row of each position, in the order given; the caller holds a slice slot."""
        n = model.n_residues
        gs_path = release.object_path(model.pae_path)
        rows = [p - 1 for p in positions]
        obj = _pae_cache_get(gs_path)
        if obj is None and model.pae_bytes <= min(_PAE_CACHE_OBJECT_MAX, _PAE_CACHE_BYTES):
            obj = await self._read_object(gs_path, model.pae_bytes)
            _pae_cache_put(gs_path, obj)
        if obj is not None:
            view = memoryview(obj)
            return [view[r * n : (r + 1) * n] for r in rows]
        wanted = set(rows)
        reads = [
            asyncio.ensure_future(self._rows_by_range(gs_path, model, lo, hi, wanted))
            for lo, hi in plan_row_reads(rows, _MAX_PAE_RANGE_READS)
        ]
        try:
            parts = await asyncio.gather(*reads)
        except BaseException:
            # gather leaves the other reads running, each still collecting rows nobody will use
            for read in reads:
                read.cancel()
            await asyncio.gather(*reads, return_exceptions=True)
            raise
        full_rows = [row for part in parts for row in part]
        if len(full_rows) != len(rows):
            raise ObjectMismatch(f"{gs_path}: {len(full_rows)} rows read of {len(rows)}")
        return full_rows

    async def pae_slice(self, release: Release, model: Model, positions: list[int]) -> bytes:
        """The k x k submatrix of the given file positions, row-major, in the order given.

        `positions` come from parse_positions: distinct, ascending and few enough that their
        full-width rows fit in MAX_RESPONSE_BYTES, which is what bounds the bytes held here.
        """
        async with _get_slice_semaphore():
            full_rows = await self._full_rows(release, model, positions)
            return await asyncio.to_thread(cut_columns, full_rows, _runs([p - 1 for p in positions]))

    async def pae_rows(self, release: Release, model: Model, positions: list[int]) -> bytes:
        """The complete row of each given file position, k x n bytes, in the order given."""
        async with _get_slice_semaphore():
            return b"".join(await self._full_rows(release, model, positions))
