import hashlib
import logging
import multiprocessing
import threading
import time
import asyncio
import aiohttp
import aiohttp.client_exceptions
from typing import AsyncGenerator
from asyncio.unix_events import subprocess
from app.core.exceptions import NotFoundException
from app.services.tabix_query import (
    TabixIndex,
    bgzf_block_size,
    filter_batch,
    parse_tabix_index,
)
from concurrent.futures import ProcessPoolExecutor
from gcloud.aio.storage import Storage
from google.auth import default
from google.auth.transport.requests import Request
import os
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# module-level GCS token management shared by all GCloudTabixBase instances
_gcs_credentials = None
_gcs_token_lock = threading.Lock()

# module-level tbi cache cleanup tracking
_tbi_cleanup_lock = threading.Lock()
_tbi_last_cleanup = 0.0
_TBI_CLEANUP_INTERVAL = 600  # only check every 10 minutes

# htslib reports transient GCS access failures (timed-out/partial HTTPS requests)
# as misleading "Invalid argument" / "No such file or directory" / index-load
# errors and never retries them itself, so a single network blip permanently
# fails an otherwise-valid file. retry tabix a few times with backoff.
_TABIX_MAX_ATTEMPTS = 3
_TABIX_RETRY_BASE_DELAY = 0.5

# parsed .tbi indexes are reused across requests for the lifetime of the process;
# the index is small and the files are effectively immutable per version
_index_cache: dict[str, "TabixIndex"] = {}
_index_cache_lock = asyncio.Lock()

# Global cap on concurrent in-flight GCS range fetches across ALL data-access
# objects and overlapping requests. Each fetch uses at most one socket, so this
# bounds total open GCS sockets regardless of how many files a request fans out
# over or how many requests overlap — which is what previously exhausted the
# process file-descriptor limit ("Too many open files"). Override with
# GCS_MAX_CONNECTIONS.
_GCS_MAX_CONNECTIONS = int(os.environ.get("GCS_MAX_CONNECTIONS", "128"))
_fetch_semaphore: "asyncio.Semaphore | None" = None


def _get_fetch_semaphore() -> "asyncio.Semaphore":
    """Process-wide semaphore bounding concurrent GCS range fetches. Created lazily
    on first use so it binds to the running event loop."""
    global _fetch_semaphore
    if _fetch_semaphore is None:
        _fetch_semaphore = asyncio.Semaphore(_GCS_MAX_CONNECTIONS)
    return _fetch_semaphore

# maximum size of a bgzf block (compressed); used to bound the final-block read
_MAX_BGZF_BLOCK = 65536

# shared process pool for CPU-bound decompress+filter work, so range queries
# parallelize across cores instead of serializing on the GIL-bound event loop
_filter_pool: ProcessPoolExecutor | None = None
_filter_pool_lock = threading.Lock()
# bound workers: os.cpu_count() reports the host's cores, not the cgroup CPU quota,
# so on a big node under a small CPU limit an unbounded count would spawn dozens of
# idle-but-FD-holding worker processes. Cap it; override with TABIX_FILTER_WORKERS.
_FILTER_WORKERS = int(os.environ.get("TABIX_FILTER_WORKERS", "0")) or max(
    1, min(4, (os.cpu_count() or 2) - 1)
)


def _get_filter_pool() -> ProcessPoolExecutor:
    global _filter_pool
    if _filter_pool is None:
        with _filter_pool_lock:
            if _filter_pool is None:
                # forkserver, not the default fork: forking a running async/threaded
                # server process can deadlock; forkserver spawns workers from a clean
                # minimal process instead
                ctx = multiprocessing.get_context("forkserver")
                _filter_pool = ProcessPoolExecutor(
                    max_workers=_FILTER_WORKERS, mp_context=ctx
                )
    return _filter_pool


def ensure_gcs_token() -> None:
    """Ensure GCS OAuth token env var is valid for tabix subprocess calls."""
    global _gcs_credentials
    # the whole check-and-refresh must hold the lock, including first init:
    # concurrent first calls would otherwise each fetch credentials and race on
    # the GCS_OAUTH_TOKEN env var, occasionally exporting a half-written token
    with _gcs_token_lock:
        if _gcs_credentials is None:
            creds, _ = default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            creds.refresh(Request())
            os.environ["GCS_OAUTH_TOKEN"] = creds.token
            # publish only after a successful refresh so a failed refresh does
            # not leave a credentials object that the None-check would skip
            _gcs_credentials = creds
            logger.info(f"GCS_OAUTH_TOKEN set, expires at: {_gcs_credentials.expiry}")
            return

        if (
            _gcs_credentials.expiry
            and _gcs_credentials.expiry > datetime.now() + timedelta(minutes=5)
        ):
            return
        logger.info("GCS token expired or expiring soon, refreshing...")
        _gcs_credentials.refresh(Request())
        os.environ["GCS_OAUTH_TOKEN"] = _gcs_credentials.token
        logger.info(f"Token refreshed, new expiry: {_gcs_credentials.expiry}")


class GCloudTabixBase:
    """
    Base class with all common GCloud Storage + tabix functionality.

    Provides shared functionality for:
    - GCloud Storage client management
    - OAuth token management and refresh
    - Tabix operations (header retrieval, range queries)
    - File streaming from GCS
    - Async context manager support
    """

    def __init__(self):
        self.storage = None
        self.session = None
        os.makedirs("/tmp/tbi_cache", exist_ok=True)
        # the aiohttp GCS client is only used by _stream_file; header and range
        # access go through the tabix subprocess. creating the session eagerly
        # here leaks an unclosed aiohttp session for every object used solely for
        # tabix (e.g. the whole credible-set/coloc path), so create it lazily.
        ensure_gcs_token()

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - cleanup session."""
        await self.cleanup()

    async def cleanup(self):
        """Clean up resources."""
        if self.session:
            await self.session.close()
        if self.storage:
            await self.storage.close()

    TBI_CACHE_ROOT = "/tmp/tbi_cache"
    TBI_CACHE_MAX_BYTES = 10 * 1024 * 1024 * 1024  # 10GB

    def _get_tbi_cache_dir(self, gs_path: str) -> str:
        """Get a unique cache directory for a GCS file's .tbi index.

        Uses a hash of the GCS directory path so files with the same basename
        but different GCS paths don't collide.
        """
        dir_path = gs_path.rsplit("/", 1)[0]
        dir_hash = hashlib.md5(dir_path.encode()).hexdigest()[:12]
        cache_dir = os.path.join(self.TBI_CACHE_ROOT, dir_hash)
        os.makedirs(cache_dir, exist_ok=True)
        return cache_dir

    @staticmethod
    def _do_tbi_cleanup():
        """Remove oldest .tbi files if cache exceeds size limit. Runs in a background thread."""
        global _tbi_last_cleanup

        now = time.time()
        if not _tbi_cleanup_lock.acquire(blocking=False):
            return
        try:
            if now - _tbi_last_cleanup < _TBI_CLEANUP_INTERVAL:
                return

            cache_root = GCloudTabixBase.TBI_CACHE_ROOT
            max_bytes = GCloudTabixBase.TBI_CACHE_MAX_BYTES
            tbi_files = []
            for dirpath, _, filenames in os.walk(cache_root):
                for f in filenames:
                    if f.endswith(".tbi"):
                        path = os.path.join(dirpath, f)
                        try:
                            stat = os.stat(path)
                            tbi_files.append((path, stat.st_atime, stat.st_size))
                        except OSError:
                            continue

            total_size = sum(size for _, _, size in tbi_files)
            _tbi_last_cleanup = now

            if total_size <= max_bytes:
                return

            tbi_files.sort(key=lambda x: x[1])
            removed = 0
            for path, _, size in tbi_files:
                if total_size <= max_bytes:
                    break
                try:
                    os.remove(path)
                    total_size -= size
                    removed += 1
                except OSError:
                    pass
            if removed:
                logger.info(f"TBI cache cleanup: removed {removed} files, {total_size // (1024*1024)}MB remaining")
        finally:
            _tbi_cleanup_lock.release()

    def _maybe_cleanup_tbi_cache(self):
        """Schedule a cache cleanup check in a background thread if due."""
        if time.time() - _tbi_last_cleanup >= _TBI_CLEANUP_INTERVAL:
            threading.Thread(target=self._do_tbi_cleanup, daemon=True).start()

    def _init_storage(self):
        """Initialize the storage client and session.

        Each instance owns its session/connector (gcloud-aio's Storage/Token binds
        a token session per instance, so sharing one connector across instances
        breaks token refresh). The total number of open GCS sockets is instead
        bounded globally by `_get_fetch_semaphore()`, which caps concurrent range
        fetches across all instances and requests.
        """
        # short keepalive prevents stale connections after inactivity (which cause
        # TimeoutError in gcloud-aio-auth's acquire_access_token) and, with one
        # connector per data-access object, keeps idle sockets from piling up after
        # a fan-out burst; concurrent fetches are already capped globally by
        # _get_fetch_semaphore()
        connector = aiohttp.TCPConnector(limit=256, keepalive_timeout=5)
        self.session = aiohttp.ClientSession(connector=connector)
        self.storage = Storage(session=self.session)

    def _ensure_storage(self):
        """Lazily create the aiohttp GCS client on first streaming use, and
        recreate it if a previous session was closed (e.g. by shutdown cleanup
        racing a late request, or a pooled connection going stale)."""
        if self.session is None or self.session.closed:
            self._init_storage()

    def _get_header(self, gs_path: str) -> list[bytes]:
        """
        Get the header for a tabix-indexed file.

        This also verifies that the file is tabix-indexed and tabix operations work.

        Args:
            gs_path: Full gs:// path to the file

        Returns:
            List of header column names as bytes

        Raises:
            RuntimeError: If tabix operation fails
        """
        cache_dir = self._get_tbi_cache_dir(gs_path)
        last_stderr = ""
        for attempt in range(_TABIX_MAX_ATTEMPTS):
            ensure_gcs_token()
            try:
                process = subprocess.run(
                    ["tabix", "-H", gs_path],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=True,
                    cwd=cache_dir,
                )
                break
            except subprocess.CalledProcessError as e:
                last_stderr = e.stderr.decode()
                if attempt < _TABIX_MAX_ATTEMPTS - 1:
                    time.sleep(_TABIX_RETRY_BASE_DELAY * (2**attempt))
                    continue
                logger.error(
                    f"Tabix header failed after {_TABIX_MAX_ATTEMPTS} attempts "
                    f"for {gs_path}: {last_stderr}"
                )
                raise RuntimeError("Getting file header failed")

        header_line = process.stdout.strip()
        # remove leading '#' from header columns
        header = [h[1:] if h.startswith(b"#") else h for h in header_line.split(b"\t")]
        logger.info(f"Header: {header}")
        return header

    async def _get_header_async(self, gs_path: str) -> list[bytes]:
        """Async, non-blocking equivalent of _get_header.

        Uses asyncio.create_subprocess_exec instead of the blocking subprocess.run
        so warming/fetching a header never stalls the event loop. Also downloads the
        .tbi index into the cache dir (same side effect as _get_header), so calling
        this at startup prefetches the index cache.
        """
        cache_dir = self._get_tbi_cache_dir(gs_path)
        last_stderr = ""
        for attempt in range(_TABIX_MAX_ATTEMPTS):
            ensure_gcs_token()
            process = await asyncio.create_subprocess_exec(
                "tabix",
                "-H",
                gs_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cache_dir,
            )
            stdout, stderr = await process.communicate()
            if process.returncode == 0:
                header_line = stdout.strip()
                header = [
                    h[1:] if h.startswith(b"#") else h for h in header_line.split(b"\t")
                ]
                logger.info(f"Header: {header}")
                return header
            last_stderr = stderr.decode()
            if attempt < _TABIX_MAX_ATTEMPTS - 1:
                await asyncio.sleep(_TABIX_RETRY_BASE_DELAY * (2**attempt))
        logger.error(
            f"Tabix header failed after {_TABIX_MAX_ATTEMPTS} attempts "
            f"for {gs_path}: {last_stderr}"
        )
        raise RuntimeError("Getting file header failed")

    async def _cache_header_async(self, cache_key: str, gs_path: str) -> list[bytes]:
        """Async equivalent of _cache_header: fetch (non-blocking) and cache a header.

        Populates the same instance attribute as _cache_header, so a later synchronous
        get_header() call hits the cache instead of the blocking subprocess.
        """
        if getattr(self, cache_key, None) is not None:
            return getattr(self, cache_key)
        header = await self._get_header_async(gs_path)
        setattr(self, cache_key, header)
        return header

    def _cache_header(self, cache_key: str, gs_path: str) -> list[bytes]:
        """
        Get and cache a header for a file.

        This helper implements the common pattern of caching headers as instance attributes.
        If the header is already cached, returns the cached version. Otherwise, fetches
        it using _get_header and caches it.

        Args:
            cache_key: The key to use for caching (e.g., 'header', 'qtl_header', 'credset_header')
            gs_path: Full gs:// path to the file

        Returns:
            List of header column names as bytes

        Example:
            self._cache_header('header', self._get_all_cs_gs_blob_path())
        """
        cache_attr = f"{cache_key}"
        if hasattr(self, cache_attr) and getattr(self, cache_attr) is not None:
            return getattr(self, cache_attr)

        logger.debug(f"Getting header for {gs_path}")
        header = self._get_header(gs_path)
        setattr(self, cache_attr, header)
        return header

    async def _stream_file(
        self, blob_path: str, chunk_size: int
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream a file from GCloud Storage.

        Args:
            blob_path: Full gs:// path to the file
            chunk_size: Size of chunks to read

        Yields:
            Chunks of file data as bytes

        Raises:
            NotFoundException: If file not found
            aiohttp.client_exceptions.ClientResponseError: For other HTTP errors
        """
        start_time = time.time()
        self._ensure_storage()
        headers = await self.storage._headers()
        url = blob_path.replace("gs://", "https://storage.googleapis.com/")
        try:
            response = await self.session.get(url, headers=headers)
            async with response:
                if response.status == 404:
                    logger.error(f"File not found: {blob_path}")
                    raise ValueError("File not found")
                async for chunk in response.content.iter_chunked(chunk_size):
                    if not chunk:
                        break
                    # remove leading '#' from first chunk if present
                    yield (
                        chunk.replace(b"#", b"", 1) if chunk.startswith(b"#") else chunk
                    )
            logger.info(f"Streamed {blob_path} in {time.time() - start_time} seconds")
        except aiohttp.client_exceptions.ClientResponseError as e:
            logger.error(f"Error streaming {blob_path}: {e}")
            if e.status == 404:
                raise NotFoundException(f"File not found: {e.status}")
            raise e

    async def _get_index(self, file_path: str) -> TabixIndex:
        """Fetch (or reuse) and parse the .tbi index for a bgzf file.

        Indexes are cached in-process; on a cold miss the raw .tbi is read from the
        on-disk tbi cache if present (htslib used the same location), otherwise
        fetched from GCS and persisted there.
        """
        idx = _index_cache.get(file_path)
        if idx is not None:
            return idx
        async with _index_cache_lock:
            idx = _index_cache.get(file_path)
            if idx is not None:
                return idx
            cache_dir = self._get_tbi_cache_dir(file_path)
            tbi_disk = os.path.join(cache_dir, file_path.rsplit("/", 1)[-1] + ".tbi")
            raw = None
            try:
                with open(tbi_disk, "rb") as fh:
                    raw = fh.read()
            except OSError:
                pass
            if raw is None:
                raw = await self._fetch_full(file_path + ".tbi")
                try:
                    with open(tbi_disk, "wb") as fh:
                        fh.write(raw)
                    self._maybe_cleanup_tbi_cache()
                except OSError:
                    pass
            idx = parse_tabix_index(raw)
            _index_cache[file_path] = idx
            return idx

    def _gcs_url(self, gs_path: str) -> str:
        return gs_path.replace("gs://", "https://storage.googleapis.com/")

    async def _fetch_full(self, gs_path: str) -> bytes:
        """GET an entire (small) object, with retry on transient errors."""
        url = self._gcs_url(gs_path)
        last_exc: Exception | None = None
        for attempt in range(_TABIX_MAX_ATTEMPTS):
            try:
                self._ensure_storage()
                async with _get_fetch_semaphore():
                    headers = await self.storage._headers()
                    async with self.session.get(url, headers=headers) as resp:
                        if resp.status == 404:
                            raise NotFoundException(f"File not found: {gs_path}")
                        resp.raise_for_status()
                        return await resp.read()
            except NotFoundException:
                raise
            except Exception as e:  # transient GCS/network errors
                last_exc = e
                if attempt < _TABIX_MAX_ATTEMPTS - 1:
                    await asyncio.sleep(_TABIX_RETRY_BASE_DELAY * (2**attempt))
        raise RuntimeError(f"Failed to fetch {gs_path}: {last_exc}")

    async def _fetch_blocks(
        self, url: str, start: int, last_block_off: int
    ) -> bytes:
        """Stream bgzf blocks from compressed offset ``start`` and stop once the
        block beginning at ``last_block_off`` has been read in full, so only the
        relevant blocks are downloaded. Retries transient errors."""
        target = last_block_off - start  # index of the final block within the buffer
        req_end = last_block_off + _MAX_BGZF_BLOCK
        last_exc: Exception | None = None
        for attempt in range(_TABIX_MAX_ATTEMPTS):
            try:
                self._ensure_storage()
                # hold a global slot only for the network op (not the backoff sleep)
                # so total concurrent GCS sockets stay bounded across all requests
                async with _get_fetch_semaphore():
                    headers = await self.storage._headers()
                    headers = {**headers, "Range": f"bytes={start}-{req_end - 1}"}
                    buf = bytearray()
                    needed: int | None = None
                    async with self.session.get(url, headers=headers) as resp:
                        resp.raise_for_status()
                        async for chunk in resp.content.iter_chunked(_MAX_BGZF_BLOCK):
                            buf += chunk
                            if needed is None and len(buf) >= target + 18:
                                bs = bgzf_block_size(buf, target)
                                if bs is not None:
                                    needed = target + bs
                            if needed is not None and len(buf) >= needed:
                                break
                    return bytes(buf if needed is None else buf[:needed])
            except Exception as e:
                last_exc = e
                if attempt < _TABIX_MAX_ATTEMPTS - 1:
                    await asyncio.sleep(_TABIX_RETRY_BASE_DELAY * (2**attempt))
        raise RuntimeError(f"Failed to fetch byte range of {url}: {last_exc}")

    async def _stream_range(
        self,
        file_path: str,
        chr: list[int],
        start: list[int],
        end: list[int],
        chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream records overlapping one or more regions from a bgzf+tbi file on GCS.

        Reuses the connection-pooled aiohttp session to fetch the relevant compressed
        byte ranges concurrently (instead of serial per-region reads via a tabix
        subprocess), decompresses them with stdlib zlib, and emits the overlapping
        records in coordinate order — matching `tabix -R` output.

        Args:
            file_path: Full gs:// path to the bgzf+tbi-indexed file
            chr: List of chromosome numbers
            start: List of start positions (1-based, inclusive)
            end: List of end positions (1-based, inclusive)
            chunk_size: Unused (kept for interface compatibility)

        Yields:
            TSV record bytes (newline-terminated), in coordinate order
        """

        async def _empty() -> AsyncGenerator[bytes, None]:
            if False:
                yield b""  # unreachable, just to make this an async generator

        if not chr or not start or not end:
            return _empty()

        idx = await self._get_index(file_path)
        self._ensure_storage()

        # build 0-based half-open regions, grouped per tid for overlap filtering
        regions: list[tuple[int, int, int]] = []
        region_intervals: dict[int, list[tuple[int, int]]] = {}
        for c, s, e in zip(chr, start, end):
            tid = idx.tid_for_chrom(c)
            if tid is None:
                continue
            beg0 = max(0, s - 1)
            end_excl = e
            regions.append((tid, beg0, end_excl))
            region_intervals.setdefault(tid, []).append((beg0, end_excl))

        byte_ranges = idx.byte_ranges(regions)
        if not byte_ranges:
            return _empty()

        url = self._gcs_url(file_path)

        # fast path: when every query is a single position and records are point
        # records (begin == end column), a record matches iff its (seqname, pos)
        # bytes are one of the queried keys — pure byte compare, no int parsing
        point_query = idx.col_beg == idx.col_end and all(
            s == e for s, e in zip(start, end)
        )
        query_keys: set[tuple[bytes, bytes]] = set()
        if point_query:
            for c, s in zip(chr, start):
                tid = idx.tid_for_chrom(c)
                if tid is not None:
                    query_keys.add((idx.names[tid], str(s).encode()))

        # small, picklable description of the query so decompress+filter can run in
        # a worker process (CPU-bound; the event loop and other files stay free)
        spec = {
            "meta": idx.meta,
            "seq_col": idx.col_seq - 1,
            "beg_col": idx.col_beg - 1,
            "end_col": idx.col_end - 1,
            "ncols": max(idx.col_seq, idx.col_beg, idx.col_end),
            "preset": idx.preset,
            "zero_based": idx.zero_based,
            "point_query": point_query,
            "query_keys": query_keys,
            "name_to_tid": idx.name_to_tid,
            "region_intervals": region_intervals,
        }

        async def _iterator() -> AsyncGenerator[bytes, None]:
            # fetch every range's blocks; _fetch_blocks bounds total concurrency via
            # the process-wide fetch semaphore, so this never opens more than
            # _GCS_MAX_CONNECTIONS sockets even across overlapping requests
            bufs = await asyncio.gather(
                *(self._fetch_blocks(url, s, lb) for s, lb, _ in byte_ranges)
            )
            items = [(bufs[i], byte_ranges[i][2]) for i in range(len(byte_ranges))]
            # decompress + parse off the event loop and across cores: split the
            # ranges into contiguous sub-batches, one worker process each, so a
            # single large file also uses all cores. Contiguous slices preserve
            # coordinate order when concatenated.
            pool = _get_filter_pool()
            loop = asyncio.get_running_loop()
            n = min(len(items), _FILTER_WORKERS)
            size = -(-len(items) // n)  # ceil division
            sub_batches = [items[k : k + size] for k in range(0, len(items), size)]
            results = await asyncio.gather(
                *(
                    loop.run_in_executor(pool, filter_batch, sb, spec)
                    for sb in sub_batches
                )
            )
            for batch in results:
                for chunk in batch:
                    if chunk:
                        yield chunk

        return _iterator()
