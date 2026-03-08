import logging
import threading
import time
import asyncio
import aiohttp
import aiohttp.client_exceptions
from typing import AsyncGenerator
from asyncio.unix_events import subprocess
from app.core.exceptions import NotFoundException
from gcloud.aio.storage import Storage
from google.auth import default
from google.auth.transport.requests import Request
import os
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# module-level GCS token management shared by all GCloudTabixBase instances
_gcs_credentials = None
_gcs_token_lock = threading.Lock()


def ensure_gcs_token() -> None:
    """Ensure GCS OAuth token env var is valid for tabix subprocess calls."""
    global _gcs_credentials
    if _gcs_credentials is None:
        _gcs_credentials, _ = default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        _gcs_credentials.refresh(Request())
        os.environ["GCS_OAUTH_TOKEN"] = _gcs_credentials.token
        logger.info(f"GCS_OAUTH_TOKEN set, expires at: {_gcs_credentials.expiry}")
        return

    with _gcs_token_lock:
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
        self._init_storage()
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

    def _init_storage(self):
        """Initialize the storage client and session."""
        # short keepalive prevents stale connections after inactivity,
        # which cause TimeoutError in gcloud-aio-auth's acquire_access_token
        connector = aiohttp.TCPConnector(keepalive_timeout=15)
        self.session = aiohttp.ClientSession(connector=connector)
        self.storage = Storage(session=self.session)

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
        ensure_gcs_token()

        try:
            process = subprocess.run(
                ["tabix", "-H", gs_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                cwd="/tmp/tbi_cache",
            )
        except subprocess.CalledProcessError as e:
            stderr = e.stderr.decode()
            logger.error(f"Tabix failed: {stderr}")
            raise RuntimeError(f"Getting file header failed")

        header_line = process.stdout.strip()
        # remove leading '#' from header columns
        header = [h[1:] if h.startswith(b"#") else h for h in header_line.split(b"\t")]
        logger.info(f"Header: {header}")
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
        headers = await self.storage._headers()
        url = blob_path.replace("gs://", "https://storage.googleapis.com/")
        try:
            response = await self.session.get(url, headers=headers)
            async with response:
                if response.status == 404:
                    logger.error(f"File not found: {blob_path}")
                    raise ValueError(f"File not found")
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

    async def _stream_range(
        self,
        file_path: str,
        chr: list[int],
        start: list[int],
        end: list[int],
        chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream data from a tabix-indexed file for chromosome range(s).

        Args:
            file_path: Full gs:// path to the tabix-indexed file
            chr: List of chromosome numbers
            start: List of start positions (1-based)
            end: List of end positions (inclusive)
            chunk_size: Size of chunks to read from tabix

        Yields:
            Chunks of tabix output as bytes

        Raises:
            RuntimeError: If tabix operation fails
        """
        # if no ranges provided, return an empty async generator
        if not chr or not start or not end:

            async def _empty() -> AsyncGenerator[bytes, None]:
                if False:
                    yield b""  # unreachable, just to make this an async generator

            return _empty()

        # coordinates for tabix are 1-based, prevent 0
        start = [max(1, s) for s in start]

        ensure_gcs_token()
        process = await asyncio.create_subprocess_exec(
            "tabix",
            "-R",
            "/dev/stdin",
            file_path,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd="/tmp/tbi_cache",
        )
        process.stdin.write(
            "\n".join(f"{c}\t{s}\t{e}" for c, s, e in zip(chr, start, end)).encode()
        )
        process.stdin.close()

        async def tabix_iterator() -> AsyncGenerator[bytes, None]:
            try:
                while True:
                    chunk = await process.stdout.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk

                await process.wait()
                if process.returncode != 0:
                    stderr = await process.stderr.read()
                    raise RuntimeError(
                        f"Tabix failed with return code {process.returncode}: {stderr.decode()}"
                    )

            finally:
                if process.returncode is None:
                    process.terminate()
                    await process.wait()

        return tabix_iterator()
