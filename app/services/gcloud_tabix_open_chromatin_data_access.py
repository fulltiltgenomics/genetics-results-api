import logging
from typing import AsyncGenerator
from app.services.data_access_open_chromatin import DataAccessObjectOpenChromatin
from app.services.gcloud_tabix_base import GCloudTabixBase
from app.config.open_chromatin import open_chromatin_data

logger = logging.getLogger(__name__)


def _to_seqname(chrom: str) -> str:
    """Normalize a chromosome token to the file's "chr"-prefixed seqname.

    open_chromatin files use "chr1".."chr22","chrX","chrY". Accepts bare or
    "chr"-prefixed names and the numeric X/Y/MT spellings (23/24/25) so both the
    region path param and a parsed variant chromosome resolve to a file seqname.
    """
    c = str(chrom).strip()
    c = c[3:] if c.lower().startswith("chr") else c
    c = c.upper()
    c = {"23": "X", "24": "Y", "25": "MT", "26": "MT"}.get(c, c)
    return "chr" + c


class GCloudTabixDataAccessOpenChromatin(
    GCloudTabixBase, DataAccessObjectOpenChromatin
):
    """GCloud Storage tabix / direct file access implementation for open_chromatin data."""

    def __init__(self, resource: str):
        DataAccessObjectOpenChromatin.__init__(self, resource)
        GCloudTabixBase.__init__(self)

        self.resource_config = [
            c for c in open_chromatin_data if c["resource"] == resource
        ][0]
        self.file = self.resource_config["file"]
        # header fetched lazily by get_header() and prefetched (non-blocking) by
        # warm() at startup, so construction no longer blocks on tabix -H
        self.header = None

    async def warm(self) -> None:
        """Prefetch the header and .tbi index without blocking the event loop."""
        self.header = await self._cache_header_async("header", self.file)

    def get_header(self) -> list[bytes]:
        """Get the header for the open chromatin data file."""
        return self._cache_header("header", self.file)

    def get_resource_name(self) -> str:
        """Get the resource name for this data access object."""
        return self.resource_config["resource"]

    def get_version(self) -> str:
        """Get the version for this data access object."""
        return self.resource_config["version"]

    async def stream_range(
        self, chrom: str, start: int, end: int, chunk_size: int
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream all peaks overlapping a genomic region.

        Reuses GCloudTabixBase._stream_range, which returns every record whose
        [start, end] interval overlaps [start, end] — so a variant point overlap is
        just start == end == pos.

        Args:
            chrom: Chromosome (e.g. "chr1", "1", "X")
            start: Region start (1-based, inclusive)
            end: Region end (1-based, inclusive)
            chunk_size: Size of chunks to read

        Returns:
            AsyncGenerator yielding overlapping record chunks
        """
        seqname = _to_seqname(chrom)

        logger.info(
            f"Querying open chromatin for region {seqname}:{start}-{end} ({self.resource})"
        )

        return await self._stream_range(
            self.file,
            [seqname],
            [start],
            [end],
            chunk_size,
        )
