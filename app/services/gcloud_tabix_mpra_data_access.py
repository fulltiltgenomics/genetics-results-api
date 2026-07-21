import logging
from typing import AsyncGenerator
from app.services.data_access_mpra import DataAccessObjectMpra
from app.services.gcloud_tabix_base import GCloudTabixBase
from app.config.mpra import mpra_data

logger = logging.getLogger(__name__)


def _to_seqname(chrom: str) -> str:
    """Normalize a chromosome token to the file's NUMERIC seqname.

    mpra files use numeric seqnames "1".."22","23"(X),"24"(Y),"25"(M/MT) with NO
    "chr" prefix. Accepts any user-supplied form (1, chr1, X, chrX, 23, chr23, Y, M,
    MT) and resolves it to that numeric seqname, so both the region path param and a
    gene-mapping numeric chrom hit the file's index.
    """
    c = str(chrom).strip()
    c = c[3:] if c.lower().startswith("chr") else c
    c = c.upper()
    return {"X": "23", "Y": "24", "M": "25", "MT": "25"}.get(c, c)


class GCloudTabixDataAccessMpra(GCloudTabixBase, DataAccessObjectMpra):
    """GCloud Storage tabix / direct file access implementation for mpra data."""

    def __init__(self, dataset_id: str):
        DataAccessObjectMpra.__init__(self, dataset_id)
        GCloudTabixBase.__init__(self)

        self.dataset_config = [
            c for c in mpra_data if c["dataset_id"] == dataset_id
        ][0]
        self.file = self.dataset_config["file"]
        # header fetched lazily by get_header() and prefetched (non-blocking) by
        # warm() at startup, so construction no longer blocks on tabix -H
        self.header = None

    async def warm(self) -> None:
        """Prefetch the header and .tbi index without blocking the event loop."""
        self.header = await self._cache_header_async("header", self.file)

    def get_header(self) -> list[bytes]:
        """Get the header for the mpra data file."""
        return self._cache_header("header", self.file)

    def get_resource_name(self) -> str:
        """Get the resource name (prepended to each row) for this dataset."""
        return self.dataset_config["resource"]

    def get_version(self) -> str:
        """Get the version for this data access object."""
        return self.dataset_config["version"]

    async def stream_range(
        self, chrom: str, start: int, end: int, chunk_size: int
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream all annotated variants at/within a positional range.

        Reuses GCloudTabixBase._stream_range on the point-indexed file (begin == end
        column): a single-position variant query is start == end == pos (the base's
        point-query fast path applies), a region query is start..end.

        Args:
            chrom: Chromosome (e.g. "chr1", "1", "X")
            start: Range start (1-based, inclusive)
            end: Range end (1-based, inclusive)
            chunk_size: Size of chunks to read

        Returns:
            AsyncGenerator yielding record chunks at/within the range
        """
        seqname = _to_seqname(chrom)

        logger.info(f"Querying mpra for {seqname}:{start}-{end} ({self.dataset_id})")

        return await self._stream_range(
            self.file,
            [seqname],
            [start],
            [end],
            chunk_size,
        )
