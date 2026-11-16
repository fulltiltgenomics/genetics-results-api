import logging
from typing import AsyncGenerator

from app.config.open_chromatin import open_chromatin_data
from app.services.data_access_open_chromatin import DataAccessObjectOpenChromatin
from app.services.gcloud_tabix_base import GCloudTabixSingleFile, to_numeric_seqname

logger = logging.getLogger(__name__)


class GCloudTabixDataAccessOpenChromatin(
    GCloudTabixSingleFile, DataAccessObjectOpenChromatin
):
    """GCloud Storage tabix / direct file access implementation for open_chromatin data."""

    def __init__(self, resource: str):
        DataAccessObjectOpenChromatin.__init__(self, resource)
        self.resource_config = self._init_single_file(open_chromatin_data, "resource", resource)

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
        seqname = to_numeric_seqname(chrom)

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
