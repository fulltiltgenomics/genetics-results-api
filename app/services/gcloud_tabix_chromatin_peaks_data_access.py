import logging
from typing import AsyncGenerator
from app.services.data_access_chromatin_peaks import DataAccessObjectChromatinPeaks
from app.services.gcloud_tabix_base import GCloudTabixBase
from app.config.chromatin_peaks import chromatin_peaks_data

logger = logging.getLogger(__name__)


class GCloudTabixDataAccessChromatinPeaks(
    GCloudTabixBase, DataAccessObjectChromatinPeaks
):
    """GCloud Storage tabix / direct file access implementation for chromatin peaks data."""

    def __init__(self, resource: str):
        DataAccessObjectChromatinPeaks.__init__(self, resource)
        GCloudTabixBase.__init__(self)

        self.resource_config = [
            c for c in chromatin_peaks_data if c["resource"] == resource
        ][0]
        self.file = self.resource_config["file"]
        self.header = None
        self.header = self.get_header()

    def get_header(self) -> list[bytes]:
        """Get the header for the chromatin peaks data file."""
        return self._cache_header("header", self.file)

    def get_resource_name(self) -> str:
        """Get the resource name for this data access object."""
        return self.resource_config["resource"]

    def get_version(self) -> str:
        """Get the version for this data access object."""
        return self.resource_config["version"]

    async def stream_range_by_peak_id(
        self, peak_id: str, chunk_size: int
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream data for the genomic region of a specific peak_id.

        This method:
        1. Parses the peak_id to extract chromosome and start position
        2. Uses tabix to query the region

        Args:
            peak_id: Peak ID in format "chr1-817095-817594"
            chunk_size: Size of chunks to read

        Returns:
            AsyncGenerator yielding chunks from the region
        """
        chrom, start, end = self.parse_peak_id(peak_id)

        logger.info(
            f"Querying chromatin peaks for peak_id={peak_id} (region: {chrom}:{start}-{end})"
        )

        return await self._stream_range(
            self.file,
            [chrom],  # chrom is a string like "chr1"
            [start],  # start position
            [end],  # end position
            chunk_size,
        )
