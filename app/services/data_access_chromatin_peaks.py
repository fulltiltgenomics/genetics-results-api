from abc import abstractmethod
import logging
from app.config.chromatin_peaks import chromatin_peaks_data
from app.config.sort_keys import create_sort_key, SORT_CONFIG_CHROMATIN_PEAKS
from app.core.streams import chunk_iterator, tsv_line_iterator_chromatin_peaks
from asyncstdlib.heapq import merge
from app.services.base_data_access import (
    BaseFactory,
    BaseDataAccess,
    BaseDataAccessObject,
)
from typing import AsyncGenerator, List
import re

logger = logging.getLogger(__name__)


class DataAccessObjectChromatinPeaks(BaseDataAccessObject):
    """Abstract base class for data access operations for chromatin peaks data."""

    def __init__(self, resource: str):
        super().__init__(resource)
        self.resource = resource

    @abstractmethod
    def get_header(self) -> list[bytes]:
        """Get the header of data files for this resource."""
        pass

    def get_primary_header(self) -> list[bytes]:
        """Get the primary header for this data source (implements BaseDataAccessObject)."""
        return self.get_header()

    @abstractmethod
    def get_resource_name(self) -> str:
        """Get the resource name for this data access object."""
        pass

    @abstractmethod
    def get_version(self) -> str:
        """Get the version for this data access object."""
        pass

    @abstractmethod
    async def stream_range_by_peak_id(
        self,
        peak_id: str,
        chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream data for the genomic region of a specific peak_id.

        Args:
            peak_id: Peak ID in format "chr1-817095-817594"
            chunk_size: Size of chunks to read

        Returns:
            AsyncGenerator yielding chunks from the region
        """
        pass

    @staticmethod
    def parse_peak_id(peak_id: str) -> tuple[str, int, int]:
        """
        Parse a peak_id to extract chromosome, start, and end positions.

        Args:
            peak_id: Peak ID in format "chr1-817095-817594"

        Returns:
            Tuple of (chromosome, start, end)

        Raises:
            ValueError: If peak_id format is invalid
        """
        # Match format: chr1-817095-817594
        match = re.match(r"^(chr[0-9XYM]+)-(\d+)-(\d+)$", peak_id)
        if not match:
            raise ValueError(
                f"Invalid peak_id format: '{peak_id}'. Expected format: 'chr1-817095-817594'"
            )

        chrom = match.group(1)
        start = int(match.group(2))
        end = int(match.group(3))

        return chrom, start, end


class DataAccessFactoryChromatinPeaks(BaseFactory):
    """Factory for creating per-resource data access objects based on configuration."""

    def get_config_entry(self, resource: str) -> dict:
        """Get configuration entry for the resource."""
        try:
            return [c for c in chromatin_peaks_data if c["resource"] == resource][0]
        except IndexError:
            raise ValueError(
                f"Chromatin peaks data access object for resource '{resource}' not found in configuration"
            )

    def get_implementation_class(self, data_source: str) -> type:
        """Get the implementation class for the data source."""
        if data_source == "gcloud":
            from app.services.gcloud_tabix_chromatin_peaks_data_access import (
                GCloudTabixDataAccessChromatinPeaks,
            )

            return GCloudTabixDataAccessChromatinPeaks
        else:
            raise ValueError(
                f"Unknown data source '{data_source}' for chromatin peaks data"
            )


class DataAccessChromatinPeaks(BaseDataAccess[DataAccessObjectChromatinPeaks]):
    """Main data access class that manages per-resource data access objects."""

    def create_factory(self) -> BaseFactory:
        """Return the factory instance for this domain."""
        return DataAccessFactoryChromatinPeaks()

    async def _get_resource_access(
        self, resource: str
    ) -> DataAccessObjectChromatinPeaks:
        """Get or create a data access object for a specific resource."""
        return await super()._get_resource_access(resource, resource)

    async def stream_by_peak_id(
        self,
        peak_id: str,
        resources: List[str],
        in_chunk_size: int,
        out_chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream data for a specific peak_id from multiple resources.

        Args:
            peak_id: Peak ID in format "chr1-817095-817594"
            resources: List of resource names to query
            in_chunk_size: Size of chunks to read from tabix
            out_chunk_size: Size of chunks to write to response

        Returns:
            AsyncGenerator yielding response chunks
        """
        if not resources:
            raise ValueError("At least one resource must be specified")

        accesses = [await self._get_resource_access(resource) for resource in resources]

        line_iterators = [
            tsv_line_iterator_chromatin_peaks(
                await access.stream_range_by_peak_id(peak_id, in_chunk_size),
                peak_id,
                access.get_resource_name(),
                access.get_version(),
            )
            for access in accesses
        ]

        header_with_resources = [b"resource", b"version"] + accesses[0].get_header()
        sort_key_fn = create_sort_key(
            header_with_resources, SORT_CONFIG_CHROMATIN_PEAKS
        )
        merged_iterator = merge(*line_iterators, key=sort_key_fn)

        header_line = b"\t".join(header_with_resources) + b"\n"

        return chunk_iterator(merged_iterator, header_line, out_chunk_size)
