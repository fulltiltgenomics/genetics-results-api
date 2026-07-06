from abc import abstractmethod
import asyncio
import logging
from app.config.open_chromatin import open_chromatin_data
from app.config.sort_keys import create_sort_key, SORT_CONFIG_OPEN_CHROMATIN
from app.core.streams import (
    chunk_iterator,
    start_iterators,
    tsv_line_iterator_open_chromatin,
)
from asyncstdlib.heapq import merge
from app.services.base_data_access import (
    BaseFactory,
    BaseDataAccess,
    BaseDataAccessObject,
)
from typing import AsyncGenerator, List
import re

logger = logging.getLogger(__name__)


class DataAccessObjectOpenChromatin(BaseDataAccessObject):
    """Abstract base class for data access operations for open_chromatin data."""

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
    async def stream_range(
        self,
        chrom: str,
        start: int,
        end: int,
        chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream all peaks overlapping a genomic region.

        A peak matches when its [start, end] interval overlaps [start, end]; a
        single position is queried as start == end.

        Args:
            chrom: Chromosome (e.g. "chr1", "1", "X")
            start: Region start (1-based, inclusive)
            end: Region end (1-based, inclusive)
            chunk_size: Size of chunks to read

        Returns:
            AsyncGenerator yielding chunks of overlapping records
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
        match = re.match(r"^(chr[0-9XYM]+)-(\d+)-(\d+)$", peak_id)
        if not match:
            raise ValueError(
                f"Invalid peak_id format: '{peak_id}'. Expected format: 'chr1-817095-817594'"
            )

        chrom = match.group(1)
        start = int(match.group(2))
        end = int(match.group(3))

        return chrom, start, end


class DataAccessFactoryOpenChromatin(BaseFactory):
    """Factory for creating per-resource data access objects based on configuration."""

    def get_config_entry(self, resource: str) -> dict:
        """Get configuration entry for the resource."""
        try:
            return [c for c in open_chromatin_data if c["resource"] == resource][0]
        except IndexError:
            raise ValueError(
                f"Open chromatin data access object for resource '{resource}' not found in configuration"
            )

    def get_implementation_class(self, data_source: str) -> type:
        """Get the implementation class for the data source."""
        if data_source == "gcloud":
            from app.services.gcloud_tabix_open_chromatin_data_access import (
                GCloudTabixDataAccessOpenChromatin,
            )

            return GCloudTabixDataAccessOpenChromatin
        else:
            raise ValueError(
                f"Unknown data source '{data_source}' for open chromatin data"
            )


class DataAccessOpenChromatin(BaseDataAccess[DataAccessObjectOpenChromatin]):
    """Main data access class that manages per-resource data access objects."""

    def create_factory(self) -> BaseFactory:
        """Return the factory instance for this domain."""
        return DataAccessFactoryOpenChromatin()

    async def _get_resource_access(
        self, resource: str
    ) -> DataAccessObjectOpenChromatin:
        """Get or create a data access object for a specific resource."""
        return await super()._get_resource_access(resource, resource)

    async def warm_all(self) -> None:
        """Construct and warm (header + .tbi prefetch) every open-chromatin data
        access object concurrently, so the first request pays no cold-start cost."""

        async def _warm(resource: str) -> None:
            try:
                access = await self._get_resource_access(resource)
                if hasattr(access, "warm"):
                    await access.warm()
            except Exception as e:
                logger.warning(f"Open-chromatin warm failed for {resource}: {e}")

        await asyncio.gather(*(_warm(c["resource"]) for c in open_chromatin_data))

    async def stream_by_region(
        self,
        chrom: str,
        start: int,
        end: int,
        resources: List[str],
        in_chunk_size: int,
        out_chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream all peaks overlapping a genomic region across multiple resources.

        Args:
            chrom: Chromosome (e.g. "chr1", "1", "X")
            start: Region start (1-based, inclusive)
            end: Region end (1-based, inclusive)
            resources: List of resource names to query
            in_chunk_size: Size of chunks to read from tabix
            out_chunk_size: Size of chunks to write to response

        Returns:
            AsyncGenerator yielding response chunks in coordinate order
        """
        if not resources:
            raise ValueError("At least one resource must be specified")

        accesses = [await self._get_resource_access(resource) for resource in resources]

        line_iterators = [
            tsv_line_iterator_open_chromatin(
                await access.stream_range(chrom, start, end, in_chunk_size),
                access.get_resource_name(),
            )
            for access in accesses
        ]

        header_with_resource = [b"resource"] + accesses[0].get_header()
        sort_key_fn = create_sort_key(header_with_resource, SORT_CONFIG_OPEN_CHROMATIN)
        merged_iterator = merge(*await start_iterators(line_iterators), key=sort_key_fn)

        header_line = b"\t".join(header_with_resource) + b"\n"

        return chunk_iterator(merged_iterator, header_line, out_chunk_size)

    async def stream_by_variant(
        self,
        chrom: str,
        pos: int,
        resources: List[str],
        in_chunk_size: int,
        out_chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """Stream peaks overlapping a variant position (point overlap: start == end == pos)."""
        return await self.stream_by_region(
            chrom, pos, pos, resources, in_chunk_size, out_chunk_size
        )

    async def stream_by_peak_id(
        self,
        peak_id: str,
        resources: List[str],
        in_chunk_size: int,
        out_chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """Stream peaks overlapping the region defined by a peak_id ("chr1-817095-817594")."""
        chrom, start, end = DataAccessObjectOpenChromatin.parse_peak_id(peak_id)
        return await self.stream_by_region(
            chrom, start, end, resources, in_chunk_size, out_chunk_size
        )
