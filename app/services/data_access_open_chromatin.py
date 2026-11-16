import logging
import re
from abc import abstractmethod
from typing import AsyncGenerator, List

from asyncstdlib.heapq import merge

from app.config.open_chromatin import open_chromatin_data
from app.config.sort_keys import SORT_CONFIG_OPEN_CHROMATIN, create_sort_key
from app.core.streams import (
    chunk_iterator,
    start_iterators,
    tsv_line_iterator_prepend_resource,
)
from app.services.base_data_access import (
    BaseDataAccessObject,
    ConfigListDataAccess,
    ConfigListFactory,
    parse_peak_id,
)

logger = logging.getLogger(__name__)

_PEAK_ID = re.compile(r"^(?:chr)?([0-9XYM]+)-(\d+)-(\d+)$", re.IGNORECASE)


class DataAccessObjectOpenChromatin(BaseDataAccessObject):
    """Abstract base class for data access operations for open_chromatin data."""

    def __init__(self, resource: str):
        super().__init__(resource)
        self.resource = resource

    @abstractmethod
    def get_header(self) -> list[bytes]:
        """Get the header of data files for this resource."""
        pass

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

        peak_id is "<numchrom>-<start>-<end>" (e.g. "23-817095-817594"), matching
        the file's numeric chrom convention. A leading "chr" (and the X/Y/M
        spellings) are still tolerated; the returned chrom is normalized to the
        file seqname downstream by to_numeric_seqname.

        Args:
            peak_id: Peak ID in format "23-817095-817594"

        Returns:
            Tuple of (chromosome, start, end)

        Raises:
            ValueError: If peak_id format is invalid
        """
        return parse_peak_id(peak_id, _PEAK_ID, "23-817095-817594")


class DataAccessFactoryOpenChromatin(ConfigListFactory):
    """Factory for creating per-resource data access objects based on configuration."""

    entries = open_chromatin_data
    label = "open chromatin"
    implementations = {"gcloud": "app.services.gcloud_tabix_open_chromatin_data_access:GCloudTabixDataAccessOpenChromatin"}


class DataAccessOpenChromatin(ConfigListDataAccess[DataAccessObjectOpenChromatin]):
    """Main data access class that manages per-resource data access objects."""

    factory_class = DataAccessFactoryOpenChromatin

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
            tsv_line_iterator_prepend_resource(
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
        """Stream peaks overlapping the region defined by a peak_id ("23-817095-817594")."""
        chrom, start, end = DataAccessObjectOpenChromatin.parse_peak_id(peak_id)
        return await self.stream_by_region(
            chrom, start, end, resources, in_chunk_size, out_chunk_size
        )
