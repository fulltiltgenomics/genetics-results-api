from abc import abstractmethod
import asyncio
import logging
from app.config.chromatin_peaks import chromatin_peaks_data
from app.config.sort_keys import create_sort_key, SORT_CONFIG_CHROMATIN_PEAKS
from app.core.exceptions import NotFoundException
from app.core.streams import (
    chunk_iterator,
    start_iterators,
    tsv_line_iterator_chromatin_peaks,
    tsv_line_iterator_chromatin_peaks_by_gene,
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

    @abstractmethod
    def has_gene_index(self) -> bool:
        """Whether this resource has a gene-indexed copy of the peak-to-gene table."""
        pass

    @abstractmethod
    async def stream_range_by_gene(
        self,
        chrom: list[int],
        start: list[int],
        end: list[int],
        chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream rows of the gene-indexed file whose linked gene overlaps the given loci.

        Args:
            chrom: Gene chromosomes in the API's numeric convention (X=23)
            start: Gene start positions
            end: Gene end positions
            chunk_size: Size of chunks to read

        Returns:
            AsyncGenerator yielding chunks from the gene loci
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

    async def warm_all(self) -> None:
        """Construct and warm (header + .tbi prefetch) every chromatin-peaks data
        access object concurrently, so the first request pays no cold-start cost."""

        async def _warm(resource: str) -> None:
            try:
                access = await self._get_resource_access(resource)
                if hasattr(access, "warm"):
                    await access.warm()
            except Exception as e:
                logger.warning(f"Chromatin-peaks warm failed for {resource}: {e}")

        await asyncio.gather(*(_warm(c["resource"]) for c in chromatin_peaks_data))

    async def stream_by_peak_id(
        self,
        peak_id: str,
        resources: List[str],
        in_chunk_size: int,
        out_chunk_size: int,
        coordinates_lookup: dict[str, tuple[int, int, int]] | None = None,
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream data for a specific peak_id from multiple resources.

        Args:
            peak_id: Peak ID in format "chr1-817095-817594"
            resources: List of resource names to query
            in_chunk_size: Size of chunks to read from tabix
            out_chunk_size: Size of chunks to write to response
            coordinates_lookup: Optional mapping from ENSG ID to (chrom, gene_start, gene_end)

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
                coordinates_lookup=coordinates_lookup,
            )
            for access in accesses
        ]

        header_with_resources = [b"resource", b"version"] + accesses[0].get_header()
        if coordinates_lookup is not None:
            header_with_resources += [b"gene_chrom", b"gene_start", b"gene_end"]
        sort_key_fn = create_sort_key(
            header_with_resources, SORT_CONFIG_CHROMATIN_PEAKS
        )
        merged_iterator = merge(*await start_iterators(line_iterators), key=sort_key_fn)

        header_line = b"\t".join(header_with_resources) + b"\n"

        return chunk_iterator(merged_iterator, header_line, out_chunk_size)

    async def stream_by_gene(
        self,
        gene_ids: set[str],
        loci: list[tuple[int, int, int]],
        resources: List[str],
        in_chunk_size: int,
        out_chunk_size: int,
        coordinates_lookup: dict[str, tuple[int, int, int]] | None = None,
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream the peaks linked to a gene from multiple resources.

        Args:
            gene_ids: ENSG IDs the queried gene resolves to (rows are filtered to these)
            loci: (chrom, start, end) of the gene, one per GENCODE version it is known in
            resources: List of resource names to query
            in_chunk_size: Size of chunks to read from tabix
            out_chunk_size: Size of chunks to write to response
            coordinates_lookup: Optional mapping from ENSG ID to (chrom, gene_start, gene_end)

        Returns:
            AsyncGenerator yielding response chunks, in the same columns as stream_by_peak_id
        """
        if not resources:
            raise ValueError("At least one resource must be specified")
        if not loci:
            raise ValueError("At least one gene locus must be specified")

        accesses = []
        for resource in resources:
            access = await self._get_resource_access(resource)
            if access.has_gene_index():
                accesses.append(access)

        if not accesses:
            raise NotFoundException(
                f"No gene-indexed peak-to-gene data for resources: {resources}"
            )

        gene_id_bytes = {gene_id.encode("utf-8") for gene_id in gene_ids}
        chrom, start, end = (list(values) for values in zip(*loci))

        line_iterators = [
            tsv_line_iterator_chromatin_peaks_by_gene(
                await access.stream_range_by_gene(chrom, start, end, in_chunk_size),
                gene_id_bytes,
                access.get_resource_name(),
                access.get_version(),
                len(access.get_header()),
                coordinates_lookup=coordinates_lookup,
            )
            for access in accesses
        ]

        header_with_resources = [b"resource", b"version"] + accesses[0].get_header()
        if coordinates_lookup is not None:
            header_with_resources += [b"gene_chrom", b"gene_start", b"gene_end"]
        sort_key_fn = create_sort_key(
            header_with_resources, SORT_CONFIG_CHROMATIN_PEAKS
        )
        merged_iterator = merge(*await start_iterators(line_iterators), key=sort_key_fn)

        header_line = b"\t".join(header_with_resources) + b"\n"

        return chunk_iterator(merged_iterator, header_line, out_chunk_size)
