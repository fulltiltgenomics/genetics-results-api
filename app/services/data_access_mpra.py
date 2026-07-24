from abc import abstractmethod
import asyncio
import logging
from app.config.mpra import mpra_data
from app.config.sort_keys import create_sort_key, SORT_CONFIG_MPRA
from app.core.streams import (
    chunk_iterator,
    start_iterators,
    tsv_line_iterator_mpra,
)
from asyncstdlib.heapq import merge
from app.services.base_data_access import (
    BaseFactory,
    BaseDataAccess,
    BaseDataAccessObject,
)
from typing import AsyncGenerator, List

logger = logging.getLogger(__name__)


class DataAccessObjectMpra(BaseDataAccessObject):
    """Abstract base class for data access operations for mpra data.

    Keyed by ``dataset_id`` (not resource): the Siraj resource ships a single LONG
    tabix file (one row per variant x cell_line), so there is one per-dataset entry.
    """

    def __init__(self, dataset_id: str):
        super().__init__(dataset_id)
        self.dataset_id = dataset_id

    @abstractmethod
    def get_header(self) -> list[bytes]:
        """Get the header of data files for this dataset."""
        pass

    def get_primary_header(self) -> list[bytes]:
        """Get the primary header for this data source (implements BaseDataAccessObject)."""
        return self.get_header()

    @abstractmethod
    def get_resource_name(self) -> str:
        """Get the resource name (prepended to each row) for this dataset."""
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
        Stream all annotated variants in a positional range.

        Files are point-indexed (begin == end == pos), so a single variant position is
        queried as start == end == pos and a region as start..end.

        Args:
            chrom: Chromosome (e.g. "chr1", "1", "X")
            start: Range start (1-based, inclusive)
            end: Range end (1-based, inclusive)
            chunk_size: Size of chunks to read

        Returns:
            AsyncGenerator yielding chunks of records at/within the range
        """
        pass


class DataAccessFactoryMpra(BaseFactory):
    """Factory for creating per-dataset data access objects based on configuration."""

    def get_config_entry(self, dataset_id: str) -> dict:
        """Get configuration entry for the dataset."""
        try:
            return [c for c in mpra_data if c["dataset_id"] == dataset_id][0]
        except IndexError:
            raise ValueError(
                f"MPRA data access object for dataset '{dataset_id}' not found in configuration"
            )

    def get_implementation_class(self, data_source: str) -> type:
        """Get the implementation class for the data source."""
        if data_source == "gcloud":
            from app.services.gcloud_tabix_mpra_data_access import (
                GCloudTabixDataAccessMpra,
            )

            return GCloudTabixDataAccessMpra
        else:
            raise ValueError(f"Unknown data source '{data_source}' for mpra data")


class DataAccessMpra(BaseDataAccess[DataAccessObjectMpra]):
    """Main data access class that manages per-dataset data access objects."""

    def create_factory(self) -> BaseFactory:
        """Return the factory instance for this domain."""
        return DataAccessFactoryMpra()

    async def _get_dataset_access(self, dataset_id: str) -> DataAccessObjectMpra:
        """Get or create a data access object for a specific dataset."""
        return await super()._get_resource_access(dataset_id, dataset_id)

    @staticmethod
    def _select_dataset_ids(resources: List[str]) -> list[str]:
        """Map requested resources to the dataset_ids they contain (config order)."""
        wanted = set(resources)
        return [c["dataset_id"] for c in mpra_data if c["resource"] in wanted]

    async def warm_all(self) -> None:
        """Construct and warm (header + .tbi prefetch) every mpra data access object
        concurrently, so the first request pays no cold-start cost."""

        async def _warm(dataset_id: str) -> None:
            try:
                access = await self._get_dataset_access(dataset_id)
                if hasattr(access, "warm"):
                    await access.warm()
            except Exception as e:
                logger.warning(f"MPRA warm failed for {dataset_id}: {e}")

        await asyncio.gather(*(_warm(c["dataset_id"]) for c in mpra_data))

    async def _stream_datasets(
        self,
        dataset_ids: List[str],
        chrom: str,
        start: int,
        end: int,
        in_chunk_size: int,
        out_chunk_size: int,
        ref: bytes | None = None,
        alt: bytes | None = None,
    ) -> AsyncGenerator[bytes, None]:
        """Merge point/range reads across the selected datasets, prepending resource."""
        if not dataset_ids:
            raise ValueError("At least one dataset must be resolved from the resources")

        accesses = [await self._get_dataset_access(ds) for ds in dataset_ids]

        line_iterators = [
            tsv_line_iterator_mpra(
                await access.stream_range(chrom, start, end, in_chunk_size),
                access.get_resource_name(),
                ref,
                alt,
            )
            for access in accesses
        ]

        header_with_resource = [b"resource"] + accesses[0].get_header()
        sort_key_fn = create_sort_key(header_with_resource, SORT_CONFIG_MPRA)
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
        ref: str | None = None,
        alt: str | None = None,
    ) -> AsyncGenerator[bytes, None]:
        """Stream MPRA annotations at a variant position (point read at pos).

        With ref/alt given, rows are filtered to the matching allele pair; without
        alleles, every cell_line row scored at the position is returned.
        """
        dataset_ids = self._select_dataset_ids(resources)
        return await self._stream_datasets(
            dataset_ids,
            chrom,
            pos,
            pos,
            in_chunk_size,
            out_chunk_size,
            ref.encode("utf-8") if ref is not None else None,
            alt.encode("utf-8") if alt is not None else None,
        )

    async def stream_by_region(
        self,
        chrom: str,
        start: int,
        end: int,
        resources: List[str],
        in_chunk_size: int,
        out_chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """Stream all annotated variants in the positional range [start, end]."""
        dataset_ids = self._select_dataset_ids(resources)
        return await self._stream_datasets(
            dataset_ids, chrom, start, end, in_chunk_size, out_chunk_size
        )

    async def stream_by_gene(
        self,
        gene: str,
        gene_name_mapping,
        resources: List[str],
        window: int,
        in_chunk_size: int,
        out_chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """Resolve a gene to its coordinates (± window) and range-read that region.

        Uses the shared gene-coordinate mapping (numeric chrom, no "chr" prefix,
        chrX=23) exactly like the credible_sets / expression by-gene paths; the
        gcloud subclass normalizes the chrom to the file's numeric seqname. Only the
        first available gencode version's first position is used (a single gene
        resolves to one locus), keeping a single-chromosome range query.
        """
        coords_by_version = gene_name_mapping.get_coordinates_by_gene_name(gene)
        position = None
        for version in coords_by_version:
            entries = coords_by_version[version]
            if entries:
                position = entries[0]
                break
        if position is None:
            from app.core.exceptions import GeneNotFoundException

            raise GeneNotFoundException(f"No coordinates found for gene {gene}")

        chrom = str(position["chrom"])
        start = int(position["gene_start"]) - window
        end = int(position["gene_end"]) + window
        if start < 1:
            start = 1

        dataset_ids = self._select_dataset_ids(resources)
        return await self._stream_datasets(
            dataset_ids, chrom, start, end, in_chunk_size, out_chunk_size
        )
