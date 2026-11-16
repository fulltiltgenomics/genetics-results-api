import asyncio
import importlib
import logging
import re
from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator, Callable, Generic, Iterable, List, TypeVar

from asyncstdlib.heapq import merge

from app.config.sort_keys import create_sort_key
from app.core.streams import (
    chunk_iterator,
    start_iterators,
    tsv_line_iterator_prepend_resource,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")  # The DAO type


class BaseDataAccessObject(ABC):
    """
    Base class for all data access objects.

    Provides the common initialization pattern shared by all DAOs.
    """

    def __init__(self, identifier: str):
        """
        Initialize the data access object.

        Args:
            identifier: The resource name, dataset name, or other unique identifier
        """
        self.identifier = identifier


def parse_peak_id(peak_id: str, pattern: re.Pattern, example: str) -> tuple[str, int, int]:
    """Split a "<chrom>-<start>-<end>" peak id; `pattern` captures those three groups.

    The spelling of the chromosome is the caller's: each peak file has its own seqname
    convention, so the pattern decides what is accepted and what the chrom group keeps.

    Raises:
        ValueError: If peak_id does not match `pattern`
    """
    match = pattern.match(peak_id)
    if not match:
        raise ValueError(
            f"Invalid peak_id format: '{peak_id}'. Expected format: '{example}'"
        )
    return match.group(1), int(match.group(2)), int(match.group(3))


class BaseFactory(ABC):
    """Base factory for creating data access objects based on configuration."""

    @abstractmethod
    def get_config_entry(self, identifier: str, *args) -> dict:
        """
        Get configuration entry for the identifier.

        Args:
            identifier: The resource name or identifier to look up
            *args: Additional arguments for config lookup

        Returns:
            Configuration dictionary for the resource

        Raises:
            ValueError: If resource not found in configuration
        """
        pass

    # data_source -> "module.path:ClassName", imported on first use rather than at module
    # load: each gcloud_tabix_* module imports its DAO from the module that defines the
    # factory, so a top-level import here would be circular
    implementations: dict[str, str] = {}

    def get_implementation_class(self, data_source: str) -> type:
        """The class to instantiate for `data_source`; ValueError if this factory has none."""
        try:
            module_name, class_name = self.implementations[data_source].split(":")
        except KeyError:
            raise ValueError(
                f"Unknown data source '{data_source}' for {type(self).__name__}"
            ) from None
        return getattr(importlib.import_module(module_name), class_name)

    async def create(self, identifier: str, *args, **kwargs) -> Any:
        """
        Generic create method - handles config lookup and instantiation.

        Args:
            identifier: The resource name or identifier
            *args: Additional arguments for config lookup and class instantiation
            **kwargs: Keyword arguments for class instantiation

        Returns:
            Instance of the appropriate data access implementation
        """
        config = self.get_config_entry(identifier, *args)
        data_source = config.get("data_source", "gcloud")
        impl_class = self.get_implementation_class(data_source)
        return impl_class(identifier, *args, **kwargs)


class ConfigListFactory(BaseFactory):
    """Factory whose identifiers are the `key_field` values of a list of config entries."""

    entries: list[dict] = []
    key_field: str = "resource"
    # names the domain in the warm log as written, and capitalized in the lookup error
    label: str = ""

    def get_config_entry(self, identifier: str) -> dict:
        """Get the configuration entry whose `key_field` is the identifier."""
        try:
            return [c for c in self.entries if c[self.key_field] == identifier][0]
        except IndexError:
            raise ValueError(
                f"{self.label[0].upper()}{self.label[1:]} data access object for "
                f"{self.key_field.removesuffix('_id')} '{identifier}' not found in configuration"
            )


class BaseDataAccess(Generic[T]):
    """Base class for data access orchestration with caching."""

    def __init__(self):
        self._resource_access_objects: dict = {}

    @abstractmethod
    def create_factory(self) -> BaseFactory:
        """
        Return the factory instance for this domain.

        Returns:
            Factory instance that can create data access objects
        """
        pass

    async def _get_or_create(
        self, key: tuple, *factory_args, **factory_kwargs
    ) -> T:
        """
        Get or create a cached data access object.

        Args:
            key: Tuple used as cache key
            *factory_args: Arguments to pass to factory.create()
            **factory_kwargs: Keyword arguments to pass to factory.create()

        Returns:
            Cached or newly created data access object
        """
        if key not in self._resource_access_objects:
            factory = self.create_factory()
            self._resource_access_objects[key] = await factory.create(
                *factory_args, **factory_kwargs
            )
        return self._resource_access_objects[key]

    async def _warm_each(
        self, label: str, get_access: Callable, keys: Iterable[tuple]
    ) -> None:
        """Construct and warm (header + .tbi prefetch) one access object per key,
        concurrently, so the first request pays no cold-start cost. `get_access` is the
        subclass's own getter and each key is its argument tuple."""

        async def warm(key: tuple) -> None:
            try:
                access = await get_access(*key)
                if hasattr(access, "warm"):
                    await access.warm()
            # swallowed by design, not omission: warm_all prefetches, it does not gate.
            # verify_all_data_files() decides reachability (see Warm.ASYNC in the container).
            except Exception as e:
                logger.warning(f"{label}: warm failed for {'/'.join(map(str, key))}: {e}")

        await asyncio.gather(*(warm(k) for k in keys))


class ConfigListDataAccess(BaseDataAccess[T]):
    """Data access over a `ConfigListFactory`: one cached access object per config entry."""

    factory_class: type[ConfigListFactory]

    def create_factory(self) -> BaseFactory:
        """Return the factory instance for this domain."""
        return self.factory_class()

    async def _get_resource_access(self, identifier: str) -> T:
        """Get or create the data access object for one config entry."""
        return await self._get_or_create(identifier, identifier)

    async def warm_all(self) -> None:
        """Construct and warm (header + .tbi prefetch) every configured data access
        object concurrently, so the first request pays no cold-start cost."""
        factory = self.factory_class
        await self._warm_each(
            factory.label,
            self._get_resource_access,
            ((c[factory.key_field],) for c in factory.entries),
        )


class DataAccessObjectPerDataset(BaseDataAccessObject):
    """Abstract base for one point-indexed variant file, keyed by ``dataset_id``.

    Keyed by dataset rather than resource because one resource can ship several files.
    """

    def __init__(self, dataset_id: str):
        super().__init__(dataset_id)
        self.dataset_id = dataset_id

    @abstractmethod
    def get_header(self) -> list[bytes]:
        """Get the header of data files for this dataset."""
        pass

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
        Stream all variants in a positional range.

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


class PerDatasetDataAccess(ConfigListDataAccess[T]):
    """Variant, region and gene reads merged across the datasets of the requested resources.

    The factory's entries carry both ``dataset_id`` (its key) and ``resource``.
    """

    # ref/alt offsets in the file row, before the resource column is prepended
    allele_columns: tuple[int, int]
    sort_config: Any

    def _select_dataset_ids(self, resources: List[str]) -> list[str]:
        """Map requested resources to the dataset_ids they contain (config order)."""
        wanted = set(resources)
        return [
            c["dataset_id"] for c in self.factory_class.entries if c["resource"] in wanted
        ]

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

        accesses = [await self._get_resource_access(ds) for ds in dataset_ids]

        line_iterators = [
            tsv_line_iterator_prepend_resource(
                await access.stream_range(chrom, start, end, in_chunk_size),
                access.get_resource_name(),
                ref,
                alt,
                allele_columns=self.allele_columns,
            )
            for access in accesses
        ]

        header_with_resource = [b"resource"] + accesses[0].get_header()
        sort_key_fn = create_sort_key(header_with_resource, self.sort_config)
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
        """Stream the rows at a variant position (point read at pos).

        With ref/alt given, rows are filtered to the matching allele pair; without
        alleles, every row scored at the position is returned.
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
        """Stream all variants in the positional range [start, end]."""
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
        gcloud subclass normalizes the chrom to the file's seqname. Only the first
        available gencode version's first position is used (a single gene resolves
        to one locus), keeping a single-chromosome range query.
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
