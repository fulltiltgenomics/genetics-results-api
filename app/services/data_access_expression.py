from abc import abstractmethod
from app.config.expression import expression_data, simple_columns
from app.config.sort_keys import create_sort_key, SORT_CONFIG_EXPRESSION
from app.core.streams import chunk_iterator, tsv_line_iterator_simple
from app.services.base_data_access import (
    BaseFactory,
    BaseDataAccess,
    BaseDataAccessObject,
)
from asyncstdlib.heapq import merge
from typing import AsyncGenerator, Literal, List
import logging

logger = logging.getLogger(__name__)


class DataAccessObjectExpression(BaseDataAccessObject):
    """Abstract base class for data access operations for a specific resource and data type."""

    def __init__(self, resource: str):
        super().__init__(resource)
        self.resource = resource

    @abstractmethod
    def get_header(self) -> list[bytes]:
        """Get the header of data files for this resource and data type"""
        pass

    def get_primary_header(self) -> list[bytes]:
        """Get the primary header for this data source (implements BaseDataAccessObject)."""
        return self.get_header()

    @abstractmethod
    async def stream_range(
        self,
        chr: list[int],
        start: list[int],
        end: list[int],
        chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """Stream data for a chromosome range for this resource and data type."""
        pass


class DataAccessFactoryExpression(BaseFactory):
    """Factory for creating per-resource data access objects based on configuration."""

    def get_config_entry(self, resource: str) -> dict:
        """Get configuration entry for the resource."""
        try:
            return [c for c in expression_data if c["resource"] == resource][0]
        except IndexError:
            raise ValueError(
                f"Expression data access object for resource '{resource}' not found in configuration"
            )

    def get_implementation_class(self, data_source: str) -> type:
        """Get the implementation class for the data source."""
        if data_source == "gcloud":
            from app.services.gcloud_tabix_expression_data_access import (
                GCloudTabixDataAccessExpression,
            )

            return GCloudTabixDataAccessExpression
        else:
            raise ValueError(f"Unknown data source '{data_source}' for expression data")


class DataAccessExpression(BaseDataAccess[DataAccessObjectExpression]):
    """Main data access class that manages per-resource data access objects."""

    def create_factory(self) -> BaseFactory:
        """Return the factory instance for this domain."""
        return DataAccessFactoryExpression()

    async def _get_resource_access(
        self, resource: str, data_type: str = "cs"
    ) -> DataAccessObjectExpression:
        """Get or create a data access object for a specific resource and data type."""
        return await super()._get_resource_access((resource, data_type), resource)

    async def stream_range(
        self,
        coords: dict[str, list[dict[Literal["chrom", "gene_start", "gene_end"], int]]],
        resources: List[str],
        in_chunk_size: int,
        out_chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """Stream data for a chromosome range from multiple resources."""

        accesses = [await self._get_resource_access(resource) for resource in resources]
        chunk_iterators = [
            await access.stream_range(
                [pos["chrom"] for pos in coords[access.gencode_version]],
                [pos["gene_start"] for pos in coords[access.gencode_version]],
                [pos["gene_start"] for pos in coords[access.gencode_version]],
                in_chunk_size,
            )
            for access in accesses
        ]
        line_iterators = [
            tsv_line_iterator_simple(iterator, access.get_header(), simple_columns)
            for access, iterator in zip(accesses, chunk_iterators)
        ]
        # Create dynamic sort key based on actual header
        header_with_resources = [b"resource", b"version"] + accesses[0].get_header()
        sort_key_fn = create_sort_key(header_with_resources, SORT_CONFIG_EXPRESSION)
        merged_iterator = merge(*line_iterators, key=sort_key_fn)
        # header_line = b"\t".join(accesses[0].get_header()) + b"\n"
        header_line = (
            b"resource\tversion\t" + b"\t".join(accesses[0].get_header()) + b"\n"
        )

        return chunk_iterator(merged_iterator, header_line, out_chunk_size)
