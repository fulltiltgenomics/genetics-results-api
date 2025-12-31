from abc import abstractmethod
import json

import fsspec
from app.config.credible_sets import (
    data_file_by_id as cs_data_file_by_id,
    variant_columns as cs_variant_columns,
    qtl_columns as cs_qtl_columns,
)
from app.config.exome_results import (
    exome_data_file_by_id,
    variant_columns as exome_variant_columns,
)
from app.config.gene_based_results import gene_based_data_file_by_id
from app.config.sort_keys import (
    create_sort_key,
    SORT_CONFIG_CS,
    SORT_CONFIG_CS_QTL,
)
from app.core.exceptions import NotFoundException
from app.core.streams import chunk_iterator, tsv_line_iterator, tsv_line_iterator_qtl
from app.core.variant import Variant
from app.services.base_data_access import (
    BaseFactory,
    BaseDataAccess,
    BaseDataAccessObject,
)
from app.services.metadata_harmonizer import MetadataHarmonizer
from asyncstdlib.heapq import merge
from typing import Any, AsyncGenerator, Literal, List
import logging

# merge all config dicts
data_file_by_id = {**cs_data_file_by_id, **exome_data_file_by_id, **gene_based_data_file_by_id}

logger = logging.getLogger(__name__)


class DataAccessObject(BaseDataAccessObject):
    """Abstract base class for data access operations for a specific resource and data type."""

    def __init__(self, resource: str, data_type: Literal["cs", "assoc"]):
        super().__init__(resource)
        self.resource = resource
        self.data_type = data_type

    @abstractmethod
    def get_header(self, qtl: bool = False) -> list[bytes]:
        """Get the header of data files for this resource and data type"""
        pass

    def get_primary_header(self) -> list[bytes]:
        """Get the primary header for this data source (implements BaseDataAccessObject)."""
        return self.get_header(qtl=False)

    @abstractmethod
    async def check_phenotype_exists(
        self, phenotype: str, interval: Literal[95, 99] | None = None
    ) -> bool:
        """Check if a phenotype's or study's data exists in the appropriate data source for the resource."""
        pass

    @abstractmethod
    async def stream_phenotype(
        self,
        phenotype: str,
        interval: Literal[95, 99] | None,
        chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """Stream phenotype data from the data source for this resource and data type."""
        pass

    @abstractmethod
    async def json_phenotype(
        self,
        phenotype: str,
        interval: Literal[95, 99] | None,
        header_schema: dict[str, type],
        data_type: str,
        chunk_size: int,
    ) -> list[dict[str, Any]]:
        """Get phenotype data as JSON response for this resource and data type."""
        pass

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

    @abstractmethod
    async def stream_qtl_gene_range(
        self,
        chr: list[int],
        pos: list[int],
        chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """Stream data for a QTL gene range for this resource and data type."""
        pass

    @abstractmethod
    async def has_qtl_gene_data(self) -> bool:
        """Check if the resource has QTL gene data."""
        pass


class DataAccessFactory(BaseFactory):
    """Factory for creating per-resource data access objects based on configuration."""

    def get_config_entry(self, data_file_id: str, data_type: str = "cs") -> dict:
        """Get configuration entry for the data file."""
        if data_file_id not in data_file_by_id:
            raise ValueError(f"Data file '{data_file_id}' not found in configuration")
        return data_file_by_id[data_file_id]

    def get_implementation_class(self, data_source: str) -> type:
        """Get the implementation class for the data source."""
        if data_source == "gcloud":
            from app.services.gcloud_tabix_data_access import GCloudTabixDataAccess

            return GCloudTabixDataAccess
        else:
            raise ValueError(f"Unknown data source '{data_source}'")


class DataAccess(BaseDataAccess[DataAccessObject]):
    """Main data access class that manages per-resource data access objects."""

    def create_factory(self) -> BaseFactory:
        """Return the factory instance for this domain."""
        return DataAccessFactory()

    async def _get_resource_access(
        self, resource: str, data_type: str = "cs"
    ) -> DataAccessObject:
        """Get or create a data access object for a specific resource and data type."""
        return await super()._get_resource_access(
            (resource, data_type), resource, data_type
        )

    async def check_phenotype_exists(
        self, resource: str, phenotype: str, interval: Literal[95, 99] | None = None, data_type: str = "cs"
    ) -> bool:
        """Check if a phenotype's or study's data exists in any data file for the resource."""
        from app.services.config_util import get_data_file_ids_for_resource

        data_file_ids = get_data_file_ids_for_resource(resource)
        if not data_file_ids:
            # fallback to treating resource as a data file ID
            data_file_ids = [resource]

        # check each data file until we find the phenotype
        for data_file_id in data_file_ids:
            try:
                access = await self._get_resource_access(data_file_id, data_type)
                if await access.check_phenotype_exists(phenotype, interval):
                    return True
            except ValueError:
                # data file doesn't support this data type, skip it
                continue
            except Exception:
                # file doesn't exist or other error, continue to next
                continue

        return False

    # TODO should this be part of DAO?
    def get_resource_metadata(self, resource: str) -> dict[str, Any]:
        """Get the metadata for a resource, merging from all data files if multiple."""
        from app.services.config_util import get_data_file_ids_for_resource

        data_file_ids = get_data_file_ids_for_resource(resource)
        if not data_file_ids:
            # fallback to treating resource as a data file ID
            data_file_ids = [resource]

        all_meta = []
        for data_file_id in data_file_ids:
            if data_file_id not in data_file_by_id:
                continue
            df = data_file_by_id[data_file_id]

            # check if metadata section exists and has metadata_file
            metadata_config = df.get("metadata", {})
            metadata_file = metadata_config.get("metadata_file")
            if not metadata_file:
                continue

            compression = (
                "gzip"
                if metadata_file.endswith(".gz")
                or metadata_file.endswith(".bgz")
                else None
            )
            with fsspec.open(metadata_file, "rt", compression=compression) as f:
                if metadata_file.endswith(".json") or metadata_file.endswith(".json.gz"):
                    meta = json.load(f)
                    if isinstance(meta, list):
                        all_meta.extend(meta)
                    else:
                        all_meta.append(meta)
                else:
                    header = f.readline().strip().split("\t")
                    for line in f:
                        s = line.strip().split("\t")
                        all_meta.append(dict(zip(header, s)))

        return all_meta

    def get_harmonized_metadata(self, resource: str) -> list[dict[str, Any]]:
        """Get harmonized metadata for a resource in unified format."""
        from app.services.config_util import get_data_file_ids_for_resource

        # get all data file IDs for the resource
        data_file_ids = get_data_file_ids_for_resource(resource)
        if not data_file_ids:
            data_file_ids = [resource]

        all_harmonized = []
        harmonizer = MetadataHarmonizer()

        for data_file_id in data_file_ids:
            if data_file_id not in data_file_by_id:
                continue
            df_config = data_file_by_id[data_file_id]

            # check if metadata section exists and has metadata_file
            metadata_config = df_config.get("metadata", {})
            metadata_file = metadata_config.get("metadata_file")
            if not metadata_file:
                continue

            # read raw metadata for this data file
            compression = (
                "gzip"
                if metadata_file.endswith(".gz")
                or metadata_file.endswith(".bgz")
                else None
            )
            raw_metadata = []
            with fsspec.open(metadata_file, "rt", compression=compression) as f:
                if metadata_file.endswith(".json") or metadata_file.endswith(".json.gz"):
                    meta = json.load(f)
                    if isinstance(meta, list):
                        raw_metadata = meta
                    else:
                        raw_metadata = [meta]
                else:
                    header = f.readline().strip().split("\t")
                    for line in f:
                        s = line.strip().split("\t")
                        raw_metadata.append(dict(zip(header, s)))

            # harmonize with config
            if raw_metadata:
                harmonized = harmonizer.harmonize_metadata(
                    resource, raw_metadata, df_config
                )
                all_harmonized.extend(harmonized)

        # convert to dicts for JSON serialization
        return [item.to_dict() for item in all_harmonized]

    async def stream_phenotype(
        self,
        resource: str,
        phenotype: str,
        interval: Literal[95, 99] | None,
        chunk_size: int,
        data_type: str = "cs",
    ) -> AsyncGenerator[bytes, None]:
        """Stream data from all data files for the resource that have this phenotype."""
        from app.services.config_util import get_data_file_ids_for_resource

        data_file_ids = get_data_file_ids_for_resource(resource)
        if not data_file_ids:
            # fallback to treating resource as a data file ID
            data_file_ids = [resource]

        # find all data files that have this phenotype
        accesses_with_data = []
        for data_file_id in data_file_ids:
            try:
                access = await self._get_resource_access(data_file_id, data_type)
                if await access.check_phenotype_exists(phenotype, interval):
                    accesses_with_data.append((data_file_id, access))
            except ValueError:
                # data file doesn't support this data type, skip it
                continue
            except Exception:
                continue

        if not accesses_with_data:
            raise NotFoundException(
                f"Phenotype {phenotype} not found in resource {resource}"
            )

        # if only one data file has the phenotype, stream from it directly
        if len(accesses_with_data) == 1:
            _, access = accesses_with_data[0]
            return await access.stream_phenotype(phenotype, interval, chunk_size)

        # if multiple data files have the phenotype, merge streams
        async def merge_streams():
            first = True
            for data_file_id, access in accesses_with_data:
                stream = await access.stream_phenotype(phenotype, interval, chunk_size)
                async for chunk in stream:
                    # skip header for all but first stream
                    if not first and b"\t" in chunk:
                        lines = chunk.split(b"\n")
                        if lines and b"\t" in lines[0]:
                            # skip first line (header)
                            chunk = b"\n".join(lines[1:])
                    yield chunk
                first = False

        return merge_streams()

    async def json_phenotype(
        self,
        resource: str,
        phenotype: str,
        interval: Literal[95, 99] | None,
        header_schema: dict[str, type],
        data_type: str = "cs",
        chunk_size: int = 1024 * 1024,
    ) -> list[dict[str, Any]]:
        """Get JSON data from all data files for the resource that have this phenotype."""
        from app.services.config_util import get_data_file_ids_for_resource

        data_file_ids = get_data_file_ids_for_resource(resource)
        if not data_file_ids:
            # fallback to treating resource as a data file ID
            data_file_ids = [resource]

        # collect results from all data files that have this phenotype
        all_results = []
        for data_file_id in data_file_ids:
            try:
                access = await self._get_resource_access(data_file_id, data_type)
                if await access.check_phenotype_exists(phenotype, interval):
                    results = await access.json_phenotype(
                        phenotype, interval, header_schema, data_type, chunk_size
                    )
                    all_results.extend(results)
            except ValueError:
                # data file doesn't support this data type, skip it
                continue
            except Exception:
                continue

        if not all_results:
            raise NotFoundException(
                f"Phenotype {phenotype} not found in resource {resource}"
            )

        return all_results

    async def stream_range(
        self,
        chr,
        start,
        end,
        resources: List[str],
        data_type: Literal["cs", "assoc"],
        in_chunk_size: int,
        out_chunk_size: int,
        variant: Variant | None = None,
    ) -> AsyncGenerator[bytes, None]:
        """Stream data for a chromosome range from multiple resources. If variant is provided, limit data to the variant."""
        from app.services.config_util import get_data_file_ids_for_resource

        # expand resource names to data file IDs
        data_file_ids = []
        for resource in resources:
            ids = get_data_file_ids_for_resource(resource)
            if ids:
                data_file_ids.extend(ids)
            else:
                # fallback: treat as data file ID directly
                data_file_ids.append(resource)

        # get access objects and chunk iterators, skipping any that fail
        accesses_and_iterators = []
        for data_file_id in data_file_ids:
            try:
                access = await self._get_resource_access(data_file_id, data_type)
                chunk_iterator_stream = await access.stream_range(
                    [chr], [start], [end], in_chunk_size
                )
                accesses_and_iterators.append((access, chunk_iterator_stream))
            except ValueError:
                # data file doesn't support this data type, expected - skip silently
                continue
            except Exception as e:
                logger.warning(f"Skipping data file {data_file_id} due to error: {e}")
                continue

        if not accesses_and_iterators:
            raise NotFoundException(
                f"No data files available for resources: {resources}"
            )

        accesses, chunk_iterators = zip(*accesses_and_iterators)

        # select column config based on data type
        columns = exome_variant_columns if data_type == "exome" else cs_variant_columns

        line_iterators = [
            tsv_line_iterator(iterator, access.get_header(), columns, variant)
            for access, iterator in zip(accesses, chunk_iterators)
        ]
        header_with_resources = [b"resource", b"version"] + accesses[0].get_header()
        sort_key_fn = create_sort_key(header_with_resources, SORT_CONFIG_CS)
        merged_iterator = merge(*line_iterators, key=sort_key_fn)
        header_line = (
            b"resource\tversion\t" + b"\t".join(accesses[0].get_header()) + b"\n"
        )

        return chunk_iterator(merged_iterator, header_line, out_chunk_size)

    async def stream_range_by_coords(
        self,
        coords: dict[str, list[dict[Literal["chrom", "gene_start", "gene_end"], int]]],
        resources: List[str],
        data_type: Literal["cs", "assoc"],
        in_chunk_size: int,
        out_chunk_size: int,
        variant: Variant | None = None,
    ) -> AsyncGenerator[bytes, None]:
        """Stream data for a chromosome range from multiple resources. If variant is provided, limit data to the variant."""
        from app.services.config_util import get_data_file_ids_for_resource

        # expand resource names to data file IDs
        data_file_ids = []
        for resource in resources:
            ids = get_data_file_ids_for_resource(resource)
            if ids:
                data_file_ids.extend(ids)
            else:
                # fallback: treat as data file ID directly
                data_file_ids.append(resource)

        # create access objects, skipping data files that don't support this data type
        accesses = []
        for data_file_id in data_file_ids:
            try:
                access = await self._get_resource_access(data_file_id, data_type)
                accesses.append(access)
            except ValueError:
                # data file doesn't support this data type, expected - skip silently
                continue

        chunk_iterators = [
            await access.stream_range(
                [pos["chrom"] for pos in coords[access.gencode_version]],
                [pos["gene_start"] for pos in coords[access.gencode_version]],
                [pos["gene_end"] for pos in coords[access.gencode_version]],
                in_chunk_size,
            )
            for access in accesses
        ]

        # select column config based on data type
        columns = exome_variant_columns if data_type == "exome" else cs_variant_columns

        line_iterators = [
            tsv_line_iterator(iterator, access.get_header(), columns, variant)
            for access, iterator in zip(accesses, chunk_iterators)
        ]
        header_with_resources = [b"resource", b"version"] + accesses[0].get_header()
        sort_key_fn = create_sort_key(header_with_resources, SORT_CONFIG_CS)
        merged_iterator = merge(*line_iterators, key=sort_key_fn)
        # header_line = b"\t".join(accesses[0].get_header()) + b"\n"
        header_line = (
            b"resource\tversion\t" + b"\t".join(accesses[0].get_header()) + b"\n"
        )

        return chunk_iterator(merged_iterator, header_line, out_chunk_size)

    async def stream_qtl_gene(
        self,
        coords: dict[str, list[dict[Literal["chrom", "gene_start", "gene_end"], int]]],
        resources: List[str],
        data_type: Literal["cs", "assoc"],
        in_chunk_size: int,
        out_chunk_size: int,
        interval: Literal[95, 99],
    ) -> AsyncGenerator[bytes, None]:
        """Stream data for a QTL gene from multiple resources."""
        from app.services.config_util import get_data_file_ids_for_resource

        # expand resource names to data file IDs
        data_file_ids = []
        for resource in resources:
            ids = get_data_file_ids_for_resource(resource)
            if ids:
                data_file_ids.extend(ids)
            else:
                # fallback: treat as data file ID directly
                data_file_ids.append(resource)

        # create access objects, skipping data files that don't support this data type
        accesses = []
        for data_file_id in data_file_ids:
            try:
                access = await self._get_resource_access(data_file_id, data_type)
                accesses.append(access)
            except ValueError:
                # data file doesn't support this data type, expected - skip silently
                continue

        # limit to resources that have QTL gene data and there are coordinates for the corresponding gencode version
        accesses = [
            access
            for access in accesses
            if access.has_qtl_gene_data()
            and access.gencode_version in coords.keys()
            and len(coords[access.gencode_version]) > 0
        ]

        if len(accesses) == 0:
            raise NotFoundException(
                f"No QTL gene data found for resources: {resources}"
            )

        chunk_iterators = [
            await access.stream_qtl_gene_range(
                [pos["chrom"] for pos in coords[access.gencode_version]],
                [pos["gene_start"] for pos in coords[access.gencode_version]],
                in_chunk_size,
            )
            for access in accesses
        ]

        line_iterators = [
            tsv_line_iterator_qtl(
                iterator,
                access.get_header(qtl=True),
                cs_qtl_columns,
                [pos["gene_start"] for pos in coords[access.gencode_version]],
                [pos["gene_end"] for pos in coords[access.gencode_version]],
            )
            for access, iterator in zip(accesses, chunk_iterators)
        ]
        header_with_resources = [b"resource", b"version"] + accesses[0].get_header(True)
        sort_key_fn = create_sort_key(header_with_resources, SORT_CONFIG_CS_QTL)
        merged_iterator = merge(*line_iterators, key=sort_key_fn)
        # header_line = b"\t".join(accesses[0].get_header()) + b"\n"
        header_line = (
            b"resource\tversion\t" + b"\t".join(accesses[0].get_header(True)) + b"\n"
        )

        return chunk_iterator(merged_iterator, header_line, out_chunk_size)
