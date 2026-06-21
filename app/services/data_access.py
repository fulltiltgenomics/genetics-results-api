from abc import abstractmethod
import json

import fsspec
from app.config.datasets import build_harmonizer_config, get_dataset
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
from app.core.gcs_retry import with_gcs_retry
from app.core.streams import (
    chunk_iterator,
    start_iterators,
    tsv_line_iterator,
    tsv_line_iterator_qtl,
)
from app.core.variant import Variant
from app.services.base_data_access import (
    BaseFactory,
    BaseDataAccess,
    BaseDataAccessObject,
)
from app.services.metadata_harmonizer import MetadataHarmonizer
from asyncstdlib.heapq import merge
from typing import Any, AsyncGenerator, Literal, List
import asyncio
import logging

# merge all config dicts
data_file_by_id = {**cs_data_file_by_id, **exome_data_file_by_id, **gene_based_data_file_by_id}

logger = logging.getLogger(__name__)

# harmonized metadata is derived from small, slowly-changing metadata files;
# cache per (resource, include_data_type) for the process lifetime so the
# /metadata and search hot paths don't re-read GCS on every request. keyed
# module-level so it is shared across the multiple DataAccess instances that
# get constructed (container, search_service, coloc).
_harmonized_metadata_cache: dict[tuple[str, bool, bool], list[dict[str, Any]]] = {}


def clear_metadata_cache() -> None:
    """Clear the harmonized metadata cache (used by tests)."""
    _harmonized_metadata_cache.clear()


def _read_metadata_file(metadata_file: str) -> list[dict[str, Any]]:
    """Read a metadata file (TSV or JSON, optionally gzipped) into a list of rows.

    Wrapped in with_gcs_retry so a transient egress-quota 429 is absorbed rather
    than surfaced. The whole read happens inside the retried closure so a retry
    re-opens the file.
    """
    compression = (
        "gzip" if metadata_file.endswith((".gz", ".bgz")) else None
    )

    def _read() -> list[dict[str, Any]]:
        with fsspec.open(metadata_file, "rt", compression=compression) as f:
            if metadata_file.endswith((".json", ".json.gz")):
                meta = json.load(f)
                return meta if isinstance(meta, list) else [meta]
            header = f.readline().strip().split("\t")
            return [dict(zip(header, line.strip().split("\t"))) for line in f]

    return with_gcs_retry(_read)


def _dedup_by_combined_file(
    data_file_ids: list[str], data_type: str
) -> list[str]:
    """Drop duplicate data file IDs that point at the same combined (all_cs/all_exome) file.

    When several datasets share one collected TSV (e.g. EXT pseudo CS), tabixing it
    once per data file ID would re-fetch the same bytes; keep only the first ID per
    unique combined file. Per-row resource attribution is preserved by tsv_line_iterator.
    """
    seen: set[str] = set()
    result: list[str] = []
    for did in data_file_ids:
        df = data_file_by_id.get(did)
        combined: str | None = None
        if df and data_type in df:
            cfg = df[data_type]
            combined = cfg.get("all_cs_file") or cfg.get("all_exome_file")
        if combined is None or combined not in seen:
            if combined is not None:
                seen.add(combined)
            result.append(did)
    return result


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
    async def lead_variants_phenotype(
        self,
        phenotype: str,
        interval: Literal[95, 99] | None,
        header_schema: dict[str, type],
        chunk_size: int,
    ) -> list[dict[str, Any]]:
        """Get the lead variant (one per cs_id) for a phenotype, streamed from the data source."""
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

    async def warm_all(self) -> None:
        """Construct and warm (header + .tbi prefetch) every configured tabix data
        access object concurrently, so the first request pays no cold-start cost.

        Per-file failures are logged and skipped rather than aborting startup;
        verify_all_data_files() is the authoritative reachability check.
        """
        targets = [
            (data_file_id, data_type)
            for data_file_id, df in data_file_by_id.items()
            if df.get("data_source", "gcloud") == "gcloud"
            for data_type in ("cs", "exome", "gene_based")
            if data_type in df
        ]

        async def _warm(data_file_id: str, data_type: str) -> None:
            try:
                access = await self._get_resource_access(data_file_id, data_type)
                if hasattr(access, "warm"):
                    await access.warm()
            except Exception as e:
                logger.warning(
                    f"Warm failed for {data_file_id}/{data_type}: {e}"
                )

        await asyncio.gather(*(_warm(did, dt) for did, dt in targets))

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
        """Get the raw metadata for a resource, merging across all datasets that
        contribute to it (including coloc-partner-only datasets), deduped by file."""
        from app.services.config_util import get_metadata_dataset_ids_for_resource

        dataset_ids = get_metadata_dataset_ids_for_resource(
            resource, include_coloc_partners=True
        )

        all_meta = []
        seen_files: set[str] = set()
        for dataset_id in dataset_ids:
            harm_config = build_harmonizer_config(dataset_id)
            if not harm_config:
                continue
            metadata_file = harm_config["metadata"]["metadata_file"]
            if metadata_file in seen_files:
                continue
            seen_files.add(metadata_file)

            all_meta.extend(_read_metadata_file(metadata_file))

        return all_meta

    def get_harmonized_metadata(
        self,
        resource: str,
        include_data_type: bool = False,
        include_coloc_partners: bool = False,
    ) -> list[dict[str, Any]]:
        """Get harmonized metadata for a resource in unified format.

        When ``include_data_type`` is set, each returned dict carries the
        owning dataset's ``data_type`` so callers (e.g. the search index) can
        match phenotypes against summary_stats (resource, data_type) pairs.

        When ``include_coloc_partners`` is set, coloc-partner-only datasets of
        this resource (e.g. ``finngen_kanta_r12``) are folded in so their
        phenocodes resolve to names in /resource_metadata; the search index
        leaves them out (default False) since they aren't independently queryable.
        """
        cache_key = (resource, include_data_type, include_coloc_partners)
        cached = _harmonized_metadata_cache.get(cache_key)
        if cached is not None:
            return cached

        from app.services.config_util import get_metadata_dataset_ids_for_resource

        dataset_ids = get_metadata_dataset_ids_for_resource(
            resource, include_coloc_partners=include_coloc_partners
        )

        all_harmonized = []
        harmonizer = MetadataHarmonizer()

        # group datasets by metadata_file so a file shared by multiple datasets
        # (e.g. genebass exome + gene_based) is read only once. the per-phenotype
        # data_type lives on the dataset registry entry, not in the metadata rows,
        # so we collect the data_types of every dataset sharing a file and expand
        # each harmonized phenotype across them when requested.
        metadata_groups: dict[str, dict[str, Any]] = {}
        for dataset_id in dataset_ids:
            harm_config = build_harmonizer_config(dataset_id)
            if not harm_config:
                continue
            metadata_file = harm_config["metadata"]["metadata_file"]

            group = metadata_groups.get(metadata_file)
            if group is None:
                # keep the first dataset's harm_config; harmonization output does
                # not depend on data_type, so any dataset sharing the file is fine
                group = {"harm_config": harm_config, "data_types": []}
                metadata_groups[metadata_file] = group

            data_type = (get_dataset(dataset_id) or {}).get("data_type")
            # ordered-unique: a file shared by datasets with the SAME data_type
            # must not duplicate rows
            if data_type not in group["data_types"]:
                group["data_types"].append(data_type)

        for metadata_file, group in metadata_groups.items():
            harm_config = group["harm_config"]

            # read raw metadata for this file
            raw_metadata = _read_metadata_file(metadata_file)

            if not raw_metadata:
                continue

            # harmonize with config from dataset registry
            harmonized = harmonizer.harmonize_metadata(
                resource, raw_metadata, harm_config
            )
            if include_data_type:
                # emit one dict per (phenotype, data_type) so a phenotype with
                # multiple result types appears once per type in the search index
                for item in harmonized:
                    base = item.to_dict()
                    for data_type in group["data_types"]:
                        item_dict = dict(base)
                        item_dict["data_type"] = data_type
                        all_harmonized.append(item_dict)
            else:
                all_harmonized.extend(item.to_dict() for item in harmonized)

        _harmonized_metadata_cache[cache_key] = all_harmonized
        return all_harmonized

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

    async def lead_variants_phenotype(
        self,
        resource: str,
        phenotype: str,
        interval: Literal[95, 99] | None,
        header_schema: dict[str, type],
        chunk_size: int = 1024 * 1024,
    ) -> list[dict[str, Any]]:
        """Lead variant (one per cs_id) across all data files for the resource that have this phenotype."""
        from app.services.config_util import get_data_file_ids_for_resource

        data_file_ids = get_data_file_ids_for_resource(resource)
        if not data_file_ids:
            # fallback to treating resource as a data file ID
            data_file_ids = [resource]

        # cs_ids are unique per dataset/trait, so a plain dict merge across files needs no tie-break
        by_cs_id: dict[str, dict[str, Any]] = {}
        found = False
        for data_file_id in data_file_ids:
            try:
                access = await self._get_resource_access(data_file_id, "cs")
                if await access.check_phenotype_exists(phenotype, interval):
                    found = True
                    for row in await access.lead_variants_phenotype(
                        phenotype, interval, header_schema, chunk_size
                    ):
                        by_cs_id[row["cs_id"]] = row
            except ValueError:
                # data file doesn't support this data type, skip it
                continue
            except Exception:
                continue

        if not found:
            raise NotFoundException(
                f"Phenotype {phenotype} not found in resource {resource}"
            )

        return list(by_cs_id.values())

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

        # dedup data file IDs that share the same combined file (tabix it once)
        data_file_ids = _dedup_by_combined_file(data_file_ids, data_type)
        # row-level filter for shared-file rows belonging to other resources
        resource_filter = {r.encode() for r in resources}

        # open access objects and chunk iterators concurrently (per-file setup is
        # network-bound on cold caches), skipping any that fail
        async def _open(data_file_id: str):
            try:
                access = await self._get_resource_access(data_file_id, data_type)
                chunk_iterator_stream = await access.stream_range(
                    [chr], [start], [end], in_chunk_size
                )
                return access, chunk_iterator_stream
            except ValueError:
                # data file doesn't support this data type, expected - skip silently
                return None
            except Exception as e:
                logger.warning(f"Skipping data file {data_file_id} due to error: {e}")
                return None

        opened = await asyncio.gather(*(_open(did) for did in data_file_ids))
        accesses_and_iterators = [r for r in opened if r is not None]

        if not accesses_and_iterators:
            raise NotFoundException(
                f"No data files available for resources: {resources}"
            )

        accesses, chunk_iterators = zip(*accesses_and_iterators)

        # select column config based on data type
        columns = exome_variant_columns if data_type == "exome" else cs_variant_columns

        line_iterators = [
            tsv_line_iterator(
                iterator, access.get_header(), columns, variant, resource_filter
            )
            for access, iterator in zip(accesses, chunk_iterators)
        ]
        header_with_resources = [b"resource", b"version"] + accesses[0].get_header()
        sort_key_fn = create_sort_key(header_with_resources, SORT_CONFIG_CS)
        # prime each file's first (network-bound) tabix read concurrently so they
        # overlap instead of serializing inside merge()'s sequential heap seeding
        merged_iterator = merge(*await start_iterators(line_iterators), key=sort_key_fn)
        header_line = (
            b"resource\tversion\t" + b"\t".join(accesses[0].get_header()) + b"\n"
        )

        return chunk_iterator(merged_iterator, header_line, out_chunk_size)

    async def stream_range_variants(
        self,
        variants: list[Variant],
        resources: List[str],
        data_type: Literal["cs", "assoc"],
        in_chunk_size: int,
        out_chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """Stream data for multiple variants from multiple resources using a single tabix -R call per data file."""
        from app.services.config_util import get_data_file_ids_for_resource

        data_file_ids = []
        for resource in resources:
            ids = get_data_file_ids_for_resource(resource)
            if ids:
                data_file_ids.extend(ids)
            else:
                data_file_ids.append(resource)

        data_file_ids = _dedup_by_combined_file(data_file_ids, data_type)
        resource_filter = {r.encode() for r in resources}

        chrs = [v.chr for v in variants]
        positions = [v.pos for v in variants]
        variant_set = set(variants)

        accesses_and_iterators = []
        for data_file_id in data_file_ids:
            try:
                access = await self._get_resource_access(data_file_id, data_type)
                chunk_iterator_stream = await access.stream_range(
                    chrs, positions, positions, in_chunk_size
                )
                accesses_and_iterators.append((access, chunk_iterator_stream))
            except ValueError:
                continue
            except Exception as e:
                logger.warning(f"Skipping data file {data_file_id} due to error: {e}")
                continue

        if not accesses_and_iterators:
            raise NotFoundException(
                f"No data files available for resources: {resources}"
            )

        accesses, chunk_iterators = zip(*accesses_and_iterators)

        columns = exome_variant_columns if data_type == "exome" else cs_variant_columns

        line_iterators = [
            tsv_line_iterator(
                iterator, access.get_header(), columns, variant_set, resource_filter
            )
            for access, iterator in zip(accesses, chunk_iterators)
        ]
        header_with_resources = [b"resource", b"version"] + accesses[0].get_header()
        sort_key_fn = create_sort_key(header_with_resources, SORT_CONFIG_CS)
        merged_iterator = merge(*await start_iterators(line_iterators), key=sort_key_fn)
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

        data_file_ids = _dedup_by_combined_file(data_file_ids, data_type)
        resource_filter = {r.encode() for r in resources}

        # create access objects concurrently, skipping data files that don't support this data type
        async def _get_access(data_file_id: str):
            try:
                return await self._get_resource_access(data_file_id, data_type)
            except ValueError:
                # data file doesn't support this data type, expected - skip silently
                return None

        accesses = [
            a
            for a in await asyncio.gather(*(_get_access(did) for did in data_file_ids))
            if a is not None
        ]

        # skip resources whose gencode version has no coordinates for the queried gene
        accesses = [
            access
            for access in accesses
            if access.gencode_version in coords
            and len(coords[access.gencode_version]) > 0
        ]

        if len(accesses) == 0:
            raise NotFoundException(
                f"No data found for resources: {resources}"
            )

        # open range streams concurrently; skip accesses whose data file has no combined
        # file for range queries (mirrors stream_range; e.g. ibd exome has per-pheno
        # files but no all_exome_file)
        async def _open(access):
            try:
                iterator = await access.stream_range(
                    [pos["chrom"] for pos in coords[access.gencode_version]],
                    [pos["gene_start"] for pos in coords[access.gencode_version]],
                    [pos["gene_end"] for pos in coords[access.gencode_version]],
                    in_chunk_size,
                )
            except ValueError:
                return None
            return access, iterator

        accesses_and_iterators = [
            r for r in await asyncio.gather(*(_open(a) for a in accesses)) if r is not None
        ]

        if not accesses_and_iterators:
            raise NotFoundException(f"No data found for resources: {resources}")

        accesses, chunk_iterators = zip(*accesses_and_iterators)

        # select column config based on data type
        columns = exome_variant_columns if data_type == "exome" else cs_variant_columns

        line_iterators = [
            tsv_line_iterator(
                iterator, access.get_header(), columns, variant, resource_filter
            )
            for access, iterator in zip(accesses, chunk_iterators)
        ]
        header_with_resources = [b"resource", b"version"] + accesses[0].get_header()
        sort_key_fn = create_sort_key(header_with_resources, SORT_CONFIG_CS)
        merged_iterator = merge(*await start_iterators(line_iterators), key=sort_key_fn)
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

        # create access objects concurrently, skipping data files that don't support this data type
        async def _get_access(data_file_id: str):
            try:
                return await self._get_resource_access(data_file_id, data_type)
            except ValueError:
                # data file doesn't support this data type, expected - skip silently
                return None

        accesses = [
            a
            for a in await asyncio.gather(*(_get_access(did) for did in data_file_ids))
            if a is not None
        ]

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

        chunk_iterators = await asyncio.gather(
            *[
                access.stream_qtl_gene_range(
                    [pos["chrom"] for pos in coords[access.gencode_version]],
                    [pos["gene_start"] for pos in coords[access.gencode_version]],
                    in_chunk_size,
                )
                for access in accesses
            ]
        )

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
        merged_iterator = merge(*await start_iterators(line_iterators), key=sort_key_fn)
        # header_line = b"\t".join(accesses[0].get_header()) + b"\n"
        header_line = (
            b"resource\tversion\t" + b"\t".join(accesses[0].get_header(True)) + b"\n"
        )

        return chunk_iterator(merged_iterator, header_line, out_chunk_size)
