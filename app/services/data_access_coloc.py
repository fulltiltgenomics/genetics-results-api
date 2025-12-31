from abc import abstractmethod
import asyncio
import time
from app.config.coloc import coloc
from app.config.credible_sets import variant_columns as cs_variant_columns
from app.config.sort_keys import (
    create_sort_key,
    SORT_CONFIG_COLOC_CREDSET,
    SORT_CONFIG_COLOC,
)
from app.core.exceptions import DataException
from app.core.streams import (
    chunk_iterator,
    tsv_line_iterator,
    tsv_line_iterator_coloc,
    tsv_line_iterator_coloc_by_trait,
    tsv_line_iterator_coloc_credset,
)
from app.core.variant import Variant
from app.services.base_data_access import (
    BaseFactory,
    BaseDataAccess,
    BaseDataAccessObject,
)
from asyncstdlib.heapq import merge
from typing import AsyncGenerator
from collections import defaultdict as dd
import logging

logger = logging.getLogger(__name__)


class DataAccessObjectColoc(BaseDataAccessObject):
    """Abstract base class for coloc data access operations"""

    def __init__(self, name: str):
        super().__init__(name)
        self.name = name

    @abstractmethod
    def get_credible_set_header(self) -> list[bytes]:
        """Get the header of the credible set data file"""
        pass

    def get_primary_header(self) -> list[bytes]:
        """Get the primary header for this data source (implements BaseDataAccessObject)."""
        return self.get_credible_set_header()

    @abstractmethod
    def get_coloc_header(self) -> list[bytes]:
        """Get the header of the coloc data file"""
        pass

    @abstractmethod
    async def stream_credible_set_range(
        self,
        chr: int,
        start: int,
        end: int,
        chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """Stream credible set data (variants included in credible sets that have been colocalized) for a chromosome range."""
        pass

    @abstractmethod
    async def stream_coloc_range(
        self,
        chr: int,
        start: int,
        end: int,
        chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """Stream coloc data (pairs of colocalizations) for a chromosome range."""
        pass


class DataAccessFactoryColoc(BaseFactory):
    """Factory for creating per-resource data access objects based on configuration."""

    def get_config_entry(self, name: str) -> dict:
        """Get configuration entry for the coloc name."""
        try:
            return [c for c in coloc if c["name"] == name][0]
        except IndexError:
            raise ValueError(
                f"Coloc data access object for name '{name}' not found in configuration"
            )

    def get_implementation_class(self, data_source: str) -> type:
        """Get the implementation class for the data source."""
        if data_source == "gcloud":
            from app.services.gcloud_tabix_coloc_data_access import (
                GCloudTabixDataAccessColoc,
            )

            return GCloudTabixDataAccessColoc
        else:
            raise ValueError(f"Unknown data source '{data_source}'")


class DataAccessColoc(BaseDataAccess[DataAccessObjectColoc]):
    """Main data access class that manages per-resource data access objects."""

    def create_factory(self) -> BaseFactory:
        """Return the factory instance for this domain."""
        return DataAccessFactoryColoc()

    async def _get_resource_access(self, name: str) -> DataAccessObjectColoc:
        """Get or create a coloc data access object."""
        return await super()._get_resource_access((name,), name)

    # TODO can we manage the data with polars fast enough? this is very complex
    async def stream_coloc_by_variant(
        self,
        variant: Variant,
        in_chunk_size: int,
        out_chunk_size: int,
        resource: str | None = None,
        phenotype_or_study: str | None = None,
        simple: bool = False,
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream colocalized credible sets for a variant.
        Outputs pairs of colocalized credible sets where either credible set contains the variant.

        Args:
            variant: Variant to query
            in_chunk_size: Input chunk size
            out_chunk_size: Output chunk size
            resource: Optional resource filter (e.g., 'finngen')
            phenotype_or_study: Optional phenotype/study filter
            simple: If True and resource/phenotype provided, reformat output to have query side as primary
        """

        names = [c["name"] for c in coloc]

        accesses = [await self._get_resource_access(name) for name in names]
        chunk_iterators = await asyncio.gather(
            *[
                access.stream_credible_set_range(
                    variant.chr, variant.pos, variant.pos, in_chunk_size
                )
                for access in accesses
            ]
        )
        # TODO create a function to just get the cs ids from the chunk iterator
        # coloc works with credible sets data
        line_iterators = [
            tsv_line_iterator(iterator, access.get_credible_set_header(), cs_variant_columns, variant)
            for access, iterator in zip(accesses, chunk_iterators)
        ]
        cs_header_with_resources = [b"resource", b"version"] + accesses[
            0
        ].get_credible_set_header()
        sort_key_fn = create_sort_key(
            cs_header_with_resources, SORT_CONFIG_COLOC_CREDSET
        )
        merged_iterator = merge(*line_iterators, key=sort_key_fn)
        index_dataset = accesses[0].get_credible_set_header().index(b"dataset") + 2
        index_trait = accesses[0].get_credible_set_header().index(b"trait") + 2
        index_cs_id = accesses[0].get_credible_set_header().index(b"cs_id") + 2
        # memorize credible set ids that have the variant
        cs_ids = [
            b"|".join([line[index_dataset], line[index_trait], line[index_cs_id]])
            async for line in merged_iterator
        ]
        # query all coloc pairs whose cs overlap with the variant
        chunk_iterators = await asyncio.gather(
            *[
                access.stream_coloc_range(
                    variant.chr, variant.pos, variant.pos, in_chunk_size
                )
                for access in accesses
            ]
        )
        header = accesses[0].get_coloc_header()

        # use filtered iterator when resource/phenotype provided
        if resource and phenotype_or_study:
            #  We need to filter by BOTH:
            # 1. Variant (cs1 or cs2 in cs_ids) - using tsv_line_iterator_coloc
            # 2. Resource/phenotype (cs1 or cs2 matches resource/phenotype) - custom filter
            # 3. Optionally swap columns if query side on side2 and simple=True - using iterator logic
            from app.services.dataset_mapping import DatasetMapping

            dataset_mapping = DatasetMapping()
            resource_bytes = resource.encode()
            phenotype_bytes = phenotype_or_study.encode()

            # First apply variant filter using standard iterator
            base_iterators = [
                tsv_line_iterator_coloc(iterator, header, cs_ids)
                for iterator in chunk_iterators
            ]

            # Wrap with resource/phenotype filter and optional swapping for simple format
            def filter_and_transform(base_iter):
                # get column indices (after resource/version columns added by tsv_line_iterator_coloc)
                resource1_idx = 0
                resource2_idx = 2
                trait1_idx = 4 + header.index(b"trait1")
                trait2_idx = 4 + header.index(b"trait2")

                async def filtered_and_swapped():
                    async for row in base_iter:
                        resource1 = row[resource1_idx]
                        resource2 = row[resource2_idx]
                        trait1 = row[trait1_idx]
                        trait2 = row[trait2_idx]

                        # Check if either side matches resource/phenotype
                        side1_matches = (
                            resource1 == resource_bytes and trait1 == phenotype_bytes
                        )
                        side2_matches = (
                            resource2 == resource_bytes and trait2 == phenotype_bytes
                        )

                        if not (side1_matches or side2_matches):
                            continue  # skip rows that don't match resource/phenotype

                        # For simple format, swap if query side is on side2, then filter columns
                        if simple:
                            # To avoid duplicates (same colocalization from both perspectives),
                            # only keep rows where query is on side1, or swap if it's on side2
                            if side2_matches and not side1_matches:
                                # Swap all paired columns so query side is on side1
                                row_copy = row[:]
                                # Swap resource/version
                                row_copy[0], row_copy[2] = row[2], row[0]
                                row_copy[1], row_copy[3] = row[3], row[1]

                                # Swap paired coloc columns
                                paired_indices = [
                                    (
                                        4 + header.index(b"dataset1"),
                                        4 + header.index(b"dataset2"),
                                    ),
                                    (
                                        4 + header.index(b"data_type1"),
                                        4 + header.index(b"data_type2"),
                                    ),
                                    (
                                        4 + header.index(b"trait1"),
                                        4 + header.index(b"trait2"),
                                    ),
                                    (
                                        4 + header.index(b"trait1_original"),
                                        4 + header.index(b"trait2_original"),
                                    ),
                                    (
                                        4 + header.index(b"cell_type1"),
                                        4 + header.index(b"cell_type2"),
                                    ),
                                    (
                                        4 + header.index(b"cs1_id"),
                                        4 + header.index(b"cs2_id"),
                                    ),
                                    (
                                        4 + header.index(b"hit1"),
                                        4 + header.index(b"hit2"),
                                    ),
                                    (
                                        4 + header.index(b"hit1_beta"),
                                        4 + header.index(b"hit2_beta"),
                                    ),
                                    (
                                        4 + header.index(b"hit1_mlog10p"),
                                        4 + header.index(b"hit2_mlog10p"),
                                    ),
                                    (
                                        4 + header.index(b"PP.H1.abf"),
                                        4 + header.index(b"PP.H2.abf"),
                                    ),  # swap H1 (trait1 only) with H2 (trait2 only)
                                    (
                                        4 + header.index(b"nsnps1"),
                                        4 + header.index(b"nsnps2"),
                                    ),
                                    (
                                        4 + header.index(b"cs1_log10bf"),
                                        4 + header.index(b"cs2_log10bf"),
                                    ),
                                    (
                                        4 + header.index(b"cs1_size"),
                                        4 + header.index(b"cs2_size"),
                                    ),
                                ]

                                for idx1, idx2 in paired_indices:
                                    row_copy[idx1], row_copy[idx2] = (
                                        row[idx2],
                                        row[idx1],
                                    )
                            elif side1_matches:
                                # Query is on side1, keep as-is
                                row_copy = row
                            else:
                                # Query is on neither side or both sides (shouldn't happen due to earlier filter)
                                continue

                            # Now filter row to remove "1" columns and rename "2" columns
                            # Build list of indices to keep (matching the simple header)
                            # Keep: resource(0->0), version(1->1), then filter coloc columns
                            filtered_row = [
                                row_copy[0],
                                row_copy[1],
                            ]  # resource, version

                            # Filter header columns and build corresponding row
                            for i, col in enumerate(header):
                                col_idx = (
                                    4 + i
                                )  # offset by resource1, version1, resource2, version2
                                # Skip columns with "1" suffix or "1_" pattern
                                if b"1_" in col or col.endswith(b"1"):
                                    continue
                                filtered_row.append(row_copy[col_idx])

                            yield filtered_row
                        else:
                            yield row

                return filtered_and_swapped()

            line_iterators = [
                filter_and_transform(base_iter) for base_iter in base_iterators
            ]

            # build header based on simple flag
            if simple:
                # simple format: filter out "1" columns and rename "2" columns
                def should_keep_col(col: bytes) -> bool:
                    """Keep columns that don't have '1' suffix or '1_' pattern."""
                    if b"1_" in col:
                        return False
                    if col.endswith(b"1"):
                        return False
                    return True

                def rename_col(col: bytes) -> bytes:
                    # replace "2_" with "_" (e.g., "trait2_original" -> "trait_original")
                    if b"2_" in col:
                        col = col.replace(b"2_", b"_")
                    # replace "2" at the end (e.g., "dataset2" -> "dataset", "hit2" -> "hit")
                    elif col.endswith(b"2"):
                        col = col[:-1]
                    return col

                header_filtered = [col for col in header if should_keep_col(col)]
                header_renamed = [rename_col(col) for col in header_filtered]
                header_with_resources = [b"resource", b"version"] + header_renamed
            else:
                header_with_resources = [
                    b"resource1",
                    b"version1",
                    b"resource2",
                    b"version2",
                ] + header
        else:
            # use standard iterator when no filtering
            line_iterators = [
                tsv_line_iterator_coloc(iterator, header, cs_ids)
                for iterator in chunk_iterators
            ]
            header_with_resources = [
                b"resource1",
                b"version1",
                b"resource2",
                b"version2",
            ] + header

        # use appropriate sort config based on simple flag
        if simple and resource and phenotype_or_study:
            from app.config.sort_keys import SORT_CONFIG_COLOC_SIMPLE

            sort_key_fn = create_sort_key(
                header_with_resources, SORT_CONFIG_COLOC_SIMPLE
            )
        else:
            sort_key_fn = create_sort_key(header_with_resources, SORT_CONFIG_COLOC)

        merged_iterator = merge(*line_iterators, key=sort_key_fn)
        header_line = b"\t".join(header_with_resources) + b"\n"

        return chunk_iterator(merged_iterator, header_line, out_chunk_size)

    # TODO can we manage the data with polars fast enough? this is very complex
    async def stream_coloc_variants_by_variant(
        self,
        variant: Variant,
        in_chunk_size: int,
        out_chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream colocalized credible sets for a variant.
        Outputs pairs of colocalized credible sets where either credible set contains the variant.
        Output includes all variants in the credible sets.
        """

        names = [c["name"] for c in coloc]

        accesses = [await self._get_resource_access(name) for name in names]

        start_time = time.time()
        chunk_iterators = await asyncio.gather(
            *[
                access.stream_credible_set_range(
                    variant.chr, variant.pos, variant.pos, in_chunk_size
                )
                for access in accesses
            ]
        )
        # TODO create a function to just get the cs ids from the chunk iterator
        # and instead of merging multiple times, just process each line iterator / coloc file pair separately
        # and merge in the end
        # coloc works with credible sets data
        line_iterators = [
            tsv_line_iterator(iterator, access.get_credible_set_header(), cs_variant_columns, variant)
            for access, iterator in zip(accesses, chunk_iterators)
        ]
        cs_header = accesses[0].get_credible_set_header()
        cs_header_with_resources = [b"resource", b"version"] + cs_header
        sort_key_fn = create_sort_key(
            cs_header_with_resources, SORT_CONFIG_COLOC_CREDSET
        )
        merged_iterator = merge(*line_iterators, key=sort_key_fn)
        index_dataset = cs_header.index(b"dataset") + 2
        index_trait = cs_header.index(b"trait") + 2
        index_cs_id = cs_header.index(b"cs_id") + 2
        # memorize credible set ids that have the variant
        cs_ids = [
            b"|".join([line[index_dataset], line[index_trait], line[index_cs_id]])
            async for line in merged_iterator
        ]
        cur_time = time.time()
        logger.info(f"Time taken to get cs ids: {cur_time - start_time:.3f}s")
        # query all coloc pairs whose cs overlap with the variant
        chunk_iterators = await asyncio.gather(
            *[
                access.stream_coloc_range(
                    variant.chr, variant.pos, variant.pos, in_chunk_size
                )
                for access in accesses
            ]
        )
        header = accesses[0].get_coloc_header()
        # filter coloc pairs to only include credible set ids that have the variant
        line_iterators = [
            tsv_line_iterator_coloc(iterator, header, cs_ids)
            for iterator in chunk_iterators
        ]
        header_with_resources = [
            b"resource1",
            b"version1",
            b"resource2",
            b"version2",
        ] + header
        sort_key_fn = create_sort_key(header_with_resources, SORT_CONFIG_COLOC)
        merged_iterator = merge(*line_iterators, key=sort_key_fn)

        index_dataset1 = header.index(b"dataset1") + 4
        index_dataset2 = header.index(b"dataset2") + 4
        index_trait1 = header.index(b"trait1") + 4
        index_trait2 = header.index(b"trait2") + 4
        index_cs1_id = header.index(b"cs1_id") + 4
        index_cs2_id = header.index(b"cs2_id") + 4
        index_region_start_min = header.index(b"region_start_min") + 4
        index_region_end_max = header.index(b"region_end_max") + 4

        # memorize coloc lines for each cs id
        cs1_ids_to_coloc_lines = dd(list)
        cs2_ids_to_coloc_lines = dd(list)
        min_cs_start = float("inf")
        max_cs_end = float("-inf")
        async for line in merged_iterator:
            cs1_ids_to_coloc_lines[
                b"|".join(
                    [line[index_dataset1], line[index_trait1], line[index_cs1_id]]
                )
            ].append(line)
            cs2_ids_to_coloc_lines[
                b"|".join(
                    [line[index_dataset2], line[index_trait2], line[index_cs2_id]]
                )
            ].append(line)
            min_cs_start = min(min_cs_start, int(line[index_region_start_min]))
            max_cs_end = max(max_cs_end, int(line[index_region_end_max]))

        logger.info(
            f"Time taken to read in and memorize coloc data: {time.time() - cur_time:.3f}s"
        )
        cur_time = time.time()

        # query credset file again with min/max region boundaries from coloc file
        chunk_iterators = [
            await access.stream_credible_set_range(
                variant.chr, min_cs_start, max_cs_end, in_chunk_size
            )
            for access in accesses
        ]
        # filter to variants in the cs ids
        line_iterators = [
            tsv_line_iterator_coloc_credset(iterator, cs_header, cs_ids)
            for iterator in chunk_iterators
        ]
        sort_key_fn = create_sort_key(cs_header, SORT_CONFIG_COLOC_CREDSET)
        merged_iterator = merge(*line_iterators, key=sort_key_fn)

        header_line = (
            b"\t".join([b"variant_" + h for h in cs_header])
            + b"\t"
            + b"\t".join(header_with_resources)
            + b"\n"
        )

        # output all coloc lines for each cs id
        async def long_coloc_iterator():
            cs_dataset_idx = cs_header.index(b"dataset")
            cs_trait_idx = cs_header.index(b"trait")
            cs_cs_id_idx = cs_header.index(b"cs_id")

            async for line in merged_iterator:
                cs_id = b"|".join(
                    [line[cs_dataset_idx], line[cs_trait_idx], line[cs_cs_id_idx]]
                )
                for coloc_line in cs1_ids_to_coloc_lines[cs_id]:
                    yield line + coloc_line
                for coloc_line in cs2_ids_to_coloc_lines[cs_id]:
                    yield line + coloc_line

        return chunk_iterator(long_coloc_iterator(), header_line, out_chunk_size)

    async def stream_coloc_by_credible_set_id(
        self,
        resource: str,
        phenotype_or_study: str,
        credible_set_id: str,
        in_chunk_size: int,
        out_chunk_size: int,
        simple: bool,
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream colocalized credible sets for a credible set id.
        Outputs pairs of colocalized credible sets where either credible set is the given one.
        """

        try:
            chrom = int(
                credible_set_id.split(":")[0]
                .strip("chr")
                .replace("X", "23")
                .replace("Y", "24")
            )
            start = int(credible_set_id.split(":")[1].split("-")[0])
            end = int(credible_set_id.split(":")[1].split("-")[1].split("_")[0])
            cs_number = int(
                credible_set_id.split(":")[1].split("-")[1].split("_")[1]
            )  # checking if format is correct
        except ValueError:
            raise DataException(f"Invalid credible set id: {credible_set_id}")

        names = [c["name"] for c in coloc]

        accesses = [await self._get_resource_access(name) for name in names]
        chunk_iterators = await asyncio.gather(
            *[
                access.stream_coloc_range(chrom, start, end, in_chunk_size)
                for access in accesses
            ]
        )
        # TODO create a function to just get the cs ids from the chunk iterator
        header = accesses[0].get_coloc_header()
        line_iterators = [
            tsv_line_iterator_coloc_by_trait(
                iterator,
                header,
                [f"{resource}|{phenotype_or_study}|{credible_set_id}".encode()],
                simple,
            )
            for iterator in chunk_iterators
        ]
        if simple:
            # for non-query side, use singular names (resource, version, dataset, trait, etc.)
            # filter out columns with "1" suffix and rename columns with "2" suffix to remove the "2"
            def should_keep_col(col: bytes) -> bool:
                """Keep columns that don't have '1' suffix or '1_' pattern."""
                if b"1_" in col:
                    return False
                if col.endswith(b"1"):
                    return False
                return True

            def rename_col(col: bytes) -> bytes:
                # replace "2_" with "_" (e.g., "trait2_original" -> "trait_original")
                if b"2_" in col:
                    col = col.replace(b"2_", b"_")
                # replace "2" at the end (e.g., "dataset2" -> "dataset", "hit2" -> "hit")
                elif col.endswith(b"2"):
                    col = col[:-1]
                return col

            # filter out columns with "1" suffix, then rename "2" suffix columns
            header_filtered = [col for col in header if should_keep_col(col)]
            header_renamed = [rename_col(col) for col in header_filtered]
            header_with_resources = [
                b"resource",
                b"version",
            ] + header_renamed
            # SORT_CONFIG_COLOC won't work here since we removed trait1 and trait2 becomes trait
            # create a modified sort config that only uses trait (no trait1)
            sort_config_modified = [
                ("chr", int),
                ("region_start_min", int),
                ("region_end_max", int),
                (
                    "trait",
                    bytes,
                ),  # trait2 becomes trait after renaming, trait1 is removed
            ]
            sort_key_fn = create_sort_key(header_with_resources, sort_config_modified)
        else:
            header_with_resources = [
                b"resource1",
                b"version1",
                b"resource2",
                b"version2",
            ] + header
            sort_key_fn = create_sort_key(header_with_resources, SORT_CONFIG_COLOC)
        merged_iterator = merge(*line_iterators, key=sort_key_fn)
        header_line = b"\t".join(header_with_resources) + b"\n"

        return chunk_iterator(merged_iterator, header_line, out_chunk_size)
