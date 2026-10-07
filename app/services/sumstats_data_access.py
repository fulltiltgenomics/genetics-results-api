import asyncio
import logging
import os
from typing import AsyncGenerator

import aiohttp.client_exceptions
from asyncstdlib.heapq import merge

from app.config.sort_keys import (
    SORT_CONFIG_HLA,
    SORT_CONFIG_SUMSTATS,
    create_sort_key,
)
from app.config.summary_stats import get_data_files_by_resource_and_type
from app.core.exceptions import NotFoundException
from app.core.streams import (
    chunk_iterator,
    start_iterators,
    tsv_line_iterator_sumstats,
    union_output_columns,
)
from app.core.variant import Variant
from app.services.gcloud_tabix_base import (
    GCloudTabixBase,
    _get_fetch_semaphore,
    _get_files_semaphore,
    validate_path_component,
)

logger = logging.getLogger(__name__)

# Data types whose files do not carry the default chr/pos/ref/alt key. Merging streams
# by a column the files lack would fail at create_sort_key, so the ordering columns are
# selected per data_type; anything not listed here uses SORT_CONFIG_SUMSTATS.
_SORT_CONFIG_BY_DATA_TYPE = {"hla": SORT_CONFIG_HLA}


class SumstatsDataAccess(GCloudTabixBase):
    """Manages summary stat queries against per-phenotype tabix-indexed files."""

    def __init__(self):
        # defer GCloudTabixBase init — it creates aiohttp objects that need an event loop
        self._initialized = False
        self._header_cache: dict[str, list[bytes]] = {}
        # whole unindexed objects, widened, keyed by path (an HLA run is ~5 KB)
        self._unindexed_cache: dict[str, tuple[list[bytes], list[list[bytes]]]] = {}

    def _ensure_initialized(self):
        if not self._initialized:
            super().__init__()
            self._initialized = True

    async def _check_file_exists(self, path: str) -> bool:
        """Check if a file exists (GCS or local)."""
        if not path.startswith("gs://"):
            return os.path.exists(path)
        headers = await self.storage._headers()
        url = path.replace("gs://", "https://storage.googleapis.com/")
        # HEAD, inside the context manager, under the fetch semaphore: a bare GET here
        # started streaming the whole multi-hundred-MB object into a response nothing
        # read or released, and 256 of those pinned every connector slot until every
        # later GCS request timed out
        async with _get_fetch_semaphore():
            try:
                async with self.session.head(url, headers=headers) as response:
                    return response.status != 404
            except aiohttp.client_exceptions.ClientResponseError as e:
                if e.status == 404:
                    return False
                raise

    def _get_file_path(self, data_file_config: dict, phenotype: str) -> str | None:
        """The object a (config, phenotype) reads; None when a catalog has no such run."""
        if "file" in data_file_config:
            return data_file_config["file"]
        # phenotype comes straight from the request; without this it can traverse out of the
        # configured prefix (and out of the bucket) into any object the workload SA can read
        validate_path_component(phenotype)
        if "catalog" in data_file_config:
            from app.services.custom_gwas_catalog import get_catalog

            catalog = get_catalog(data_file_config["catalog"])
            if data_file_config.get("catalog_product") == "hla":
                return catalog.hla_path(phenotype)
            return catalog.sumstats_path(phenotype)
        return f"{data_file_config['prefix']}{phenotype}{data_file_config['suffix']}"

    async def get_file_header(self, gs_path: str) -> list[bytes]:
        """The raw header of one object, cached per object and never per config entry.

        A prefix's files are not one shape: the R13 MVP meta-analysis prefixes hold 43-,
        54- and 65-column files side by side, and a header borrowed from a sibling
        misaligns every row of a file of another width (an rsid read as a p-value).
        """
        self._ensure_initialized()
        cached = self._header_cache.get(gs_path)
        if cached is None:
            # off the event loop and under the files cap: a PheWAS fans this out over
            # hundreds of objects the first time each is asked for
            async with _get_files_semaphore():
                cached = await self._get_header_async(gs_path)
            self._header_cache[gs_path] = cached
        return cached

    async def _unindexed_file(
        self, data_file_config: dict, gs_path: str
    ) -> tuple[list[bytes], list[list[bytes]]]:
        """Read a whole unindexed object and apply the config's widening transform."""
        cached = self._unindexed_cache.get(gs_path)
        if cached is not None:
            return cached
        from app.services.custom_gwas_hla import UNINDEXED_TRANSFORMS, parse_gzip_tsv

        transform = UNINDEXED_TRANSFORMS[data_file_config["unindexed"]]
        header, rows = transform(*parse_gzip_tsv(await self._fetch_full(gs_path)))
        self._unindexed_cache[gs_path] = (header, rows)
        return header, rows

    async def _stream_unindexed(
        self,
        data_file_config: dict,
        gs_path: str,
        chrs: list[int],
        starts: list[int],
        ends: list[int],
    ) -> AsyncGenerator[bytes, None]:
        """The rows of an unindexed object inside the regions, as one chunk of lines
        (the same shape a tabix range read yields: data lines only)."""
        header, rows = await self._unindexed_file(data_file_config, gs_path)
        chr_idx = header.index(b"chrom")
        pos_idx = header.index(b"pos")
        regions = list(zip(chrs, starts, ends))

        def _inside(row: list[bytes]) -> bool:
            try:
                chrom, pos = int(row[chr_idx]), int(row[pos_idx])
            except (ValueError, IndexError):
                return False
            return any(c == chrom and s <= pos <= e for c, s, e in regions)

        payload = b"".join(b"\t".join(r) + b"\n" for r in rows if _inside(r))

        async def _gen() -> AsyncGenerator[bytes, None]:
            if payload:
                yield payload

        return _gen()


    async def stream_sumstats(
        self,
        resource: str,
        data_type: str,
        phenotypes: list[str],
        variants: list[Variant],
        in_chunk_size: int,
        out_chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """Query summary stats for variant(s) across phenotype(s).

        Launches parallel tabix queries across phenotypes and data files,
        then heap-merges all streams in sorted order.
        """
        variant_set = set(variants) if len(variants) > 1 else None
        single_variant = variants[0] if len(variants) == 1 else None
        variant_filter = variant_set if variant_set else single_variant

        positions = [v.pos for v in variants]
        return await self._stream_regions(
            resource,
            data_type,
            phenotypes,
            [v.chr for v in variants],
            positions,
            positions,
            variant_filter,
            in_chunk_size,
            out_chunk_size,
        )

    async def stream_sumstats_range(
        self,
        resource: str,
        data_type: str,
        phenotypes: list[str],
        chr: int,
        start: int,
        end: int,
        in_chunk_size: int,
        out_chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """Query summary stats for every variant in a genomic range across phenotype(s).

        Same fan-out and merge as stream_sumstats, but the tabix result is returned
        as-is (no per-variant filtering) since every record in the range qualifies.
        """
        return await self._stream_regions(
            resource,
            data_type,
            phenotypes,
            [chr],
            [start],
            [end],
            None,
            in_chunk_size,
            out_chunk_size,
        )

    async def stream_sumstats_positions(
        self,
        resource: str,
        data_type: str,
        phenotypes: list[str],
        chr: int,
        positions: list[int],
        in_chunk_size: int,
        out_chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """Query several exact positions on one chromosome across phenotype(s).

        Used where the rows of interest sit at known discrete anchors rather than in a
        contiguous interval (HLA gene anchors), so that selecting two of them does not
        also drag in everything positioned between the two.
        """
        return await self._stream_regions(
            resource,
            data_type,
            phenotypes,
            [chr] * len(positions),
            positions,
            positions,
            None,
            in_chunk_size,
            out_chunk_size,
        )

    async def _stream_regions(
        self,
        resource: str,
        data_type: str,
        phenotypes: list[str],
        chrs: list[int],
        starts: list[int],
        ends: list[int],
        variant_filter: "Variant | set[Variant] | None",
        in_chunk_size: int,
        out_chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """Fan a region query out over every phenotype file and heap-merge the results."""
        self._ensure_initialized()

        data_file_configs = get_data_files_by_resource_and_type(resource, data_type)
        if not data_file_configs:
            raise NotFoundException(
                f"No summary stats configured for resource '{resource}', data type '{data_type}'"
            )

        # phase 1: find every (config, phenotype) that has a file, fetching each
        # file's header. Headers determine the shared output schema, which must be
        # known before any row is serialized — otherwise rows from a narrower file
        # (e.g. Kanta labs) misalign against a wider file's header (e.g. core GWAS).
        contributions = []  # (df_config, phenotype, gs_path, file_header)
        schema_configs = []  # (column_mapping, file_header) per distinct header
        seen_shapes = set()

        candidates = []  # (df_config, phenotype, gs_path), in config order
        for df_config in data_file_configs:
            # single-file configs only serve their configured phenotype
            if "file" in df_config:
                configured_phenotype = df_config["phenotype"]
                effective_phenotypes = [configured_phenotype] if configured_phenotype in phenotypes else []
            else:
                effective_phenotypes = phenotypes
            for phenotype in effective_phenotypes:
                gs_path = self._get_file_path(df_config, phenotype)
                if gs_path is None:
                    logger.info(f"Phenotype {phenotype} not in catalog {df_config['id']}")
                    continue
                candidates.append((df_config, phenotype, gs_path))

        # one round trip per candidate, so a PheWAS over hundreds of phenotypes against
        # several configs is thousands of them; concurrently they cost one round trip
        exists = await asyncio.gather(
            *(self._check_file_exists(gs_path) for _, _, gs_path in candidates)
        )

        present = []
        for (df_config, phenotype, gs_path), found in zip(candidates, exists):
            if not found:
                logger.info(f"Phenotype file not found: {gs_path}")
                continue
            present.append((df_config, phenotype, gs_path))

        async def header_of(df_config: dict, gs_path: str) -> list[bytes]:
            if df_config.get("unindexed"):
                file_header, _ = await self._unindexed_file(df_config, gs_path)
                return file_header
            return await self.get_file_header(gs_path)

        # one header per object, concurrently like the existence checks above
        headers = await asyncio.gather(
            *(header_of(df_config, gs_path) for df_config, _, gs_path in present),
            return_exceptions=True,
        )

        for (df_config, phenotype, gs_path), file_header in zip(present, headers):
            if isinstance(file_header, BaseException):
                logger.warning(
                    f"Skipping {phenotype} for {df_config['id']}: header fetch failed: "
                    f"{file_header}"
                )
                continue

            contributions.append((df_config, phenotype, gs_path, file_header))
            # one schema entry per distinct header, not per config: files of one config
            # differ in width, and the union has to see every shape that contributes
            shape = (df_config["id"], tuple(file_header))
            if shape not in seen_shapes:
                seen_shapes.add(shape)
                schema_configs.append((df_config["column_mapping"], file_header))

        if not contributions:
            raise NotFoundException(
                f"No data found for resource '{resource}', data type '{data_type}', "
                f"phenotypes {phenotypes}"
            )

        unified_columns = union_output_columns(schema_configs)
        output_header = [b"resource", b"version", b"phenotype"] + [
            c.encode() for c in unified_columns
        ]

        # phase 2: open each tabix stream and align its rows to the shared schema.
        # Opened concurrently: opening loads the file's index, and awaiting the
        # files one by one would serialise those loads however parallel the loader is
        raw_streams = await asyncio.gather(
            *(
                self._stream_unindexed(df_config, gs_path, chrs, starts, ends)
                if df_config.get("unindexed")
                else self._stream_range(gs_path, chrs, starts, ends, in_chunk_size)
                for df_config, _, gs_path, _ in contributions
            ),
            return_exceptions=True,
        )
        line_iterators = []
        for (df_config, phenotype, gs_path, file_header), raw_stream in zip(
            contributions, raw_streams
        ):
            if isinstance(raw_stream, BaseException):
                logger.warning(
                    f"Skipping {phenotype} for {df_config['id']}: tabix failed: {raw_stream}"
                )
                continue

            line_iter = tsv_line_iterator_sumstats(
                raw_stream,
                file_header,
                df_config["column_mapping"],
                unified_columns,
                df_config["resource"].encode(),
                df_config["version"].encode(),
                phenotype.encode(),
                variant_filter,
            )
            line_iterators.append(line_iter)

        if not line_iterators:
            raise NotFoundException(
                f"No data found for resource '{resource}', data type '{data_type}', "
                f"phenotypes {phenotypes}"
            )

        sort_key_fn = create_sort_key(
            output_header,
            _SORT_CONFIG_BY_DATA_TYPE.get(data_type, SORT_CONFIG_SUMSTATS),
        )
        merged_iterator = merge(*await start_iterators(line_iterators), key=sort_key_fn)
        header_line = b"\t".join(output_header) + b"\n"

        return chunk_iterator(merged_iterator, header_line, out_chunk_size)
