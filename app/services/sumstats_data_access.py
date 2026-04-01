import logging
from typing import AsyncGenerator

import aiohttp.client_exceptions
from asyncstdlib.heapq import merge

from app.config.summary_stats import get_data_files_by_resource_and_type
from app.config.sort_keys import create_sort_key, SORT_CONFIG_SUMSTATS
from app.core.exceptions import NotFoundException
from app.core.streams import tsv_line_iterator_sumstats, chunk_iterator
from app.core.variant import Variant
from app.services.gcloud_tabix_base import GCloudTabixBase

logger = logging.getLogger(__name__)


class SumstatsDataAccess(GCloudTabixBase):
    """Manages summary stat queries against per-phenotype tabix-indexed files."""

    def __init__(self):
        # defer GCloudTabixBase init — it creates aiohttp objects that need an event loop
        self._initialized = False
        self._header_cache: dict[str, list[bytes]] = {}

    def _ensure_initialized(self):
        if not self._initialized:
            super().__init__()
            self._initialized = True

    async def _check_file_exists(self, gs_path: str) -> bool:
        """Check if a GCS file exists via HEAD request."""
        headers = await self.storage._headers()
        url = gs_path.replace("gs://", "https://storage.googleapis.com/")
        try:
            response = await self.session.get(url, headers=headers)
            return response.status != 404
        except aiohttp.client_exceptions.ClientResponseError as e:
            if e.status == 404:
                return False
            raise

    def _get_file_path(self, data_file_config: dict, phenotype: str) -> str:
        return f"{data_file_config['prefix']}{phenotype}{data_file_config['suffix']}"

    def get_file_header(self, data_file_config: dict, phenotype: str) -> list[bytes]:
        """Get and cache the raw file header for a data file config."""
        self._ensure_initialized()
        cache_key = data_file_config["id"]
        if cache_key in self._header_cache:
            return self._header_cache[cache_key]
        gs_path = self._get_file_path(data_file_config, phenotype)
        header = self._get_header(gs_path)
        self._header_cache[cache_key] = header
        return header

    def get_output_header(self, data_file_config: dict, file_header: list[bytes]) -> list[bytes]:
        """Build the unified output header from column mapping."""
        mapping = data_file_config["column_mapping"]
        file_header_str = [h.decode() for h in file_header]
        mapped = [
            mapping[src_col].encode()
            for src_col in mapping
            if src_col in file_header_str
        ]
        return [b"resource", b"version", b"phenotype"] + mapped

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
        self._ensure_initialized()

        data_file_configs = get_data_files_by_resource_and_type(resource, data_type)
        if not data_file_configs:
            raise NotFoundException(
                f"No summary stats configured for resource '{resource}', data type '{data_type}'"
            )

        variant_set = set(variants) if len(variants) > 1 else None
        single_variant = variants[0] if len(variants) == 1 else None
        variant_filter = variant_set if variant_set else single_variant

        chrs = [v.chr for v in variants]
        positions = [v.pos for v in variants]

        # collect all (data_file_config, phenotype) stream tasks
        line_iterators = []
        output_header = None

        for df_config in data_file_configs:
            resource_bytes = df_config["resource"].encode()
            version_bytes = df_config["version"].encode()
            column_mapping = df_config["column_mapping"]

            for phenotype in phenotypes:
                gs_path = self._get_file_path(df_config, phenotype)

                if not await self._check_file_exists(gs_path):
                    logger.info(f"Phenotype file not found: {gs_path}")
                    continue

                try:
                    file_header = self.get_file_header(df_config, phenotype)
                except Exception as e:
                    logger.warning(
                        f"Skipping {phenotype} for {df_config['id']}: header fetch failed: {e}"
                    )
                    continue

                if output_header is None:
                    output_header = self.get_output_header(df_config, file_header)

                try:
                    raw_stream = await self._stream_range(
                        gs_path, chrs, positions, positions, in_chunk_size
                    )
                except Exception as e:
                    logger.warning(
                        f"Skipping {phenotype} for {df_config['id']}: tabix failed: {e}"
                    )
                    continue

                phenotype_bytes = phenotype.encode()
                line_iter = tsv_line_iterator_sumstats(
                    raw_stream,
                    file_header,
                    column_mapping,
                    resource_bytes,
                    version_bytes,
                    phenotype_bytes,
                    variant_filter,
                )
                line_iterators.append(line_iter)

        if not line_iterators or output_header is None:
            raise NotFoundException(
                f"No data found for resource '{resource}', data type '{data_type}', "
                f"phenotypes {phenotypes}"
            )

        sort_key_fn = create_sort_key(output_header, SORT_CONFIG_SUMSTATS)
        merged_iterator = merge(*line_iterators, key=sort_key_fn)
        header_line = b"\t".join(output_header) + b"\n"

        return chunk_iterator(merged_iterator, header_line, out_chunk_size)
