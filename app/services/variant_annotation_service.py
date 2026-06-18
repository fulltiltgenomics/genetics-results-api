import logging
from typing import AsyncGenerator

import app.config.common as config
from app.core.variant import Variant
from app.services.gcloud_tabix_base import GCloudTabixBase

logger = logging.getLogger(__name__)


class VariantAnnotationService(GCloudTabixBase):
    # default cpra (chr/pos/ref/alt) column indices when a source omits "cpra_cols"
    _DEFAULT_CPRA_COLS = (1, 2, 3, 4)

    def __init__(self) -> None:
        super().__init__()
        self._sources = config.variant_annotation_sources
        self._headers: dict[str, list[bytes]] = {}

    def _cpra_cols(self, source: str) -> tuple[int, int, int, int]:
        return tuple(self._sources[source].get("cpra_cols", self._DEFAULT_CPRA_COLS))

    def get_available_sources(self) -> list[str]:
        return list(self._sources.keys())

    def get_header(self, source: str) -> list[bytes]:
        if source not in self._headers:
            self._headers[source] = self._cache_header(
                f"_va_header_{source}", self._sources[source]["file"]
            )
        return self._headers[source]

    async def stream_by_range(
        self, source: str, chr: int, start: int, end: int
    ) -> AsyncGenerator[bytes, None]:
        return await self._stream_range(
            self._sources[source]["file"],
            [chr],
            [start],
            [end],
            config.read_chunk_size,
        )

    async def stream_by_variants(
        self, source: str, variants: list[Variant]
    ) -> AsyncGenerator[bytes, None]:
        chrs = [v.chr for v in variants]
        starts = [v.pos for v in variants]
        ends = [v.pos for v in variants]
        raw_stream = await self._stream_range(
            self._sources[source]["file"],
            chrs,
            starts,
            ends,
            config.read_chunk_size,
        )
        return self._filter_by_variants(raw_stream, variants, self._cpra_cols(source))

    async def _filter_by_variants(
        self,
        stream: AsyncGenerator[bytes, None],
        variants: list[Variant],
        cpra_cols: tuple[int, int, int, int],
    ) -> AsyncGenerator[bytes, None]:
        chr_col, pos_col, ref_col, alt_col = cpra_cols
        variant_keys = {
            (v.chr_bytes, v.pos_bytes, v.ref_bytes, v.alt_bytes) for v in variants
        }
        buffer = b""

        async for chunk in stream:
            data = buffer + chunk
            lines = data.split(b"\n")

            for line in lines[:-1]:
                if line.strip() == b"":
                    continue
                fields = line.split(b"\t")
                key = (
                    fields[chr_col],
                    fields[pos_col],
                    fields[ref_col],
                    fields[alt_col],
                )
                if key in variant_keys:
                    yield line + b"\n"

            buffer = lines[-1]

        if buffer.strip() != b"":
            fields = buffer.split(b"\t")
            key = (
                fields[chr_col],
                fields[pos_col],
                fields[ref_col],
                fields[alt_col],
            )
            if key in variant_keys:
                yield buffer + b"\n"
