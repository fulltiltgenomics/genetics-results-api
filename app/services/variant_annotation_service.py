import logging
from typing import AsyncGenerator

import app.config.common as config
from app.core.variant import Variant
from app.services.gcloud_tabix_base import GCloudTabixBase

logger = logging.getLogger(__name__)


class VariantAnnotationService(GCloudTabixBase):
    # column indices in the annotation file
    _CHR_COL = 1
    _POS_COL = 2
    _REF_COL = 3
    _ALT_COL = 4

    def _init_storage(self):
        pass

    def __init__(self) -> None:
        super().__init__()
        self._sources = config.variant_annotation_sources
        self._headers: dict[str, list[bytes]] = {}

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
        return self._filter_by_variants(raw_stream, variants)

    async def _filter_by_variants(
        self, stream: AsyncGenerator[bytes, None], variants: list[Variant]
    ) -> AsyncGenerator[bytes, None]:
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
                    fields[self._CHR_COL],
                    fields[self._POS_COL],
                    fields[self._REF_COL],
                    fields[self._ALT_COL],
                )
                if key in variant_keys:
                    yield line + b"\n"

            buffer = lines[-1]

        if buffer.strip() != b"":
            fields = buffer.split(b"\t")
            key = (
                fields[self._CHR_COL],
                fields[self._POS_COL],
                fields[self._REF_COL],
                fields[self._ALT_COL],
            )
            if key in variant_keys:
                yield buffer + b"\n"
