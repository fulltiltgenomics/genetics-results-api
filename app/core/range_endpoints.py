"""Request handling shared by the routers that range-read per-resource tabix files.

The route declarations stay in each router, because their paths, parameters and
descriptions are that router's OpenAPI surface; what a handler does once called is here.
"""

import logging
import time
from typing import Literal

from fastapi import HTTPException, Request, Response

import app.config.common as config_common
from app.core.exceptions import NotFoundException
from app.core.responses import range_response
from app.core.variant import var_re


def parse_chrom_pos(variant: str) -> tuple[str, int, list[str]]:
    """Split a chr:pos[:ref:alt] path value into (chrom, pos, all parts); 422 if malformed."""
    parts = var_re.split(variant)
    if len(parts) < 2:
        raise HTTPException(
            status_code=422,
            detail="variant must be chr:pos:ref:alt or chr:pos (separators - _ : |)",
        )
    try:
        pos = int(parts[1])
    except ValueError:
        raise HTTPException(status_code=422, detail="position must be an integer")
    return parts[0], pos, parts


class RangeEndpoints:
    """The handlers of one router's variant / region / gene range routes."""

    def __init__(
        self,
        entries: list[dict],
        header_schema: dict[str, type],
        logger: logging.Logger,
        *,
        per_dataset: bool = False,
    ):
        """`entries` are the domain's config entries, each naming its ``resource``.

        With `per_dataset`, several entries can share one resource, so the resource set
        is deduped and sorted; otherwise it is the entries' own order.
        """
        self._entries = entries
        self._per_dataset = per_dataset
        self._header_schema = header_schema
        # the router's own, so a failure is logged under the router that served it
        self._logger = logger

    def resolve_resources(self, resources: list[str] | None) -> list[str]:
        """Default to all configured resources and reject unknown ones."""
        available = [c["resource"] for c in self._entries]
        if self._per_dataset:
            available = sorted(set(available))
        if resources is None:
            return available
        invalid = [r for r in resources if r not in available]
        if invalid:
            raise HTTPException(
                status_code=404,
                detail=f"Unrecognized resource(s): {', '.join(invalid)}. Available resources: "
                + ", ".join(available),
            )
        return resources

    async def respond(
        self,
        request: Request,
        stream_coro,
        format: Literal["tsv", "json"],
        start_time: float,
    ) -> Response:
        """Await a data-access stream coroutine and wrap it in a range response."""
        try:
            stream = await stream_coro
        except NotFoundException as e:
            # includes GeneNotFoundException (by-gene resolution) -> 404
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            self._logger.exception(e)
            raise HTTPException(status_code=500, detail="Internal server error")

        return await range_response(
            str(request.url),
            stream,
            self._header_schema,
            format,
            start_time,
        )

    async def by_variant(
        self,
        request: Request,
        variant: str,
        resources: list[str] | None,
        format: Literal["tsv", "json"],
        data_access,
    ) -> Response:
        """Point read at the variant's position, filtered to ref/alt when both are given."""
        start_time = time.time()

        chrom, pos, parts = parse_chrom_pos(variant)
        ref = parts[2] if len(parts) >= 4 else None
        alt = parts[3] if len(parts) >= 4 else None

        resources = self.resolve_resources(resources)

        return await self.respond(
            request,
            data_access.stream_by_variant(
                chrom,
                pos,
                resources,
                config_common.read_chunk_size,
                config_common.response_chunk_size,
                ref,
                alt,
            ),
            format,
            start_time,
        )

    async def by_region(
        self,
        request: Request,
        chrom: str,
        start: int,
        end: int,
        resources: list[str] | None,
        format: Literal["tsv", "json"],
        data_access,
    ) -> Response:
        """Range read over [start, end]."""
        start_time = time.time()

        if end < start:
            raise HTTPException(status_code=422, detail="end must be >= start")

        resources = self.resolve_resources(resources)

        return await self.respond(
            request,
            data_access.stream_by_region(
                chrom,
                start,
                end,
                resources,
                config_common.read_chunk_size,
                config_common.response_chunk_size,
            ),
            format,
            start_time,
        )

    async def by_gene(
        self,
        request: Request,
        gene: str,
        window: int,
        resources: list[str] | None,
        format: Literal["tsv", "json"],
        data_access,
        gene_name_and_position_mapping,
    ) -> Response:
        """Range read over the gene's locus ± window."""
        start_time = time.time()

        resources = self.resolve_resources(resources)

        return await self.respond(
            request,
            data_access.stream_by_gene(
                gene,
                gene_name_and_position_mapping,
                resources,
                window,
                config_common.read_chunk_size,
                config_common.response_chunk_size,
            ),
            format,
            start_time,
        )
