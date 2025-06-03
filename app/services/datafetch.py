import timeit
from typing import Any, Literal, Union
from collections import defaultdict as dd
import asyncio
import logging

from app.core.datatypes import (
    AssociationResults,
    FineMappedResults,
)
from app.core.logging_config import setup_logging
from app.core.singleton import Singleton
from app.core.variant import Variant
from app.services.datafetch_single_resource import DatafetchSingleResource

setup_logging()
logger = logging.getLogger(__name__)


class Datafetch(object, metaclass=Singleton):
    def __init__(self, conf: dict[str, Any]) -> None:
        self.conf = conf
        self.assoc_resources = [
            DatafetchSingleResource("assoc", resource)
            for resource in self.conf.get("assoc_files", [])
        ]
        self.finemapped_resources = [
            DatafetchSingleResource("finemapped", resource)
            for resource in self.conf.get("finemapped_files", [])
        ]
        logger.info(f"{len(self.assoc_resources)} assoc resources initialized")
        logger.info(
            f"{len(self.finemapped_resources)} finemapped resources initialized"
        )

    async def _fetch(
        self,
        resource_type: Literal["assoc", "finemapped"],
        tabix_ranges_tab_delim: str | None = None,
        variants: list[Variant] | None = None,
    ) -> Union[AssociationResults, FineMappedResults]:
        start_time = timeit.default_timer()

        tasks = [
            (
                resource.fetch_variants(resource_type, variants)
                if variants is not None
                else resource.fetch_ranges(resource_type, tabix_ranges_tab_delim)
            )
            for resource in (
                self.assoc_resources
                if resource_type == "assoc"
                else self.finemapped_resources
            )
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        combined_results = dd(list)
        resources_used = set()
        variants_used = set()
        for result in results:
            if isinstance(result, Exception):
                logger.error(
                    f"Error fetching {resource_type} data from a resource: {str(result)}"
                )
                continue
            for variant, variant_results in result.items():
                combined_results[variant].extend(variant_results)
                for v in variant_results:
                    resources_used.add(v["resource"])
                variants_used.add(variant)

        for variant in variants_used:
            combined_results[variant] = sorted(
                combined_results[variant],
                key=lambda x: (
                    float("-inf") if x["mlog10p"] == "NA" else -x["mlog10p"]
                ),
            )

        end_time = timeit.default_timer() - start_time
        logger.info(f"{resource_type} total time (s): {end_time}")
        return {
            "data": combined_results,
            "resources": list(resources_used),
            "time": end_time,
        }

    async def fetch_ranges(
        self,
        resource_type: Literal["assoc", "finemapped"],
        tabix_ranges_tab_delim: str,
    ) -> Union[AssociationResults, FineMappedResults]:
        return await self._fetch(resource_type, tabix_ranges_tab_delim, None)

    async def fetch_variants(
        self,
        resource_type: Literal["assoc", "finemapped"],
        variants: list[Variant],
    ) -> Union[AssociationResults, FineMappedResults]:
        return await self._fetch(resource_type, None, variants)
