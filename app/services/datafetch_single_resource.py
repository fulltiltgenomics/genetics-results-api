import gzip
import math
import os
import logging
import tempfile
from typing import Any, Literal, Union
import numpy as np
import timeit
from collections import OrderedDict as od, defaultdict as dd
import asyncio

import scipy as sp  # type: ignore
from app.core.datatypes import (
    AssociationResult,
    AssociationResults,
    FineMappedResult,
    FineMappedResults,
)
from app.core.exceptions import DataException
from app.core.variant import Variant
from app.core.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


class DatafetchSingleResource(object):
    _instances = {}

    def __new__(
        cls, resource_type: Literal["assoc", "finemapped"], resource: dict[str, Any]
    ) -> "DatafetchSingleResource":
        resource_id = resource["resource"]
        instance_key = f"{resource_type}_{resource_id}"
        if instance_key not in cls._instances:
            cls._instances[instance_key] = super().__new__(cls)
            cls._instances[instance_key]._initialized = False
        return cls._instances[instance_key]

    def _init_tabix(self, filename: str) -> None:
        self.filename = filename
        with gzip.open(filename, "rt") as f:
            headers = f.readline().strip().split("\t")
        if not os.path.exists(filename + ".tbi"):
            raise DataException(f"Index file {filename}.tbi does not exist")
        self.headers: dict[str, int] = od({h: idx for idx, h in enumerate(headers)})

    def __init__(
        self, resource_type: Literal["assoc", "finemapped"], resource: dict[str, Any]
    ) -> None:  # TODO type this
        if self._initialized:
            raise DataException(
                f"{resource_type} {resource['resource']} already initialized"
            )
        self.resource = resource
        self._init_tabix(resource["file"])
        self._initialized = True
        logger.info(f"Initialized {resource_type} {resource['resource']}")

    def _parse_assoc_row(
        self, row: str, variants: list[Variant] | None
    ) -> AssociationResult:
        data = row.split("\t")
        variant = Variant(
            f"{data[self.headers['chr']]}:{data[self.headers['pos']]}:{data[self.headers['ref']]}:{data[self.headers['alt']]}"
        )
        if variants is not None and variant not in variants:
            return None
        if data[
            self.headers["beta"]
        ] != "NA" and (  # TODO check when munging data in that there is no NA or allow and report it
            "ignore_phenos" not in self.resource
            or f"{data[self.headers['trait']]}" not in self.resource["ignore_phenos"]
        ):
            dataset = data[self.headers["dataset"]]
            data_type = data[self.headers["data_type"]]
            phenocode = data[self.headers["trait"]]
            beta = float(data[self.headers["beta"]])
            sebeta = float(data[self.headers["se"]])
            mlogp = float(data[self.headers["mlog10p"]])
            # if mlog10p is missing, calculate it from beta and se
            # this is off when t distribution was used for the original
            # but we don't currently have sample size available here to use t distribution
            # this will be a lot off if there would be case/control studies
            # TODO prepare the data up front so that mlog10p is always available
            if mlogp == np.inf:
                mlogp = -sp.stats.norm.logsf(abs(beta) / sebeta) / math.log(
                    10
                ) - math.log10(2)
            result = {
                "variant": str(variant),
                "ld": False,
                "resource": self.resource["resource"],
                "dataset": dataset,
                "data_type": data_type,
                "phenocode": (
                    phenocode if data_type != "sQTL" else dataset + ":" + phenocode
                ),
                "mlog10p": mlogp,
                "beta": beta,
                "sebeta": sebeta,
            }
            return result
        return None

    def _parse_finemapped_row(self, row: str) -> FineMappedResult:
        data = row.split("\t")
        variant = Variant(
            f"{data[self.headers['chr']]}:{data[self.headers['pos']]}:{data[self.headers['ref']]}:{data[self.headers['alt']]}"
        )
        resource = data[self.headers["#resource"]]
        if (
            "method" not in self.headers or data[self.headers["method"]] == "SuSiE-inf"
        ) and (  # only use susie results from OT
            "ignore_phenos" not in self.resource
            or f"{data[self.headers['trait']]}" not in self.resource["ignore_phenos"]
        ):
            data_type = data[self.headers["data_type"]]
            # TODO fix OT files so that data_type is always GWAS
            if data_type == "gwas":
                data_type = "GWAS"
            dataset = data[self.headers["dataset"]]
            if (
                dataset == "FinnGen_drugs"
            ):  # TODO fix data file or remove resource from it
                resource = "FinnGen_drugs"
            phenocode = data[self.headers["trait"]]
            mlog10p = data[self.headers["mlog10p"]]
            beta = data[self.headers["beta"]]
            se = data[self.headers["se"]]
            pip = float(data[self.headers["pip"]])
            cs_size = data[self.headers["cs_size"]]
            cs_min_r2 = data[self.headers["cs_min_r2"]]
            result: FineMappedResult = {
                "variant": str(variant),
                "resource": resource,
                "dataset": dataset,
                "data_type": data_type,
                "phenocode": (
                    phenocode if data_type != "sQTL" else dataset + ":" + phenocode
                ),
                "mlog10p": float(mlog10p) if mlog10p != "NA" else "NA",
                "beta": float(beta) if beta != "NA" else "NA",
                "se": float(se) if se != "NA" else "NA",
                "pip": pip,
                "cs_size": int(cs_size) if cs_size != "NA" else "NA",
                "cs_min_r2": float(cs_min_r2) if cs_min_r2 != "NA" else "NA",
            }
            return result
        return None

    async def _fetch(
        self,
        resource_type: Literal["assoc", "finemapped"],
        tabix_ranges_tab_delim: str,
        variants: list[Variant] | None,
    ) -> Union[AssociationResults, FineMappedResults]:
        start_time = timeit.default_timer()
        results = dd(list)  # variant -> list of results
        try:
            with tempfile.NamedTemporaryFile(mode="w") as tmp:
                tmp.write(tabix_ranges_tab_delim)
                tmp.flush()
                process = await asyncio.create_subprocess_exec(
                    "tabix",
                    "-R",
                    tmp.name,
                    self.filename,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await process.communicate()
                if process.returncode != 0 or stderr:
                    raise DataException(
                        stderr.decode() if stderr else "Non-zero return code from tabix"
                    )
                tabix_result = stdout.decode()
        except Exception as e:
            raise DataException(str(e))

        for row in tabix_result.strip().split("\n"):
            if row == "":  # empty result
                continue
            if resource_type == "finemapped":
                result = self._parse_finemapped_row(row)
            elif resource_type == "assoc":
                result = self._parse_assoc_row(row, variants)
            if result:
                results[result["variant"]].append(result)
        if resource_type == "assoc":
            # return also placeholders so that the frontend can show something when data are filtered
            for variant in results:
                placeholder: AssociationResult = {
                    "resource": self.resource["resource"],
                    "dataset": "NA",
                    "data_type": "NA",
                    "phenocode": "NA",
                    # the NA resource will be before the others in sorted order
                    # feels hacky but is useful for the frontend
                    # TODO add NA resource to the config or remove it altogether
                    # "mlogp": 0 if resource == "NA" else -1,
                    "mlog10p": -1,
                    "beta": 0,
                    "sebeta": 0,
                }
                results[variant].append(placeholder)
        end_time = timeit.default_timer() - start_time
        return results

    async def fetch_variants(
        self,
        resource_type: Literal["assoc", "finemapped"],
        variants: list[Variant],
    ) -> Union[AssociationResults, FineMappedResults]:
        tabix_ranges_tab_delim = "\n".join(
            [f"{v.chr}\t{v.pos}\t{v.pos}" for v in variants]
        )
        return await self._fetch(resource_type, tabix_ranges_tab_delim, variants)

    async def fetch_ranges(
        self,
        resource_type: Literal["assoc", "finemapped"],
        tabix_ranges_tab_delim: str,
    ) -> Union[AssociationResults, FineMappedResults]:
        return await self._fetch(resource_type, tabix_ranges_tab_delim, None)
