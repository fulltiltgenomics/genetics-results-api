import gzip
import math
import subprocess
from typing import Any
import numpy as np
import timeit
from collections import OrderedDict as od, defaultdict as dd

import scipy as sp  # type: ignore
from app.core.datatypes import (
    AssociationResult,
    AssociationResults,
)
from app.core.exceptions import DataException
from app.core.variant import Variant
from app.core.singleton import Singleton


class Datafetch(object, metaclass=Singleton):
    def _init_tabix(self) -> None:
        with gzip.open(self.conf["assoc"]["file"], "rt") as f:
            headers = f.readline().strip().split("\t")
        self.assoc_headers: dict[str, int] = od(
            {h: idx for idx, h in enumerate(headers)}
        )
        with gzip.open(self.conf["ld_assoc"]["file"], "rt") as f:
            headers = f.readline().strip().split("\t")
        self.ld_assoc_headers: dict[str, int] = od(
            {h: idx for idx, h in enumerate(headers)}
        )

    def __init__(self, conf: dict[str, Any]) -> None:
        self.conf = conf
        self._init_tabix()
        self.assoc_resource_ids = set(
            [resource["resource"] for resource in self.conf["assoc"]["resources"]]
        )
        # NA resource placeholder
        self.assoc_resource_ids.add("NA")

    def get_assoc_range(self, tabix_range: str) -> AssociationResults:
        start_time = timeit.default_timer()
        assoc = dd(lambda: {"data": [], "resources": set()})
        try:
            result = subprocess.run(
                ["tabix", self.conf["assoc"]["file"], tabix_range],
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise DataException from e
        if result.stderr:
            raise DataException(result.stderr)
        for row in result.stdout.strip().split("\n"):
            if row == "":
                continue
            data = row.split("\t")
            resource = data[self.assoc_headers["#resource"]]
            if (
                resource
                in self.assoc_resource_ids  # skip results for resources not in the config
                and data[self.assoc_headers["beta"]]
                != "NA"  # TODO check when munging data in that there is no NA or allow and report it
                and f"{data[self.assoc_headers['dataset']]}:{data[self.assoc_headers['trait']]}"
                not in self.conf["ignore_phenos"]["assoc"]
            ):
                dataset = data[self.assoc_headers["dataset"]]
                data_type = data[self.assoc_headers["data_type"]]
                phenocode = data[self.assoc_headers["trait"]]
                beta = float(data[self.assoc_headers["beta"]])
                sebeta = float(data[self.assoc_headers["se"]])
                mlogp = float(data[self.assoc_headers["mlog10p"]])
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
                    "ld": False,
                    "resource": resource,
                    "dataset": dataset,
                    "data_type": data_type,
                    "phenocode": (
                        phenocode if data_type != "sQTL" else dataset + ":" + phenocode
                    ),
                    "mlogp": mlogp,
                    "beta": beta,
                    "sebeta": sebeta,
                }
                variant = Variant(
                    f"{data[self.assoc_headers['chr']]}-{data[self.assoc_headers['pos']]}-{data[self.assoc_headers['ref']]}-{data[self.assoc_headers['alt']]}"
                )
                assoc[str(variant)]["data"] = assoc[str(variant)]["data"] + [result]
                assoc[str(variant)]["resources"].add(resource)
        # return also placeholders so that the frontend can show something when data are filtered
        for variant in assoc:
            for resource in self.assoc_resource_ids:
                placeholder: AssociationResult = {
                    "resource": resource,
                    "dataset": "NA",
                    "data_type": "NA",
                    "phenocode": "NA",
                    # the NA resource will be before the others in sorted order
                    # feels hacky but is useful for the frontend
                    "mlogp": 0 if resource == "NA" else -1,
                    "beta": 0,
                    "sebeta": 0,
                }
                assoc[variant]["data"].append(placeholder)
            assoc[variant]["data"] = sorted(
                assoc[variant]["data"], key=lambda x: -float(x["mlogp"])
            )
            assoc[variant]["resources"] = sorted(list(assoc[variant]["resources"]))
        end_time = timeit.default_timer() - start_time
        return {
            "assoc": {
                "data": assoc,
            },
            "time": end_time,
        }

    def get_assoc(self, variant: Variant) -> AssociationResults:
        start_time = timeit.default_timer()
        assoc: list[AssociationResult] = []
        resources = set()
        try:
            result = subprocess.run(
                [
                    "tabix",
                    self.conf["assoc"]["file"],
                    f"{variant.chr}:{variant.pos}-{variant.pos}",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise DataException from e
        if result.stderr:
            raise DataException(result.stderr)
        for row in result.stdout.strip().split("\n"):
            if row == "":
                continue
            d = row.split("\t")
            ref = d[self.assoc_headers["ref"]]
            alt = d[self.assoc_headers["alt"]]
            resource = d[self.assoc_headers["#resource"]]
            if (
                ref == variant.ref
                and alt == variant.alt
                and resource
                in self.assoc_resource_ids  # skip results for resources not in the config
                and d[self.assoc_headers["beta"]]
                != "NA"  # TODO check when munging data in that there is no NA or allow and report it
                and f"{d[self.assoc_headers['dataset']]}:{d[self.assoc_headers['trait']]}"
                not in self.conf["ignore_phenos"]["assoc"]
            ):
                dataset = d[self.assoc_headers["dataset"]]
                data_type = d[self.assoc_headers["data_type"]]
                phenocode = d[self.assoc_headers["trait"]]
                beta = float(d[self.assoc_headers["beta"]])
                sebeta = float(d[self.assoc_headers["se"]])
                mlogp = float(d[self.assoc_headers["mlog10p"]])
                # if mlog10p is missing, calculate it from beta and se
                # this is off when t distribution was used for the original
                # but we don't currently have sample size available here to use t distribution
                # this will be a lot off if there would be case/control studies
                # TODO prepare the data up front so that mlog10p is always available
                if mlogp == np.inf:
                    mlogp = -sp.stats.norm.logsf(abs(beta) / sebeta) / math.log(
                        10
                    ) - math.log10(2)
                assoc.append(
                    {
                        "ld": False,
                        "resource": resource,
                        "dataset": dataset,
                        "data_type": data_type,
                        "phenocode": (
                            phenocode
                            if data_type != "sQTL"
                            else dataset + ":" + phenocode
                        ),
                        "mlogp": mlogp,
                        "beta": beta,
                        "sebeta": sebeta,
                    }
                )
                resources.add(resource)
        # return also placeholders so that the frontend can show something when data are filtered
        for resource in self.assoc_resource_ids:
            placeholder: AssociationResult = {
                "resource": resource,
                "dataset": "NA",
                "data_type": "NA",
                "phenocode": "NA",
                # the NA resource will be before the others in sorted order
                # feels hacky but is useful for the frontend
                "mlogp": 0 if resource == "NA" else -1,
                "beta": 0,
                "sebeta": 0,
            }
            assoc.append(placeholder)
        assoc = sorted(assoc, key=lambda x: -float(x["mlogp"]))
        end_time = timeit.default_timer() - start_time
        return {
            "variant": str(variant),
            "assoc": {
                "data": assoc,
                "resources": sorted(list(resources)),
            },
            "time": end_time,
        }

    def get_ld_assoc(self, variant: Variant) -> AssociationResults:
        start_time = timeit.default_timer()
        assoc: list[AssociationResult] = []
        resources = set()
        try:
            result = subprocess.run(
                [
                    "tabix",
                    self.conf["ld_assoc"]["file"],
                    f"{variant.chr}:{variant.pos}-{variant.pos}",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise DataException from e
        if result.stderr:
            raise DataException(result.stderr)
        for row in result.stdout.strip().split("\n"):
            if row == "":
                continue
            d = row.split("\t")
            ref = d[self.ld_assoc_headers["tag_ref"]]
            alt = d[self.ld_assoc_headers["tag_alt"]]
            lead_pos = int(d[self.ld_assoc_headers["lead_pos"]])
            lead_ref = d[self.ld_assoc_headers["lead_ref"]]
            lead_alt = d[self.ld_assoc_headers["lead_alt"]]
            resource = "Open_Targets"  # TODO include in data file
            if (
                ref == variant.ref
                and alt == variant.alt
                and resource
                in self.assoc_resource_ids  # skip results for resources not in the config
            ):
                dataset = "Open_Targets_22.09"  # TODO include in data file
                data_type = "GWAS"  # TODO include in data file
                phenocode = d[self.ld_assoc_headers["#study_id"]]
                beta_str = d[self.ld_assoc_headers["beta"]]
                odds_ratio_str = d[self.ld_assoc_headers["odds_ratio"]]
                try:
                    overall_r2 = float(d[self.ld_assoc_headers["overall_r2"]])
                except ValueError:
                    overall_r2 = 0
                try:
                    if beta_str != "None":
                        beta = float(beta_str)
                    elif odds_ratio_str != "None":
                        beta = math.log(float(odds_ratio_str))
                    else:  # there are missing effect sizes in the data
                        beta = 0
                except ValueError:
                    print(
                        f"Could not parse beta or odds ratio: {beta_str} {odds_ratio_str}"
                    )
                    beta = 0
                mlogp = -math.log10(float(d[self.ld_assoc_headers["pval"]]))
                if mlogp == np.inf:
                    mlogp = -math.log10(
                        5e-324
                    )  # this is the smallest number in the ot file
                result = {
                    "ld": True,
                    "resource": resource,
                    "dataset": dataset,
                    "data_type": data_type,
                    "phenocode": phenocode,
                    "mlogp": mlogp,
                    "beta": beta,
                    "sebeta": -1,
                    "overall_r2": overall_r2,
                }
                if (
                    lead_pos == variant.pos
                    and lead_ref == variant.ref
                    and lead_alt == variant.alt
                ):
                    result["lead"] = True
                else:
                    result["lead"] = False
                    result["lead_chr"] = variant.chr
                    result["lead_pos"] = lead_pos
                    result["lead_ref"] = lead_ref
                    result["lead_alt"] = lead_alt
                assoc.append(result)
                resources.add(resource)
        assoc = sorted(assoc, key=lambda x: -float(x["mlogp"]))
        end_time = timeit.default_timer() - start_time
        return {
            "variant": str(variant),
            "assoc": {
                "data": assoc,
                "resources": sorted(list(resources)),
            },
            "time": end_time,
        }

    def get_assoc_and_ld_assoc(self, variant: Variant) -> AssociationResults:
        assoc = self.get_assoc(variant)
        ld_assoc = self.get_ld_assoc(variant)
        # merge the two results
        assoc["assoc"]["data"].extend(ld_assoc["assoc"]["data"])
        assoc["assoc"]["resources"].extend(ld_assoc["assoc"]["resources"])
        # sort by mlogp
        assoc["assoc"]["data"] = sorted(
            assoc["assoc"]["data"], key=lambda x: -float(x["mlogp"])
        )
        return assoc
