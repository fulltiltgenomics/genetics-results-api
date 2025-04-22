import gzip
import sqlite3
import subprocess
from typing import Any
import timeit
from collections import OrderedDict as od, defaultdict as dd

from app.core.datatypes import (
    FineMappedResult,
    FineMappedResults,
)
from app.core.exceptions import DataException
from app.core.variant import Variant
from app.core.singleton import Singleton


class Finemapped(object, metaclass=Singleton):
    def _init_tabix(self) -> None:
        with gzip.open(self.conf["finemapped"]["file"], "rt") as f:
            headers = f.readline().strip().split("\t")
        self.headers: dict[str, int] = od({h: idx for idx, h in enumerate(headers)})

    def __init__(self, conf: dict[str, Any]) -> None:
        self.conf = conf
        self._init_tabix()
        self.rsid_conn: dict[int, sqlite3.Connection] = dd(
            lambda: sqlite3.connect(conf["finemapped"]["metadata_db"])
        )
        self.finemapped_resources = set(
            [resource["resource"] for resource in self.conf["finemapped"]["resources"]]
        )

    def get_finemapped_range(self, tabix_range: str) -> FineMappedResults:
        start_time = timeit.default_timer()
        finemapped = dd(lambda: {"data": [], "resources": set()})
        try:
            result = subprocess.run(
                [
                    "tabix",
                    self.conf["finemapped"]["file"],
                    tabix_range,
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
            data = row.split("\t")
            resource = data[self.headers["#resource"]]
            if (
                # skip results for resources not in the config
                resource
                in self.finemapped_resources
                and f"{data[self.headers['dataset']]}:{data[self.headers['trait']]}"
                not in self.conf["ignore_phenos"]["assoc"]
            ):
                data_type = data[self.headers["data_type"]]
                dataset = data[self.headers["dataset"]]
                phenocode = data[self.headers["trait"]]
                result: FineMappedResult = {
                    "resource": resource,
                    "dataset": dataset,
                    "data_type": data_type,
                    "phenocode": (
                        phenocode if data_type != "sQTL" else dataset + ":" + phenocode
                    ),
                    "mlog10p": float(data[self.headers["mlog10p"]]),
                    "beta": float(data[self.headers["beta"]]),
                    "se": float(data[self.headers["se"]]),
                    "pip": float(data[self.headers["pip"]]),
                    "cs_size": int(data[self.headers["cs_size"]]),
                    "cs_min_r2": float(data[self.headers["cs_min_r2"]]),
                }
                variant = Variant(
                    f"{data[self.headers['chr']]}-{data[self.headers['pos']]}-{data[self.headers['ref']]}-{data[self.headers['alt']]}"
                )
                finemapped[str(variant)]["data"] = finemapped[str(variant)]["data"] + [
                    result
                ]
                finemapped[str(variant)]["resources"].add(resource)

        for variant in finemapped:
            finemapped[variant]["data"] = sorted(
                finemapped[variant]["data"], key=lambda x: -float(x["pip"])
            )
            # keep order of resources from the config
            # TODO why not doing the same in assoc?
            finemapped[str(variant)]["resources"] = [
                resource["resource"]
                for resource in self.conf["finemapped"]["resources"]
                if resource["resource"] in finemapped[str(variant)]["resources"]
            ]
        end_time = timeit.default_timer() - start_time
        return {
            "finemapped": {
                "data": finemapped,
            },
            "time": end_time,
        }

    def get_finemapped(self, variant: Variant) -> FineMappedResults:
        start_time = timeit.default_timer()
        finemapped: list[FineMappedResult] = []
        found_resources = set()
        try:
            result = subprocess.run(
                [
                    "tabix",
                    self.conf["finemapped"]["file"],
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
            data = row.split("\t")
            resource = data[self.headers["#resource"]]
            if (
                data[self.headers["ref"]] == variant.ref
                and data[self.headers["alt"]] == variant.alt
                # skip results for resources not in the config
                and resource in self.finemapped_resources
                and f"{data[self.headers['dataset']]}:{data[self.headers['trait']]}"
                not in self.conf["ignore_phenos"]["assoc"]
            ):
                found_resources.add(resource)
                data_type = data[self.headers["data_type"]]
                dataset = data[self.headers["dataset"]]
                phenocode = data[self.headers["trait"]]
                result: FineMappedResult = {
                    "resource": resource,
                    "dataset": dataset,
                    "data_type": data_type,
                    "phenocode": (
                        phenocode if data_type != "sQTL" else dataset + ":" + phenocode
                    ),
                    "mlog10p": float(data[self.headers["mlog10p"]]),
                    "beta": float(data[self.headers["beta"]]),
                    "se": float(data[self.headers["se"]]),
                    "pip": float(data[self.headers["pip"]]),
                    "cs_size": int(data[self.headers["cs_size"]]),
                    "cs_min_r2": float(data[self.headers["cs_min_r2"]]),
                }
                finemapped.append(result)
        end_time = timeit.default_timer() - start_time
        return {
            "variant": str(variant),
            "finemapped": {
                "data": sorted(finemapped, key=lambda x: -float(x["pip"])),
                # keep order of resources from the config
                "resources": [
                    resource["resource"]
                    for resource in self.conf["finemapped"]["resources"]
                    if resource["resource"] in found_resources
                ],
            },
            "time": end_time,
        }
