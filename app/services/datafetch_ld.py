import asyncio
import gzip
import math
import logging
import tempfile
from collections import defaultdict as dd
from collections import OrderedDict as od
from typing import Any
import numpy as np
import timeit
from app.core.datatypes import (
    AssociationResults,
)
from app.core.exceptions import DataException
from app.core.variant import Variant
from app.core.singleton import Singleton
from app.core.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


class LDDatafetch(object, metaclass=Singleton):
    def __init__(self, conf: dict[str, Any]) -> None:
        self.conf = conf
        self.filename = self.conf["ld_assoc"]["file"]
        with gzip.open(self.filename, "rt") as f:
            headers = f.readline().strip().split("\t")
        self.ld_assoc_headers: dict[str, int] = od(
            {h: idx for idx, h in enumerate(headers)}
        )

    async def fetch_variants(self, variants: list[Variant]) -> AssociationResults:
        start_time = timeit.default_timer()
        results = dd(list)  # variant -> list of results
        tabix_ranges_tab_delim = "\n".join(
            [f"{v.chr}\t{v.pos}\t{v.pos}" for v in variants]
        )
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

        resource = "Open_Targets"  # TODO include in data file
        for row in tabix_result.strip().split("\n"):
            if row == "":
                continue
            d = row.split("\t")
            variant = Variant(
                f"{d[self.ld_assoc_headers['tag_chrom']]}:{d[self.ld_assoc_headers['tag_pos']]}:{d[self.ld_assoc_headers['tag_ref']]}:{d[self.ld_assoc_headers['tag_alt']]}"
            )
            lead_pos = int(d[self.ld_assoc_headers["lead_pos"]])
            lead_ref = d[self.ld_assoc_headers["lead_ref"]]
            lead_alt = d[self.ld_assoc_headers["lead_alt"]]
            if variant in variants:
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
                    logger.error(
                        f"Could not parse beta or odds ratio: {beta_str} {odds_ratio_str}"
                    )
                    beta = 0
                mlogp = -math.log10(float(d[self.ld_assoc_headers["pval"]]))
                if mlogp == np.inf:
                    mlogp = -math.log10(
                        5e-324
                    )  # this is the smallest number in the ot file
                result = {
                    "variant": str(variant),
                    "ld": True,
                    "resource": resource,
                    "dataset": dataset,
                    "data_type": data_type,
                    "phenocode": phenocode,
                    "mlog10p": mlogp,
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
                results[str(variant)].append(result)
        end_time = timeit.default_timer() - start_time
        return {
            "data": results,
            "resources": [resource],
            "time": end_time,
        }
