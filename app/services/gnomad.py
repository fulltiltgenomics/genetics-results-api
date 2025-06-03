import gzip
import logging
from typing import Any, Optional, TypedDict
import timeit
import json
from collections import OrderedDict as od, defaultdict as dd
from app.core.exceptions import DataException, VariantNotFoundException
from app.core.singleton import Singleton
from app.core.variant import Variant
from app.core.logging_config import setup_logging
from typing import TypedDict
import tempfile
import asyncio

setup_logging()
logger = logging.getLogger(__name__)


class Csq(TypedDict):
    gene_ids: list[str]
    consequences: set[str]


class Csq_dict(TypedDict):
    gene_symbol: str
    consequence: str


class GnomAD(object, metaclass=Singleton):
    def __init__(self, conf: dict[str, Any]) -> None:
        self.conf = conf
        self._init_tabix()

    def _init_tabix(self) -> None:
        with gzip.open(self.conf["gnomad"]["file"], "rt") as f:
            headers = f.readline().strip().split("\t")
        self.gnomad_headers: dict[str, int] = od({h: i for i, h in enumerate(headers)})

    async def _fetch(
        self,
        tabix_ranges_tab_delim: str,
        variants: list[Variant] | None,
        gene: str | None,
    ) -> dict[str, Any]:
        start_time: float = timeit.default_timer()
        with tempfile.NamedTemporaryFile(mode="w") as tmp:
            tmp.write(tabix_ranges_tab_delim)
            tmp.flush()
            process = await asyncio.create_subprocess_exec(
                "tabix",
                "-R",
                tmp.name,
                self.conf["gnomad"]["file"],
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            if process.returncode != 0 or stderr:
                raise DataException(
                    stderr.decode() if stderr else "Non-zero return code from tabix"
                )
            if not stdout:
                raise VariantNotFoundException(f"No variants found")
            result = stdout.decode()

        ac0_variants = set()
        found_variants = set()
        gnomad_results = dd(lambda: {"exomes": None, "genomes": None})
        for row in result.strip().split("\n"):
            data = row.split("\t")
            variant = Variant(
                f"{data[self.gnomad_headers['#chr']]}:{data[self.gnomad_headers['pos']]}:{data[self.gnomad_headers['ref']]}:{data[self.gnomad_headers['alt']]}"
            )
            if (
                row == ""
                or (variants is not None and variant not in variants)
                or (
                    gene is not None
                    and data[self.gnomad_headers["gene_most_severe"]].upper()
                    != gene.upper()
                )
            ):
                continue
            found_variants.add(str(variant))
            data = self._get_gnomad_fields(data)
            if data["genome_or_exome"] == "e":
                gnomad_results[str(variant)]["exomes"] = data
            elif data["genome_or_exome"] == "g":
                gnomad_results[str(variant)]["genomes"] = data
            if (
                gnomad_results[str(variant)]["exomes"] is None
                or (
                    gnomad_results[str(variant)]["exomes"]["filters"] is not None
                    and "AC0" in gnomad_results[str(variant)]["exomes"]["filters"]
                )
            ) and (
                gnomad_results[str(variant)]["genomes"] is None
                or (
                    gnomad_results[str(variant)]["genomes"]["filters"] is not None
                    and "AC0" in gnomad_results[str(variant)]["genomes"]["filters"]
                )
            ):
                ac0_variants.add(str(variant))

        if len(found_variants) == 0:
            raise VariantNotFoundException(f"No variants found")

        # calculate min and max AFs over populations
        for v in gnomad_results:
            for ge in ["exomes", "genomes"]:
                d = gnomad_results[v][ge]
                if d is not None:
                    popmax_pop = "NA"
                    popmax_af = 0
                    popmin_pop = "NA"
                    popmin_af = 1
                    for k in d.keys():
                        if k.startswith("AF_") and d[k] is not None:
                            if d[k] > popmax_af:
                                popmax_af = d[k]
                                popmax_pop = k.replace("AF_", "")
                            if d[k] < popmin_af:
                                popmin_af = d[k]
                                popmin_pop = k.replace("AF_", "")
                    d["popmax"] = {"pop": popmax_pop, "af": popmax_af}
                    d["popmin"] = {"pop": popmin_pop, "af": popmin_af}
            # prefer exomes over genomes only if AN in exomes is higher than AN in genomes
            gnomad_results[v]["preferred"] = "genomes"
            if gnomad_results[v]["genomes"] is None or (
                gnomad_results[v]["exomes"] is not None
                and gnomad_results[v]["exomes"]["AN"]
                > gnomad_results[v]["genomes"]["AN"]
            ):
                gnomad_results[v]["preferred"] = "exomes"

        try:
            freq_summary = self.summarize_freq(list(gnomad_results.values()))
        except IndexError as e:
            logger.error(e)
            freq_summary = []

        end_time: float = timeit.default_timer() - start_time
        logger.info(f"gnomad fetch time (s): {end_time}")

        return {
            "found_variants": list(found_variants),
            "ac0_variants": list(ac0_variants),
            "data": gnomad_results,
            "freq_summary": freq_summary,
            "time": end_time,
        }

    async def fetch_ranges(
        self, tabix_ranges_tab_delim: str, gene: str | None
    ) -> dict[str, Any]:
        return await self._fetch(tabix_ranges_tab_delim, None, gene)

    async def fetch_variants(
        self, variants: list[Variant], gene: str | None
    ) -> dict[str, Any]:
        tabix_ranges_tab_delim = "\n".join(
            [f"{v.chr}\t{v.pos}\t{v.pos}" for v in variants]
        )
        return await self._fetch(tabix_ranges_tab_delim, variants, gene)

    def summarize_freq(
        self, data: list[Any]
    ) -> list[dict[str, str | int | float]]:  # TODO type
        max_freqs: dict[str, float] = {}
        for c in data:
            max_freq = max(
                (
                    (k.split("_")[1], v)
                    for k, v in (c.get("exomes") or c.get("genomes")).items()
                    if k.startswith("AF_") and v is not None
                ),
                key=lambda x: x[1],
                default=("", 0),
            )
            max_freqs[max_freq[0]] = max_freqs.get(max_freq[0], 0) + 1

        min_freqs: dict[str, float] = {}
        for c in data:
            min_freq = min(
                (
                    (k.split("_")[1], v)
                    for k, v in (c.get("exomes") or c.get("genomes")).items()
                    if k.startswith("AF_") and v is not None
                ),
                key=lambda x: x[1],
                default=("", 1),
            )
            min_freqs[min_freq[0]] = min_freqs.get(min_freq[0], 0) + 1

        try:
            keys = data[0]["genomes"].keys()
        except AttributeError:
            keys = data[0]["exomes"].keys()
        all_pops = [k.split("_")[1] for k in keys if k.startswith("AF_")]
        all_pops_freqs = [
            {
                "pop": pop,
                "max": max_freqs.get(pop, 0),
                "maxPerc": max_freqs.get(pop, 0) / len(data),
                "min": min_freqs.get(pop, 0),
                "minPerc": min_freqs.get(pop, 0) / len(data),
            }
            for pop in all_pops
        ]

        return all_pops_freqs

    # workaround for Union with empty list
    # https://stackoverflow.com/questions/58906541/incompatible-types-in-assignment-expression-has-type-listnothing-variabl
    def _get_empty_csq(self) -> list[Csq_dict]:
        return []

    def _get_gnomad_fields(
        self, data: dict[int, str | int | float]
    ) -> dict[str, str | int | float | Optional[list[Csq_dict]]]:
        gnomad: dict[
            str,
            str | int | float | Optional[list[Csq_dict]],
        ] = od()
        for h in self.gnomad_headers:
            if (
                data[self.gnomad_headers[h]] == "NA"
                or data[self.gnomad_headers[h]] == ""
            ):
                gnomad[h] = (
                    None if h.lower() != "consequences" else self._get_empty_csq()
                )
            elif h.lower() == "consequences":
                gnomad[h] = self._group_gnomad_consequences(
                    json.loads(str(data[self.gnomad_headers[h]]))
                )
            elif h.lower() == "pos" or h.lower() == "an":
                gnomad[h] = int(data[self.gnomad_headers[h]])
            elif h.lower().startswith("af"):
                gnomad[h] = float(data[self.gnomad_headers[h]])
            elif h.lower() == "most_severe":
                gnomad[h] = (
                    str(data[self.gnomad_headers[h]])
                    .replace("_variant", "")
                    .replace("_", " ")
                )
            else:
                gnomad[h] = data[self.gnomad_headers[h]]
        return gnomad

    def _group_gnomad_consequences(
        self, consequences: list[dict[str, str]]
    ) -> list[Csq_dict]:
        csq: dict[str, Csq] = dd(lambda: {"gene_ids": [], "consequences": set()})
        for c in consequences:
            csq[c["gene_symbol"]]["gene_ids"].append(c["gene_id"])
            csq[c["gene_symbol"]]["consequences"].update(c["consequences"])
        return [
            {
                "gene_symbol": k,
                "consequence": c.replace("_variant", "").replace("_", " "),
            }
            for k, v in csq.items()
            for c in v["consequences"]
        ]
