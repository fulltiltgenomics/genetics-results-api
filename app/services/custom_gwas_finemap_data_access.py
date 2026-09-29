"""Credible sets of a sandbox custom GWAS, read from the fine-mapping pipeline's own files.

The pipeline writes, per phenotype, `<name>.SUSIE.snp.filter.tsv` (every variant of every
95% credible set) and `<name>.SUSIE.cred.summary.tsv` (one row per set, with its size and
purity). The core FinnGen sets were munged offline from the same two files into the
columns every credible-set endpoint serves; this class does that translation at read time,
so a run's fine-mapping is queryable with nothing staged.

There is no combined file across phenotypes, so a custom GWAS resource takes part in the
per-phenotype endpoints only: a by-variant, by-gene or by-region query skips it, exactly as
the exome resources with per-phenotype files do.
"""

import asyncio
import logging
import math
import time
from typing import Any, AsyncGenerator, Literal

from app.core.exceptions import NotFoundException
from app.core.streams import (
    accumulate_cs_leads,
    tsv_line_iterator_str,
    tsv_stream_to_list_with_header,
)
from app.services.custom_gwas_catalog import CustomGwasCatalog, get_catalog
from app.services.data_access import DataAccessObject
from app.services.gcloud_tabix_base import GCloudTabixBase, validate_path_component

logger = logging.getLogger(__name__)

# the served columns, in the order the core per-phenotype files carry them; cs_id at
# index 13 is what filter_stream_by_cs_id defaults to
SERVED_HEADER = [
    "dataset", "data_type", "trait", "trait_original", "cell_type",
    "chr", "pos", "ref", "alt",
    "mlog10p", "beta", "se", "pip", "cs_id", "cs_size", "cs_min_r2", "aaf",
    "most_severe", "gene_most_severe",
]

_CHR_ALIASES = {"X": "23", "Y": "24", "MT": "25", "M": "25"}


def _parse_tsv(text: str) -> tuple[list[str], list[dict[str, str]]]:
    lines = [line for line in text.split("\n") if line.strip()]
    if not lines:
        return [], []
    header = lines[0].split("\t")
    if header[0].startswith("#"):
        header[0] = header[0][1:]
    return header, [dict(zip(header, line.split("\t"))) for line in lines[1:]]


def _mlog10p(p: str) -> str:
    try:
        value = float(p)
    except ValueError:
        return "NA"
    if value <= 0:
        return "inf"
    return f"{-math.log10(value):.4f}"


def _round(value: str, digits: int) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except ValueError:
        return "NA"


def _na(value: str | None) -> str:
    return value if value not in (None, "") else "NA"


def translate_susie(
    members_text: str, summary_text: str, dataset: str, trait: str
) -> list[list[str]]:
    """The served rows (SERVED_HEADER order) of one phenotype's SuSiE output.

    `pip` is the variant's probability within its own set (`cs_specific_prob`), the
    quantity the core munge and the coloc munge both serve as pip; `cs_min_r2` and
    `cs_size` are joined from the summary file on (region, cs). `aaf` is NA: the pipeline
    records the minor-allele frequency, which is not the alt-allele frequency the column
    promises. Rows are ordered by position, as the core files are.
    """
    _, members = _parse_tsv(members_text)
    _, summary = _parse_tsv(summary_text)
    sets = {(row.get("region"), row.get("cs")): row for row in summary}

    rows: list[tuple[tuple[int, int], list[str]]] = []
    for m in members:
        v = m.get("v", "")
        parts = v.split(":")
        if len(parts) != 4:
            logger.warning(f"{trait}: skipping credible-set row with variant {v!r}")
            continue
        chrom, pos, ref, alt = parts
        chrom = _CHR_ALIASES.get(chrom.removeprefix("chr"), chrom.removeprefix("chr"))
        cs = sets.get((m.get("region"), m.get("cs")), {})
        try:
            sort_key = (int(chrom), int(pos))
        except ValueError:
            logger.warning(f"{trait}: skipping credible-set row with variant {v!r}")
            continue
        rows.append((sort_key, [
            dataset, "GWAS", trait, trait, "NA",
            chrom, pos, ref, alt,
            _mlog10p(m.get("p", "")), _na(m.get("beta")), _na(m.get("se")),
            _round(m.get("cs_specific_prob", ""), 4),
            f"{m.get('region')}_{m.get('cs')}",
            _na(cs.get("cs_size")), _round(cs.get("cs_min_r2", ""), 4), "NA",
            _na(m.get("most_severe")), _na(m.get("gene_most_severe")),
        ]))
    rows.sort(key=lambda r: r[0])
    return [row for _, row in rows]


class CustomGwasFinemapDataAccess(GCloudTabixBase, DataAccessObject):
    """Per-phenotype credible sets of one custom GWAS release, translated on read."""

    def __init__(self, data_file_id: str, data_type: Literal["cs", "assoc"]):
        DataAccessObject.__init__(self, data_file_id, data_type)
        from app.config.credible_sets import data_file_by_id

        df = data_file_by_id[data_file_id]
        if data_type != "cs" or "cs" not in df:
            raise ValueError(f"Data file '{data_file_id}' does not support '{data_type}'")
        GCloudTabixBase.__init__(self)
        self.resource_config = df["cs"]
        self.catalog_id: str = df["cs"]["catalog"]
        # by-gene queries select data files by the gencode version their coordinates were
        # looked up in; None keeps this one out of them, which is right: no combined file
        self.gencode_version = None
        self.header = None
        self.qtl_header = None

    @property
    def catalog(self) -> CustomGwasCatalog:
        return get_catalog(self.catalog_id)

    def get_header(self, qtl: bool = False) -> list[bytes] | None:
        return None

    async def warm(self) -> None:
        return None

    async def _paths(self, phenotype: str, interval: Literal[95, 99] | None) -> tuple[str, str]:
        validate_path_component(phenotype)
        if interval not in (None, 95):
            raise ValueError(f"Interval {interval} not available for resource {self.resource}")
        paths = self.catalog.susie_paths(phenotype)
        if paths is None:
            raise NotFoundException(f"File not found: {phenotype}")
        return paths

    async def check_phenotype_exists(
        self, phenotype: str, interval: Literal[95, 99] | None = None
    ) -> bool:
        try:
            members, _ = await self._paths(phenotype, interval)
        except (NotFoundException, ValueError):
            return False
        if self.catalog.phenotype(phenotype) is not None:
            return True
        # a name the listing has not seen yet: the convention path may or may not exist
        headers = await self.storage._headers()
        async with self.session.head(self._gcs_url(members), headers=headers) as response:
            return response.status != 404

    async def _served_rows(
        self, phenotype: str, interval: Literal[95, 99] | None
    ) -> list[list[str]]:
        started = time.time()
        members_path, summary_path = await self._paths(phenotype, interval)
        members, summary = await asyncio.gather(
            self._fetch_full(members_path), self._fetch_full(summary_path)
        )
        rows = translate_susie(
            members.decode(), summary.decode(), self.catalog.dataset_label, phenotype
        )
        logger.debug(
            f"translated {len(rows)} credible-set rows of {phenotype} from "
            f"{self.resource} in {time.time() - started:.3f}s"
        )
        return rows

    async def _line_stream(
        self, phenotype: str, interval: Literal[95, 99] | None
    ) -> AsyncGenerator[bytes, None]:
        rows = await self._served_rows(phenotype, interval)
        yield ("\t".join(SERVED_HEADER) + "\n").encode()
        for row in rows:
            yield ("\t".join(row) + "\n").encode()

    async def stream_phenotype(
        self, phenotype: str, interval: Literal[95, 99] | None, chunk_size: int
    ) -> AsyncGenerator[bytes, None]:
        return self._line_stream(phenotype, interval)

    async def json_phenotype(
        self,
        phenotype: str,
        interval: Literal[95, 99] | None,
        header_schema: dict[str, type],
        data_type: str,
        chunk_size: int,
    ) -> list[dict[str, Any]]:
        _, rows = await self.json_phenotype_with_header(
            phenotype, interval, header_schema, data_type, chunk_size
        )
        return rows

    async def json_phenotype_with_header(
        self,
        phenotype: str,
        interval: Literal[95, 99] | None,
        header_schema: dict[str, type],
        data_type: str,
        chunk_size: int,
    ) -> tuple[list[str], list[dict[str, Any]]]:
        line_stream = tsv_line_iterator_str(self._line_stream(phenotype, interval))
        return await tsv_stream_to_list_with_header(line_stream, header_schema)

    async def lead_variants_phenotype_with_header(
        self,
        phenotype: str,
        interval: Literal[95, 99] | None,
        header_schema: dict[str, type],
        chunk_size: int,
    ) -> tuple[list[str], list[dict[str, Any]]]:
        line_stream = tsv_line_iterator_str(self._line_stream(phenotype, interval))
        return await accumulate_cs_leads(line_stream, header_schema)

    async def stream_range(self, chr, start, end, chunk_size):
        raise ValueError("No combined file available for range queries")

    async def stream_qtl_gene_range(self, chr, pos, chunk_size):
        raise ValueError("No QTL gene file available")

    def has_qtl_gene_data(self) -> bool:
        return False
