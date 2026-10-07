"""A prefix's files are not one shape, so the header is cached per object.

The R13 MVP meta-analysis prefixes hold 43-, 54- and 65-column files side by side. With one
header cached per config entry, a 44-column labs file was read against a sibling's 65-column
header and its trailing rsid landed in a float column: "could not convert string to float:
'rs1023064'", on every JSON query that touched such a file after a wider one had been seen.
"""

import asyncio
from unittest.mock import patch

from app.services.sumstats_data_access import SumstatsDataAccess

MAPPING = {
    "CHR": "chr",
    "POS": "pos",
    "REF": "ref",
    "ALT": "alt",
    "SNP": "variant",
    "all_inv_var_meta_p": "pval",
    "leave_fg_inv_var_meta_p": "leave_fg_pval",
    "rsid": "rsid",
}
CONFIG = {
    "id": "mvp",
    "resource": "finngen_mvp_ukbb",
    "version": "R13",
    "prefix": "gs://b/mvp/",
    "suffix": ".tsv.gz",
    "column_mapping": MAPPING,
}
WIDE_HEADER = b"#CHR\tPOS\tREF\tALT\tSNP\tall_inv_var_meta_p\tleave_fg_inv_var_meta_p\trsid"
NARROW_HEADER = b"#CHR\tPOS\tREF\tALT\tSNP\tall_inv_var_meta_p\trsid"
FILES = {
    "gs://b/mvp/WIDE.tsv.gz": (WIDE_HEADER, b"1\t100\tA\tC\t1:100:A:C\t1e-8\t2e-8\trs1"),
    "gs://b/mvp/NARROW.tsv.gz": (NARROW_HEADER, b"1\t100\tA\tC\t1:100:A:C\t3e-8\trs2"),
}


def _header(line: bytes) -> list[bytes]:
    return [h[1:] if h.startswith(b"#") else h for h in line.split(b"\t")]


async def _rows(phenotypes):
    access = SumstatsDataAccess()
    access._initialized = True
    fetched = []

    async def exists(path):
        return path in FILES

    async def header(path):
        fetched.append(path)
        return _header(FILES[path][0])

    async def stream(path, *_args):
        async def gen():
            yield FILES[path][1] + b"\n"

        return gen()

    access._check_file_exists = exists
    access._get_header_async = header
    access._stream_range = stream
    with patch(
        "app.services.sumstats_data_access.get_data_files_by_resource_and_type",
        return_value=[CONFIG],
    ):
        chunks = await access._stream_regions(
            "finngen_mvp_ukbb", "gwas", phenotypes, [1], [1], [200], None, 1, 1
        )
        body = b"".join([c async for c in chunks])
    lines = [line.split(b"\t") for line in body.rstrip(b"\n").split(b"\n")]
    header = [h.decode() for h in lines[0]]
    return [dict(zip(header, (v.decode() for v in row))) for row in lines[1:]], fetched


def test_each_object_is_read_against_its_own_header():
    rows, fetched = asyncio.run(_rows(["WIDE", "NARROW"]))
    assert sorted(fetched) == sorted(FILES)
    by_pheno = {r["phenotype"]: r for r in rows}
    assert by_pheno["WIDE"]["rsid"] == "rs1" and by_pheno["WIDE"]["leave_fg_pval"] == "2e-8"
    assert by_pheno["NARROW"]["rsid"] == "rs2" and by_pheno["NARROW"]["pval"] == "3e-8"
    # the narrower file fills the column it lacks rather than shifting into it
    assert by_pheno["NARROW"]["leave_fg_pval"] == "NA"


def test_the_order_files_are_first_seen_in_does_not_matter():
    rows, _ = asyncio.run(_rows(["NARROW", "WIDE"]))
    by_pheno = {r["phenotype"]: r for r in rows}
    assert by_pheno["WIDE"]["leave_fg_pval"] == "2e-8"
    assert by_pheno["NARROW"]["rsid"] == "rs2"
