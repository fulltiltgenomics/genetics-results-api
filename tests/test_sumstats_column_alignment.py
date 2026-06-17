"""Regression tests for summary-stats column alignment across heterogeneous files.

Reproduces the bug where a single sumstats query spanning files with different
schemas (FinnGen R14 core GWAS has 13 columns incl. rsids/nearest_genes/af_cases/
af_controls; the R14 Kanta lab files have 9) serialized every row against the
schema of whichever file returned data first. Kanta rows then misaligned against
the core header — e.g. Kanta's beta was emitted in the `pval` slot, producing a
negative "p-value", and Kanta's pval landed in the `rsids` slot.

The fix builds a shared output schema as the union of all contributing files'
columns; each row positions its values by name and fills NA for absent columns.
"""

import asyncio

from app.core.streams import (
    mapped_output_columns,
    union_output_columns,
    tsv_line_iterator_sumstats,
)

# the two real FinnGen schemas involved in the bug
CORE_MAPPING = {
    "chrom": "chr",
    "pos": "pos",
    "ref": "ref",
    "alt": "alt",
    "rsids": "rsids",
    "nearest_genes": "nearest_genes",
    "pval": "pval",
    "mlogp": "mlog10p",
    "beta": "beta",
    "sebeta": "se",
    "af_alt": "af",
    "af_alt_cases": "af_cases",
    "af_alt_controls": "af_controls",
}
CORE_HEADER = [c.encode() for c in CORE_MAPPING]  # source names == file header

KANTA_MAPPING = {
    "chrom": "chr",
    "pos": "pos",
    "ref": "ref",
    "alt": "alt",
    "pval": "pval",
    "mlogp": "mlog10p",
    "beta": "beta",
    "sebeta": "se",
    "af_alt": "af",
}
KANTA_HEADER = [c.encode() for c in KANTA_MAPPING]

# real LDLadjStatin_IRN row at 1:55039974:G:T (chrom pos ref alt pval mlogp beta sebeta af_alt)
KANTA_ROW = b"1\t55039974\tG\tT\t0\t1592.11\t-0.47656\t0.00556909\t0.0361582"
CORE_ROW = (
    b"1\t55039974\tG\tT\trs12\tPCSK9\t1.2e-30\t29.9\t0.31\t0.02\t0.04\t0.05\t0.039"
)


async def _aiter(*lines: bytes):
    yield b"\n".join(lines) + b"\n"


def _collect(file_header, mapping, output_columns, row):
    it = tsv_line_iterator_sumstats(
        _aiter(row),
        file_header,
        mapping,
        output_columns,
        b"finngen",
        b"R14",
        b"PHENO",
        None,
    )

    async def run():
        return [r async for r in it]

    rows = asyncio.run(run())
    assert len(rows) == 1
    full_header = ["resource", "version", "phenotype"] + output_columns
    return dict(zip(full_header, [c.decode() for c in rows[0]]))


def test_mapped_output_columns_drops_absent_source_columns():
    assert mapped_output_columns(KANTA_MAPPING, KANTA_HEADER) == [
        "chr", "pos", "ref", "alt", "pval", "mlog10p", "beta", "se", "af",
    ]


def test_union_takes_superset_in_first_seen_order():
    # core first, then kanta (a subset) -> union is exactly the core columns
    unified = union_output_columns([(CORE_MAPPING, CORE_HEADER), (KANTA_MAPPING, KANTA_HEADER)])
    assert unified == [
        "chr", "pos", "ref", "alt", "rsids", "nearest_genes",
        "pval", "mlog10p", "beta", "se", "af", "af_cases", "af_controls",
    ]
    # order independent: kanta first still yields the same superset, kanta cols first
    unified_rev = union_output_columns([(KANTA_MAPPING, KANTA_HEADER), (CORE_MAPPING, CORE_HEADER)])
    assert set(unified_rev) == set(unified)
    assert unified_rev[:9] == [
        "chr", "pos", "ref", "alt", "pval", "mlog10p", "beta", "se", "af",
    ]


def test_kanta_row_aligns_against_shared_core_schema():
    """The core of the bug: a Kanta (9-col) row served under the union schema must
    keep beta in `beta` and pval in `pval`, with NA for columns it lacks."""
    unified = union_output_columns([(CORE_MAPPING, CORE_HEADER), (KANTA_MAPPING, KANTA_HEADER)])
    row = _collect(KANTA_HEADER, KANTA_MAPPING, unified, KANTA_ROW)

    # values land in the correct named columns, not shifted
    assert row["chr"] == "1"
    assert row["pos"] == "55039974"
    assert row["pval"] == "0"           # was reading beta (-0.47656) before the fix
    assert row["beta"] == "-0.47656"    # beta stays in beta
    assert row["mlog10p"] == "1592.11"
    assert row["se"] == "0.00556909"
    assert row["af"] == "0.0361582"
    # columns the Kanta file does not have are NA, not borrowed from neighbors
    assert row["rsids"] == "NA"         # was reading pval (0) before the fix
    assert row["nearest_genes"] == "NA"
    assert row["af_cases"] == "NA"
    assert row["af_controls"] == "NA"


def test_core_row_unchanged_under_shared_schema():
    unified = union_output_columns([(CORE_MAPPING, CORE_HEADER), (KANTA_MAPPING, KANTA_HEADER)])
    row = _collect(CORE_HEADER, CORE_MAPPING, unified, CORE_ROW)
    assert row["rsids"] == "rs12"
    assert row["nearest_genes"] == "PCSK9"
    assert row["pval"] == "1.2e-30"
    assert row["beta"] == "0.31"
    assert row["af"] == "0.04"
    assert row["af_cases"] == "0.05"
    assert row["af_controls"] == "0.039"


def test_single_file_query_output_is_just_that_file():
    """Single-dataset queries are unaffected: union of one config == its own columns."""
    unified = union_output_columns([(KANTA_MAPPING, KANTA_HEADER)])
    assert unified == mapped_output_columns(KANTA_MAPPING, KANTA_HEADER)
    row = _collect(KANTA_HEADER, KANTA_MAPPING, unified, KANTA_ROW)
    assert row["pval"] == "0"
    assert row["beta"] == "-0.47656"
    assert "rsids" not in row
