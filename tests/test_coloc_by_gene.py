"""Offline pins for /colocalization_by_gene's row selection.

A range read of the gene-indexed coloc file returns every gene that overlaps the queried
locus, so the rows are kept by the exact coordinates the file was built with. Rows here are
shaped like the served file; no server and no credentials.
"""

import asyncio

from app.config.coloc import coloc_header_schema, coloc_header_schema_gene
from app.core.streams import tsv_line_iterator_coloc_gene

HEADER = [
    name.encode()
    for name in coloc_header_schema_gene
    if name not in ("resource1", "version1", "resource2", "version2")
]
LABEL_VERSIONS = {
    b"FinnGen_R14": (b"finngen", b"R14"),
    b"UKB_PPP": (b"ukbb", b"2023"),
}
SLAMF8 = {"chrom": 1, "gene_start": 159826693, "gene_end": 159837492}


def _row(**cols: str) -> bytes:
    row = {name: b"NA" for name in HEADER}
    row |= {name.encode(): value.encode() for name, value in cols.items()}
    return b"\t".join(row[name] for name in HEADER)


def _gene_row(gene: str, chrom: str, start: str, end: str, pair_chr: str) -> bytes:
    return _row(
        dataset1="FinnGen_R14", dataset2="UKB_PPP", data_type1="GWAS", data_type2="pQTL",
        trait1="T1D_WIDE", trait2=gene, chr=pair_chr, qtl_gene=gene, qtl_side="2",
        gene_chr=chrom, gene_start=start, gene_end=end,
    )  # fmt: skip


CIS = _gene_row("SLAMF8", "1", "159826693", "159837492", pair_chr="1")
TRANS = _gene_row("SLAMF8", "1", "159826693", "159837492", pair_chr="12")
# overlaps the SLAMF8 locus, so a range read returns it too
NEIGHBOUR = _gene_row("NEIGHBOUR", "1", "159800000", "159830000", pair_chr="1")


async def _collect(rows: list[bytes], loci: list[dict]) -> list[list[bytes]]:
    async def stream():
        # split mid-line: the iterator has to reassemble rows across chunks
        data = b"\n".join(rows) + b"\n"
        yield data[: len(data) // 2]
        yield data[len(data) // 2 :]

    return [
        row
        async for row in tsv_line_iterator_coloc_gene(stream(), HEADER, loci, LABEL_VERSIONS)
    ]


def test_schema_extends_the_pair_schema():
    assert list(coloc_header_schema_gene)[: len(coloc_header_schema)] == list(coloc_header_schema)


def test_keeps_cis_and_trans_rows_of_the_gene_only():
    rows = asyncio.run(_collect([NEIGHBOUR, CIS, TRANS], [SLAMF8]))
    gene = HEADER.index(b"qtl_gene") + 4
    pair_chr = HEADER.index(b"chr") + 4
    assert [(r[gene], r[pair_chr]) for r in rows] == [(b"SLAMF8", b"1"), (b"SLAMF8", b"12")]


def test_rows_carry_resource_and_version_of_both_sides():
    (row,) = asyncio.run(_collect([CIS], [SLAMF8]))
    assert row[:4] == [b"finngen", b"R14", b"ukbb", b"2023"]
    assert len(row) == len(coloc_header_schema_gene)


def test_other_coordinates_for_the_same_symbol_do_not_match():
    moved = {**SLAMF8, "gene_end": SLAMF8["gene_end"] + 1}
    assert asyncio.run(_collect([CIS, TRANS], [moved])) == []
