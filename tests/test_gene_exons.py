"""Exon structure on the region lookup: where it comes from, and where it deliberately isn't.

The gene track in a locuszoom and the browser's CSPlot both draw exons, and both reach them
through /genes_in_region. What the API adds is four positional lists per gene; what it must
never do is attach one GENCODE release's exons to another release's gene bodies, because the
coordinates move between releases and the result would be wrong rather than absent.

Nothing here touches GCS: the frames are built in memory and _exons_by_gene is pointed at a
local file, which is the whole of what it does with its argument.
"""

import gzip
import json

import polars as pl
import pytest

from app.routers.genes import _tsv_value
from app.services.gene_name_and_position_mapping import (
    EXON_COLUMNS,
    GENES_IN_REGION_COLUMNS,
    NEAREST_GENES_COLUMNS,
    GeneNameAndPositionMapping,
    _exons_by_gene,
    _with_exon_columns,
)

EXON_TSV = "\n".join(
    [
        "gene_id\ttranscript_id\tchrom\texon_start\texon_end\tcds_start\tcds_end",
        # deliberately out of positional order, and the second exon of PCSK9 first, so a
        # missing sort shows up as a scrambled list rather than as an equal one
        "ENSG2.1\tENST2.1\t1\t55043843\t55044063\t55043843\t55044063",
        "ENSG2.1\tENST2.1\t1\t55039445\t55039763\tNA\tNA",
        "ENSG1.4\tENST1.4\t1\t54998933\t54999100\t54998950\t54999100",
    ]
)


@pytest.fixture
def exon_file(tmp_path):
    path = tmp_path / "exons.tsv.gz"
    path.write_bytes(gzip.compress(EXON_TSV.encode()))
    return str(path)


def gene_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "gene_id": ["ENSG1.4", "ENSG2.1", "ENSG3.2"],
            "gene_name": ["BSND", "PCSK9", "NOEXONS"],
            "chrom": [1, 1, 1],
            "gene_start": [54998933, 55039445, 55070000],
            "gene_end": [55017172, 55064852, 55080000],
            "gene_strand": ["+", "+", "-"],
            "gene_type": ["protein_coding"] * 3,
            "hgnc_symbol": ["BSND", "PCSK9", None],
            "hgnc_name": [None, None, None],
            "hgnc_alias_symbol": [None, None, None],
            "hgnc_prev_symbol": [None, None, None],
        }
    )


def mapping_with(frame: pl.DataFrame, version=49) -> GeneNameAndPositionMapping:
    mapping = object.__new__(GeneNameAndPositionMapping)
    mapping.gene_positions = {version: frame}
    return mapping


# ------------------------------------------------------------------ reading the exon file


def test_exons_are_grouped_per_gene_and_ordered_by_position(exon_file):
    frame = _exons_by_gene(exon_file)
    rows = {row["gene_id"]: row for row in frame.iter_rows(named=True)}

    assert rows["ENSG2.1"]["exon_starts"] == [55039445, 55043843]
    assert rows["ENSG2.1"]["exon_ends"] == [55039763, 55044063]
    assert rows["ENSG1.4"]["exon_starts"] == [54998933]


def test_an_untranslated_exon_holds_its_place_with_a_null(exon_file):
    """The lists are positional against each other, so dropping the UTR exon's absent CDS
    would silently shift every later exon's coding bounds onto the wrong exon."""
    rows = {r["gene_id"]: r for r in _exons_by_gene(exon_file).iter_rows(named=True)}
    row = rows["ENSG2.1"]

    assert row["cds_starts"] == [None, 55043843]
    assert len(row["cds_starts"]) == len(row["exon_starts"])
    assert len(row["cds_ends"]) == len(row["exon_ends"])


# ------------------------------------------------------------------ attaching them


def test_a_gene_with_no_exon_row_gets_empty_lists_not_nulls(exon_file):
    joined = gene_frame().join(_exons_by_gene(exon_file), on="gene_id", how="left")
    rows = _with_exon_columns(joined).to_dicts()
    absent = next(r for r in rows if r["gene_name"] == "NOEXONS")

    assert [absent[c] for c in EXON_COLUMNS] == [[], [], [], []]


def test_a_version_with_no_exon_file_still_carries_the_columns():
    """One declaration serves both response formats, so a version without exons must have
    the columns rather than a shorter row — otherwise every request at that version fails
    the column-declaration check instead of answering with no exon structure."""
    rows = _with_exon_columns(gene_frame()).to_dicts()

    assert all(set(EXON_COLUMNS) <= set(row) for row in rows)
    assert all(row[c] == [] for row in rows for c in EXON_COLUMNS)


def test_exons_are_joined_on_the_versioned_gene_id(exon_file):
    """The version suffix is what makes borrowing another release's exons impossible: a
    frame whose ids carry a different suffix must come back with no exons at all, not with
    exons placed at coordinates from the release they were built for."""
    other_release = gene_frame().with_columns(
        pl.col("gene_id").str.replace(r"\.\d+$", ".99")
    )
    joined = other_release.join(_exons_by_gene(exon_file), on="gene_id", how="left")
    rows = _with_exon_columns(joined).to_dicts()

    assert all(row["exon_starts"] == [] for row in rows)


# ------------------------------------------------------------------ the two lookups


def test_genes_in_region_returns_the_exon_columns(exon_file):
    joined = gene_frame().join(_exons_by_gene(exon_file), on="gene_id", how="left")
    rows = mapping_with(_with_exon_columns(joined)).get_genes_in_region(
        1, 55000000, 55100000, gencode_version=49
    )

    assert list(rows[0]) == list(GENES_IN_REGION_COLUMNS)
    pcsk9 = next(r for r in rows if r["gene_name"] == "PCSK9")
    assert pcsk9["exon_starts"] == [55039445, 55043843]
    assert pcsk9["cds_starts"] == [None, 55043843]


def test_nearest_genes_stays_free_of_exons(exon_file):
    """A nearest-gene answer is a lookup nobody draws, so it carries no exon arrays."""
    joined = gene_frame().join(_exons_by_gene(exon_file), on="gene_id", how="left")
    rows = mapping_with(_with_exon_columns(joined)).get_nearest_genes(
        1, 55050000, n=2, gencode_version=49
    )

    assert list(rows[0]) == list(NEAREST_GENES_COLUMNS)
    assert not set(EXON_COLUMNS) & set(rows[0])


# ------------------------------------------------------------------ serialization


def test_the_tsv_cell_joins_a_list_and_keeps_its_holes():
    assert _tsv_value([55039445, 55043843]) == "55039445,55043843"
    assert _tsv_value([None, 55043843]) == "NA,55043843"
    assert _tsv_value([]) == ""
    assert _tsv_value(None) == "NA"
    assert _tsv_value(55039445) == "55039445"


def test_the_json_path_carries_the_lists_as_arrays(exon_file):
    """The SDK and the browser both read JSON, and a comma-joined string there would make
    every consumer parse it back."""
    joined = gene_frame().join(_exons_by_gene(exon_file), on="gene_id", how="left")
    rows = mapping_with(_with_exon_columns(joined)).get_genes_in_region(
        1, 55000000, 55100000, gencode_version=49
    )
    pcsk9 = next(r for r in json.loads(json.dumps(rows)) if r["gene_name"] == "PCSK9")

    assert pcsk9["exon_starts"] == [55039445, 55043843]
