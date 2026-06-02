"""
Endpoint tests for GET /api/v1/gene/normalize.

Self-contained and fully offline: the route handler is a plain async function,
so it is invoked directly. The search index is a real SearchIndex instance
populated from a small in-repo HGNC-native TSV fixture via the real _load_genes
path, with __init__/_initialize bypassed so no GCS / phenotype loading happens.
This exercises the actual exact-match normalize_symbol logic (not a stub).
"""

import asyncio

import polars as pl
import pytest

from app.routers.gene_groups import normalize_gene_symbols
from app.services.search_service import SearchIndex


# minimal HGNC complete-set-style TSV: BRCA1 has a previous symbol (RNF53) and
# an alias (PPP1R53); TP53 has alias P53 / previous LFS1; symbol-only gene PCSK9.
HGNC_TSV = """hgnc_id\tsymbol\tname\talias_symbol\tprev_symbol\tensembl_gene_id
HGNC:1100\tBRCA1\tBRCA1 DNA repair associated\tPPP1R53\tRNF53\tENSG00000012048
HGNC:11998\tTP53\ttumor protein p53\tP53\tLFS1\tENSG00000141510
HGNC:20001\tPCSK9\tproprotein convertase\t\t\tENSG00000169174
"""


def _make_search_index(tmp_path) -> SearchIndex:
    hgnc_file = tmp_path / "hgnc_complete_set.txt"
    hgnc_file.write_text(HGNC_TSV)

    # build a real SearchIndex without __init__ (which would hit GCS for
    # phenotypes/coords); then run only the gene-loading path we need.
    idx = object.__new__(SearchIndex)
    idx.phenotypes = []
    idx.genes = []
    idx.search_items = []
    idx.genes_by_hgnc_id = {}
    idx._symbol_index = None
    idx._hgnc_file = str(hgnc_file)
    idx._data_access = None
    idx._gene_name_mapping = None
    idx._load_genes()
    return idx


def _call(search_index, symbols):
    return asyncio.run(
        normalize_gene_symbols(symbols=symbols, search_index=search_index)
    )


@pytest.fixture
def search_index(tmp_path):
    return _make_search_index(tmp_path)


def test_approved_symbol_maps_to_itself(search_index):
    body = _call(search_index, "BRCA1")
    assert body["unresolved"] == []
    assert body["mappings"] == [
        {"input": "BRCA1", "approved": "BRCA1", "matched_on": "approved"}
    ]


def test_previous_symbol_resolves_to_approved(search_index):
    body = _call(search_index, "RNF53")
    assert body["unresolved"] == []
    assert body["mappings"] == [
        {"input": "RNF53", "approved": "BRCA1", "matched_on": "alias_or_previous"}
    ]


def test_alias_symbol_resolves_to_approved(search_index):
    body = _call(search_index, "P53")
    assert body["mappings"] == [
        {"input": "P53", "approved": "TP53", "matched_on": "alias_or_previous"}
    ]


def test_unknown_symbol_is_unresolved(search_index):
    body = _call(search_index, "NOTAGENE")
    assert body["mappings"] == []
    assert body["unresolved"] == ["NOTAGENE"]


def test_case_insensitive(search_index):
    body = _call(search_index, "brca1, rnf53")
    by_input = {m["input"]: m for m in body["mappings"]}
    assert by_input["brca1"]["approved"] == "BRCA1"
    assert by_input["brca1"]["matched_on"] == "approved"
    assert by_input["rnf53"]["approved"] == "BRCA1"
    assert by_input["rnf53"]["matched_on"] == "alias_or_previous"
    assert body["unresolved"] == []


def test_multiple_comma_separated_inputs_mixed(search_index):
    body = _call(search_index, " BRCA1 , LFS1 , PCSK9 , FOO ")
    by_input = {m["input"]: m for m in body["mappings"]}
    assert by_input["BRCA1"] == {
        "input": "BRCA1",
        "approved": "BRCA1",
        "matched_on": "approved",
    }
    assert by_input["LFS1"] == {
        "input": "LFS1",
        "approved": "TP53",
        "matched_on": "alias_or_previous",
    }
    assert by_input["PCSK9"] == {
        "input": "PCSK9",
        "approved": "PCSK9",
        "matched_on": "approved",
    }
    assert body["unresolved"] == ["FOO"]


def test_empty_and_whitespace_entries_dropped(search_index):
    body = _call(search_index, "BRCA1,,  ,TP53")
    assert {m["input"] for m in body["mappings"]} == {"BRCA1", "TP53"}
    assert body["unresolved"] == []


def test_no_fuzzy_false_positive(search_index):
    # near-miss of an approved symbol must NOT resolve (exact matching only)
    body = _call(search_index, "BRCA")
    assert body["mappings"] == []
    assert body["unresolved"] == ["BRCA"]


def test_gene_name_is_not_a_match(search_index):
    # the free-text gene "name" column must not pollute the symbol namespace
    body = _call(search_index, "tumor protein p53")
    assert body["unresolved"] == ["tumor protein p53"]


# polars import kept to assert the fixture parses as the loader expects
def test_fixture_parses_as_tsv(tmp_path):
    f = tmp_path / "h.txt"
    f.write_text(HGNC_TSV)
    df = pl.read_csv(str(f), separator="\t", infer_schema_length=0)
    assert df["symbol"].to_list() == ["BRCA1", "TP53", "PCSK9"]
