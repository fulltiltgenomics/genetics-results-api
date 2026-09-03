"""The declared-columns contract for the JSON endpoints that do not stream a TSV.

genetics-results-suite-8a1: `range_response` advertises a JSON result's columns by reading
the data file's own header line, so `gene_burden`, `gene_annotations`, `gene_disease` and
`search` — which compute their JSON instead — had no schema to advertise and handed the SDK
a nameless empty frame.

They now DECLARE their columns, which is the thing that rots, so the declaration is checked
rather than trusted:

  * where the rows are projected in this repo, the declaration is the SAME OBJECT the
    projection uses (`.select(GENES_IN_REGION_COLUMNS)`, `_member_row`,
    `gene_disease["output_columns"]`), so it cannot drift — the tests below run the real
    projection and compare;
  * where the rows are assembled from a live index (search), nothing offline can produce a
    real row, so `verified_columns_header` refuses on the response path instead. The
    mutation tests at the bottom are what prove the refusal fires.

Fully offline: the route handlers are plain async functions, invoked directly with fakes.
"""

import asyncio
import json

import polars as pl
import pytest

from app.core.responses import (
    COLUMNS_HEADER,
    ColumnDeclarationError,
    verified_columns_header,
)
from app.routers.gene_groups import MEMBER_COLUMNS, _member_row
from app.routers.search import _verified_search_columns
from app.services.gene_name_and_position_mapping import (
    GENES_IN_REGION_COLUMNS,
    NEAREST_GENES_COLUMNS,
    GeneNameAndPositionMapping,
)
from app.services.search_service import (
    GENE_RESULT_COLUMNS,
    PHENOTYPE_RESULT_COLUMNS,
)

# a gencode frame with MORE columns than either lookup projects, so a projection that
# picked up an extra column would show up as a header that no longer matches
GENE_POSITIONS = pl.DataFrame(
    {
        "gene_name": ["PCSK9", "USP24"],
        "ensg": ["ENSG00000169174", "ENSG00000162402"],
        "chrom": [1, 1],
        "gene_start": [55039447, 55066359],
        "gene_end": [55064852, 55215753],
        "gene_strand": ["+", "-"],
        "gene_type": ["protein_coding", "protein_coding"],
        "hgnc_symbol": ["PCSK9", "USP24"],
        "hgnc_name": ["proprotein convertase", "ubiquitin specific peptidase 24"],
        "hgnc_alias_symbol": ["NARC-1", "KIAA1057"],
        "hgnc_prev_symbol": ["HCHOLA3", None],
        "exon_starts": [[55039445, 55043843], []],
        "exon_ends": [[55039763, 55044063], []],
        "cds_starts": [[None, 55043843], []],
        "cds_ends": [[None, 55044063], []],
    }
)


def _mapping() -> GeneNameAndPositionMapping:
    """The real lookup methods over an in-memory gencode frame (__init__ reads GCS)."""
    mapping = object.__new__(GeneNameAndPositionMapping)
    mapping.gene_positions = {"v39": GENE_POSITIONS}
    return mapping


# --------------------------------------------------------------- gene_annotations


def test_genes_in_region_rows_are_keyed_by_the_declaration():
    rows = _mapping().get_genes_in_region(1, 55000000, 55100000, gencode_version="v39")
    assert rows, "fixture must produce rows, or this proves nothing"
    assert list(rows[0]) == list(GENES_IN_REGION_COLUMNS)


def test_nearest_genes_rows_are_keyed_by_the_declaration():
    rows = _mapping().get_nearest_genes(1, 55050000, n=2, gencode_version="v39")
    assert rows
    assert list(rows[0]) == list(NEAREST_GENES_COLUMNS)


def test_nearest_genes_declares_distance_where_it_actually_lands():
    """Order is part of the contract: the SDK builds a frame from this list."""
    rows = _mapping().get_nearest_genes(1, 55050000, n=1, gencode_version="v39")
    assert list(rows[0]).index("distance") == NEAREST_GENES_COLUMNS.index("distance")


def test_gene_group_member_rows_are_built_from_the_declaration():
    row = _member_row("HGNC:5286", {"symbol": "HTR1A", "chrom": 5})
    assert list(row) == list(MEMBER_COLUMNS)
    assert list(_member_row("HGNC:9", None)) == list(MEMBER_COLUMNS)


def test_an_empty_gene_group_still_advertises_its_columns():
    from app.routers.gene_groups import gene_group_members

    class _NotLoaded:
        def is_loaded(self):
            return False

    resp = asyncio.run(
        gene_group_members(
            group_id=110,
            group_name=None,
            exclude_olfactory=False,
            gene_group_service=_NotLoaded(),
            search_index=None,
        )
    )
    assert json.loads(bytes(resp.body))["members"] == []
    assert resp.headers[COLUMNS_HEADER] == ",".join(MEMBER_COLUMNS)


# --------------------------------------------------------------- gene_disease


def test_gene_disease_declares_the_columns_its_loader_selects():
    """`GeneDiseaseData` ends every source with `.select(output_columns)`."""
    from app.config.gene_disease import gene_disease

    columns = gene_disease["output_columns"]
    frame = pl.DataFrame({c: ["x"] for c in columns})
    assert list(frame.select(columns).to_dicts()[0]) == list(columns)
    assert verified_columns_header(columns, frame.to_dicts()) == {
        COLUMNS_HEADER: ",".join(columns)
    }


def test_an_empty_gene_disease_result_advertises_its_columns_on_the_404():
    """The empty result for this endpoint IS a 404; the client turns it into an empty frame."""
    from fastapi import HTTPException

    from app.config.gene_disease import gene_disease
    from app.routers.gene_disease import get_gene_disease

    class _Empty:
        def get_by_gene_symbol(self, gene):
            return pl.DataFrame(
                {c: [] for c in gene_disease["output_columns"]}
            )

    class _Url:
        url = "http://t/api/v1/gene_disease/NOSUCHGENE"

    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            get_gene_disease(
                request=_Url(),
                gene_name="NOSUCHGENE",
                format="json",
                gene_disease_data=_Empty(),
            )
        )
    assert exc.value.status_code == 404
    assert exc.value.headers[COLUMNS_HEADER] == ",".join(
        gene_disease["output_columns"]
    )


# --------------------------------------------------------------- search


def _search_row(type_: str, columns) -> dict:
    return {c: type_ for c in columns}


def test_search_advertises_the_declaration_for_a_single_type():
    rows = [_search_row("gene", GENE_RESULT_COLUMNS)]
    assert _verified_search_columns(rows, ["genes"]) == {
        COLUMNS_HEADER: ",".join(GENE_RESULT_COLUMNS)
    }
    rows = [_search_row("phenotype", PHENOTYPE_RESULT_COLUMNS)]
    assert _verified_search_columns(rows, ["phenotypes"]) == {
        COLUMNS_HEADER: ",".join(PHENOTYPE_RESULT_COLUMNS)
    }


def test_search_advertises_nothing_for_a_mixed_result():
    """Two column sets in one array have no honest single answer — but both are verified."""
    rows = [
        _search_row("gene", GENE_RESULT_COLUMNS),
        _search_row("phenotype", PHENOTYPE_RESULT_COLUMNS),
    ]
    assert _verified_search_columns(rows, None) == {}


# --------------------------------------------------------------- the refusal itself


def test_a_row_that_contradicts_the_declaration_is_refused():
    with pytest.raises(ColumnDeclarationError):
        verified_columns_header(["a", "b"], [{"a": 1, "b": 2, "c": 3}])
    with pytest.raises(ColumnDeclarationError):
        verified_columns_header(["a", "b", "c"], [{"a": 1, "b": 2}])
    with pytest.raises(ColumnDeclarationError):
        verified_columns_header(["a", "b"], [{"a": 1, "renamed": 2}])


def test_a_reordered_declaration_is_refused():
    """A reordering is a silently wrong schema, not a cosmetic difference."""
    with pytest.raises(ColumnDeclarationError):
        verified_columns_header(["b", "a"], [{"a": 1, "b": 2}])


def test_an_empty_result_cannot_trigger_the_refusal():
    """Nothing to compare against: the declaration is all there is, which is the point."""
    assert verified_columns_header(["a", "b"], []) == {COLUMNS_HEADER: "a,b"}


def test_search_refuses_a_row_that_drifted_from_its_declaration():
    """The mutation that matters for search: a new key in the result dict."""
    row = _search_row("gene", GENE_RESULT_COLUMNS)
    row["newly_added_field"] = 1
    with pytest.raises(ColumnDeclarationError):
        _verified_search_columns([row], ["genes"])


def test_search_refuses_an_undeclared_result_type():
    with pytest.raises(ColumnDeclarationError):
        _verified_search_columns([{"type": "variant", "x": 1}], None)


# --------------------------- search: the declaration vs the construction sites
#
# The other three declarations are the same object their rows are projected through, so
# they cannot drift. Search's cannot be: its result dicts are assembled while loading a
# live index, and there is no offline row to compare against. So the construction sites are
# read out of the source instead — a key added to either literal, or a key added to the
# ranking splice, fails here rather than reaching the SDK as a wrong empty schema.


def _dict_literal_keys(source, name: str) -> list[list[str]]:
    """Every `<name> = {"a": ..., "b": ...}` literal in `source`, as key lists."""
    import ast

    found = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Dict):
            continue
        if not any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            continue
        keys = [
            k.value
            for k in node.value.keys
            if isinstance(k, ast.Constant) and isinstance(k.value, str)
        ]
        if len(keys) == len(node.value.keys):
            found.append(keys)
    return found


@pytest.fixture(scope="module")
def search_service_source() -> str:
    import inspect

    import app.services.search_service as module

    return inspect.getsource(module)


@pytest.mark.parametrize(
    ("literal", "declared"),
    [
        ("phenotype", PHENOTYPE_RESULT_COLUMNS),
        ("gene", GENE_RESULT_COLUMNS),
    ],
)
def test_every_indexed_item_literal_matches_its_declaration(
    search_service_source, literal, declared
):
    from app.services.search_service import _RANKING_COLUMNS

    literals = _dict_literal_keys(search_service_source, literal)
    # fail closed: a construction site that stopped being a plain dict literal must break
    # this test rather than silently leave the declaration unchecked
    assert literals, f"no `{literal} = {{...}}` literal found in search_service.py"
    item_columns = list(declared[: -len(_RANKING_COLUMNS)])
    for keys in literals:
        assert keys == item_columns


def test_the_ranking_splice_matches_the_tail_of_both_declarations(search_service_source):
    """`result = {**item, "match_type": ...}` — the four keys spliced onto every row."""
    import ast

    from app.services.search_service import _RANKING_COLUMNS

    spliced = []
    for node in ast.walk(ast.parse(search_service_source)):
        if (
            isinstance(node, ast.Assign)
            and isinstance(node.value, ast.Dict)
            and any(isinstance(t, ast.Name) and t.id == "result" for t in node.targets)
            and node.value.keys
            and node.value.keys[0] is None  # the `**item` splat
        ):
            spliced.append(
                [k.value for k in node.value.keys[1:] if isinstance(k, ast.Constant)]
            )
    assert spliced, "no `result = {**item, ...}` splice found in search_service.py"
    for keys in spliced:
        assert keys == list(_RANKING_COLUMNS)
        assert (
            tuple(keys) == PHENOTYPE_RESULT_COLUMNS[-len(keys) :]
            == GENE_RESULT_COLUMNS[-len(keys) :]
        )


# ------------------------------- the JSON branches must encode like FastAPI would
#
# These three routes changed from returning a value to returning a JSONResponse so they can
# carry the header, which skips the `jsonable_encoder` FastAPI runs on a returned value. No
# dtype on these paths needs it today; the point is that a future one would become a 500
# rather than serializing, so the encoding is restored explicitly.


class _FakeMapping:
    def __init__(self, rows):
        self._rows = rows

    def get_genes_in_region(self, *a, **kw):
        return self._rows

    def get_nearest_genes(self, *a, **kw):
        return self._rows


def _dated_row(columns) -> dict:
    import datetime

    row = {c: "x" for c in columns}
    row["gene_start"] = datetime.date(2020, 1, 1)
    return row


def test_genes_in_region_json_encodes_a_dtype_json_dumps_cannot():
    from app.routers.genes import genes_in_region

    rows = [_dated_row(GENES_IN_REGION_COLUMNS)]
    resp = asyncio.run(
        genes_in_region(
            chr="1",
            start=1,
            end=2,
            gene_type="protein_coding",
            gencode_version=None,
            format="json",
            gene_name_and_position_mapping=_FakeMapping(rows),
        )
    )
    assert resp.status_code == 200
    assert json.loads(bytes(resp.body))[0]["gene_start"] == "2020-01-01"
    assert resp.headers[COLUMNS_HEADER] == ",".join(GENES_IN_REGION_COLUMNS)


def test_nearest_genes_json_encodes_a_dtype_json_dumps_cannot():
    from app.routers.genes import nearest_genes

    rows = [_dated_row(NEAREST_GENES_COLUMNS)]
    resp = asyncio.run(
        nearest_genes(
            variant="1-55039447-C-T",
            gene_type="protein_coding",
            n=1,
            max_distance=1000,
            gencode_version=None,
            return_hgnc_symbol_if_only_ensg=False,
            format="json",
            gene_name_and_position_mapping=_FakeMapping(rows),
        )
    )
    assert json.loads(bytes(resp.body))[0]["gene_start"] == "2020-01-01"


def test_gene_group_members_json_encodes_a_dtype_json_dumps_cannot():
    import datetime

    from app.routers.gene_groups import gene_group_members

    class _Service:
        def is_loaded(self):
            return True

        def resolve_group_id(self, name):
            return 110

        def group_name(self, gid):
            return "GPCRs"

        def members_of_group(self, group_id, exclude_olfactory):
            return {"HGNC:1"}

    class _Index:
        def get_gene_by_hgnc_id(self, hgnc_id):
            return {"symbol": "A", "gene_start": datetime.date(2020, 1, 1)}

    resp = asyncio.run(
        gene_group_members(
            group_id=110,
            group_name=None,
            exclude_olfactory=False,
            gene_group_service=_Service(),
            search_index=_Index(),
        )
    )
    body = json.loads(bytes(resp.body))
    assert body["members"][0]["gene_start"] == "2020-01-01"
    assert resp.headers[COLUMNS_HEADER] == ",".join(MEMBER_COLUMNS)
