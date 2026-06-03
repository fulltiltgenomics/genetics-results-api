"""
Endpoint tests for GET /api/v1/gene_group/members.

Self-contained and fully offline: the route handler is a plain async function,
so it is invoked directly with a real GeneGroupService (fed small in-repo
HGNC-native CSV fixtures) and a fake search index providing the
hgnc_id -> symbol/ensembl/coords bridge. This avoids both a live server and the
TestClient/httpx dependency (the repo's other router tests are integration
tests run against a live server).
"""

import asyncio

import pytest
from fastapi import HTTPException

from app.routers.gene_groups import gene_group_members
from app.services.gene_group_service import GeneGroupService


# hierarchy: 100 (root "Receptors") -> 110 ("GPCRs") -> 170 ("5-HT1 receptors")
#            100 -> 120 ("Ion channels")
# HTR1A's leaf is the deep child 170 -> it must still surface under root 100
FAMILY_CSV = '''id,name,abbreviation
100,"Receptors","RCPT"
110,"GPCRs","GPCR"
120,"Ion channels","ICH"
170,"5-HT1 receptors","HTR1"
180,"Olfactory receptors","OR"
'''

HIERARCHY_CLOSURE_CSV = '''parent_fam_id,child_fam_id,distance
100,110,1
100,170,2
110,170,1
100,120,1
100,180,2
110,180,1
'''

# real hgnc_gene_has_family.csv uses BARE numeric hgnc ids; the search index
# (fake below) keys on the prefixed "HGNC:NNNN" form. The endpoint must resolve
# across that mismatch -> symbols populated, not null.
GENE_HAS_FAMILY_CSV = '''hgnc_id,family_id
5286,170
5287,170
9999,120
8000,180
'''


# minimal fake of SearchIndex exposing only the bridge the router uses.
# HGNC:5287 intentionally has NO record to exercise the null-fill path.
class _FakeSearchIndex:
    def __init__(self, by_hgnc_id):
        self._by_hgnc_id = by_hgnc_id

    def get_gene_by_hgnc_id(self, hgnc_id):
        return self._by_hgnc_id.get(hgnc_id)


GENE_RECORDS = {
    "HGNC:5286": {
        "symbol": "HTR1A",
        "ensembl_id": "ENSG00000178394",
        "chrom": 5,
        "gene_start": 63952518,
        "gene_end": 63968146,
    },
    "HGNC:9999": {
        "symbol": "CHAN1",
        "ensembl_id": "ENSG00000999999",
        # X encoded as 23 (mirrors the API-wide convention)
        "chrom": 23,
        "gene_start": 1000,
        "gene_end": 2000,
    },
    "HGNC:8000": {
        "symbol": "OR1A1",
        "ensembl_id": "ENSG00000888888",
        "chrom": 17,
        "gene_start": 3000,
        "gene_end": 4000,
    },
}


def _make_service(tmp_path):
    fam = tmp_path / "hgnc_family.csv"
    closure = tmp_path / "hgnc_hierarchy_closure.csv"
    ghf = tmp_path / "hgnc_gene_has_family.csv"
    fam.write_text(FAMILY_CSV)
    closure.write_text(HIERARCHY_CLOSURE_CSV)
    ghf.write_text(GENE_HAS_FAMILY_CSV)
    return GeneGroupService(
        gene_has_family_file=str(ghf),
        hierarchy_closure_file=str(closure),
        family_file=str(fam),
    )


def _call(service, search_index, **kwargs):
    # the route is invoked directly (no FastAPI layer), so Query(...) defaults
    # are not resolved -- supply the same default FastAPI would inject.
    kwargs.setdefault("exclude_olfactory", False)
    return asyncio.run(
        gene_group_members(
            gene_group_service=service, search_index=search_index, **kwargs
        )
    )


@pytest.fixture
def service(tmp_path):
    return _make_service(tmp_path)


@pytest.fixture
def search_index():
    return _FakeSearchIndex(GENE_RECORDS)


def test_resolve_by_group_id(service, search_index):
    body = _call(service, search_index, group_id=170, group_name=None)
    assert body["group_id"] == 170
    assert body["group_name"] == "5-HT1 receptors"
    assert body["count"] == 2
    by_hgnc = {m["hgnc_id"]: m for m in body["members"]}
    assert set(by_hgnc) == {"HGNC:5286", "HGNC:5287"}
    # member with a record is fully populated -- proves the bare->prefixed
    # hgnc_id canonicalization resolves against the (prefixed) search index
    assert by_hgnc["HGNC:5286"]["symbol"] == "HTR1A"
    assert by_hgnc["HGNC:5286"]["chr"] == 5
    assert by_hgnc["HGNC:5286"]["ensembl_id"] == "ENSG00000178394"
    # member lacking a record is still listed, with null fields
    assert by_hgnc["HGNC:5287"]["symbol"] is None
    assert by_hgnc["HGNC:5287"]["chr"] is None


def test_resolve_by_group_name_case_insensitive(service, search_index):
    body = _call(service, search_index, group_id=None, group_name="gpcrs")
    assert body["group_id"] == 110
    assert body["group_name"] == "GPCRs"
    assert {m["hgnc_id"] for m in body["members"]} == {
        "HGNC:5286",
        "HGNC:5287",
        "HGNC:8000",
    }


def test_exclude_olfactory_drops_olfactory_members(service, search_index):
    full = _call(service, search_index, group_id=110, group_name=None)
    assert full["exclude_olfactory"] is False
    assert "HGNC:8000" in {m["hgnc_id"] for m in full["members"]}

    trimmed = _call(
        service, search_index, group_id=110, group_name=None, exclude_olfactory=True
    )
    assert trimmed["exclude_olfactory"] is True
    assert {m["hgnc_id"] for m in trimmed["members"]} == {"HGNC:5286", "HGNC:5287"}


def test_hierarchical_group_returns_descendants(service, search_index):
    # root group 100 returns every descendant gene including the deep-leaf HTR1A
    body = _call(service, search_index, group_id=100, group_name=None)
    assert body["count"] == 4
    hgnc_ids = {m["hgnc_id"] for m in body["members"]}
    assert hgnc_ids == {"HGNC:5286", "HGNC:5287", "HGNC:9999", "HGNC:8000"}
    # X=23 convention is passed through unchanged
    chan1 = next(m for m in body["members"] if m["hgnc_id"] == "HGNC:9999")
    assert chan1["symbol"] == "CHAN1"
    assert chan1["chr"] == 23


def test_400_when_neither_arg(service, search_index):
    with pytest.raises(HTTPException) as exc:
        _call(service, search_index, group_id=None, group_name=None)
    assert exc.value.status_code == 400


def test_400_when_both_args(service, search_index):
    with pytest.raises(HTTPException) as exc:
        _call(service, search_index, group_id=100, group_name="Receptors")
    assert exc.value.status_code == 400


def test_404_unknown_group_name(service, search_index):
    with pytest.raises(HTTPException) as exc:
        _call(service, search_index, group_id=None, group_name="does-not-exist")
    assert exc.value.status_code == 404


def test_404_unknown_group_id(service, search_index):
    with pytest.raises(HTTPException) as exc:
        _call(service, search_index, group_id=99999, group_name=None)
    assert exc.value.status_code == 404


def test_graceful_empty_when_service_not_loaded(tmp_path, search_index):
    # missing files -> service.is_loaded() is False -> empty members, no error
    not_loaded = GeneGroupService(
        gene_has_family_file=str(tmp_path / "missing_ghf.csv"),
        hierarchy_closure_file=str(tmp_path / "missing_closure.csv"),
        family_file=str(tmp_path / "missing_family.csv"),
    )
    body = _call(not_loaded, search_index, group_id=100, group_name=None)
    assert body["count"] == 0
    assert body["members"] == []
    assert body["group_id"] == 100
