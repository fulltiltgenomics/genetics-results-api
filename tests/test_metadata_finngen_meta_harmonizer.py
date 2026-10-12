"""
Unit tests for the `finngen_meta` harmonizer (FinnGen multi-cohort meta-analysis metadata)
and for how /resource_metadata writes its optional list and dict fields as TSV.

Self-contained: the harmonizer runs on an inline fixture shaped like
finngen_r14_mvp_ukbb_aou_meta.json and the router coroutine on a stub DataAccess. The
harmonizer is selected through the fixture config, not through the registry entry.
"""

import asyncio
import json

import pytest

from app.routers import metadata as metadata_router
from app.services.metadata_harmonizer import MetadataHarmonizer

_ALL_COHORTS = [
    "fg", "aou_eur", "aou_afr", "aou_amr", "aou_eas", "aou_sas", "aou_mid",
    "ukbb_eur", "mvp_eur", "mvp_afr", "mvp_amr",
]

ITEMS = [
    {
        "phenocode": "AB1_AMOEBIASIS",
        "name": "Amoebiasis",
        "category": "I Certain infectious and parasitic diseases (AB1_)",
        "category_index": 1,
        "num_cases": 296,
        "num_controls": 828810,
        "contributing_cohorts": ["fg", "ukbb_eur"],
        "cohort_families": ["fg", "ukbb"],
        "n_by_cohort": {
            "fg": {"n_cases": 204, "n_controls": 439752},
            "ukbb_eur": {"n_cases": 92, "n_controls": 389058},
        },
    },
    {
        # quantitative: the whole sample sits in num_cases and there is no num_controls
        "phenocode": "LDL_IRN",
        "name": "LDL cholesterol, inverse-rank normalized",
        "category": "Quantitative endpoints",
        "category_index": 99,
        "num_cases": 500000,
        "contributing_cohorts": ["fg", "mvp_eur"],
        "cohort_families": ["fg", "mvp"],
        "n_by_cohort": {"fg": {"n_cases": 300000}, "mvp_eur": {"n_cases": 200000}},
    },
    {
        "phenocode": "BMI_IRN",
        "name": "Body mass index, inverse-rank normalized",
        "category": "Quantitative endpoints",
        "category_index": 99,
        "num_cases": 1729159,
        "contributing_cohorts": _ALL_COHORTS,
        "cohort_families": ["fg", "aou", "mvp", "ukbb"],
        "n_by_cohort": {c: {"n_cases": 1000} for c in _ALL_COHORTS},
    },
]

CONFIG = {
    "type": "finngen_meta",
    "author": "FinnGen Consortium",
    "publication_date": "2026-07-24",
    "version_label": "R14",
    "resource": "finngen_mvp_ukbb_aou",
}


def _harmonize():
    rows = MetadataHarmonizer().harmonize_metadata(
        "finngen_mvp_ukbb_aou", ITEMS, {"metadata": CONFIG}
    )
    return {row.phenotype_code: row for row in rows}


def test_binary_item_keeps_counts_and_name():
    row = _harmonize()["AB1_AMOEBIASIS"]
    assert row.trait_type == "binary"
    assert (row.n_cases, row.n_controls, row.n_samples) == (296, 828810, 296 + 828810)
    assert row.phenotype_string == "Amoebiasis"


def test_num_cases_alone_is_quantitative():
    row = _harmonize()["LDL_IRN"]
    assert row.trait_type == "quantitative"
    assert (row.n_cases, row.n_controls, row.n_samples) == (500000, "NA", 500000)


def test_cohort_fields_pass_through():
    harmonized = _harmonize()
    full = harmonized["BMI_IRN"]
    assert full.contributing_cohorts == _ALL_COHORTS
    assert full.cohort_families == ["fg", "aou", "mvp", "ukbb"]
    assert set(full.n_by_cohort) == set(_ALL_COHORTS)
    assert harmonized["AB1_AMOEBIASIS"].n_by_cohort["ukbb_eur"] == {
        "n_cases": 92, "n_controls": 389058,
    }


def test_provenance_comes_from_the_config():
    row = _harmonize()["BMI_IRN"]
    assert (row.resource, row.version, row.author, row.date) == (
        "finngen_mvp_ukbb_aou", "R14", "FinnGen Consortium", "2026-07-24",
    )


def test_other_harmonizers_keep_their_row_shape():
    rows = MetadataHarmonizer().harmonize_metadata(
        "brava",
        [{"phenocode": "ALT", "phenostring": "ALT", "num_samples": 10}],
        {"metadata": {"type": "pheweb", "resource": "brava"}},
    )
    assert not {"contributing_cohorts", "cohort_families", "n_by_cohort"} & set(rows[0].to_dict())


class _StubDataAccess:
    def __init__(self, rows):
        self._rows = rows

    def get_harmonized_metadata(self, resource, include_coloc_partners=False):
        return self._rows


async def _body(response) -> str:
    return "".join(
        [c if isinstance(c, str) else c.decode() async for c in response.body_iterator]
    )


@pytest.fixture
def known_resource(monkeypatch):
    import app.services.config_util as config_util

    monkeypatch.setattr(config_util, "get_resources", lambda: ["finngen_mvp_ukbb_aou"])


def test_tsv_joins_lists_and_writes_dicts_as_json(known_resource):
    rows = [row.to_dict() for row in _harmonize().values()]
    response = asyncio.run(
        metadata_router.resource_metadata(
            resource="finngen_mvp_ukbb_aou", format="tsv", phenotypes=None,
            data_access=_StubDataAccess(rows),
        )
    )
    lines = asyncio.run(_body(response)).splitlines()
    header = lines[0].split("\t")
    by_code = {line.split("\t")[0]: dict(zip(header, line.split("\t"))) for line in lines[1:]}
    assert all(len(line.split("\t")) == len(header) for line in lines)
    assert by_code["AB1_AMOEBIASIS"]["contributing_cohorts"] == "fg;ukbb_eur"
    assert by_code["BMI_IRN"]["cohort_families"] == "fg;aou;mvp;ukbb"
    assert json.loads(by_code["AB1_AMOEBIASIS"]["n_by_cohort"])["fg"] == {
        "n_cases": 204, "n_controls": 439752,
    }


def test_tsv_header_is_the_union_when_rows_differ(known_resource):
    meta_row = _harmonize()["AB1_AMOEBIASIS"].to_dict()
    plain_row = {k: v for k, v in meta_row.items() if k not in ("contributing_cohorts", "cohort_families", "n_by_cohort")}
    plain_row["phenotype_code"] = "PLAIN"
    response = asyncio.run(
        metadata_router.resource_metadata(
            resource="finngen_mvp_ukbb_aou", format="tsv", phenotypes=None,
            data_access=_StubDataAccess([plain_row, meta_row]),
        )
    )
    lines = asyncio.run(_body(response)).splitlines()
    header = lines[0].split("\t")
    assert "contributing_cohorts" in header
    plain = dict(zip(header, lines[1].split("\t")))
    assert plain["contributing_cohorts"] == "NA"
