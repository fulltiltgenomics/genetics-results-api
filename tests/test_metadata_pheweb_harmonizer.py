"""
Unit tests for the `pheweb` metadata harmonizer and the trait-name mapping it feeds.

Self-contained: the harmonizer runs on an inline fixture and the router coroutine on a
stub DataAccess, so neither a live server nor GCS access is needed.
"""

import asyncio
import json

from app.routers.metadata import trait_name_mapping
from app.services.config_util import get_harmonizer_types_for_resource
from app.services.metadata_harmonizer import MetadataHarmonizer

# one binary meta-analysis trait, one ancestry stratum of it, one quantitative trait.
# `category` is the sex stratum in this file shape, not a trait type.
PHEWEB_ITEMS = [
    {
        "phenocode": "AFib",
        "phenostring": "Atrial fibrillation",
        "category": "Both",
        "num_cases": 72722,
        "num_controls": 945868,
    },
    {
        "phenocode": "AFib|EUR",
        "phenostring": "Atrial fibrillation",
        "category": "Both",
        "num_cases": 60000,
        "num_controls": 800000,
    },
    {
        "phenocode": "ALT",
        "phenostring": "Alanine transaminase",
        "category": "Female",
        "num_samples": 766501,
    },
]

CONFIG = {
    "type": "pheweb",
    "author": "BRaVa Consortium",
    "publication_date": "2026-05-24",
    "version_label": "2026",
    "resource": "brava",
}


def _harmonize():
    rows = MetadataHarmonizer().harmonize_metadata(
        "brava", PHEWEB_ITEMS, {"metadata": CONFIG}
    )
    return {row.phenotype_code: row for row in rows}


def test_binary_item_keeps_case_and_control_counts():
    row = _harmonize()["AFib"]
    assert row.trait_type == "binary"
    assert (row.n_cases, row.n_controls) == (72722, 945868)
    # no num_samples in the file for binary traits; it is the sum
    assert row.n_samples == 72722 + 945868
    assert row.phenotype_string == "Atrial fibrillation"


def test_stratum_code_is_kept_verbatim():
    harmonized = _harmonize()
    assert "AFib|EUR" in harmonized
    assert harmonized["AFib|EUR"].n_samples == 860000


def test_quantitative_item_has_sample_size_but_no_counts():
    row = _harmonize()["ALT"]
    # "Female" is a sex stratum: deriving the trait type from `category` would call this binary
    assert row.trait_type == "quantitative"
    assert row.n_samples == 766501
    assert (row.n_cases, row.n_controls) == ("NA", "NA")


def test_provenance_comes_from_the_registry_entry():
    row = _harmonize()["AFib"]
    assert (row.resource, row.version) == ("brava", "2026")
    assert (row.author, row.date) == ("BRaVa Consortium", "2026-05-24")


def test_brava_is_wired_to_the_pheweb_harmonizer():
    assert "pheweb" in get_harmonizer_types_for_resource("brava")


class _StubDataAccess:
    """Serves the pheweb fixture for brava and nothing for every other resource."""

    def get_resource_metadata(self, resource):
        return PHEWEB_ITEMS if resource == "brava" else []


def test_trait_name_mapping_resolves_stratum_codes():
    response = asyncio.run(trait_name_mapping(request=None, data_access=_StubDataAccess()))
    trait_map = json.loads(response.body)
    assert trait_map["AFib|EUR"] == "Atrial fibrillation"
    assert trait_map["ALT"] == "Alanine transaminase"
