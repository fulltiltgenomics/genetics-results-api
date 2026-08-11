"""
Offline pins for the HLA served column names.

Every other HLA test in this repo talks to a live server (`server_url`), so with no
server up they all fail and pin nothing. These names are the contract two other repos
depend on — genetics-results-db's `hla_associations_v` renames FinnGen's native columns
to match them, and genetics-mcp-server concatenates rows from both branches — so a
rename here has to fail a test that runs without any server or network.
"""

import importlib

import pytest

# the house spelling the HLA output uses on both backends
HLA_STAT_COLUMNS = frozenset({"mlog10p", "se", "af", "af_cases", "af_controls"})

# FinnGen's native spellings; legal as column_mapping SOURCES, never as served names
HLA_LEGACY_STAT_COLUMNS = frozenset(
    {"mlogp", "sebeta", "af_alt", "af_alt_cases", "af_alt_controls"}
)

PROFILES = ("finngen", "daly")

HLA_DATA_FILE_ID = "finngen_hla_sumstats"


def _hla_column_mapping(profile: str) -> dict[str, str]:
    module = importlib.import_module(f"app.config.profiles.{profile}.summary_stats")
    entries = [d for d in module.data_files if d["id"] == HLA_DATA_FILE_ID]
    assert len(entries) == 1, f"expected exactly one {HLA_DATA_FILE_ID} in {profile}"
    return entries[0]["column_mapping"]


def test_header_schema_emits_the_house_names():
    from app.routers.hla import _HLA_HEADER_SCHEMA

    assert HLA_STAT_COLUMNS <= set(_HLA_HEADER_SCHEMA)


def test_header_schema_emits_no_legacy_names():
    from app.routers.hla import _HLA_HEADER_SCHEMA

    assert HLA_LEGACY_STAT_COLUMNS.isdisjoint(_HLA_HEADER_SCHEMA)


@pytest.mark.parametrize("profile", PROFILES)
def test_column_mapping_targets_the_house_names(profile):
    """The mapping's VALUES are what reaches the client, so they carry the contract."""
    assert HLA_STAT_COLUMNS <= set(_hla_column_mapping(profile).values())


@pytest.mark.parametrize("profile", PROFILES)
def test_column_mapping_never_serves_a_legacy_name(profile):
    assert HLA_LEGACY_STAT_COLUMNS.isdisjoint(_hla_column_mapping(profile).values())


@pytest.mark.parametrize("profile", PROFILES)
def test_column_mapping_agrees_with_the_header_schema(profile):
    """A mapped target the router cannot emit is silently dropped from the response."""
    from app.routers.hla import _HLA_HEADER_SCHEMA

    assert set(_hla_column_mapping(profile).values()) <= set(_HLA_HEADER_SCHEMA)
