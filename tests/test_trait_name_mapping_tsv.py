"""Offline pins for the TSV form of /trait_name_mapping."""

from app.core.query_params import _declared_query_params
from app.routers.metadata import _trait_map_tsv, router


def test_tsv_is_a_header_and_one_row_per_trait():
    assert _trait_map_tsv({"I9_CHD": "Coronary heart disease", "3000963": "Hemoglobin"}) == (
        "trait\tname\nI9_CHD\tCoronary heart disease\n3000963\tHemoglobin\n"
    )


def test_a_tab_or_newline_in_a_name_cannot_split_the_row():
    rows = _trait_map_tsv({"X": "a\tb\nc"}).splitlines()
    assert rows == ["trait\tname", "X\ta b c"]


def test_the_route_declares_format():
    route = next(r for r in router.routes if r.path == "/trait_name_mapping")
    assert _declared_query_params(route.dependant) == {"format"}
