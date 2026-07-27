"""
Tests for the data_types filter on the credible set endpoints.

The filter was previously advertised by API clients but never implemented, so requests
asking for one association type silently received every type. These tests pin the
filter down at both the row level (unit) and the endpoint level (integration).
"""

import asyncio

import pytest
import requests

from app.core.streams import filter_rows_by_column, filter_stream_by_column


async def _aiter(*chunks: bytes):
    for chunk in chunks:
        yield chunk


def _run_stream(gen) -> bytes:
    async def collect():
        return b"".join([chunk async for chunk in gen])

    return asyncio.run(collect())


HEADER = b"resource\tdataset\tdata_type\ttrait\n"
ROWS = (
    b"finngen\tFinnGen_ATACseq\tcaQTL\tchr5-35863122-35863905\n"
    b"ukbb\tUKB_PPP\tpQTL\tIL7R\n"
    b"finngen\tFinnGen_R13\tGWAS\tK11_IBD_STRICT\n"
)


def test_filter_stream_keeps_only_requested_data_types():
    out = _run_stream(filter_stream_by_column(_aiter(HEADER + ROWS), "data_type", {"caQTL"}))
    assert out.count(b"\n") == 2  # header + one row
    assert b"caQTL" in out
    assert b"pQTL" not in out


def test_filter_stream_matches_data_type_case_insensitively():
    out = _run_stream(
        filter_stream_by_column(
            _aiter(HEADER + ROWS), "data_type", {"caqtl", "gwas"}, case_insensitive=True
        )
    )
    assert out.count(b"\n") == 3
    assert b"pQTL" not in out


def test_filter_stream_passes_through_when_column_absent():
    body = b"resource\ttrait\nfinngen\tK11_IBD_STRICT\n"
    out = _run_stream(filter_stream_by_column(_aiter(body), "data_type", {"GWAS"}))
    assert out == body


def test_filter_rows_matches_case_insensitively_and_drops_missing():
    rows = [
        {"data_type": "caQTL"},
        {"data_type": "pQTL"},
        {"data_type": None},
        {},
    ]
    kept = filter_rows_by_column(rows, "data_type", {"caqtl"}, case_insensitive=True)
    assert kept == [{"data_type": "caQTL"}]


@pytest.mark.parametrize(
    "endpoint,params",
    [
        ("credible_sets_by_qtl_gene/IL7R", {}),
        ("credible_sets_by_gene/IL7R", {"window": 100000}),
        ("credible_sets_by_variant/19-44908684-T-C", {}),
    ],
)
def test_data_types_filters_endpoint_rows(server_url, endpoint, params):
    """Every returned row must carry a requested data type, and the filter must narrow."""
    base = {"format": "json", **params}
    unfiltered = requests.get(f"{server_url}/api/v1/{endpoint}", params=base, timeout=120)
    assert unfiltered.status_code == 200
    all_types = {row["data_type"] for row in unfiltered.json()}
    if len(all_types) < 2:
        pytest.skip(f"{endpoint} returns a single data type; nothing to filter")

    wanted = sorted(all_types)[0]
    filtered = requests.get(
        f"{server_url}/api/v1/{endpoint}",
        params={**base, "data_types": wanted},
        timeout=120,
    )
    assert filtered.status_code == 200
    rows = filtered.json()
    assert rows, f"{endpoint} returned no rows for data_types={wanted}"
    assert {row["data_type"] for row in rows} == {wanted}
    assert len(rows) < len(unfiltered.json())


def test_data_types_is_case_insensitive(server_url):
    response = requests.get(
        f"{server_url}/api/v1/credible_sets_by_qtl_gene/IL7R",
        params={"format": "json", "data_types": "caqtl"},
        timeout=120,
    )
    assert response.status_code == 200
    rows = response.json()
    assert rows
    assert {row["data_type"] for row in rows} == {"caQTL"}


def test_data_types_accepts_multiple_types(server_url):
    response = requests.get(
        f"{server_url}/api/v1/credible_sets_by_qtl_gene/IL7R",
        params={"format": "json", "data_types": "caQTL,pQTL"},
        timeout=120,
    )
    assert response.status_code == 200
    assert {row["data_type"] for row in response.json()} == {"caQTL", "pQTL"}


def test_data_types_unknown_type_returns_no_rows(server_url):
    """An unrecognized type yields an empty result rather than unfiltered data."""
    response = requests.get(
        f"{server_url}/api/v1/credible_sets_by_qtl_gene/IL7R",
        params={"format": "json", "data_types": "notAType"},
        timeout=120,
    )
    assert response.status_code == 200
    assert response.json() == []
