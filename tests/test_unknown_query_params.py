"""
Tests that undeclared query parameters are rejected instead of silently ignored.

FastAPI's default is to ignore them, which let a client-side filter that the API never
implemented pass unnoticed and return unfiltered data.
"""

import pytest
import requests


@pytest.mark.parametrize(
    "endpoint,params",
    [
        ("credible_sets_by_variant/19-44908684-T-C", {"format": "json"}),
        ("credible_sets_by_gene/PCSK9", {"format": "json", "window": 0}),
        ("credible_sets_by_qtl_gene/PCSK9", {"format": "json"}),
        ("datasets", {}),
        ("search", {"q": "PCSK9", "types": "genes"}),
    ],
)
def test_unknown_query_param_is_rejected(server_url, endpoint, params):
    response = requests.get(
        f"{server_url}/api/v1/{endpoint}",
        params={**params, "not_a_real_param": "1"},
        timeout=60,
    )
    assert response.status_code == 422
    assert "not_a_real_param" in response.json()["detail"]


def test_error_lists_the_accepted_params(server_url):
    response = requests.get(
        f"{server_url}/api/v1/credible_sets_by_gene/PCSK9",
        params={"format": "json", "data_typez": "GWAS"},
        timeout=60,
    )
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "data_typez" in detail
    for accepted in ("coding_only", "data_types", "format", "interval", "resources", "window"):
        assert accepted in detail


def test_declared_params_are_still_accepted(server_url):
    response = requests.get(
        f"{server_url}/api/v1/credible_sets_by_gene/PCSK9",
        params={"format": "json", "window": 0, "interval": 95, "coding_only": False},
        timeout=60,
    )
    assert response.status_code == 200
