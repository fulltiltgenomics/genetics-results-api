"""An on-request resource with a combined credible-set file answers a variant, gene or
region query only when the request names it.

The path parameter gates the per-phenotype routes; these three default to every resource,
so they are where an on-request dataset would otherwise reach an answer nobody asked for.
"""

import pytest
import requests

from app.services import config_util


def test_default_resources_leave_out_the_on_request_ones(monkeypatch):
    monkeypatch.setattr(config_util, "get_resources", lambda data_type=None: ["a", "b", "c"])
    monkeypatch.setattr(config_util, "on_request_resources", lambda: {"b"})
    assert config_util.get_default_resources("cs") == ["a", "c"]


@pytest.fixture(scope="session")
def hidden():
    """(resource, example phenotype) of an on-request resource that has a combined file."""
    from app.config.credible_sets import data_files

    on_request = config_util.on_request_resources()
    for df in data_files:
        if df["resource"] in on_request and "all_cs_file" in df.get("cs", {}):
            return df["resource"], df["example_pheno_or_study"]
    pytest.skip("No on-request resource with a combined credible-set file in this profile")


@pytest.fixture(scope="session")
def hidden_row(server_url, hidden):
    """A credible-set row of the example phenotype that sits in a gene."""
    resource, phenotype = hidden
    r = requests.get(
        f"{server_url}/api/v1/credible_sets_by_phenotype/{resource}/{phenotype}",
        params={"format": "json"},
    )
    assert r.status_code == 200, r.text
    rows = [row for row in r.json() if row.get("gene_most_severe") not in (None, "NA", "")]
    assert rows, "example phenotype has no credible-set row with a gene"
    return max(rows, key=lambda row: row["pip"])


def _resources(server_url, route, **params):
    r = requests.get(f"{server_url}/api/v1/{route}", params={"format": "json", **params})
    assert r.status_code == 200, r.text
    return {row["resource"] for row in r.json()}


def test_it_is_hidden_unless_named(server_url, hidden, hidden_row):
    resource, _ = hidden
    row = hidden_row
    routes = (
        f"credible_sets_by_variant/{row['chr']}:{row['pos']}:{row['ref']}:{row['alt']}",
        f"credible_sets_by_region/{row['chr']}:{max(row['pos'] - 1000, 1)}-{row['pos'] + 1000}",
    )
    for route in routes:
        assert resource not in _resources(server_url, route), route
        assert _resources(server_url, route, resources=resource) == {resource}, route


def test_a_gene_query_reaches_it_only_when_named(server_url, hidden, hidden_row):
    resource, _ = hidden
    route = f"credible_sets_by_gene/{hidden_row['gene_most_severe']}"
    assert resource not in _resources(server_url, route)
    assert _resources(server_url, route, resources=resource) == {resource}


def test_it_is_still_a_valid_resource_to_name(hidden):
    resource, _ = hidden
    assert resource in config_util.get_resources("cs")
    assert resource not in config_util.get_default_resources("cs")
