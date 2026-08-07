"""
Tests for classical HLA allele association endpoints.
"""

import pytest
import requests
from helpers.validators import validate_json_response, validate_tsv_response


@pytest.fixture(scope="session")
def hla_resources():
    """Resources configured with HLA results, from the summary_stats registry."""
    from app.config.hla import HLA_DATA_TYPE
    from app.config.summary_stats import get_available_resources_and_types

    return [r for r, dt in get_available_resources_and_types() if dt == HLA_DATA_TYPE]


@pytest.fixture(scope="session")
def hla_resource(hla_resources):
    if not hla_resources:
        pytest.skip("No HLA resources configured")
    return hla_resources[0]


# an endpoint whose HLA signal is unambiguous in any FinnGen release, so the test does
# not depend on a marginal association staying significant
HLA_PHENOTYPE = "K11_COELIAC"


class TestHlaGenes:
    """Test GET /api/v1/hla/genes."""

    def test_gene_registry(self, server_url):
        response = requests.get(f"{server_url}/api/v1/hla/genes")
        assert response.status_code == 200
        body = response.json()

        assert body["chrom"] == 6
        assert body["region"].startswith("6:")

        genes = {g["gene"]: g["pos"] for g in body["genes"]}
        for expected in ("HLA-A", "HLA-B", "HLA-C", "HLA-DQB1", "HLA-DRB1"):
            assert expected in genes
        assert all(g.startswith("HLA-") for g in genes)
        assert body["genes"] == sorted(body["genes"], key=lambda g: (g["pos"], g["gene"]))

    def test_drb_loci_share_one_anchor(self, server_url):
        """DRB3/4/5 sit at a placeholder anchor, so they cannot be separated by position."""
        body = requests.get(f"{server_url}/api/v1/hla/genes").json()
        genes = {g["gene"]: g["pos"] for g in body["genes"]}
        assert genes["HLA-DRB3"] == genes["HLA-DRB4"] == genes["HLA-DRB5"]


class TestHlaAssociations:
    """Test GET /api/v1/hla/{resource}."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_single_phenotype(self, server_url, hla_resource, format):
        response = requests.get(
            f"{server_url}/api/v1/hla/{hla_resource}",
            params={"phenotypes": HLA_PHENOTYPE, "format": format},
        )
        assert response.status_code == 200

        if format == "tsv":
            result = validate_tsv_response(response.text, min_data_lines=1)
            assert result["valid"], result["errors"]
            header = result["header"]
        else:
            result = validate_json_response(response.json(), min_items=1)
            assert result["valid"], result["errors"]
            header = list(response.json()[0].keys())

        # allele-keyed, not variant-keyed: gene/allele present, ref/alt deliberately absent
        for col in ("phenotype", "gene", "allele", "mlog10p", "info"):
            assert col in header
        for col in ("ref", "alt"):
            assert col not in header

    def test_returns_the_whole_hla_profile(self, server_url, hla_resource):
        """One read covers every typed allele, which is what makes a by-variant form unnecessary."""
        rows = requests.get(
            f"{server_url}/api/v1/hla/{hla_resource}",
            params={"phenotypes": HLA_PHENOTYPE, "format": "json"},
        ).json()

        assert len(rows) > 100
        assert len({r["allele"] for r in rows}) == len(rows), "an allele appeared twice"
        assert len({r["gene"] for r in rows}) == 10
        assert {r["phenotype"] for r in rows} == {HLA_PHENOTYPE}

    def test_multiple_phenotypes_are_merged(self, server_url, hla_resource):
        single = requests.get(
            f"{server_url}/api/v1/hla/{hla_resource}",
            params={"phenotypes": HLA_PHENOTYPE, "format": "json"},
        ).json()
        both = requests.get(
            f"{server_url}/api/v1/hla/{hla_resource}",
            params={"phenotypes": f"{HLA_PHENOTYPE},M13_ANKYLOSPON", "format": "json"},
        ).json()

        assert len(both) == 2 * len(single)
        assert {r["phenotype"] for r in both} == {HLA_PHENOTYPE, "M13_ANKYLOSPON"}

    def test_genes_filter_is_exact_not_a_span(self, server_url, hla_resource):
        """The two extreme genes must not drag in the eight genes lying between them."""
        rows = requests.get(
            f"{server_url}/api/v1/hla/{hla_resource}",
            params={
                "phenotypes": HLA_PHENOTYPE,
                "genes": "HLA-A,HLA-DPB1",
                "format": "json",
            },
        ).json()

        assert {r["gene"] for r in rows} == {"HLA-A", "HLA-DPB1"}

    def test_genes_filter_single_gene(self, server_url, hla_resource):
        rows = requests.get(
            f"{server_url}/api/v1/hla/{hla_resource}",
            params={"phenotypes": HLA_PHENOTYPE, "genes": "HLA-B", "format": "json"},
        ).json()

        assert rows
        assert {r["gene"] for r in rows} == {"HLA-B"}
        assert all(r["allele"].startswith("B*") for r in rows)

    def test_quantitative_endpoint_has_no_case_control_frequencies(
        self, server_url, hla_resource
    ):
        rows = requests.get(
            f"{server_url}/api/v1/hla/{hla_resource}",
            params={"phenotypes": "BMI_IRN", "format": "json"},
        ).json()

        assert rows
        assert all(r["af_cases"] is None for r in rows)
        assert all(r["af"] is not None for r in rows)


class TestHlaErrors:
    """Parameter validation."""

    def test_unknown_resource(self, server_url):
        response = requests.get(
            f"{server_url}/api/v1/hla/not_a_resource",
            params={"phenotypes": HLA_PHENOTYPE},
        )
        assert response.status_code == 404

    def test_missing_phenotypes(self, server_url, hla_resource):
        response = requests.get(f"{server_url}/api/v1/hla/{hla_resource}")
        assert response.status_code == 422

    def test_unknown_phenotype(self, server_url, hla_resource):
        response = requests.get(
            f"{server_url}/api/v1/hla/{hla_resource}",
            params={"phenotypes": "NOT_A_PHENOTYPE"},
        )
        assert response.status_code == 404

    def test_unknown_gene(self, server_url, hla_resource):
        response = requests.get(
            f"{server_url}/api/v1/hla/{hla_resource}",
            params={"phenotypes": HLA_PHENOTYPE, "genes": "HLA-ZZZ"},
        )
        assert response.status_code == 422
        assert "HLA-ZZZ" in response.json()["detail"]

    def test_path_traversal_in_phenotype_is_rejected(self, server_url, hla_resource):
        """The phenotype is interpolated into a GCS path, so it must be validated."""
        response = requests.get(
            f"{server_url}/api/v1/hla/{hla_resource}",
            params={"phenotypes": "../../../etc/passwd"},
        )
        assert response.status_code == 422

    def test_undeclared_query_param(self, server_url, hla_resource):
        response = requests.get(
            f"{server_url}/api/v1/hla/{hla_resource}",
            params={"phenotypes": HLA_PHENOTYPE, "nosuchparam": "1"},
        )
        assert response.status_code == 422
