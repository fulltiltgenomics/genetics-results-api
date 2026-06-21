"""
Comprehensive tests for credible set endpoints.
"""

import asyncio

import pytest
import requests
from helpers.validators import (
    validate_tsv_response,
    validate_json_response,
)


class TestCredibleSetsByPhenotype:
    """Test /api/v1/credible_sets_by_phenotype/{resource}/{phenotype} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_credible_sets_by_phenotype_formats(
        self, server_url, example_phenotypes, format
    ):
        """Test credible sets by phenotype with both TSV and JSON formats."""
        if not example_phenotypes:
            pytest.skip("No example phenotypes available")

        resource, phenotype = next(iter(example_phenotypes.items()))

        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_phenotype/{resource}/{phenotype}",
            params={"format": format, "interval": 95},
            timeout=30,
        )

        assert response.status_code == 200

        if format == "tsv":
            assert "text/tab-separated-values" in response.headers.get(
                "content-type", ""
            )
            validation = validate_tsv_response(response.text)
            assert validation["valid"], f"TSV validation failed: {validation['errors']}"
            assert validation["has_header"], "TSV should have header"
        else:  # json
            assert "application/json" in response.headers.get("content-type", "")
            data = response.json()
            validation = validate_json_response(data)
            assert validation[
                "valid"
            ], f"JSON validation failed: {validation['errors']}"

    def test_credible_sets_by_phenotype_invalid_interval(
        self, server_url, example_phenotypes
    ):
        """Test that invalid interval values return 422."""
        if not example_phenotypes:
            pytest.skip("No example phenotypes available")

        resource, phenotype = next(iter(example_phenotypes.items()))

        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_phenotype/{resource}/{phenotype}",
            params={"format": "json", "interval": 90},
            timeout=10,
        )

        assert response.status_code == 422

    def test_credible_sets_by_phenotype_not_found(
        self, server_url, cs_resources
    ):
        """Test that non-existent phenotype returns 404."""
        if not cs_resources:
            pytest.skip("No resources available")

        resource = cs_resources[0]
        fake_phenotype = "NONEXISTENT_PHENOTYPE_12345"

        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_phenotype/{resource}/{fake_phenotype}",
            params={"format": "json", "interval": 95},
            timeout=10,
        )

        assert response.status_code == 404


class TestCredibleSetsByRegion:
    """Test /api/v1/credible_sets_by_region/{region} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_credible_sets_by_region_formats(self, server_url, test_region, format):
        """Test credible sets by region with both TSV and JSON formats."""
        endpoint = f"{server_url}/api/v1/credible_sets_by_region/{test_region}"
        params = {"format": format, "interval": 95}
        response = requests.get(endpoint, params=params, timeout=30)

        # Build full URL for error messages
        from urllib.parse import urlencode

        full_url = f"{endpoint}?{urlencode(params)}"

        assert response.status_code == 200, (
            f"Request failed!\n"
            f"URL: {full_url}\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text[:200]}"
        )

        if format == "tsv":
            assert "text/tab-separated-values" in response.headers.get(
                "content-type", ""
            )
            validation = validate_tsv_response(response.text)
            assert validation["valid"], f"TSV validation failed: {validation['errors']}"
            assert validation["has_header"], "TSV should have header"
        else:  # json
            assert "application/json" in response.headers.get("content-type", "")
            data = response.json()
            validation = validate_json_response(data)
            assert validation[
                "valid"
            ], f"JSON validation failed: {validation['errors']}"

    def test_credible_sets_by_region_with_single_resource(
        self, server_url, test_region, cs_resources
    ):
        """Test credible sets by region with single resource specified."""
        if not cs_resources:
            pytest.skip("No resources available")

        resource = cs_resources[0]

        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_region/{test_region}",
            params={"format": "json", "interval": 95, "resources": [resource]},
            timeout=30,
        )

        assert response.status_code == 200

    def test_credible_sets_by_region_with_multiple_resources(
        self, server_url, test_region, cs_resources
    ):
        """Test credible sets by region with multiple resources."""
        if len(cs_resources) < 2:
            pytest.skip("Need at least 2 resources for this test")

        resources = cs_resources[:2]
        params = {"format": "json", "interval": 95, "resources": resources}
        from urllib.parse import urlencode

        full_url = f"{server_url}/api/v1/credible_sets_by_region/{test_region}?{urlencode(params)}"

        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_region/{test_region}",
            params=params,
            timeout=30,
        )

        assert response.status_code == 200, (
            f"Request failed!\n"
            f"URL: {full_url}\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text[:200]}"
        )

    def test_credible_sets_by_region_invalid_region(self, server_url, invalid_region):
        """Test that invalid region format returns 422."""
        endpoint = f"{server_url}/api/v1/credible_sets_by_region/{invalid_region}"
        params = {"format": "json", "interval": 95}
        response = requests.get(endpoint, params=params, timeout=10)

        from urllib.parse import urlencode

        full_url = f"{endpoint}?{urlencode(params)}"

        assert response.status_code == 422, (
            f"Expected 422 for invalid region!\n"
            f"URL: {full_url}\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text[:200]}"
        )

    def test_credible_sets_by_region_invalid_resource(
        self, server_url, test_region, invalid_resource
    ):
        """Test that invalid resource returns 404."""
        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_region/{test_region}",
            params={"format": "json", "interval": 95, "resources": [invalid_resource]},
            timeout=10,
        )

        assert response.status_code == 404

    def test_credible_sets_by_region_single_base_pair(self, server_url):
        """Test credible sets by region with single base pair region."""
        region = "1:1000000-1000000"

        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_region/{region}",
            params={"format": "json", "interval": 95},
            timeout=30,
        )

        assert response.status_code == 200

    def test_credible_sets_by_region_x_chromosome(self, server_url):
        """Test credible sets by region on X chromosome."""
        region = "X:5000000-5000100"

        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_region/{region}",
            params={"format": "json", "interval": 95},
            timeout=30,
        )

        assert response.status_code == 200


class TestCredibleSetsByVariant:
    """Test /api/v1/credible_sets_by_variant/{variant} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_credible_sets_by_variant_formats(self, server_url, test_variant, format):
        """Test credible sets by variant with both TSV and JSON formats."""
        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_variant/{test_variant}",
            params={"format": format, "interval": 95},
            timeout=30,
        )

        assert response.status_code == 200

        if format == "tsv":
            assert "text/tab-separated-values" in response.headers.get(
                "content-type", ""
            )
            validation = validate_tsv_response(response.text)
            assert validation["valid"], f"TSV validation failed: {validation['errors']}"
        else:  # json
            assert "application/json" in response.headers.get("content-type", "")
            data = response.json()
            validation = validate_json_response(data)
            assert validation[
                "valid"
            ], f"JSON validation failed: {validation['errors']}"

    def test_credible_sets_by_variant_with_resources(
        self, server_url, test_variant, cs_resources
    ):
        """Test credible sets by variant with resources parameter."""
        if not cs_resources:
            pytest.skip("No resources available")

        resource = cs_resources[0]

        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_variant/{test_variant}",
            params={"format": "json", "interval": 95, "resources": [resource]},
            timeout=30,
        )

        assert response.status_code == 200

    def test_credible_sets_by_variant_invalid_variant(
        self, server_url, invalid_variant
    ):
        """Test that invalid variant format returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_variant/{invalid_variant}",
            params={"format": "json", "interval": 95},
            timeout=10,
        )

        assert response.status_code == 422


class TestCredibleSetsByVariantPost:
    """Test POST /api/v1/credible_sets_by_variant endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_post_credible_sets_by_variant_formats(
        self, server_url, test_variant, format
    ):
        """Test POST with a single variant returns same data as GET."""
        post_response = requests.post(
            f"{server_url}/api/v1/credible_sets_by_variant",
            json={"variants": test_variant},
            params={"format": format, "interval": 95},
            timeout=30,
        )
        assert post_response.status_code == 200

        if format == "tsv":
            assert "text/tab-separated-values" in post_response.headers.get(
                "content-type", ""
            )
            validation = validate_tsv_response(post_response.text)
            assert validation["valid"], f"TSV validation failed: {validation['errors']}"
        else:
            assert "application/json" in post_response.headers.get("content-type", "")
            data = post_response.json()
            validation = validate_json_response(data)
            assert validation["valid"], f"JSON validation failed: {validation['errors']}"

    def test_post_credible_sets_by_variant_multiple(self, server_url, test_variant):
        """Test POST with multiple variants."""
        # use the same variant twice to ensure multi-variant path works
        variants = f"{test_variant}\n{test_variant}"
        response = requests.post(
            f"{server_url}/api/v1/credible_sets_by_variant",
            json={"variants": variants},
            params={"format": "json", "interval": 95},
            timeout=30,
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_post_credible_sets_by_variant_with_resources(
        self, server_url, test_variant, cs_resources
    ):
        """Test POST with resources parameter."""
        if not cs_resources:
            pytest.skip("No resources available")

        resource = cs_resources[0]
        response = requests.post(
            f"{server_url}/api/v1/credible_sets_by_variant",
            json={"variants": test_variant},
            params={"format": "json", "interval": 95, "resources": [resource]},
            timeout=30,
        )
        assert response.status_code == 200

    def test_post_credible_sets_by_variant_invalid(self, server_url):
        """Test POST with invalid variant returns 422."""
        response = requests.post(
            f"{server_url}/api/v1/credible_sets_by_variant",
            json={"variants": "invalid-variant"},
            params={"format": "json", "interval": 95},
            timeout=10,
        )
        assert response.status_code == 422

    def test_post_credible_sets_by_variant_empty(self, server_url):
        """Test POST with empty variants string returns 422."""
        response = requests.post(
            f"{server_url}/api/v1/credible_sets_by_variant",
            json={"variants": ""},
            params={"format": "json", "interval": 95},
            timeout=10,
        )
        assert response.status_code == 422


class TestCredibleSetsByGene:
    """Test /api/v1/credible_sets_by_gene/{gene} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_credible_sets_by_gene_formats(self, server_url, test_gene, format):
        """Test credible sets by gene with both TSV and JSON formats."""
        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_gene/{test_gene}",
            params={"format": format, "interval": 95, "window": 0},
            timeout=30,
        )

        assert response.status_code == 200

        if format == "tsv":
            assert "text/tab-separated-values" in response.headers.get(
                "content-type", ""
            )
            validation = validate_tsv_response(response.text)
            assert validation["valid"], f"TSV validation failed: {validation['errors']}"
        else:  # json
            assert "application/json" in response.headers.get("content-type", "")
            data = response.json()
            validation = validate_json_response(data)
            assert validation[
                "valid"
            ], f"JSON validation failed: {validation['errors']}"

    @pytest.mark.parametrize("window", [0, 50000, 1000000])
    def test_credible_sets_by_gene_with_windows(
        self, server_url, test_gene_large_window, window
    ):
        """Test credible sets by gene with different window sizes."""
        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_gene/{test_gene_large_window}",
            params={"format": "json", "interval": 95, "window": window},
            timeout=30,
        )

        assert response.status_code == 200

    def test_credible_sets_by_gene_with_resources(
        self, server_url, test_gene, cs_resources
    ):
        """Test credible sets by gene with resources parameter."""
        if not cs_resources:
            pytest.skip("No resources available")

        resource = cs_resources[0]

        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_gene/{test_gene}",
            params={
                "format": "json",
                "interval": 95,
                "window": 0,
                "resources": [resource],
            },
            timeout=30,
        )

        assert response.status_code == 200

    def test_credible_sets_by_gene_negative_window(self, server_url, test_gene):
        """Test that negative window returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_gene/{test_gene}",
            params={"format": "json", "interval": 95, "window": -1000},
            timeout=10,
        )

        assert response.status_code == 422

    def test_credible_sets_by_gene_invalid_gene(self, server_url, invalid_gene):
        """Test that invalid gene returns 404."""
        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_gene/{invalid_gene}",
            params={"format": "json", "interval": 95, "window": 0},
            timeout=10,
        )

        assert response.status_code == 404

    def test_credible_sets_by_gene_invalid_window_type(self, server_url, test_gene):
        """Test that invalid window type returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_gene/{test_gene}",
            params={"format": "json", "interval": 95, "window": "bad_window"},
            timeout=10,
        )

        assert response.status_code == 422


class TestCredibleSetsByQTLGene:
    """Test /api/v1/credible_sets_by_qtl_gene/{gene} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_credible_sets_by_qtl_gene_formats(
        self, server_url, test_gene_large_window, format
    ):
        """Test credible sets by QTL gene with both TSV and JSON formats."""
        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_qtl_gene/{test_gene_large_window}",
            params={"format": format, "interval": 95},
            timeout=30,
        )

        assert response.status_code == 200

        if format == "tsv":
            assert "text/tab-separated-values" in response.headers.get(
                "content-type", ""
            )
            validation = validate_tsv_response(response.text)
            assert validation["valid"], f"TSV validation failed: {validation['errors']}"
        else:  # json
            assert "application/json" in response.headers.get("content-type", "")
            data = response.json()
            validation = validate_json_response(data)
            assert validation[
                "valid"
            ], f"JSON validation failed: {validation['errors']}"

    def test_credible_sets_by_qtl_gene_with_resources(
        self, server_url, test_gene_large_window, cs_resources
    ):
        """Test credible sets by QTL gene with resources parameter."""
        if not cs_resources:
            pytest.skip("No resources available")

        resource = "eqtl_catalogue"

        endpoint = (
            f"{server_url}/api/v1/credible_sets_by_qtl_gene/{test_gene_large_window}"
        )
        params = {"format": "json", "interval": 95, "resources": [resource]}
        response = requests.get(endpoint, params=params, timeout=30)

        from urllib.parse import urlencode

        full_url = f"{endpoint}?{urlencode(params, doseq=True)}"

        assert response.status_code == 200, (
            f"Request failed!\n"
            f"URL: {full_url}\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text[:500]}"
        )

    def test_credible_sets_by_qtl_gene_invalid_gene(self, server_url, invalid_gene):
        """Test that invalid gene returns 404."""
        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_qtl_gene/{invalid_gene}",
            params={"format": "json", "interval": 95},
            timeout=10,
        )

        assert response.status_code == 404

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_credible_sets_by_qtl_gene_multiple_genes(
        self, server_url, format
    ):
        """Test credible sets by QTL gene with multiple comma-separated genes."""
        genes = "PCSK9,APOE"

        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_qtl_gene/{genes}",
            params={"format": format, "interval": 95},
            timeout=60,
        )

        assert response.status_code == 200

        if format == "tsv":
            assert "text/tab-separated-values" in response.headers.get(
                "content-type", ""
            )
            validation = validate_tsv_response(response.text)
            assert validation["valid"], f"TSV validation failed: {validation['errors']}"
        else:
            assert "application/json" in response.headers.get("content-type", "")
            data = response.json()
            validation = validate_json_response(data)
            assert validation[
                "valid"
            ], f"JSON validation failed: {validation['errors']}"

    def test_credible_sets_by_qtl_gene_multiple_genes_one_invalid(
        self, server_url, invalid_gene
    ):
        """Test that a multi-gene query with one invalid gene returns 404."""
        genes = f"PCSK9,{invalid_gene}"

        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_qtl_gene/{genes}",
            params={"format": "json", "interval": 95},
            timeout=10,
        )

        assert response.status_code == 404


class TestCredibleSetsByGeneMultiGene:
    """Test multi-gene functionality for /api/v1/credible_sets_by_gene/{gene} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_credible_sets_by_gene_multiple_genes(
        self, server_url, format
    ):
        """Test credible sets by gene with multiple comma-separated genes."""
        genes = "GPT,PCSK9"

        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_gene/{genes}",
            params={"format": format, "interval": 95, "window": 0},
            timeout=60,
        )

        assert response.status_code == 200

        if format == "tsv":
            assert "text/tab-separated-values" in response.headers.get(
                "content-type", ""
            )
            validation = validate_tsv_response(response.text)
            assert validation["valid"], f"TSV validation failed: {validation['errors']}"
        else:
            assert "application/json" in response.headers.get("content-type", "")
            data = response.json()
            validation = validate_json_response(data)
            assert validation[
                "valid"
            ], f"JSON validation failed: {validation['errors']}"

    def test_credible_sets_by_gene_multiple_genes_with_window(
        self, server_url
    ):
        """Test credible sets by gene with multiple genes and a window."""
        genes = "GPT,PCSK9"

        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_gene/{genes}",
            params={"format": "json", "interval": 95, "window": 50000},
            timeout=60,
        )

        assert response.status_code == 200

    def test_credible_sets_by_gene_multiple_genes_one_invalid(
        self, server_url, invalid_gene
    ):
        """Test that a multi-gene query with one invalid gene returns 404."""
        genes = f"GPT,{invalid_gene}"

        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_gene/{genes}",
            params={"format": "json", "interval": 95, "window": 0},
            timeout=10,
        )

        assert response.status_code == 404

    def test_credible_sets_by_gene_empty_gene_list(self, server_url):
        """Test that empty gene list returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_gene/,,,",
            params={"format": "json", "interval": 95, "window": 0},
            timeout=10,
        )

        assert response.status_code == 422

    def test_credible_sets_by_gene_genes_with_whitespace(self, server_url):
        """Test that genes with surrounding whitespace are handled correctly."""
        genes = " GPT , PCSK9 "

        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_gene/{genes}",
            params={"format": "json", "interval": 95, "window": 0},
            timeout=60,
        )

        assert response.status_code == 200

    @pytest.mark.parametrize("gene_case", ["gpt", "Gpt", "GPT", "gPt"])
    def test_credible_sets_by_gene_case_insensitive(self, server_url, gene_case):
        """Test that gene names are case-insensitive."""
        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_gene/{gene_case}",
            params={"format": "json", "interval": 95, "window": 0},
            timeout=30,
        )

        assert response.status_code == 200

    def test_credible_sets_by_gene_multiple_genes_mixed_case(self, server_url):
        """Test multi-gene query with mixed case gene names."""
        genes = "gpt,Pcsk9,APOE"

        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_gene/{genes}",
            params={"format": "json", "interval": 95, "window": 0},
            timeout=60,
        )

        assert response.status_code == 200


class TestCredibleSetsByQTLGeneMultiGene:
    """Test multi-gene functionality for /api/v1/credible_sets_by_qtl_gene/{gene} endpoint."""

    @pytest.mark.parametrize("gene_case", ["pcsk9", "Pcsk9", "PCSK9", "pCsK9"])
    def test_credible_sets_by_qtl_gene_case_insensitive(self, server_url, gene_case):
        """Test that gene names are case-insensitive for QTL gene endpoint."""
        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_qtl_gene/{gene_case}",
            params={"format": "json", "interval": 95},
            timeout=30,
        )

        assert response.status_code == 200

    def test_credible_sets_by_qtl_gene_multiple_genes_mixed_case(self, server_url):
        """Test multi-gene QTL query with mixed case gene names."""
        genes = "pcsk9,Apoe"

        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_qtl_gene/{genes}",
            params={"format": "json", "interval": 95},
            timeout=60,
        )

        assert response.status_code == 200


def _expected_leads(cs_rows):
    """Reference lead-per-cs_id: highest pip, ties broken by highest mlog10p."""
    best = {}
    for row in cs_rows:
        cs_id = row["cs_id"]
        key = (row.get("pip") or float("-inf"), row.get("mlog10p") or float("-inf"))
        if cs_id not in best or key > best[cs_id][0]:
            best[cs_id] = (key, row)
    return {cs_id: row for cs_id, (_, row) in best.items()}


class TestCredibleSetsByPhenotypeLeads:
    """Test /api/v1/credible_sets_by_phenotype_leads/{resource}/{phenotype} endpoint."""

    def test_leads_match_max_pip_per_cs(self, server_url, example_phenotypes):
        """Each returned lead is the highest-pip (tie: mlog10p) variant of its cs_id."""
        if not example_phenotypes:
            pytest.skip("No example phenotypes available")

        resource, phenotype = next(iter(example_phenotypes.items()))

        full = requests.get(
            f"{server_url}/api/v1/credible_sets_by_phenotype/{resource}/{phenotype}",
            params={"format": "json", "interval": 95},
            timeout=30,
        )
        assert full.status_code == 200
        cs_rows = full.json()

        leads = requests.get(
            f"{server_url}/api/v1/credible_sets_by_phenotype_leads/{resource}/{phenotype}",
            params={"format": "json", "interval": 95},
            timeout=30,
        )
        assert leads.status_code == 200
        lead_rows = leads.json()

        expected = _expected_leads(cs_rows)
        # exactly one lead per credible set
        assert len(lead_rows) == len(expected)
        assert {r["cs_id"] for r in lead_rows} == set(expected)
        # and each lead is the expected max-pip variant
        for r in lead_rows:
            exp = expected[r["cs_id"]]
            assert (r["chr"], r["pos"], r["ref"], r["alt"]) == (
                exp["chr"], exp["pos"], exp["ref"], exp["alt"]
            )

    def test_leads_invalid_interval(self, server_url, example_phenotypes):
        if not example_phenotypes:
            pytest.skip("No example phenotypes available")
        resource, phenotype = next(iter(example_phenotypes.items()))
        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_phenotype_leads/{resource}/{phenotype}",
            params={"format": "json", "interval": 90},
            timeout=10,
        )
        assert response.status_code == 422

    def test_leads_not_found(self, server_url, cs_resources):
        if not cs_resources:
            pytest.skip("No resources available")
        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_phenotype_leads/{cs_resources[0]}/NONEXISTENT_PHENO_12345",
            params={"format": "json", "interval": 95},
            timeout=10,
        )
        assert response.status_code == 404


class TestAccumulateCsLeads:
    """Unit tests for the streaming lead accumulator (no server needed)."""

    HEADER = ["chr", "pos", "ref", "alt", "mlog10p", "beta", "pip", "cs_id"]
    SCHEMA = {
        "chr": int, "pos": int, "ref": str, "alt": str,
        "mlog10p": float, "beta": float, "pip": float, "cs_id": str,
    }

    def _run(self, rows):
        from app.core.streams import accumulate_cs_leads

        async def line_stream():
            yield list(self.HEADER)
            for r in rows:
                yield [str(x) for x in r]

        return asyncio.run(accumulate_cs_leads(line_stream(), self.SCHEMA))

    def test_picks_max_pip_across_interleaved_cs_ids(self):
        rows = [
            # cs A, cs B rows interleaved; leads are A@pos2 (pip .8) and B@pos5 (pip .6)
            (1, 1, "A", "T", 5.0, 0.1, 0.3, "csA"),
            (1, 4, "G", "C", 4.0, 0.2, 0.4, "csB"),
            (1, 2, "C", "G", 7.0, -0.3, 0.8, "csA"),
            (1, 5, "T", "A", 9.0, 0.5, 0.6, "csB"),
            (1, 3, "A", "G", 6.0, 0.2, 0.5, "csA"),
        ]
        leads = {r["cs_id"]: r for r in self._run(rows)}
        assert len(leads) == 2
        assert (leads["csA"]["pos"], leads["csA"]["pip"]) == (2, 0.8)
        assert (leads["csB"]["pos"], leads["csB"]["pip"]) == (5, 0.6)
        # the data beta rides along on the lead row
        assert leads["csA"]["beta"] == -0.3

    def test_pip_tie_broken_by_mlog10p(self):
        rows = [
            (2, 10, "A", "T", 3.0, 0.1, 0.5, "cs1"),
            (2, 11, "G", "C", 8.0, 0.2, 0.5, "cs1"),  # same pip, higher mlog10p -> lead
        ]
        leads = self._run(rows)
        assert len(leads) == 1
        assert leads[0]["pos"] == 11
