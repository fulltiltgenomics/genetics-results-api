"""
Comprehensive tests for credible set endpoints.
"""

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
        self, server_url, available_resources
    ):
        """Test that non-existent phenotype returns 404."""
        if not available_resources:
            pytest.skip("No resources available")

        resource = available_resources[0]
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
        self, server_url, test_region, available_resources
    ):
        """Test credible sets by region with single resource specified."""
        if not available_resources:
            pytest.skip("No resources available")

        resource = available_resources[0]

        response = requests.get(
            f"{server_url}/api/v1/credible_sets_by_region/{test_region}",
            params={"format": "json", "interval": 95, "resources": [resource]},
            timeout=30,
        )

        assert response.status_code == 200

    def test_credible_sets_by_region_with_multiple_resources(
        self, server_url, test_region, available_resources
    ):
        """Test credible sets by region with multiple resources."""
        if len(available_resources) < 2:
            pytest.skip("Need at least 2 resources for this test")

        resources = available_resources[:2]
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
        self, server_url, test_variant, available_resources
    ):
        """Test credible sets by variant with resources parameter."""
        if not available_resources:
            pytest.skip("No resources available")

        resource = available_resources[0]

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
        self, server_url, test_variant, available_resources
    ):
        """Test POST with resources parameter."""
        if not available_resources:
            pytest.skip("No resources available")

        resource = available_resources[0]
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
        self, server_url, test_gene, available_resources
    ):
        """Test credible sets by gene with resources parameter."""
        if not available_resources:
            pytest.skip("No resources available")

        resource = available_resources[0]

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
        self, server_url, test_gene_large_window, available_resources
    ):
        """Test credible sets by QTL gene with resources parameter."""
        if not available_resources:
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
