"""
Tests for exome results endpoints.
"""

import pytest
import requests
from helpers.validators import (
    validate_tsv_response,
    validate_json_response,
)


class TestExomeResultsByPhenotype:
    """Test /api/v1/exome_results_by_phenotype/{resource}/{phenotype} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_exome_results_by_phenotype_formats(self, server_url, format):
        """Test exome results by phenotype with both TSV and JSON formats."""
        # use genebass as the resource and a sample phenotype
        resource = "genebass"
        phenotype = "categorical_41210_both_sexes_S068_"

        response = requests.get(
            f"{server_url}/api/v1/exome_results_by_phenotype/{resource}/{phenotype}",
            params={"format": format},
            timeout=30,
        )

        assert response.status_code in [200, 404], f"Unexpected status: {response.status_code}"

        # if phenotype doesn't exist yet, skip validation
        if response.status_code == 404:
            pytest.skip("Phenotype not available yet")

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

    def test_exome_results_by_phenotype_not_found(self, server_url):
        """Test that non-existent phenotype returns 404."""
        resource = "genebass"
        fake_phenotype = "NONEXISTENT_PHENOTYPE_12345"

        response = requests.get(
            f"{server_url}/api/v1/exome_results_by_phenotype/{resource}/{fake_phenotype}",
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 404


class TestExomeResultsByRegion:
    """Test /api/v1/exome_results_by_region/{region} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_exome_results_by_region_formats(self, server_url, format):
        """Test exome results by region with both TSV and JSON formats."""
        # use a small region on chromosome 1
        region = "1:925947-925977"
        endpoint = f"{server_url}/api/v1/exome_results_by_region/{region}"
        params = {"format": format}
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

    def test_exome_results_by_region_with_resource(self, server_url):
        """Test exome results by region with specific resource."""
        region = "1:925947-925977"
        resource = "genebass"

        response = requests.get(
            f"{server_url}/api/v1/exome_results_by_region/{region}",
            params={"format": "json", "resources": [resource]},
            timeout=30,
        )

        assert response.status_code == 200

    def test_exome_results_by_region_invalid_region(self, server_url):
        """Test that invalid region format returns 422."""
        invalid_region = "invalid-region-format"

        response = requests.get(
            f"{server_url}/api/v1/exome_results_by_region/{invalid_region}",
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 422


class TestExomeResultsByVariant:
    """Test /api/v1/exome_results_by_variant/{variant} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_exome_results_by_variant_formats(self, server_url, format):
        """Test exome results by variant with both TSV and JSON formats."""
        # use a variant from the sample data
        variant = "1-925947-C-T"

        response = requests.get(
            f"{server_url}/api/v1/exome_results_by_variant/{variant}",
            params={"format": format},
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

    def test_exome_results_by_variant_with_resource(self, server_url):
        """Test exome results by variant with specific resource."""
        variant = "1-925947-C-T"
        resource = "genebass"

        response = requests.get(
            f"{server_url}/api/v1/exome_results_by_variant/{variant}",
            params={"format": "json", "resources": [resource]},
            timeout=30,
        )

        assert response.status_code == 200

    def test_exome_results_by_variant_invalid_variant(self, server_url):
        """Test that invalid variant format returns 422."""
        invalid_variant = "invalid-variant"

        response = requests.get(
            f"{server_url}/api/v1/exome_results_by_variant/{invalid_variant}",
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 422


class TestExomeResultsByGene:
    """Test /api/v1/exome_results_by_gene/{gene} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_exome_results_by_gene_formats(self, server_url, format):
        """Test exome results by gene with both TSV and JSON formats."""
        # use SAMD11 gene from the sample data
        gene = "SAMD11"

        response = requests.get(
            f"{server_url}/api/v1/exome_results_by_gene/{gene}",
            params={"format": format},
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

    def test_exome_results_by_gene_with_window(self, server_url):
        """Test exome results by gene with custom window."""
        gene = "SAMD11"
        window = 50000

        response = requests.get(
            f"{server_url}/api/v1/exome_results_by_gene/{gene}",
            params={"format": "json", "window": window},
            timeout=30,
        )

        assert response.status_code == 200

    def test_exome_results_by_gene_with_resource(self, server_url):
        """Test exome results by gene with specific resource."""
        gene = "SAMD11"
        resource = "genebass"

        response = requests.get(
            f"{server_url}/api/v1/exome_results_by_gene/{gene}",
            params={"format": "json", "resources": [resource]},
            timeout=30,
        )

        assert response.status_code == 200

    def test_exome_results_by_gene_not_found(self, server_url):
        """Test that non-existent gene returns 404."""
        fake_gene = "NONEXISTENT_GENE_12345"

        response = requests.get(
            f"{server_url}/api/v1/exome_results_by_gene/{fake_gene}",
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 404

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_exome_results_by_gene_multiple_genes(self, server_url, format):
        """Test exome results by gene with multiple comma-separated genes."""
        genes = "SAMD11,PCSK9"

        response = requests.get(
            f"{server_url}/api/v1/exome_results_by_gene/{genes}",
            params={"format": format},
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

    def test_exome_results_by_gene_multiple_genes_with_window(self, server_url):
        """Test exome results by gene with multiple genes and window."""
        genes = "SAMD11,PCSK9"
        window = 50000

        response = requests.get(
            f"{server_url}/api/v1/exome_results_by_gene/{genes}",
            params={"format": "json", "window": window},
            timeout=60,
        )

        assert response.status_code == 200

    def test_exome_results_by_gene_multiple_genes_one_invalid(self, server_url):
        """Test that multi-gene query with one invalid gene returns 404."""
        genes = "SAMD11,NONEXISTENT_GENE_12345"

        response = requests.get(
            f"{server_url}/api/v1/exome_results_by_gene/{genes}",
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 404

    def test_exome_results_by_gene_empty_gene_list(self, server_url):
        """Test that empty gene list returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/exome_results_by_gene/,,,",
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 422

    @pytest.mark.parametrize("gene_case", ["samd11", "Samd11", "SAMD11", "SaMd11"])
    def test_exome_results_by_gene_case_insensitive(self, server_url, gene_case):
        """Test that gene names are case-insensitive."""
        response = requests.get(
            f"{server_url}/api/v1/exome_results_by_gene/{gene_case}",
            params={"format": "json"},
            timeout=30,
        )

        assert response.status_code == 200

    def test_exome_results_by_gene_multiple_genes_mixed_case(self, server_url):
        """Test multi-gene query with mixed case gene names."""
        genes = "samd11,Pcsk9"

        response = requests.get(
            f"{server_url}/api/v1/exome_results_by_gene/{genes}",
            params={"format": "json"},
            timeout=60,
        )

        assert response.status_code == 200
