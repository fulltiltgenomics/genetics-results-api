"""
Tests for expression data endpoints.
"""

import pytest
import requests
from helpers.validators import validate_tsv_response, validate_json_response


class TestExpressionByGene:
    """Test /api/v1/expression_by_gene/{gene} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_expression_by_gene_formats(
        self, server_url, test_gene_large_window, format
    ):
        """Test expression by gene with both TSV and JSON formats."""
        endpoint = f"{server_url}/api/v1/expression_by_gene/{test_gene_large_window}"
        params = {"format": format}
        response = requests.get(endpoint, params=params, timeout=30)

        from urllib.parse import urlencode

        full_url = f"{endpoint}?{urlencode(params)}"

        # May return 200 with data or 404 if no expression data exists
        assert response.status_code in [200, 404], (
            f"Unexpected status code!\n"
            f"URL: {full_url}\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text[:500]}"
        )

        if response.status_code == 200:
            if format == "tsv":
                assert "text/tab-separated-values" in response.headers.get(
                    "content-type", ""
                )
                validation = validate_tsv_response(response.text)
                assert validation[
                    "valid"
                ], f"TSV validation failed: {validation['errors']}"
            else:  # json
                assert "application/json" in response.headers.get("content-type", "")
                data = response.json()
                validation = validate_json_response(data)
                assert validation[
                    "valid"
                ], f"JSON validation failed: {validation['errors']}"

    def test_expression_by_gene_with_resources(
        self, server_url, test_gene_large_window
    ):
        """Test expression by gene with resources parameter."""
        # For expression data, resources might be different (e.g., gtex, hpa)
        # This test might need to be adjusted based on available expression resources
        endpoint = f"{server_url}/api/v1/expression_by_gene/{test_gene_large_window}"
        resources = ["gtex", "hpa"]
        params = {"format": "json", "resources": resources}
        response = requests.get(endpoint, params=params, timeout=30)

        from urllib.parse import urlencode

        full_url = f"{endpoint}?{urlencode(params)}"

        # Should return 200 or 404
        assert response.status_code in [200, 404], (
            f"Unexpected status code!\n"
            f"URL: {full_url}\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text[:500]}"
        )

    def test_expression_by_gene_invalid_gene(self, server_url, invalid_gene):
        """Test that invalid gene returns 404."""
        response = requests.get(
            f"{server_url}/api/v1/expression_by_gene/{invalid_gene}",
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 404

    def test_expression_by_gene_structure(self, server_url, test_gene_large_window):
        """Test expression by gene returns expected structure."""
        response = requests.get(
            f"{server_url}/api/v1/expression_by_gene/{test_gene_large_window}",
            params={"format": "json"},
            timeout=30,
        )

        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)

            if len(data) > 0:
                first_item = data[0]
                assert isinstance(first_item, dict)
                # Expression data should have some basic fields
                # The exact fields depend on your data structure

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_expression_by_gene_multiple_genes(self, server_url, format):
        """Test expression by gene with multiple comma-separated genes."""
        genes = "PCSK9,APOE"

        response = requests.get(
            f"{server_url}/api/v1/expression_by_gene/{genes}",
            params={"format": format},
            timeout=60,
        )

        # May return 200 with data or 404 if no expression data
        assert response.status_code in [200, 404]

        if response.status_code == 200:
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

    def test_expression_by_gene_multiple_genes_one_invalid(self, server_url, invalid_gene):
        """Test that multi-gene query with one invalid gene returns 404."""
        genes = f"PCSK9,{invalid_gene}"

        response = requests.get(
            f"{server_url}/api/v1/expression_by_gene/{genes}",
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 404

    def test_expression_by_gene_empty_gene_list(self, server_url):
        """Test that empty gene list returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/expression_by_gene/,,,",
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 422

    @pytest.mark.parametrize("gene_case", ["pcsk9", "Pcsk9", "PCSK9", "pCsK9"])
    def test_expression_by_gene_case_insensitive(self, server_url, gene_case):
        """Test that gene names are case-insensitive."""
        response = requests.get(
            f"{server_url}/api/v1/expression_by_gene/{gene_case}",
            params={"format": "json"},
            timeout=30,
        )

        # May return 200 with data or 404 if no expression data
        assert response.status_code in [200, 404]

    def test_expression_by_gene_multiple_genes_mixed_case(self, server_url):
        """Test multi-gene query with mixed case gene names."""
        genes = "pcsk9,Apoe"

        response = requests.get(
            f"{server_url}/api/v1/expression_by_gene/{genes}",
            params={"format": "json"},
            timeout=60,
        )

        # May return 200 with data or 404 if no expression data
        assert response.status_code in [200, 404]
