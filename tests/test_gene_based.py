"""
Tests for gene-based burden results endpoints.
"""

import pytest
import requests


class TestGeneBased:
    """Test /api/v1/gene_based/{gene} endpoint."""

    def test_gene_based_single_gene(self, server_url, test_gene):
        """Test gene-based results for a single gene."""
        response = requests.get(
            f"{server_url}/api/v1/gene_based/{test_gene}",
            timeout=30,
        )

        assert response.status_code == 200
        assert "application/octet-stream" in response.headers.get("content-type", "")

    def test_gene_based_invalid_gene(self, server_url, invalid_gene):
        """Test that invalid gene returns 404."""
        response = requests.get(
            f"{server_url}/api/v1/gene_based/{invalid_gene}",
            timeout=10,
        )

        assert response.status_code == 404

    def test_gene_based_multiple_genes(self, server_url):
        """Test gene-based results for multiple comma-separated genes."""
        genes = "GPT,PCSK9"

        response = requests.get(
            f"{server_url}/api/v1/gene_based/{genes}",
            timeout=60,
        )

        assert response.status_code == 200
        assert "application/octet-stream" in response.headers.get("content-type", "")

    def test_gene_based_multiple_genes_one_invalid(self, server_url, invalid_gene):
        """Test that a multi-gene query with one invalid gene returns 404."""
        genes = f"GPT,{invalid_gene}"

        response = requests.get(
            f"{server_url}/api/v1/gene_based/{genes}",
            timeout=10,
        )

        assert response.status_code == 404

    def test_gene_based_empty_gene_list(self, server_url):
        """Test that empty gene list returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/gene_based/,,,",
            timeout=10,
        )

        assert response.status_code == 422

    def test_gene_based_genes_with_whitespace(self, server_url):
        """Test that genes with surrounding whitespace are handled correctly."""
        genes = " GPT , PCSK9 "

        response = requests.get(
            f"{server_url}/api/v1/gene_based/{genes}",
            timeout=60,
        )

        assert response.status_code == 200

    @pytest.mark.parametrize("gene_case", ["gpt", "Gpt", "GPT", "gPt"])
    def test_gene_based_case_insensitive(self, server_url, gene_case):
        """Test that gene names are case-insensitive."""
        response = requests.get(
            f"{server_url}/api/v1/gene_based/{gene_case}",
            timeout=30,
        )

        assert response.status_code == 200

    def test_gene_based_multiple_genes_mixed_case(self, server_url):
        """Test multi-gene query with mixed case gene names."""
        genes = "gpt,Pcsk9"

        response = requests.get(
            f"{server_url}/api/v1/gene_based/{genes}",
            timeout=60,
        )

        assert response.status_code == 200
