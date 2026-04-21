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

    def test_gene_based_multiple_resources(self, server_url):
        """Test that results include data from all configured resources (genebass and SCHEMA)."""
        response = requests.get(
            f"{server_url}/api/v1/gene_based/GRIN2A",
            timeout=30,
        )

        assert response.status_code == 200
        text = response.text
        lines = [l for l in text.strip().split("\n") if l]
        assert len(lines) > 1, "Expected header + data rows"

        # first line should be the header (without # prefix)
        header = lines[0]
        assert header.startswith("dataset\t"), f"Expected header to start with 'dataset', got: {header[:50]}"

        # collect dataset values from data rows
        datasets = set()
        for line in lines[1:]:
            dataset = line.split("\t")[0]
            datasets.add(dataset)

        assert "genebass" in datasets, f"Expected genebass in datasets, got: {datasets}"
        assert "SCHEMA" in datasets, f"Expected SCHEMA in datasets, got: {datasets}"

    def test_gene_based_response_has_single_header(self, server_url):
        """Test that merged response contains exactly one header line."""
        response = requests.get(
            f"{server_url}/api/v1/gene_based/GRIN2A",
            timeout=30,
        )

        assert response.status_code == 200
        lines = response.text.strip().split("\n")

        # header lines would start with 'dataset' (after # stripping)
        header_lines = [l for l in lines if l.startswith("dataset\t")]
        assert len(header_lines) == 1, f"Expected exactly 1 header line, got {len(header_lines)}"

    def test_gene_based_response_columns(self, server_url):
        """Test that response has the expected column structure."""
        response = requests.get(
            f"{server_url}/api/v1/gene_based/GPT",
            timeout=30,
        )

        assert response.status_code == 200
        lines = response.text.strip().split("\n")
        assert len(lines) > 0

        header = lines[0].split("\t")
        expected_columns = [
            "dataset", "trait", "gene", "gene_id", "gene_chr",
            "gene_start_pos", "gene_end_pos", "annotation",
        ]
        for col in expected_columns:
            assert col in header, f"Expected column '{col}' in header, got: {header}"
