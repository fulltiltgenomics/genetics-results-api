"""
Tests for gene-related endpoints (nearest genes, gene models).
"""

import pytest
import requests
from helpers.validators import validate_tsv_response


class TestNearestGenes:
    """Test /api/v1/nearest_genes/{variant} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_nearest_genes_formats(self, server_url, test_variant, format):
        """Test nearest genes with both TSV and JSON formats."""
        response = requests.get(
            f"{server_url}/api/v1/nearest_genes/{test_variant}",
            params={"format": format, "n": 3},
            timeout=30,
        )

        assert response.status_code in [200, 404]

        if response.status_code == 200:
            if format == "tsv":
                assert "text/tab-separated-values" in response.headers.get(
                    "content-type", ""
                )
                validation = validate_tsv_response(response.text, min_data_lines=1)
                assert validation["valid"], f"TSV validation failed: {validation['errors']}"
            else:  # json
                data = response.json()
                assert isinstance(data, list)
                assert len(data) > 0
                assert len(data) <= 3  # Should return at most n genes

    @pytest.mark.parametrize("n", [1, 3, 5, 10])
    def test_nearest_genes_with_n_parameter(self, server_url, test_variant, n):
        """Test nearest genes with different n values."""
        response = requests.get(
            f"{server_url}/api/v1/nearest_genes/{test_variant}",
            params={"format": "json", "n": n},
            timeout=30,
        )

        assert response.status_code in [200, 404]

        if response.status_code == 200:
            data = response.json()
            assert len(data) <= n

    @pytest.mark.parametrize("gene_type", ["protein_coding", "all"])
    def test_nearest_genes_with_gene_type(self, server_url, test_variant, gene_type):
        """Test nearest genes with different gene types."""
        response = requests.get(
            f"{server_url}/api/v1/nearest_genes/{test_variant}",
            params={"format": "json", "gene_type": gene_type, "n": 3},
            timeout=30,
        )

        assert response.status_code in [200, 404]

    @pytest.mark.parametrize("max_distance", [100000, 1000000, 5000000])
    def test_nearest_genes_with_max_distance(
        self, server_url, test_variant, max_distance
    ):
        """Test nearest genes with different max distances."""
        response = requests.get(
            f"{server_url}/api/v1/nearest_genes/{test_variant}",
            params={"format": "json", "max_distance": max_distance, "n": 3},
            timeout=30,
        )

        assert response.status_code in [200, 404]

    def test_nearest_genes_invalid_variant(self, server_url, invalid_variant):
        """Test that invalid variant format returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/nearest_genes/{invalid_variant}",
            params={"format": "json", "n": 3},
            timeout=10,
        )

        assert response.status_code == 422

    def test_nearest_genes_structure(self, server_url, test_variant):
        """Test nearest genes returns expected structure."""
        response = requests.get(
            f"{server_url}/api/v1/nearest_genes/{test_variant}",
            params={"format": "json", "n": 3},
            timeout=30,
        )

        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)

            if len(data) > 0:
                first_gene = data[0]
                assert isinstance(first_gene, dict)
                # Should have gene name and position info
                # The exact fields depend on your implementation

    def test_nearest_genes_no_genes_found(self, server_url):
        """Test nearest genes when no genes are within max_distance."""
        # Use a variant in a gene desert with very small max_distance
        variant = "7-5397122-C-T"
        response = requests.get(
            f"{server_url}/api/v1/nearest_genes/{variant}",
            params={"format": "json", "n": 3, "max_distance": 1},
            timeout=30,
        )

        # Should return 404 when no genes found within distance
        assert response.status_code in [200, 404]


# TODO add back
# class TestGeneModel:
#     """Test gene model endpoints."""

#     def test_gene_model_by_coordinates(self, server_url):
#         """Test gene model by chromosome coordinates."""
#         chr = "1"
#         start = 1000000
#         end = 1100000

#         response = requests.get(
#             f"{server_url}/api/v1/gene_model/{chr}/{start}/{end}",
#             timeout=30,
#         )

#         # This endpoint might not be in the public schema
#         # Should return 200 or 404
#         assert response.status_code in [200, 404]

#     def test_gene_model_by_gene(self, server_url, test_gene):
#         """Test gene model by gene name."""
#         padding = 10000

#         response = requests.get(
#             f"{server_url}/api/v1/gene_model_by_gene/{test_gene}/{padding}",
#             timeout=30,
#         )

#         # This endpoint might not be in the public schema
#         # Should return 200 or 404
#         assert response.status_code in [200, 404]

#     def test_gene_model_by_gene_negative_padding(self, server_url, test_gene):
#         """Test gene model with negative padding returns 422."""
#         padding = -1000

#         response = requests.get(
#             f"{server_url}/api/v1/gene_model_by_gene/{test_gene}/{padding}",
#             timeout=10,
#         )

#         assert response.status_code == 422

#     def test_gene_model_by_gene_invalid_gene(self, server_url, invalid_gene):
#         """Test gene model with invalid gene returns 404."""
#         padding = 10000

#         response = requests.get(
#             f"{server_url}/api/v1/gene_model_by_gene/{invalid_gene}/{padding}",
#             timeout=10,
#         )

#         assert response.status_code == 404
