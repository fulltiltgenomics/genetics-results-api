"""
Tests for the variant annotation endpoint.
"""

import pytest
import requests
from helpers.validators import validate_tsv_response, validate_json_response

BASE_PATH = "/api/v1/variant_annotation"

# columns expected in the response
EXPECTED_COLUMNS = {
    # the header's leading '#' is stripped by the tabix header reader, so the
    # first column surfaces as 'variant' (not '#variant')
    "variant", "chr", "pos", "ref", "alt", "INFO", "AF", "AC_Het",
    "AC_Hom", "most_severe", "gene_most_severe", "rsid",
    "EXOME_enrichment_nfe", "GENOME_enrichment_nfe", "index",
}

TEST_REGION = "1:13000-15000"
TEST_VARIANT = "1:13668:G:A"
TEST_VARIANT_2 = "1:14506:G:A"


class TestVariantAnnotationByRegion:
    """Test GET /api/v1/variant_annotation/{source} with region query."""

    def test_get_by_region_tsv(self, server_url):
        response = requests.get(
            f"{server_url}{BASE_PATH}/finngen",
            params={"region": TEST_REGION, "format": "tsv"},
            timeout=30,
        )
        assert response.status_code == 200
        assert "text/tab-separated-values" in response.headers.get("content-type", "")
        validation = validate_tsv_response(response.text, min_data_lines=1)
        assert validation["valid"], f"TSV validation failed: {validation['errors']}"
        assert validation["has_header"]

    def test_get_by_region_json(self, server_url):
        response = requests.get(
            f"{server_url}{BASE_PATH}/finngen",
            params={"region": TEST_REGION, "format": "json"},
            timeout=30,
        )
        assert response.status_code == 200
        assert "application/json" in response.headers.get("content-type", "")
        data = response.json()
        validation = validate_json_response(data, min_items=1)
        assert validation["valid"], f"JSON validation failed: {validation['errors']}"
        # verify expected keys are present
        assert EXPECTED_COLUMNS.issubset(validation["keys"]), (
            f"Missing keys: {EXPECTED_COLUMNS - validation['keys']}"
        )

    def test_get_by_region_default_format(self, server_url):
        """Default format (no format param) should return TSV."""
        response = requests.get(
            f"{server_url}{BASE_PATH}/finngen",
            params={"region": TEST_REGION},
            timeout=30,
        )
        assert response.status_code == 200
        assert "text/tab-separated-values" in response.headers.get("content-type", "")


class TestVariantAnnotationByVariant:
    """Test GET /api/v1/variant_annotation/{source} with variant query."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_get_by_variant(self, server_url, format):
        response = requests.get(
            f"{server_url}{BASE_PATH}/finngen",
            params={"variant": TEST_VARIANT, "format": format},
            timeout=30,
        )
        assert response.status_code == 200

        if format == "tsv":
            validation = validate_tsv_response(response.text)
            assert validation["valid"], f"TSV validation failed: {validation['errors']}"
        else:
            data = response.json()
            validation = validate_json_response(data)
            assert validation["valid"], f"JSON validation failed: {validation['errors']}"


class TestVariantAnnotationByGene:
    """Test GET /api/v1/variant_annotation/{source} with gene query."""

    def test_get_by_gene(self, server_url):
        response = requests.get(
            f"{server_url}{BASE_PATH}/finngen",
            params={"gene": "PCSK9", "format": "tsv"},
            timeout=30,
        )
        assert response.status_code == 200
        validation = validate_tsv_response(response.text, min_data_lines=1)
        assert validation["valid"], f"TSV validation failed: {validation['errors']}"


class TestVariantAnnotationPost:
    """Test POST /api/v1/variant_annotation/{source}."""

    def test_post_variants_tsv(self, server_url):
        response = requests.post(
            f"{server_url}{BASE_PATH}/finngen",
            json={"variants": [TEST_VARIANT, TEST_VARIANT_2]},
            params={"format": "tsv"},
            timeout=30,
        )
        assert response.status_code == 200
        assert "text/tab-separated-values" in response.headers.get("content-type", "")
        validation = validate_tsv_response(response.text, min_data_lines=1)
        assert validation["valid"], f"TSV validation failed: {validation['errors']}"

    def test_post_variants_json(self, server_url):
        response = requests.post(
            f"{server_url}{BASE_PATH}/finngen",
            json={"variants": [TEST_VARIANT, TEST_VARIANT_2]},
            params={"format": "json"},
            timeout=30,
        )
        assert response.status_code == 200
        assert "application/json" in response.headers.get("content-type", "")
        data = response.json()
        validation = validate_json_response(data, min_items=1)
        assert validation["valid"], f"JSON validation failed: {validation['errors']}"


class TestVariantAnnotationErrors:
    """Test error handling for variant annotation endpoint."""

    def test_invalid_source(self, server_url):
        response = requests.get(
            f"{server_url}{BASE_PATH}/invalid",
            params={"region": "1:1-100"},
            timeout=10,
        )
        assert response.status_code == 404

    def test_no_query_param(self, server_url):
        response = requests.get(
            f"{server_url}{BASE_PATH}/finngen",
            timeout=10,
        )
        assert response.status_code == 422

    def test_multiple_query_params(self, server_url):
        response = requests.get(
            f"{server_url}{BASE_PATH}/finngen",
            params={"variant": "1:1:A:T", "region": "1:1-100"},
            timeout=10,
        )
        assert response.status_code == 422

    def test_invalid_variant_format(self, server_url):
        response = requests.get(
            f"{server_url}{BASE_PATH}/finngen",
            params={"variant": "invalid"},
            timeout=10,
        )
        assert response.status_code == 422

    def test_invalid_region_format(self, server_url):
        response = requests.get(
            f"{server_url}{BASE_PATH}/finngen",
            params={"region": "invalid"},
            timeout=10,
        )
        assert response.status_code == 422
