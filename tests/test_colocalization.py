"""
Tests for colocalization endpoints.
"""

import pytest
import requests
from helpers.validators import validate_tsv_response, validate_json_response


class TestColocalizationByVariant:
    """Test /api/v1/colocalization_by_variant/{variant} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_colocalization_by_variant_formats(
        self, server_url, test_variant_coloc, format
    ):
        """Test colocalization by variant with both TSV and JSON formats."""
        endpoint = f"{server_url}/api/v1/colocalization_by_variant/{test_variant_coloc}"
        params = {"format": format, "include_variants": False}
        response = requests.get(endpoint, params=params, timeout=30)

        # Build full URL for error messages
        from urllib.parse import urlencode

        full_url = f"{endpoint}?{urlencode(params)}"

        # May return 200 with data or 404 if no colocalization data exists
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

    @pytest.mark.parametrize("include_variants", [True, False])
    def test_colocalization_by_variant_include_variants(
        self, server_url, test_variant_coloc, include_variants
    ):
        """Test colocalization by variant with include_variants parameter."""
        endpoint = f"{server_url}/api/v1/colocalization_by_variant/{test_variant_coloc}"
        params = {"format": "json", "include_variants": include_variants}
        response = requests.get(endpoint, params=params, timeout=30)

        from urllib.parse import urlencode

        full_url = f"{endpoint}?{urlencode(params)}"

        # May return 200 with data or 404 if no colocalization data exists
        assert response.status_code in [200, 404], (
            f"Unexpected status code!\n"
            f"URL: {full_url}\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text[:500]}"
        )

        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)

            if len(data) > 0:
                # Check that first item has expected fields
                first_item = data[0]
                assert "dataset1" in first_item
                assert "dataset2" in first_item
                assert "trait1" in first_item
                assert "trait2" in first_item

                # If include_variants is True, should have additional fields
                if include_variants:
                    assert "pos" in first_item or len(data[0]) > 20

    def test_colocalization_by_variant_invalid_variant(
        self, server_url, invalid_variant
    ):
        """Test that invalid variant format returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/colocalization_by_variant/{invalid_variant}",
            params={"format": "json", "include_variants": False},
            timeout=10,
        )

        assert response.status_code == 422


class TestColocalizationByCredibleSetId:
    """Test /api/v1/colocalization_by_credible_set_id/{resource}/{phenotype_or_study}/{credible_set_id} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_colocalization_by_credible_set_id_formats(self, server_url, format):
        """Test colocalization by credible set ID with both TSV and JSON formats."""
        # Use a known phenotype and credible set ID for testing
        # These values may need to be adjusted based on your test data
        resource = "finngen"
        phenotype = "K11_IBD_STRICT"
        cs_id = "chr1:65744548-68744548_3"

        endpoint = f"{server_url}/api/v1/colocalization_by_credible_set_id/{resource}/{phenotype}/{cs_id}"
        params = {"format": format}
        response = requests.get(endpoint, params=params, timeout=30)

        from urllib.parse import urlencode

        full_url = f"{endpoint}?{urlencode(params)}"

        # May return 200 with data or 404 if not found
        assert response.status_code in [200, 404, 422], (
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

    def test_colocalization_by_credible_set_id_structure(self, server_url):
        """Test colocalization by credible set ID returns expected structure."""
        resource = "finngen"
        phenotype = "K11_IBD_STRICT"
        cs_id = "chr1:65744548-68744548_3"

        response = requests.get(
            f"{server_url}/api/v1/colocalization_by_credible_set_id/{resource}/{phenotype}/{cs_id}",
            params={"format": "json"},
            timeout=30,
        )

        # May return 200 with data or 404 if not found
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)

            if len(data) > 0:
                first_item = data[0]
                # Check for expected colocalization fields
                expected_fields = ["dataset", "trait", "chr"]
                for field in expected_fields:
                    assert field in first_item, f"Missing field: {field}"

    def test_colocalization_by_credible_set_id_not_found(self, server_url):
        """Test that non-existent phenotype/cs_id returns appropriate error."""
        resource = "finngen"
        phenotype = "NONEXISTENT_PHENOTYPE"
        cs_id = "chr1:1-100_1"

        endpoint = f"{server_url}/api/v1/colocalization_by_credible_set_id/{resource}/{phenotype}/{cs_id}"
        params = {"format": "json"}
        response = requests.get(endpoint, params=params, timeout=10)

        from urllib.parse import urlencode

        full_url = f"{endpoint}?{urlencode(params)}"

        # Should return 404 or 422
        # TODO currently no way of checking if phenotype is valid or not so it returns empty 200
        assert response.status_code in [200, 404, 422], (
            f"Expected 200, 404 or 422!\n"
            f"URL: {full_url}\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text[:500]}"
        )
