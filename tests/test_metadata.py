"""
Tests for metadata and resource information endpoints.
"""

import pytest
import requests
from helpers.validators import validate_tsv_response, validate_json_response


class TestResourceMetadata:
    """Test resource metadata endpoints."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_resource_metadata_formats(self, server_url, resources_with_metadata, format):
        """Test resource metadata endpoint with both TSV and JSON formats."""
        if not resources_with_metadata:
            pytest.skip("No resources with metadata available")

        resource = resources_with_metadata[0]
        response = requests.get(
            f"{server_url}/api/v1/resource_metadata/{resource}",
            params={"format": format},
            timeout=30,
        )

        assert response.status_code == 200

        if format == "tsv":
            assert "text/tab-separated-values" in response.headers.get("content-type", "")
            validation = validate_tsv_response(response.text, min_data_lines=1)
            assert validation["valid"], f"TSV validation failed: {validation['errors']}"
            assert validation["has_header"], "TSV should have header"
            assert validation["has_data"], "TSV should have data"
        else:  # json
            assert "application/json" in response.headers.get("content-type", "")
            data = response.json()
            validation = validate_json_response(data, min_items=1)
            assert validation["valid"], f"JSON validation failed: {validation['errors']}"
            assert validation["has_data"], "JSON should have data"

    def test_resource_metadata_all_resources(self, server_url, resources_with_metadata):
        """Test that all resources with metadata return valid data."""
        if not resources_with_metadata:
            pytest.skip("No resources with metadata available")

        for resource in resources_with_metadata:
            response = requests.get(
                f"{server_url}/api/v1/resource_metadata/{resource}",
                params={"format": "json"},
                timeout=30,
            )
            assert response.status_code == 200, f"Failed for resource: {resource}"

            data = response.json()
            assert len(data) > 0, f"No metadata for resource: {resource}"

    def test_resource_metadata_invalid_resource(self, server_url, invalid_resource):
        """Test resource metadata with invalid resource returns 404."""
        response = requests.get(
            f"{server_url}/api/v1/resource_metadata/{invalid_resource}",
            params={"format": "json"},
            timeout=10,
        )
        assert response.status_code == 404

    def test_resource_metadata_invalid_format(self, server_url, resources_with_metadata):
        """Test resource metadata with invalid format parameter."""
        if not resources_with_metadata:
            pytest.skip("No resources with metadata available")

        resource = resources_with_metadata[0]
        response = requests.get(
            f"{server_url}/api/v1/resource_metadata/{resource}",
            params={"format": "csv"},  # Invalid format
            timeout=10,
        )
        assert response.status_code == 422


class TestTraitNameMapping:
    """Test trait name mapping endpoint."""

    def test_trait_name_mapping_returns_json(self, server_url):
        """Test that trait name mapping returns valid JSON."""
        response = requests.get(
            f"{server_url}/api/v1/trait_name_mapping",
            timeout=30,
        )

        assert response.status_code == 200
        assert "application/json" in response.headers.get("content-type", "")

        data = response.json()
        assert isinstance(data, dict), "Trait mapping should be a dictionary"
        assert len(data) > 0, "Trait mapping should not be empty"

    def test_trait_name_mapping_structure(self, server_url):
        """Test that trait name mapping has expected structure."""
        response = requests.get(
            f"{server_url}/api/v1/trait_name_mapping",
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        # Check that values are strings
        for trait_id, trait_name in data.items():
            assert isinstance(trait_id, str), "Trait IDs should be strings"
            assert isinstance(trait_name, str), "Trait names should be strings"
            assert trait_name, "Trait name should not be empty"
