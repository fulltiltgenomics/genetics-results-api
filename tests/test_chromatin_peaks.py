"""
Tests for chromatin peaks endpoints.
"""

import pytest
import requests
from helpers.validators import validate_tsv_response, validate_json_response


@pytest.fixture(scope="session")
def test_peak_id():
    """A peak ID that should exist in the database."""
    return "chr1-817095-817594"


@pytest.fixture(scope="session")
def invalid_peak_id():
    """An invalid peak ID format for negative testing."""
    return "invalid-peak-id"


@pytest.fixture(scope="session")
def available_chromatin_resources():
    """Get list of available chromatin peaks resources from config."""
    import app.config.chromatin_peaks as config

    return [c["resource"] for c in config.chromatin_peaks_data]


class TestPeakToGenes:
    """Test /api/v1/peak_to_genes/{peak_id} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_peak_to_genes_formats(self, server_url, test_peak_id, format):
        """Test peak to genes with both TSV and JSON formats."""
        endpoint = f"{server_url}/api/v1/peak_to_genes/{test_peak_id}"
        params = {"format": format}
        response = requests.get(endpoint, params=params, timeout=30)

        from urllib.parse import urlencode

        full_url = f"{endpoint}?{urlencode(params)}"

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
            else:
                assert "application/json" in response.headers.get("content-type", "")
                data = response.json()
                validation = validate_json_response(data)
                assert validation[
                    "valid"
                ], f"JSON validation failed: {validation['errors']}"

    def test_peak_to_genes_with_resources(
        self, server_url, test_peak_id, available_chromatin_resources
    ):
        """Test peak to genes with resources parameter."""
        endpoint = f"{server_url}/api/v1/peak_to_genes/{test_peak_id}"
        params = {"format": "json", "resources": available_chromatin_resources}
        response = requests.get(endpoint, params=params, timeout=30)

        from urllib.parse import urlencode

        full_url = f"{endpoint}?{urlencode(params, doseq=True)}"

        assert response.status_code in [200, 404], (
            f"Unexpected status code!\n"
            f"URL: {full_url}\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text[:500]}"
        )

    def test_peak_to_genes_invalid_peak_id_format(self, server_url, invalid_peak_id):
        """Test that invalid peak ID format returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/peak_to_genes/{invalid_peak_id}",
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 422

    def test_peak_to_genes_nonexistent_peak(self, server_url):
        """Test that a valid format but nonexistent peak returns 200."""
        response = requests.get(
            f"{server_url}/api/v1/peak_to_genes/chr99-1-100",
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 200

    def test_peak_to_genes_invalid_resource(self, server_url, test_peak_id):
        """Test that invalid resource returns 404."""
        response = requests.get(
            f"{server_url}/api/v1/peak_to_genes/{test_peak_id}",
            params={"format": "json", "resources": ["nonexistent_resource"]},
            timeout=10,
        )

        assert response.status_code == 404

    def test_peak_to_genes_structure(self, server_url, test_peak_id):
        """Test peak to genes returns expected structure."""
        response = requests.get(
            f"{server_url}/api/v1/peak_to_genes/{test_peak_id}",
            params={"format": "json"},
            timeout=30,
        )

        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)

            if len(data) > 0:
                first_item = data[0]
                assert isinstance(first_item, dict)
                # check expected fields from chromatin_peaks_header_schema
                expected_fields = [
                    "resource",
                    "chrom",
                    "start",
                    "end",
                    "peak_id",
                    "gene_id",
                    "symbol",
                    "cell_type",
                ]
                for field in expected_fields:
                    assert field in first_item, f"Missing expected field: {field}"

    def test_peak_to_genes_tsv_header(self, server_url, test_peak_id):
        """Test that TSV response has correct header columns."""
        response = requests.get(
            f"{server_url}/api/v1/peak_to_genes/{test_peak_id}",
            params={"format": "tsv"},
            timeout=30,
        )

        if response.status_code == 200:
            lines = response.text.strip().split("\n")
            assert len(lines) >= 1, "TSV should have at least a header line"
            header = lines[0].split("\t")
            expected_columns = [
                "resource",
                "version",
                "chrom",
                "start",
                "end",
                "peak_id",
                "gene_id",
                "symbol",
                "cell_type",
            ]
            for col in expected_columns:
                assert col in header, f"Missing expected column: {col}"
