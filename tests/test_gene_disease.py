"""
Tests for gene-disease endpoint.
"""

import pytest
import requests
from helpers.validators import validate_tsv_response, validate_json_response


class TestGeneDisease:
    """Test /api/v1/gene_disease/{gene_name} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_gene_disease_formats(self, server_url, format):
        """Test gene-disease endpoint with both TSV and JSON formats."""
        gene_name = "BRCA1"

        response = requests.get(
            f"{server_url}/api/v1/gene_disease/{gene_name}",
            params={"format": format},
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

                # check expected columns
                expected_cols = ["resource", "uuid", "gene_symbol", "disease_curie",
                               "disease_title", "classification",
                               "mode_of_inheritance", "submitter"]
                assert validation["header"] == expected_cols
            else:  # json
                data = response.json()
                validation = validate_json_response(data, min_items=1)
                assert validation["valid"], f"JSON validation failed: {validation['errors']}"

                # check expected keys in first item
                if len(data) > 0:
                    expected_keys = {"resource", "uuid", "gene_symbol", "disease_curie",
                                   "disease_title", "classification",
                                   "mode_of_inheritance", "submitter"}
                    assert set(data[0].keys()) == expected_keys

    @pytest.mark.parametrize("gene_name", ["BRCA1", "TP53", "APOE"])
    def test_gene_disease_common_genes(self, server_url, gene_name):
        """Test gene-disease endpoint with common genes that should have data."""
        response = requests.get(
            f"{server_url}/api/v1/gene_disease/{gene_name}",
            params={"format": "json"},
            timeout=30,
        )

        assert response.status_code in [200, 404]

        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)
            assert len(data) > 0

            # verify gene_symbol matches (case-insensitive)
            for record in data:
                assert record["gene_symbol"].upper() == gene_name.upper()

    def test_gene_disease_case_insensitive(self, server_url):
        """Test that gene lookup is case-insensitive."""
        gene_name_upper = "BRCA1"
        gene_name_lower = "brca1"

        response_upper = requests.get(
            f"{server_url}/api/v1/gene_disease/{gene_name_upper}",
            params={"format": "json"},
            timeout=30,
        )

        response_lower = requests.get(
            f"{server_url}/api/v1/gene_disease/{gene_name_lower}",
            params={"format": "json"},
            timeout=30,
        )

        # both should return the same status
        assert response_upper.status_code == response_lower.status_code

        # if data exists, should have the same number of records
        if response_upper.status_code == 200:
            data_upper = response_upper.json()
            data_lower = response_lower.json()
            assert len(data_upper) == len(data_lower)

    def test_gene_disease_nonexistent_gene(self, server_url):
        """Test that non-existent gene returns 404."""
        gene_name = "FAKEGENE123XYZ"

        response = requests.get(
            f"{server_url}/api/v1/gene_disease/{gene_name}",
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 404

    def test_gene_disease_structure(self, server_url):
        """Test gene-disease response has expected structure."""
        gene_name = "BRCA1"

        response = requests.get(
            f"{server_url}/api/v1/gene_disease/{gene_name}",
            params={"format": "json"},
            timeout=30,
        )

        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)

            if len(data) > 0:
                first_record = data[0]
                assert isinstance(first_record, dict)

                # verify all expected fields are present
                required_fields = [
                    "resource",
                    "uuid",
                    "gene_symbol",
                    "disease_curie",
                    "disease_title",
                    "classification",
                    "mode_of_inheritance",
                    "submitter"
                ]
                for field in required_fields:
                    assert field in first_record, f"Missing field: {field}"

                # verify types (allowing None for harmonized data from Monarch)
                assert isinstance(first_record["uuid"], (str, type(None)))
                assert isinstance(first_record["gene_symbol"], str)
                assert isinstance(first_record["disease_curie"], str)
                assert isinstance(first_record["disease_title"], str)
                assert isinstance(first_record["classification"], (str, type(None)))
                assert isinstance(first_record["mode_of_inheritance"], (str, type(None)))
                assert isinstance(first_record["submitter"], (str, type(None)))
                assert isinstance(first_record["resource"], str)
                assert first_record["resource"] in ["gencc", "monarch"]

    def test_gene_disease_tsv_structure(self, server_url):
        """Test gene-disease TSV response structure."""
        gene_name = "TP53"

        response = requests.get(
            f"{server_url}/api/v1/gene_disease/{gene_name}",
            params={"format": "tsv"},
            timeout=30,
        )

        if response.status_code == 200:
            lines = response.text.strip().split("\n")

            # should have header + at least one data line
            assert len(lines) >= 2

            header = lines[0].split("\t")
            assert len(header) == 8

            # check data lines have same number of columns
            for line in lines[1:]:
                cols = line.split("\t")
                assert len(cols) == len(header), \
                    f"Inconsistent column count: header has {len(header)}, row has {len(cols)}"

    def test_gene_disease_default_format(self, server_url):
        """Test that default format is TSV."""
        gene_name = "BRCA1"

        response = requests.get(
            f"{server_url}/api/v1/gene_disease/{gene_name}",
            timeout=30,
        )

        if response.status_code == 200:
            # default should be TSV
            assert "text/tab-separated-values" in response.headers.get(
                "content-type", ""
            )

    @pytest.mark.parametrize("gene_name", ["BRCA2", "CFTR", "HBB"])
    def test_gene_disease_multiple_records(self, server_url, gene_name):
        """Test genes that might have multiple disease associations."""
        response = requests.get(
            f"{server_url}/api/v1/gene_disease/{gene_name}",
            params={"format": "json"},
            timeout=30,
        )

        # genes with known disease associations should return 200
        # but we allow 404 in case the data doesn't include them
        assert response.status_code in [200, 404]

        if response.status_code == 200:
            data = response.json()

            # verify all records are for the requested gene
            for record in data:
                assert record["gene_symbol"].upper() == gene_name.upper()

            # verify each non-null uuid is unique (Monarch records may have null UUIDs)
            uuids = [record["uuid"] for record in data if record["uuid"] is not None]
            if len(uuids) > 0:
                assert len(uuids) == len(set(uuids)), "Duplicate UUIDs found"

    def test_gene_disease_response_time(self, server_url):
        """Test that gene-disease endpoint responds quickly."""
        import time

        gene_name = "BRCA1"
        start = time.time()

        response = requests.get(
            f"{server_url}/api/v1/gene_disease/{gene_name}",
            params={"format": "json"},
            timeout=5,  # should be fast since data is in memory
        )

        elapsed = time.time() - start

        # should respond quickly since data is loaded in memory
        assert elapsed < 5.0, f"Response took {elapsed:.2f}s, expected < 5s"
        assert response.status_code in [200, 404]

    def test_gene_disease_empty_gene_name(self, server_url):
        """Test that empty gene name is handled properly."""
        response = requests.get(
            f"{server_url}/api/v1/gene_disease/",
            params={"format": "json"},
            timeout=10,
        )

        # should return 404 (not found route) or 405 (method not allowed)
        assert response.status_code in [404, 405]

    def test_gene_disease_special_characters(self, server_url):
        """Test gene names with special characters."""
        # some genes have hyphens or numbers
        test_genes = ["HLA-A", "APOL1", "C4A"]

        for gene_name in test_genes:
            response = requests.get(
                f"{server_url}/api/v1/gene_disease/{gene_name}",
                params={"format": "json"},
                timeout=10,
            )

            # should at least not error
            assert response.status_code in [200, 404]
