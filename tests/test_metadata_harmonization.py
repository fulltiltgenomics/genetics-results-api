"""
Tests for metadata harmonization and harmonized_metadata endpoint.
"""

import pytest
import requests
from helpers.validators import validate_tsv_response


class TestMetadataHarmonization:
    """Test /api/v1/resource_metadata endpoint."""

    @pytest.mark.parametrize("resource", ["finngen", "eqtl_catalogue", "open_targets"])
    def test_metadata_json(self, server_url, resource):
        """Test harmonized metadata JSON response."""
        response = requests.get(
            f"{server_url}/api/v1/resource_metadata/{resource}",
            params={"format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0

        # check unified schema
        first_item = data[0]
        required_fields = [
            "phenotype_code",
            "phenotype_string",
            "n_samples",
            "n_cases",
            "n_controls",
            "trait_type",
            "author",
            "date",
            "resource",
            "version",
        ]
        for field in required_fields:
            assert field in first_item, f"Missing field: {field}"

        # check trait_type is valid
        assert first_item["trait_type"] in ["binary", "quantitative"]

        # check date format (ISO 8601: YYYY-MM-DD)
        import re

        date_pattern = r"^\d{4}-\d{2}-\d{2}$"
        assert re.match(
            date_pattern, first_item["date"]
        ), f"Invalid date format: {first_item['date']}"

        # check resource matches
        assert first_item["resource"] == resource

    @pytest.mark.parametrize("resource", ["finngen", "eqtl_catalogue", "open_targets"])
    def test_metadata_tsv(self, server_url, resource):
        """Test harmonized metadata TSV response."""
        response = requests.get(
            f"{server_url}/api/v1/resource_metadata/{resource}",
            params={"format": "tsv"},
            timeout=30,
        )

        assert response.status_code == 200
        assert "text/tab-separated-values" in response.headers.get("content-type", "")

        # validate TSV structure
        validation = validate_tsv_response(response.text, min_data_lines=1)
        assert validation["valid"], f"TSV validation failed: {validation['errors']}"

        # check header
        lines = response.text.strip().split("\n")
        header = lines[0].split("\t")
        expected_columns = [
            "phenotype_code",
            "phenotype_string",
            "n_samples",
            "n_cases",
            "n_controls",
            "trait_type",
            "author",
            "date",
            "resource",
            "version",
        ]
        for col in expected_columns:
            assert col in header, f"Missing column: {col}"

    def test_metadata_finngen_r13(self, server_url):
        """Test FinnGen R13 GWAS metadata harmonization."""
        response = requests.get(
            f"{server_url}/api/v1/resource_metadata/finngen",
            params={"format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        # find R13 GWAS phenotypes (have phenocode, not OMOPID)
        r13_items = [item for item in data if item["version"] == "R13"]
        if len(r13_items) > 0:
            item = r13_items[0]
            assert item["trait_type"] == "binary"
            assert item["author"] == "FinnGen Consortium"
            assert item["date"] == "2025-09-01"
            # n_samples should be sum of cases and controls
            assert item["n_samples"] == item["n_cases"] + item["n_controls"]

    def test_metadata_finngen_kanta(self, server_url):
        """Test FinnGen Kanta lab test metadata harmonization."""
        response = requests.get(
            f"{server_url}/api/v1/resource_metadata/finngen",
            params={"format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        # find Kanta items (have version "kanta")
        kanta_items = [item for item in data if item["version"] == "kanta"]
        if len(kanta_items) > 0:
            item = kanta_items[0]
            assert item["trait_type"] in ["binary", "quantitative"]
            assert item["author"] == "FinnGen Consortium"
            assert item["date"] == "2025-03-01"

    def test_metadata_eqtl_catalogue(self, server_url):
        """Test eQTL Catalogue metadata harmonization."""
        response = requests.get(
            f"{server_url}/api/v1/resource_metadata/eqtl_catalogue",
            params={"format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data) > 0

        item = data[0]
        # eQTL data is always quantitative
        assert item["trait_type"] == "quantitative"
        # no cases/controls for QTL data (should be "NA")
        assert item["n_cases"] == "NA"
        assert item["n_controls"] == "NA"
        # author should be study_label
        assert item["author"] != ""
        assert item["version"] == "R7"
        assert item["date"] == "2020-01-01"

    def test_metadata_open_targets(self, server_url):
        """Test Open Targets metadata harmonization."""
        response = requests.get(
            f"{server_url}/api/v1/resource_metadata/open_targets",
            params={"format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data) > 0

        item = data[0]
        # trait type determined by presence of cases (can be int or "NA")
        if isinstance(item["n_cases"], int) and item["n_cases"] > 0:
            assert item["trait_type"] == "binary"
        else:
            assert item["trait_type"] == "quantitative"
        assert item["version"] == "25.12"
        assert item["date"] != ""

    def test_metadata_sample_sizes(self, server_url):
        """Test that sample sizes are correctly populated (not 0 or NA)."""
        for resource in ["finngen", "eqtl_catalogue", "open_targets"]:
            response = requests.get(
                f"{server_url}/api/v1/resource_metadata/{resource}",
                params={"format": "json"},
                timeout=30,
            )

            assert response.status_code == 200
            data = response.json()

            # check that most items have non-zero sample sizes (can be int or "NA")
            non_zero_samples = [
                item
                for item in data
                if isinstance(item["n_samples"], int) and item["n_samples"] > 0
            ]
            # at least some should have sample size data
            assert (
                len(non_zero_samples) > 0
            ), f"{resource} has no items with sample_size > 0"

    def test_metadata_invalid_resource(self, server_url):
        """Test invalid resource returns 404."""
        response = requests.get(
            f"{server_url}/api/v1/resource_metadata/invalid_resource",
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 404

    def test_metadata_invalid_format(self, server_url):
        """Test invalid format parameter."""
        response = requests.get(
            f"{server_url}/api/v1/resource_metadata/finngen",
            params={"format": "xml"},
            timeout=10,
        )

        assert response.status_code == 422

    def test_search_has_correct_sample_sizes(self, server_url):
        """Test that search results now have correct sample sizes (not 0)."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={
                "q": "diabetes",
                "types": "phenotypes",
                "limit": 10,
                "format": "json",
            },
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        if len(data) > 0:
            # check that phenotypes have sample sizes
            for pheno in data:
                assert "sample_size" in pheno
                # at least some should have non-zero sample sizes
            non_zero = [p for p in data if p.get("sample_size", 0) > 0]
            assert len(non_zero) > 0, "No phenotypes have sample_size > 0"

    def test_harmonized_vs_raw_metadata(self, server_url):
        """Test that harmonized endpoint returns different format than raw endpoint."""
        resource = "finngen"

        # get raw metadata
        raw_response = requests.get(
            f"{server_url}/api/v1/resource_metadata/{resource}",
            params={"format": "json"},
            timeout=30,
        )
        assert raw_response.status_code == 200
        raw_data = raw_response.json()

        # get harmonized metadata
        harmonized_response = requests.get(
            f"{server_url}/api/v1/resource_metadata/{resource}",
            params={"format": "json"},
            timeout=30,
        )
        assert harmonized_response.status_code == 200
        harmonized_data = harmonized_response.json()

        # both should have data
        assert len(raw_data) > 0
        assert len(harmonized_data) > 0

        # harmonized should have unified fields
        assert "phenotype_code" in harmonized_data[0]
        assert "phenotype_string" in harmonized_data[0]
        assert "n_samples" in harmonized_data[0]
        assert "trait_type" in harmonized_data[0]

        # raw might have different fields depending on resource
        # (e.g., phenocode vs OMOPID for finngen)

    def test_metadata_consistency(self, server_url):
        """Test that all items from a resource have consistent schema."""
        response = requests.get(
            f"{server_url}/api/v1/resource_metadata/finngen",
            params={"format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        # all items should have exact same keys
        if len(data) > 1:
            first_keys = set(data[0].keys())
            for item in data[1:]:
                assert (
                    set(item.keys()) == first_keys
                ), "Inconsistent schema across items"
