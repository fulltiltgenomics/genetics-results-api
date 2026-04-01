"""
Tests for summary statistics endpoints.
"""

import pytest
import requests
from helpers.validators import (
    validate_tsv_response,
    validate_json_response,
    validate_variant_fields,
)


@pytest.fixture(scope="session")
def sumstats_resources_and_types():
    """Get available summary stats resource/data_type pairs from config."""
    from app.config.summary_stats import get_available_resources_and_types

    return get_available_resources_and_types()


@pytest.fixture(scope="session")
def sumstats_example_phenotypes():
    """Get example phenotypes for each summary stats data file."""
    from app.config.summary_stats import data_files

    # return list of (resource, data_type, phenotype) tuples
    # derived from the GCS paths: e.g. finngen GWAS has T2D
    examples = []
    for df in data_files:
        resource = df["resource"]
        data_type = df["data_type"]
        # use a known phenotype per resource/data_type
        if resource == "finngen" and data_type == "gwas":
            examples.append((resource, data_type, "AUTOIMMUNE"))
        elif resource == "finngen_mvp_ukbb" and data_type == "gwas":
            examples.append((resource, data_type, "T2D"))
    return examples


class TestSummaryStatsGet:
    """Test GET /api/v1/summary_stats/{resource}/{data_type} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_summary_stats_single_variant_single_phenotype(
        self, server_url, test_variant, sumstats_example_phenotypes, format
    ):
        """Test basic query with one variant and one phenotype."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]

        response = requests.get(
            f"{server_url}/api/v1/summary_stats/{resource}/{data_type}",
            params={
                "variants": test_variant,
                "phenotypes": phenotype,
                "format": format,
            },
            timeout=30,
        )

        assert response.status_code == 200

        if format == "tsv":
            assert "text/tab-separated-values" in response.headers.get(
                "content-type", ""
            )
            validation = validate_tsv_response(response.text)
            assert validation["valid"], f"TSV validation failed: {validation['errors']}"
            assert validation["has_header"]
            # verify expected columns are present in header
            header = validation["header"]
            assert "resource" in header
            assert "version" in header
            assert "phenotype" in header
            assert "chr" in header
            assert "pos" in header
        else:
            assert "application/json" in response.headers.get("content-type", "")
            data = response.json()
            validation = validate_json_response(data)
            assert validation["valid"], f"JSON validation failed: {validation['errors']}"

    def test_summary_stats_multiple_phenotypes(
        self, server_url, test_variant, sumstats_example_phenotypes
    ):
        """Test query with multiple phenotypes."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]
        # query same phenotype twice to test multi-phenotype path
        phenotypes = f"{phenotype},{phenotype}"

        response = requests.get(
            f"{server_url}/api/v1/summary_stats/{resource}/{data_type}",
            params={
                "variants": test_variant,
                "phenotypes": phenotypes,
                "format": "tsv",
            },
            timeout=30,
        )

        assert response.status_code == 200
        validation = validate_tsv_response(response.text)
        assert validation["valid"], f"TSV validation failed: {validation['errors']}"

    def test_summary_stats_multiple_variants(
        self, server_url, test_variant, sumstats_example_phenotypes
    ):
        """Test query with multiple variants (triggers tabix -R path)."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]
        # use same variant twice to test multi-variant path
        variants = f"{test_variant},{test_variant}"

        response = requests.get(
            f"{server_url}/api/v1/summary_stats/{resource}/{data_type}",
            params={
                "variants": variants,
                "phenotypes": phenotype,
                "format": "tsv",
            },
            timeout=30,
        )

        assert response.status_code == 200
        validation = validate_tsv_response(response.text)
        assert validation["valid"], f"TSV validation failed: {validation['errors']}"

    def test_summary_stats_multiple_variants_and_phenotypes(
        self, server_url, test_variant, sumstats_example_phenotypes
    ):
        """Test query with multiple variants and multiple phenotypes."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]

        response = requests.get(
            f"{server_url}/api/v1/summary_stats/{resource}/{data_type}",
            params={
                "variants": f"{test_variant},{test_variant}",
                "phenotypes": f"{phenotype},{phenotype}",
                "format": "tsv",
            },
            timeout=30,
        )

        assert response.status_code == 200
        validation = validate_tsv_response(response.text)
        assert validation["valid"], f"TSV validation failed: {validation['errors']}"

    def test_summary_stats_json_has_correct_types(
        self, server_url, test_variant, sumstats_example_phenotypes
    ):
        """Test that JSON response has correctly typed fields."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]

        response = requests.get(
            f"{server_url}/api/v1/summary_stats/{resource}/{data_type}",
            params={
                "variants": test_variant,
                "phenotypes": phenotype,
                "format": "json",
            },
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        if len(data) > 0:
            item = data[0]
            assert isinstance(item["resource"], str)
            assert isinstance(item["version"], str)
            assert isinstance(item["phenotype"], str)
            assert isinstance(item["chr"], int)
            assert isinstance(item["pos"], int)

    def test_summary_stats_tsv_column_consistency(
        self, server_url, test_variant, sumstats_example_phenotypes
    ):
        """Test that all TSV rows have the same number of columns as the header."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]

        response = requests.get(
            f"{server_url}/api/v1/summary_stats/{resource}/{data_type}",
            params={
                "variants": test_variant,
                "phenotypes": phenotype,
                "format": "tsv",
            },
            timeout=30,
        )

        assert response.status_code == 200
        validation = validate_tsv_response(response.text)
        assert validation["consistent_columns"], (
            f"Column counts are inconsistent: {validation['errors']}"
        )

    @pytest.mark.parametrize(
        "resource,data_type",
        [
            ("finngen", "gwas"),
            ("finngen_mvp_ukbb", "gwas"),
        ],
    )
    def test_summary_stats_per_resource(
        self, server_url, test_variant, resource, data_type
    ):
        """Test each configured resource/data_type returns data."""
        # use known phenotype per resource
        phenotype_map = {
            "finngen": "AUTOIMMUNE",
            "finngen_mvp_ukbb": "T2D",
        }
        phenotype = phenotype_map.get(resource)
        if not phenotype:
            pytest.skip(f"No known phenotype for {resource}")

        response = requests.get(
            f"{server_url}/api/v1/summary_stats/{resource}/{data_type}",
            params={
                "variants": test_variant,
                "phenotypes": phenotype,
                "format": "tsv",
            },
            timeout=30,
        )

        assert response.status_code == 200
        validation = validate_tsv_response(response.text)
        assert validation["valid"], f"TSV validation failed: {validation['errors']}"

    def test_summary_stats_phenotype_column_value(
        self, server_url, test_variant, sumstats_example_phenotypes
    ):
        """Test that the phenotype column contains the queried phenotype."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]

        response = requests.get(
            f"{server_url}/api/v1/summary_stats/{resource}/{data_type}",
            params={
                "variants": test_variant,
                "phenotypes": phenotype,
                "format": "json",
            },
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        for item in data:
            assert item["phenotype"] == phenotype

    def test_summary_stats_resource_column_value(
        self, server_url, test_variant, sumstats_example_phenotypes
    ):
        """Test that the resource column matches the queried resource."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]

        response = requests.get(
            f"{server_url}/api/v1/summary_stats/{resource}/{data_type}",
            params={
                "variants": test_variant,
                "phenotypes": phenotype,
                "format": "json",
            },
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        for item in data:
            assert item["resource"] == resource


class TestSummaryStatsPost:
    """Test POST /api/v1/summary_stats/{resource}/{data_type} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_post_summary_stats_formats(
        self, server_url, test_variant, sumstats_example_phenotypes, format
    ):
        """Test POST with both TSV and JSON response formats."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]

        response = requests.post(
            f"{server_url}/api/v1/summary_stats/{resource}/{data_type}",
            json={
                "variants": [test_variant],
                "phenotypes": [phenotype],
            },
            params={"format": format},
            timeout=30,
        )

        assert response.status_code == 200

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
            assert validation["valid"], f"JSON validation failed: {validation['errors']}"

    def test_post_summary_stats_multiple_variants(
        self, server_url, test_variant, sumstats_example_phenotypes
    ):
        """Test POST with multiple variants."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]

        response = requests.post(
            f"{server_url}/api/v1/summary_stats/{resource}/{data_type}",
            json={
                "variants": [test_variant, test_variant],
                "phenotypes": [phenotype],
            },
            params={"format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_post_summary_stats_multiple_phenotypes(
        self, server_url, test_variant, sumstats_example_phenotypes
    ):
        """Test POST with multiple phenotypes."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]

        response = requests.post(
            f"{server_url}/api/v1/summary_stats/{resource}/{data_type}",
            json={
                "variants": [test_variant],
                "phenotypes": [phenotype, phenotype],
            },
            params={"format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_post_summary_stats_multiple_variants_and_phenotypes(
        self, server_url, test_variant, sumstats_example_phenotypes
    ):
        """Test POST with both multiple variants and phenotypes."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]

        response = requests.post(
            f"{server_url}/api/v1/summary_stats/{resource}/{data_type}",
            json={
                "variants": [test_variant, test_variant],
                "phenotypes": [phenotype, phenotype],
            },
            params={"format": "tsv"},
            timeout=30,
        )

        assert response.status_code == 200
        validation = validate_tsv_response(response.text)
        assert validation["valid"], f"TSV validation failed: {validation['errors']}"


class TestSummaryStatsGetPostConsistency:
    """Test that GET and POST return consistent results."""

    def test_get_post_same_results(
        self, server_url, test_variant, sumstats_example_phenotypes
    ):
        """Test that GET and POST return the same data for the same query."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]

        get_response = requests.get(
            f"{server_url}/api/v1/summary_stats/{resource}/{data_type}",
            params={
                "variants": test_variant,
                "phenotypes": phenotype,
                "format": "json",
            },
            timeout=30,
        )

        post_response = requests.post(
            f"{server_url}/api/v1/summary_stats/{resource}/{data_type}",
            json={
                "variants": [test_variant],
                "phenotypes": [phenotype],
            },
            params={"format": "json"},
            timeout=30,
        )

        assert get_response.status_code == 200
        assert post_response.status_code == 200

        get_data = get_response.json()
        post_data = post_response.json()
        assert len(get_data) == len(post_data)


class TestSummaryStatsErrorHandling:
    """Test error handling for summary stats endpoints."""

    def test_invalid_resource(self, server_url, test_variant):
        """Test that invalid resource returns 404."""
        response = requests.get(
            f"{server_url}/api/v1/summary_stats/nonexistent_resource/gwas",
            params={
                "variants": test_variant,
                "phenotypes": "T2D",
                "format": "json",
            },
            timeout=10,
        )

        assert response.status_code == 404

    def test_invalid_data_type(self, server_url, test_variant):
        """Test that invalid data_type returns 404."""
        response = requests.get(
            f"{server_url}/api/v1/summary_stats/finngen/nonexistent_type",
            params={
                "variants": test_variant,
                "phenotypes": "T2D",
                "format": "json",
            },
            timeout=10,
        )

        assert response.status_code == 404

    def test_invalid_variant(self, server_url, invalid_variant):
        """Test that invalid variant format returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/summary_stats/finngen/gwas",
            params={
                "variants": invalid_variant,
                "phenotypes": "T2D",
                "format": "json",
            },
            timeout=10,
        )

        assert response.status_code == 422

    def test_missing_variants_param(self, server_url):
        """Test that missing variants parameter returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/summary_stats/finngen/gwas",
            params={
                "phenotypes": "T2D",
                "format": "json",
            },
            timeout=10,
        )

        assert response.status_code == 422

    def test_missing_phenotypes_param(self, server_url, test_variant):
        """Test that missing phenotypes parameter returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/summary_stats/finngen/gwas",
            params={
                "variants": test_variant,
                "format": "json",
            },
            timeout=10,
        )

        assert response.status_code == 422

    def test_nonexistent_phenotype(self, server_url, test_variant):
        """Test that a non-existent phenotype returns 404."""
        response = requests.get(
            f"{server_url}/api/v1/summary_stats/finngen/gwas",
            params={
                "variants": test_variant,
                "phenotypes": "NONEXISTENT_PHENOTYPE_12345",
                "format": "json",
            },
            timeout=10,
        )

        assert response.status_code == 404

    def test_post_empty_variants(self, server_url):
        """Test POST with empty variants list returns 422."""
        response = requests.post(
            f"{server_url}/api/v1/summary_stats/finngen/gwas",
            json={
                "variants": [],
                "phenotypes": ["T2D"],
            },
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 422

    def test_post_empty_phenotypes(self, server_url, test_variant):
        """Test POST with empty phenotypes list returns 422."""
        response = requests.post(
            f"{server_url}/api/v1/summary_stats/finngen/gwas",
            json={
                "variants": [test_variant],
                "phenotypes": [],
            },
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 422

    def test_post_invalid_variant(self, server_url, invalid_variant):
        """Test POST with invalid variant returns 422."""
        response = requests.post(
            f"{server_url}/api/v1/summary_stats/finngen/gwas",
            json={
                "variants": [invalid_variant],
                "phenotypes": ["T2D"],
            },
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 422

    def test_post_invalid_body(self, server_url):
        """Test POST with invalid JSON body returns 422."""
        response = requests.post(
            f"{server_url}/api/v1/summary_stats/finngen/gwas",
            json={"invalid": "body"},
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 422
