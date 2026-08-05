"""
Tests for summary statistics endpoints.
"""

import pytest
import requests
from helpers.validators import (
    validate_tsv_response,
    validate_json_response,
)


@pytest.fixture(scope="session")
def sumstats_resources_and_types():
    """Get available summary stats resource/data_type pairs from config."""
    from app.config.summary_stats import get_available_resources_and_types

    return get_available_resources_and_types()


@pytest.fixture(scope="session")
def sumstats_example_phenotypes():
    """Get example phenotypes for each summary stats data file."""
    from app.config.datasets import datasets
    from app.config.summary_stats import data_files

    # return list of (resource, data_type, phenotype) tuples
    # derived from the GCS paths: e.g. finngen GWAS has T2D
    examples = []
    for df in data_files:
        resource = df["resource"]
        data_type = datasets[df["dataset_id"]]["data_type"]
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


class TestSummaryStatsByRange:
    """Test GET /api/v1/summary_stats_by_range/{resource}/{data_type}/{region} endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_range_formats(
        self, server_url, test_region, sumstats_example_phenotypes, format
    ):
        """Test range query with both TSV and JSON formats."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]

        response = requests.get(
            f"{server_url}/api/v1/summary_stats_by_range/{resource}/{data_type}/{test_region}",
            params={"phenotypes": phenotype, "format": format},
            timeout=30,
        )

        assert response.status_code == 200, response.text[:200]

        if format == "tsv":
            assert "text/tab-separated-values" in response.headers.get("content-type", "")
            validation = validate_tsv_response(response.text, min_data_lines=1)
            assert validation["valid"], f"TSV validation failed: {validation['errors']}"
            header = validation["header"]
            for col in ("resource", "version", "phenotype", "chr", "pos", "ref", "alt"):
                assert col in header, f"missing column {col} in TSV header"
        else:
            assert "application/json" in response.headers.get("content-type", "")
            data = response.json()
            validation = validate_json_response(data, min_items=1)
            assert validation["valid"], f"JSON validation failed: {validation['errors']}"

    def test_range_rows_within_region(
        self, server_url, test_region, sumstats_example_phenotypes
    ):
        """Every returned row must fall inside the queried region."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]
        chrom, start_end = test_region.split(":")
        start, end = (int(x) for x in start_end.split("-"))

        response = requests.get(
            f"{server_url}/api/v1/summary_stats_by_range/{resource}/{data_type}/{test_region}",
            params={"phenotypes": phenotype, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data) > 0
        for item in data:
            assert item["chr"] == int(chrom)
            assert start <= item["pos"] <= end
            assert item["resource"] == resource
            assert item["phenotype"] == phenotype

    def test_range_multiple_phenotypes(
        self, server_url, test_region, sumstats_example_phenotypes
    ):
        """A multi-phenotype range query returns rows for each phenotype."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]
        # a second phenotype known to exist for the same resource/data_type
        if resource != "finngen":
            pytest.skip(f"No known second phenotype for {resource}/{data_type}")
        second = "T2D" if phenotype != "T2D" else "AUTOIMMUNE"

        response = requests.get(
            f"{server_url}/api/v1/summary_stats_by_range/{resource}/{data_type}/{test_region}",
            params={"phenotypes": f"{phenotype},{second}", "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert {item["phenotype"] for item in data} == {phenotype, second}

    def test_range_matches_variant_query(
        self, server_url, test_region, sumstats_example_phenotypes
    ):
        """A variant inside the range returns the same row from both endpoints."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]

        range_response = requests.get(
            f"{server_url}/api/v1/summary_stats_by_range/{resource}/{data_type}/{test_region}",
            params={"phenotypes": phenotype, "format": "json"},
            timeout=30,
        )
        assert range_response.status_code == 200
        rows = range_response.json()
        if not rows:
            pytest.skip("No rows in the test region")

        row = rows[0]
        variant = f"{row['chr']}-{row['pos']}-{row['ref']}-{row['alt']}"

        variant_response = requests.get(
            f"{server_url}/api/v1/summary_stats/{resource}/{data_type}",
            params={"variants": variant, "phenotypes": phenotype, "format": "json"},
            timeout=30,
        )
        assert variant_response.status_code == 200
        assert row in variant_response.json()

    def test_range_single_base_pair(self, server_url, sumstats_example_phenotypes):
        """A single-base-pair range is a valid query."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]

        response = requests.get(
            f"{server_url}/api/v1/summary_stats_by_range/{resource}/{data_type}/1:1000018-1000018",
            params={"phenotypes": phenotype, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        for item in response.json():
            assert item["pos"] == 1000018

    def test_range_x_chromosome(self, server_url, sumstats_example_phenotypes):
        """X is accepted as a chromosome (mapped to 23 like the other range endpoints)."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]

        response = requests.get(
            f"{server_url}/api/v1/summary_stats_by_range/{resource}/{data_type}/X:5000000-5010000",
            params={"phenotypes": phenotype, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200

    def test_range_tsv_column_consistency(
        self, server_url, test_region, sumstats_example_phenotypes
    ):
        """All TSV rows have the same number of columns as the header."""
        if not sumstats_example_phenotypes:
            pytest.skip("No summary stats example phenotypes available")

        resource, data_type, phenotype = sumstats_example_phenotypes[0]

        response = requests.get(
            f"{server_url}/api/v1/summary_stats_by_range/{resource}/{data_type}/{test_region}",
            params={"phenotypes": phenotype, "format": "tsv"},
            timeout=30,
        )

        assert response.status_code == 200
        validation = validate_tsv_response(response.text, min_data_lines=1)
        assert validation["consistent_columns"], (
            f"Column counts are inconsistent: {validation['errors']}"
        )


class TestSummaryStatsByRangeErrorHandling:
    """Test error handling for the summary stats range endpoint."""

    def test_range_invalid_region(self, server_url, invalid_region):
        """Test that an invalid region format returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/summary_stats_by_range/finngen/gwas/{invalid_region}",
            params={"phenotypes": "AUTOIMMUNE", "format": "json"},
            timeout=10,
        )

        assert response.status_code == 422

    def test_range_start_after_end(self, server_url):
        """Test that a reversed range returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/summary_stats_by_range/finngen/gwas/1:2000000-1000000",
            params={"phenotypes": "AUTOIMMUNE", "format": "json"},
            timeout=10,
        )

        assert response.status_code == 422

    def test_range_too_large(self, server_url):
        """Test that a range exceeding the JSON size limit returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/summary_stats_by_range/finngen/gwas/1:1000000-9000000",
            params={"phenotypes": "AUTOIMMUNE", "format": "json"},
            timeout=10,
        )

        assert response.status_code == 422

    def test_range_invalid_resource(self, server_url, test_region):
        """Test that an invalid resource returns 404."""
        response = requests.get(
            f"{server_url}/api/v1/summary_stats_by_range/nonexistent_resource/gwas/{test_region}",
            params={"phenotypes": "T2D", "format": "json"},
            timeout=10,
        )

        assert response.status_code == 404

    def test_range_invalid_data_type(self, server_url, test_region):
        """Test that an invalid data type returns 404."""
        response = requests.get(
            f"{server_url}/api/v1/summary_stats_by_range/finngen/nonexistent_type/{test_region}",
            params={"phenotypes": "T2D", "format": "json"},
            timeout=10,
        )

        assert response.status_code == 404

    def test_range_missing_phenotypes_param(self, server_url, test_region):
        """Test that a missing phenotypes parameter returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/summary_stats_by_range/finngen/gwas/{test_region}",
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 422

    def test_range_nonexistent_phenotype(self, server_url, test_region):
        """Test that a non-existent phenotype returns 404."""
        response = requests.get(
            f"{server_url}/api/v1/summary_stats_by_range/finngen/gwas/{test_region}",
            params={"phenotypes": "NONEXISTENT_PHENOTYPE_12345", "format": "json"},
            timeout=10,
        )

        assert response.status_code == 404

    def test_range_phenotype_path_traversal(self, server_url, test_region):
        """Test that a phenotype with path separators is rejected with 422."""
        response = requests.get(
            f"{server_url}/api/v1/summary_stats_by_range/finngen/gwas/{test_region}",
            params={"phenotypes": "../../etc/passwd", "format": "json"},
            timeout=10,
        )

        assert response.status_code == 422


class TestSummaryStatsPqtlMeta:
    """Test the finngen_ukbb pQTL 3-way meta dataset (finngen_ukbb_pqtl)."""

    # PCSK9 cis variant present in the unfiltered pan-asset meta sumstats
    _PROTEIN = "PCSK9"
    _VARIANT = "1-55039774-C-T"

    def test_pqtl_meta_returns_mapped_columns(self, server_url):
        """Query a known protein/variant and assert meta + per-study columns map through."""
        response = requests.get(
            f"{server_url}/api/v1/summary_stats/finngen_ukbb/pqtl",
            params={
                "variants": self._VARIANT,
                "phenotypes": self._PROTEIN,
                "format": "json",
            },
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        validation = validate_json_response(data, min_items=1)
        assert validation["valid"], f"JSON validation failed: {validation['errors']}"

        item = data[0]
        assert item["resource"] == "finngen_ukbb"
        assert item["phenotype"] == self._PROTEIN
        assert item["chr"] == 1
        assert item["pos"] == 55039774

        # mapped meta columns
        for col in ("beta", "se", "pval", "mlog10p", "het_p", "meta_n"):
            assert col in item, f"missing meta column {col}"
        # mapped per-study columns for all three studies
        for col in (
            "fg2_3k_beta", "ukbb_ppp_beta", "fg3_cb_beta",
            "leave_fg2_3k_beta", "leave_ukbb_ppp_beta", "leave_fg3_cb_beta",
        ):
            assert col in item, f"missing per-study column {col}"

    def test_pqtl_meta_tsv(self, server_url):
        """Same query in TSV format returns a consistent table with the mapped header."""
        response = requests.get(
            f"{server_url}/api/v1/summary_stats/finngen_ukbb/pqtl",
            params={
                "variants": self._VARIANT,
                "phenotypes": self._PROTEIN,
                "format": "tsv",
            },
            timeout=30,
        )

        assert response.status_code == 200
        validation = validate_tsv_response(response.text, min_data_lines=1)
        assert validation["valid"], f"TSV validation failed: {validation['errors']}"
        header = validation["header"]
        for col in ("phenotype", "beta", "mlog10p", "fg2_3k_beta", "ukbb_ppp_beta", "fg3_cb_beta"):
            assert col in header, f"missing column {col} in TSV header"


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
