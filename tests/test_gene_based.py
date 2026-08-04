"""
Tests for gene-based burden results endpoints.
"""

import pytest
import requests

from helpers.validators import (
    validate_tsv_response,
    validate_json_response,
)


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
        # SETD1A is present across all gene-based resources (genebass, SCHEMA2, BipEx2,
        # IBD); GRIN2A is absent from genebass so it can't verify multi-resource merging
        response = requests.get(
            f"{server_url}/api/v1/gene_based/SETD1A",
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
        assert "SCHEMA2" in datasets, f"Expected SCHEMA2 in datasets, got: {datasets}"

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


class TestGeneBasedResultsByPhenotype:
    """Test /api/v1/gene_based_results_by_phenotype/{resource}/{phenotype} endpoint."""

    @pytest.mark.parametrize(
        "resource,phenotype",
        [
            ("genebass", "categorical_41210_both_sexes_S068_"),
            ("schema", "schizophrenia"),
            ("bipex", "bipolar_disorder"),
            # the burden files spell the IBD traits out; only the exome variant
            # files use the IBD/UC/CD short codes
            ("ibd", "inflammatory_bowel_disease"),
            ("ibd", "ulcerative_colitis"),
            ("ibd", "crohns_disease"),
        ],
    )
    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_gene_based_by_phenotype_formats(self, server_url, resource, phenotype, format):
        """Every gene-based resource serves its traits in both TSV and JSON."""
        response = requests.get(
            f"{server_url}/api/v1/gene_based_results_by_phenotype/{resource}/{phenotype}",
            params={"format": format},
            timeout=60,
        )

        assert response.status_code == 200, f"Unexpected status: {response.status_code}"

        if format == "tsv":
            assert "text/tab-separated-values" in response.headers.get("content-type", "")
            validation = validate_tsv_response(response.text)
            assert validation["valid"], f"TSV validation failed: {validation['errors']}"
            assert validation["has_header"], "TSV should have header"
        else:
            assert "application/json" in response.headers.get("content-type", "")
            data = response.json()
            validation = validate_json_response(data)
            assert validation["valid"], f"JSON validation failed: {validation['errors']}"

    def test_gene_based_by_phenotype_is_unfiltered(self, server_url):
        """The per-trait files carry every gene, not just the mlog10p_burden > 4 hits."""
        response = requests.get(
            f"{server_url}/api/v1/gene_based_results_by_phenotype/genebass/categorical_41210_both_sexes_S068_",
            params={"format": "json"},
            timeout=60,
        )

        assert response.status_code == 200
        rows = response.json()
        # ~19k genes x 4 annotation classes; /gene_based/{gene} would return only the hits
        assert len(rows) > 10000, f"Expected the full gene set, got {len(rows)} rows"
        assert any(
            r["mlog10p_burden"] is not None and r["mlog10p_burden"] < 4 for r in rows
        ), "Expected sub-threshold results in the unfiltered per-trait file"

    def test_gene_based_by_phenotype_not_found(self, server_url):
        """Test that non-existent phenotype returns 404."""
        response = requests.get(
            f"{server_url}/api/v1/gene_based_results_by_phenotype/genebass/NONEXISTENT_PHENOTYPE_12345",
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 404

    def test_gene_based_by_phenotype_unknown_resource(self, server_url):
        """Test that an unknown resource returns 404."""
        response = requests.get(
            f"{server_url}/api/v1/gene_based_results_by_phenotype/not_a_resource/inflammatory_bowel_disease",
            timeout=10,
        )

        assert response.status_code == 404


class TestGeneBasedTraitFilter:
    """Test the traits= filter on /api/v1/gene_based/{gene}."""

    def test_traits_filter_returns_only_requested_traits(self, server_url):
        """NOD2 in Crohn's and UC, from the unfiltered per-trait files."""
        response = requests.get(
            f"{server_url}/api/v1/gene_based/NOD2",
            params={"traits": "crohns_disease,ulcerative_colitis"},
            timeout=60,
        )

        assert response.status_code == 200
        lines = [l for l in response.text.strip().split("\n") if l]
        assert len(lines) > 1, "Expected header + data rows"
        assert sum(1 for l in lines if l.startswith("dataset\t")) == 1

        rows = [l.split("\t") for l in lines[1:]]
        assert {r[1] for r in rows} == {"crohns_disease", "ulcerative_colitis"}
        # every row must be NOD2 exactly once per (trait, annotation): the three IBD
        # data files share a per-trait directory, so an undeduped query would triple
        keys = [(r[1], r[7]) for r in rows]
        assert len(keys) == len(set(keys)), f"Duplicated rows: {keys}"

    def test_traits_filter_returns_sub_threshold_results(self, server_url):
        """The point of the filter: a non-significant result is still a result."""
        response = requests.get(
            f"{server_url}/api/v1/gene_based/NOD2",
            params={"traits": "ulcerative_colitis"},
            timeout=60,
        )

        assert response.status_code == 200
        rows = [l.split("\t") for l in response.text.strip().split("\n")[1:] if l]
        assert rows, "Expected NOD2 rows for ulcerative_colitis"
        assert any(float(r[8]) < 4 for r in rows), "Expected a sub-threshold burden result"

    def test_traits_filter_unknown_trait(self, server_url):
        """A trait no gene-based dataset has is a 404."""
        response = requests.get(
            f"{server_url}/api/v1/gene_based/NOD2",
            params={"traits": "NONEXISTENT_TRAIT_12345"},
            timeout=30,
        )

        assert response.status_code == 404

    def test_traits_filter_empty(self, server_url):
        """An empty traits list is a 422 rather than a silent unfiltered query."""
        response = requests.get(
            f"{server_url}/api/v1/gene_based/NOD2",
            params={"traits": ",,"},
            timeout=30,
        )

        assert response.status_code == 422

    def test_traits_filter_too_many(self, server_url):
        """More traits than the per-request cap is a 422."""
        response = requests.get(
            f"{server_url}/api/v1/gene_based/NOD2",
            params={"traits": ",".join(f"trait_{i}" for i in range(60))},
            timeout=30,
        )

        assert response.status_code == 422
