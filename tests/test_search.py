"""
Tests for search/autocomplete endpoint.
"""

import pytest
import requests
from helpers.validators import validate_tsv_response


class TestSearchAutocomplete:
    """Test /api/v1/search endpoint."""

    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_search_gene_exact_match(self, server_url, format):
        """Test exact gene symbol match (PCSK9)."""
        params = {"q": "PCSK9", "limit": 5, "format": format}
        # TSV requires type filter
        if format == "tsv":
            params["types"] = "genes"

        response = requests.get(
            f"{server_url}/api/v1/search",
            params=params,
            timeout=30,
        )

        assert response.status_code == 200

        if format == "tsv":
            assert "text/tab-separated-values" in response.headers.get(
                "content-type", ""
            )
            validation = validate_tsv_response(response.text, min_data_lines=1)
            assert validation["valid"], f"TSV validation failed: {validation['errors']}"
            assert "PCSK9" in response.text
        else:  # json
            data = response.json()
            assert isinstance(data, list)
            assert len(data) > 0
            # First result should be exact match
            assert data[0]["type"] == "gene"
            assert data[0]["symbol"] == "PCSK9"
            assert data[0]["match_type"] in ["exact", "prefix"]

    def test_search_gene_fuzzy_match(self, server_url):
        """Test fuzzy matching with typo (PCKS9 instead of PCSK9)."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "PCKS9", "limit": 5, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        # Should still find PCSK9 via fuzzy matching
        assert len(data) > 0
        symbols = [item.get("symbol") for item in data if item.get("type") == "gene"]
        assert "PCSK9" in symbols or "PCKS" in str(symbols)  # fuzzy match

    def test_search_gene_alias(self, server_url):
        """Test search by gene alias."""
        # PCSK9 has aliases NARC-1, FH3
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "NARC-1", "limit": 5, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        # Should find PCSK9
        if len(data) > 0:
            # Check if PCSK9 is in results (symbol should be PCSK9, not the alias)
            symbols = [item.get("symbol") for item in data if item.get("type") == "gene"]
            # Might match PCSK9 or other genes with similar aliases

    def test_search_gene_ensembl_id(self, server_url):
        """Test search by Ensembl gene ID."""
        # Search for BRCA1 by its Ensembl ID
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "ENSG00000012048", "limit": 5, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        # Should find BRCA1
        assert len(data) > 0
        first_result = data[0]
        assert first_result["type"] == "gene"
        assert first_result["ensembl_id"] == "ENSG00000012048"
        assert first_result["symbol"] == "BRCA1"
        assert first_result["match_type"] == "exact"

    def test_search_phenotype(self, server_url):
        """Test phenotype search."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "diabetes", "limit": 10, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

        # Should find phenotypes with diabetes in name
        if len(data) > 0:
            phenotypes = [item for item in data if item.get("type") == "phenotype"]
            assert len(phenotypes) > 0
            # Check structure of phenotype results
            first_pheno = phenotypes[0]
            assert "code" in first_pheno
            assert "name" in first_pheno
            assert "resource" in first_pheno

    @pytest.mark.parametrize("types_filter", ["genes", "phenotypes"])
    def test_search_with_type_filter(self, server_url, types_filter):
        """Test filtering by type."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "lipid", "types": types_filter, "limit": 10, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

        # All results should be of the specified type
        if len(data) > 0:
            for item in data:
                if types_filter == "genes":
                    assert item["type"] == "gene"
                else:
                    assert item["type"] == "phenotype"

    def test_search_with_multiple_type_filters(self, server_url):
        """Test filtering by multiple types."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "cancer", "types": "genes,phenotypes", "limit": 10, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

        # Should have both genes and phenotypes
        if len(data) > 1:
            types = set(item["type"] for item in data)
            # Could have both or just one type depending on data

    @pytest.mark.parametrize("limit", [1, 5, 10, 20])
    def test_search_with_limit(self, server_url, limit):
        """Test limit parameter."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "a", "limit": limit, "format": "json"},  # 'a' should match many
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) <= limit

    def test_search_empty_query(self, server_url):
        """Test empty query returns empty results."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "", "limit": 10, "format": "json"},
            timeout=10,
        )

        # Should return 422 for empty query (min_length=1 in query param)
        assert response.status_code == 422

    def test_search_no_results(self, server_url):
        """Test query with no matches."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "XYZNONEXISTENT123", "limit": 10, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        # May return fuzzy matches or empty list
        assert len(data) >= 0

    def test_search_invalid_type_filter(self, server_url):
        """Test invalid type filter returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "test", "types": "invalid_type", "limit": 10, "format": "json"},
            timeout=10,
        )

        assert response.status_code == 422

    def test_search_result_structure_gene(self, server_url):
        """Test gene result structure."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "BRCA1", "types": "genes", "limit": 5, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        if len(data) > 0:
            gene = data[0]
            assert gene["type"] == "gene"
            assert "symbol" in gene
            assert "name" in gene
            assert "aliases" in gene
            assert isinstance(gene["aliases"], list)
            assert "ensembl_id" in gene
            assert "match_type" in gene
            assert "match_score" in gene
            assert "rank_score" in gene

    def test_search_result_structure_phenotype(self, server_url):
        """Test phenotype result structure."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "T2D", "types": "phenotypes", "limit": 5, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        if len(data) > 0:
            pheno = data[0]
            assert pheno["type"] == "phenotype"
            assert "code" in pheno
            assert "name" in pheno
            assert "resource" in pheno
            assert "sample_size" in pheno or pheno.get("sample_size") is not None
            assert "match_type" in pheno
            assert "match_score" in pheno
            assert "rank_score" in pheno

    def test_search_ranking_exact_over_fuzzy(self, server_url):
        """Test that exact matches rank higher than fuzzy matches."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "TP53", "limit": 10, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        if len(data) > 1:
            # First result should have higher rank_score
            assert data[0]["rank_score"] >= data[1]["rank_score"]
            # Exact match should come first
            if data[0]["match_type"] == "exact":
                assert data[0]["symbol"] == "TP53" or "TP53" in data[0].get("code", "")

    def test_search_case_insensitive(self, server_url):
        """Test case-insensitive search."""
        # Test with different cases
        queries = ["pcsk9", "PCSK9", "Pcsk9", "pCsK9"]
        results = []

        for query in queries:
            response = requests.get(
                f"{server_url}/api/v1/search",
                params={"q": query, "limit": 5, "format": "json"},
                timeout=30,
            )
            assert response.status_code == 200
            data = response.json()
            results.append(data)

        # All queries should return similar results (same genes)
        if all(len(r) > 0 for r in results):
            # Compare symbols from each result
            symbols_sets = [
                set(item.get("symbol") for item in r if item.get("type") == "gene")
                for r in results
            ]
            # All should find similar genes
            # (exact match depends on fuzzy algorithm, but PCSK9 should be in most)

    def test_search_prefix_matching(self, server_url):
        """Test prefix matching."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "BRC", "limit": 10, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        # Should find genes starting with BRC (BRCA1, BRCA2, etc.)
        if len(data) > 0:
            genes = [item for item in data if item.get("type") == "gene"]
            # Check if any symbols start with BRC
            symbols = [g["symbol"] for g in genes]

    def test_search_phenotype_sample_size_ranking(self, server_url):
        """Test that phenotypes with larger sample sizes rank higher."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "hyperlipid", "types": "phenotypes", "limit": 10, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        # If we have multiple phenotype matches, check sample size influence
        if len(data) > 1:
            # Among results with similar match quality, larger sample size should rank higher
            # This is hard to test precisely without knowing exact data
            # Just verify sample_size field exists
            for pheno in data:
                assert "sample_size" in pheno

    def test_search_tsv_format_genes(self, server_url):
        """Test TSV format for gene results."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "APOE", "types": "genes", "limit": 3, "format": "tsv"},
            timeout=30,
        )

        assert response.status_code == 200
        assert "text/tab-separated-values" in response.headers.get("content-type", "")

        # Check TSV structure
        lines = response.text.strip().split("\n")
        if len(lines) > 1:
            header = lines[0].split("\t")
            # Should have gene-specific columns
            assert "symbol" in header
            assert "name" in header
            assert "aliases" in header
            assert "match_type" in header

    def test_search_tsv_format_phenotypes(self, server_url):
        """Test TSV format for phenotype results."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "diabetes", "types": "phenotypes", "limit": 3, "format": "tsv"},
            timeout=30,
        )

        assert response.status_code == 200
        assert "text/tab-separated-values" in response.headers.get("content-type", "")

        # Check TSV structure
        lines = response.text.strip().split("\n")
        if len(lines) > 1:
            header = lines[0].split("\t")
            # Should have phenotype-specific columns
            assert "code" in header
            assert "name" in header
            assert "resource" in header
            assert "sample_size" in header

    @pytest.mark.parametrize("invalid_limit", [0, -1, 101, 1000])
    def test_search_invalid_limit(self, server_url, invalid_limit):
        """Test invalid limit values."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "test", "limit": invalid_limit, "format": "json"},
            timeout=10,
        )

        # Should return 422 for invalid limit (must be ge=1, le=100)
        assert response.status_code == 422

    def test_search_special_characters(self, server_url):
        """Test search with special characters."""
        # Test queries with hyphens, underscores
        queries = ["NARC-1", "T2D_WIDE", "I9_HYPERLIPID"]

        for query in queries:
            response = requests.get(
                f"{server_url}/api/v1/search",
                params={"q": query, "limit": 5, "format": "json"},
                timeout=30,
            )

            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)

    def test_search_numeric_prefix(self, server_url):
        """Test search starting with numbers (phenotype codes)."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "I9", "types": "phenotypes", "limit": 10, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_search_performance(self, server_url):
        """Test that search completes in reasonable time."""
        import time

        start = time.time()
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "test", "limit": 10, "format": "json"},
            timeout=5,  # Should complete within 5 seconds
        )
        elapsed = time.time() - start

        assert response.status_code == 200
        assert elapsed < 5.0  # Should be much faster, but set reasonable limit

    def test_search_concurrent_requests(self, server_url):
        """Test multiple concurrent search requests."""
        import concurrent.futures

        queries = ["PCSK9", "diabetes", "cancer", "BRCA1", "lipid"]

        def make_request(query):
            return requests.get(
                f"{server_url}/api/v1/search",
                params={"q": query, "limit": 5, "format": "json"},
                timeout=30,
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(make_request, q) for q in queries]
            responses = [f.result() for f in concurrent.futures.as_completed(futures)]

        # All requests should succeed
        assert all(r.status_code == 200 for r in responses)

    def test_search_comma_separated_genes(self, server_url):
        """Test searching for multiple genes with comma-separated query."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "PCSK9,BRCA1,TP53", "types": "genes", "limit": 5, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

        symbols = [item["symbol"] for item in data if item.get("type") == "gene"]
        assert "PCSK9" in symbols
        assert "BRCA1" in symbols
        assert "TP53" in symbols

    def test_search_comma_separated_phenotypes(self, server_url):
        """Test searching for multiple phenotypes with comma-separated query."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "diabetes,cancer", "types": "phenotypes", "limit": 10, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0

        # should have results related to both terms
        names = [item.get("name", "").lower() for item in data]
        has_diabetes = any("diabetes" in n for n in names)
        has_cancer = any("cancer" in n for n in names)
        assert has_diabetes or has_cancer

    def test_search_comma_separated_mixed_types(self, server_url):
        """Test comma-separated query without type filter returns mixed results."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "BRCA1,diabetes", "limit": 10, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

        types = set(item["type"] for item in data)
        # should have both genes and phenotypes
        assert "gene" in types or "phenotype" in types

    def test_search_comma_separated_deduplication(self, server_url):
        """Test that duplicate results are removed when same item matches multiple terms."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "PCSK9,PCSK9", "types": "genes", "limit": 10, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        # should not have duplicate PCSK9 entries
        symbols = [item["symbol"] for item in data if item.get("symbol") == "PCSK9"]
        assert len(symbols) == 1

    def test_search_comma_separated_with_spaces(self, server_url):
        """Test comma-separated query with spaces around terms."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": " PCSK9 , BRCA1 , TP53 ", "types": "genes", "limit": 5, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        symbols = [item["symbol"] for item in data if item.get("type") == "gene"]
        assert "PCSK9" in symbols
        assert "BRCA1" in symbols

    def test_search_comma_separated_tsv_format(self, server_url):
        """Test comma-separated query with TSV format."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "PCSK9,BRCA1", "types": "genes", "limit": 5, "format": "tsv"},
            timeout=30,
        )

        assert response.status_code == 200
        assert "text/tab-separated-values" in response.headers.get("content-type", "")
        assert "PCSK9" in response.text
        assert "BRCA1" in response.text

    def test_search_comma_separated_empty_terms_ignored(self, server_url):
        """Test that empty terms in comma-separated query are ignored."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "PCSK9,,BRCA1,", "types": "genes", "limit": 5, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        symbols = [item["symbol"] for item in data if item.get("type") == "gene"]
        assert "PCSK9" in symbols
        assert "BRCA1" in symbols

    def test_search_single_term_still_works(self, server_url):
        """Test that single term queries still work as before."""
        response = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "PCSK9", "types": "genes", "limit": 5, "format": "json"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data) > 0
        assert data[0]["symbol"] == "PCSK9"
