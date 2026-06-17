"""
Tests for the named curated variant set endpoints.
"""

import requests


class TestVariantSets:
    """Test /api/v1/variant_sets endpoints."""

    def test_list_sets(self, server_url):
        """GET /variant_sets returns the configured set names."""
        response = requests.get(f"{server_url}/api/v1/variant_sets", timeout=30)

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        # the finngen/daly profiles both configure these curated sets
        assert "FinnGen_enriched_202505" in data

    def test_get_set_returns_canonical_variants(self, server_url):
        """GET /variant_sets/{name} expands a set to canonical chr:pos:ref:alt ids."""
        response = requests.get(
            f"{server_url}/api/v1/variant_sets/COVID19_HGI_severity", timeout=60
        )

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "COVID19_HGI_severity"
        assert isinstance(data["variants"], list)
        assert len(data["variants"]) > 0
        # canonical colon-separated form, four fields each
        for v in data["variants"]:
            parts = v.split(":")
            assert len(parts) == 4, v

    def test_unknown_set_returns_404(self, server_url):
        """An unknown set name is a 404, not a 500."""
        response = requests.get(
            f"{server_url}/api/v1/variant_sets/NoSuchSet_xyz", timeout=30
        )

        assert response.status_code == 404
