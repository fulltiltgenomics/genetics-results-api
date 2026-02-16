"""
Tests for rsID to variants endpoint.
"""

import pytest
import requests


class TestRsidToVariants:
    """Test /api/v1/rsid/variants endpoint."""

    def test_get_single_rsid(self, server_url):
        """Test GET with a single valid rsid."""
        response = requests.get(
            f"{server_url}/api/v1/rsid/variants",
            params={"rsids": "rs1234567"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["rsid"] == "rs1234567"
        assert isinstance(data[0]["variants"], list)
        # rs1234567 exists in db: 7:97795920:T:C
        assert len(data[0]["variants"]) > 0

    def test_get_multiple_rsids(self, server_url):
        """Test GET with multiple valid rsids."""
        response = requests.get(
            f"{server_url}/api/v1/rsid/variants",
            params={"rsids": "rs1234567,rs7654321"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 2
        rsids_returned = [item["rsid"] for item in data]
        assert "rs1234567" in rsids_returned
        assert "rs7654321" in rsids_returned

    def test_post_rsids(self, server_url):
        """Test POST with comma-separated rsids in body."""
        response = requests.post(
            f"{server_url}/api/v1/rsid/variants",
            data="rs1234567,rs7654321",
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 2

    def test_case_insensitive_rsid(self, server_url):
        """Test that rsid validation is case insensitive."""
        test_cases = ["RS1234567", "Rs1234567", "rS1234567"]

        for rsid in test_cases:
            response = requests.get(
                f"{server_url}/api/v1/rsid/variants",
                params={"rsids": rsid},
                timeout=30,
            )
            assert response.status_code == 200, f"Failed for {rsid}"
            data = response.json()
            # all should return lowercased rsid
            assert data[0]["rsid"] == "rs1234567"

    def test_rsid_not_found(self, server_url):
        """Test that rsid not in database returns empty variants array."""
        response = requests.get(
            f"{server_url}/api/v1/rsid/variants",
            params={"rsids": "rs999999999999"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["rsid"] == "rs999999999999"
        assert data[0]["variants"] == []

    def test_invalid_rsid_format_no_rs(self, server_url):
        """Test invalid rsid without 'rs' prefix returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/rsid/variants",
            params={"rsids": "1234567"},
            timeout=10,
        )

        assert response.status_code == 422
        data = response.json()
        assert "Invalid rsid format" in data["detail"]

    def test_invalid_rsid_format_letters(self, server_url):
        """Test invalid rsid with letters after rs returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/rsid/variants",
            params={"rsids": "rsABC123"},
            timeout=10,
        )

        assert response.status_code == 422
        data = response.json()
        assert "Invalid rsid format" in data["detail"]

    def test_invalid_rsid_format_extra_chars(self, server_url):
        """Test invalid rsid with extra characters returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/rsid/variants",
            params={"rsids": "rs1234567-A"},
            timeout=10,
        )

        assert response.status_code == 422

    def test_mixed_valid_invalid_rsids(self, server_url):
        """Test that mix of valid and invalid rsids returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/rsid/variants",
            params={"rsids": "rs1234567,invalid,rs7654321"},
            timeout=10,
        )

        assert response.status_code == 422
        data = response.json()
        assert "invalid" in data["detail"].lower()

    def test_empty_rsids_param(self, server_url):
        """Test empty rsids parameter returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/rsid/variants",
            params={"rsids": ""},
            timeout=10,
        )

        assert response.status_code == 422

    def test_missing_rsids_param(self, server_url):
        """Test missing rsids parameter returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/rsid/variants",
            timeout=10,
        )

        assert response.status_code == 422

    def test_whitespace_only_rsids(self, server_url):
        """Test whitespace-only rsids returns 422."""
        response = requests.get(
            f"{server_url}/api/v1/rsid/variants",
            params={"rsids": "   "},
            timeout=10,
        )

        assert response.status_code == 422

    def test_rsids_with_spaces(self, server_url):
        """Test rsids with spaces around them are trimmed correctly."""
        response = requests.get(
            f"{server_url}/api/v1/rsid/variants",
            params={"rsids": " rs1234567 , rs7654321 "},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2

    def test_duplicate_rsids_deduplicated(self, server_url):
        """Test that duplicate rsids are deduplicated in response."""
        response = requests.get(
            f"{server_url}/api/v1/rsid/variants",
            params={"rsids": "rs1234567,rs1234567,RS1234567"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        # should only have one entry for rs1234567
        assert len(data) == 1
        assert data[0]["rsid"] == "rs1234567"

    def test_empty_commas_ignored(self, server_url):
        """Test that empty values from consecutive commas are ignored."""
        response = requests.get(
            f"{server_url}/api/v1/rsid/variants",
            params={"rsids": "rs1234567,,rs7654321,"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2

    def test_variant_format(self, server_url):
        """Test that variants are returned in chr:pos:ref:alt format."""
        response = requests.get(
            f"{server_url}/api/v1/rsid/variants",
            params={"rsids": "rs1234567"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        if data[0]["variants"]:
            variant = data[0]["variants"][0]
            parts = variant.split(":")
            assert len(parts) == 4, f"Expected chr:pos:ref:alt format, got {variant}"

    def test_post_empty_body(self, server_url):
        """Test POST with empty body returns 422."""
        response = requests.post(
            f"{server_url}/api/v1/rsid/variants",
            data="",
            timeout=10,
        )

        assert response.status_code == 422

    def test_response_preserves_order(self, server_url):
        """Test that response preserves input order (first occurrence)."""
        response = requests.get(
            f"{server_url}/api/v1/rsid/variants",
            params={"rsids": "rs7654321,rs1234567"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert data[0]["rsid"] == "rs7654321"
        assert data[1]["rsid"] == "rs1234567"

    def test_rsid_with_many_variants(self, server_url):
        """Test rsid that maps to multiple variants."""
        # some rsids can map to multiple variants (different alts at same position)
        response = requests.get(
            f"{server_url}/api/v1/rsid/variants",
            params={"rsids": "rs1234567"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data[0]["variants"], list)
        # just verify it's a list, actual count depends on data

    def test_large_batch(self, server_url):
        """Test handling of larger batch of rsids."""
        rsids = [f"rs{i}" for i in range(1000000, 1000020)]
        rsids_str = ",".join(rsids)

        response = requests.get(
            f"{server_url}/api/v1/rsid/variants",
            params={"rsids": rsids_str},
            timeout=60,
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 20
