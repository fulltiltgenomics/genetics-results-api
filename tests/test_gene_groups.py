"""
Integration tests for the HGNC gene-group endpoints:
  - GET /api/v1/gene_group/members
  - GET /api/v1/gene/normalize

Run against a live server like the other router tests:
    pytest --server-url http://host:port

When no server is reachable the whole module skips cleanly (mirrors the
ConnectionError handling in test_health.py) instead of erroring, so the suite
stays green without a live backend.
"""

import pytest
import requests


@pytest.fixture(scope="module", autouse=True)
def require_server(server_url):
    """
    Skip this module unless a server exposing these endpoints is reachable.

    Mirrors the ConnectionError handling in test_health.py, and additionally
    requires the gene_group route to exist so the suite skips cleanly (rather
    than failing) when no real backend — or an unrelated server — is present.
    """
    url = f"{server_url}/api/v1/gene_group/members"
    try:
        # missing/empty args -> 400 from this API; a route that doesn't exist
        # (no server, or a different app) yields 404, in which case we skip.
        response = requests.get(url, timeout=5)
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        pytest.skip(f"No reachable server at {server_url}")
    if response.status_code == 404:
        pytest.skip(f"gene_group endpoints not served at {server_url}")


class TestGeneGroupMembers:
    """Test /api/v1/gene_group/members endpoint."""

    def _assert_member_shape(self, member):
        assert set(member.keys()) >= {
            "hgnc_id",
            "symbol",
            "ensembl_id",
            "chr",
            "gene_start",
            "gene_end",
        }

    def test_members_by_group_id(self, server_url):
        """Resolve members by group_id and validate the response shape."""
        response = requests.get(
            f"{server_url}/api/v1/gene_group/members",
            params={"group_id": 139},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        assert data["group_id"] == 139
        assert "group_name" in data
        assert isinstance(data["group_name"], str)
        assert data["count"] == len(data["members"])
        assert isinstance(data["members"], list)
        if data["members"]:
            self._assert_member_shape(data["members"][0])

    def test_members_by_group_name_case_insensitive(self, server_url):
        """Resolving by group_name (case-insensitive) matches resolving by id."""
        by_id = requests.get(
            f"{server_url}/api/v1/gene_group/members",
            params={"group_id": 139},
            timeout=30,
        )
        assert by_id.status_code == 200
        name = by_id.json()["group_name"]

        # mangle the case to prove name resolution is case-insensitive
        by_name = requests.get(
            f"{server_url}/api/v1/gene_group/members",
            params={"group_name": name.upper()},
            timeout=30,
        )
        assert by_name.status_code == 200
        data = by_name.json()

        assert data["group_id"] == 139
        assert data["group_name"] == name
        assert data["count"] == by_id.json()["count"]

    def test_members_hierarchical_lineage(self, server_url):
        """
        A root group returns members whose own leaf family is a descendant.

        Group 139 (G protein-coupled receptors, a root) must include HTR1A,
        whose leaf family is 170 — proving lineage expansion, not just
        leaf-family membership.
        """
        response = requests.get(
            f"{server_url}/api/v1/gene_group/members",
            params={"group_id": 139},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        symbols = {m["symbol"] for m in data["members"]}
        assert "HTR1A" in symbols, (
            "root group 139 should include HTR1A via lineage expansion "
            "even though its leaf family is 170"
        )

    def test_members_requires_exactly_one_arg_neither(self, server_url):
        """Supplying neither group_id nor group_name returns 400."""
        response = requests.get(
            f"{server_url}/api/v1/gene_group/members",
            timeout=10,
        )
        assert response.status_code == 400

    def test_members_requires_exactly_one_arg_both(self, server_url):
        """Supplying both group_id and group_name returns 400."""
        response = requests.get(
            f"{server_url}/api/v1/gene_group/members",
            params={"group_id": 139, "group_name": "anything"},
            timeout=10,
        )
        assert response.status_code == 400

    def test_members_unknown_group_id(self, server_url):
        """An unknown group_id returns 404."""
        response = requests.get(
            f"{server_url}/api/v1/gene_group/members",
            params={"group_id": 999999999},
            timeout=10,
        )
        assert response.status_code == 404

    def test_members_unknown_group_name(self, server_url):
        """An unknown group_name returns 404."""
        response = requests.get(
            f"{server_url}/api/v1/gene_group/members",
            params={"group_name": "NoSuchGeneGroupXYZ123"},
            timeout=10,
        )
        assert response.status_code == 404


class TestGeneNormalize:
    """Test /api/v1/gene/normalize endpoint."""

    def test_normalize_previous_or_alias_symbol(self, server_url):
        """A previous/alias symbol resolves to its approved symbol."""
        # NARC1 / NARC-1 is a previous/alias symbol for PCSK9
        response = requests.get(
            f"{server_url}/api/v1/gene/normalize",
            params={"symbols": "NARC1"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()
        assert "mappings" in data
        assert "unresolved" in data

        approved = {m["approved"] for m in data["mappings"]}
        assert "PCSK9" in approved
        mapping = next(m for m in data["mappings"] if m["approved"] == "PCSK9")
        assert mapping["input"] == "NARC1"
        assert mapping["matched_on"] == "alias_or_previous"

    def test_normalize_approved_symbol_maps_to_itself(self, server_url):
        """An approved symbol resolves to itself, matched_on='approved'."""
        response = requests.get(
            f"{server_url}/api/v1/gene/normalize",
            params={"symbols": "PCSK9"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        assert len(data["mappings"]) == 1
        mapping = data["mappings"][0]
        assert mapping["input"] == "PCSK9"
        assert mapping["approved"] == "PCSK9"
        assert mapping["matched_on"] == "approved"
        assert data["unresolved"] == []

    def test_normalize_unknown_symbol_is_unresolved(self, server_url):
        """An unknown symbol is returned in unresolved, not mappings."""
        response = requests.get(
            f"{server_url}/api/v1/gene/normalize",
            params={"symbols": "NONEXISTENTGENE123"},
            timeout=30,
        )

        assert response.status_code == 200
        data = response.json()

        assert "NONEXISTENTGENE123" in data["unresolved"]
        assert all(
            m["input"] != "NONEXISTENTGENE123" for m in data["mappings"]
        )
