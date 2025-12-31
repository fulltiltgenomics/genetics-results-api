"""
Tests for health and basic server endpoints.
"""

import pytest
import requests


class TestHealth:
    """Test health check and basic server functionality."""

    def test_healthz_endpoint(self, server_url):
        """Test that the health check endpoint returns 200 OK."""
        url = f"{server_url}/healthz"
        response = requests.get(url, timeout=5)

        # Enhanced assertion with URL in error message
        assert response.status_code == 200, (
            f"Health check failed!\n"
            f"URL: {url}\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text[:200]}"
        )

        assert response.headers.get("content-type") == "application/json", (
            f"Wrong content type!\n"
            f"URL: {url}\n"
            f"Expected: application/json\n"
            f"Got: {response.headers.get('content-type')}"
        )

        data = response.json()
        assert "status" in data, f"URL: {url}\nResponse missing 'status' field: {data}"
        assert data["status"] == "ok!", f"URL: {url}\nUnexpected status: {data['status']}"

    def test_api_root_endpoint(self, server_url):
        """Test that the API root endpoint returns basic info."""
        url = f"{server_url}/api/v1"
        response = requests.get(url, timeout=5)

        assert response.status_code == 200, (
            f"API root failed!\n"
            f"URL: {url}\n"
            f"Status: {response.status_code}"
        )

        assert "application/json" in response.headers.get("content-type", ""), (
            f"Wrong content type!\n"
            f"URL: {url}\n"
            f"Content-Type: {response.headers.get('content-type')}"
        )

        data = response.json()
        assert "name" in data, f"URL: {url}\nMissing 'name' field"
        assert data["name"] == "Genetics Results API", f"URL: {url}\nWrong API name: {data['name']}"
        assert "status" in data, f"URL: {url}\nMissing 'status' field"
        assert data["status"] == "ok", f"URL: {url}\nWrong status: {data['status']}"

    def test_server_is_reachable(self, server_url):
        """Test that the server is reachable and responding."""
        url = f"{server_url}/healthz"
        try:
            response = requests.get(url, timeout=5)
            assert response.status_code == 200, (
                f"Server is not healthy!\n"
                f"URL: {url}\n"
                f"Status: {response.status_code}"
            )
        except requests.exceptions.ConnectionError:
            pytest.fail(f"Cannot connect to server.\nURL: {url}\nIs the server running?")
        except requests.exceptions.Timeout:
            pytest.fail(f"Server timed out.\nURL: {url}\nIs the server overloaded?")
