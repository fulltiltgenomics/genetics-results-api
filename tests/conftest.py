import pytest
import requests

# Add the project root to Python path so we can import app modules
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ============================================================================
# Integration Test Fixtures (for testing against live server)
# ============================================================================


def pytest_addoption(parser):
    """Add custom command line options for pytest."""
    parser.addoption(
        "--server-url",
        action="store",
        default="http://localhost:4000",
        help="Base URL of the server to test against (default: http://localhost:4000)",
    )


@pytest.fixture(scope="session")
def server_url(request):
    """Get the server URL from command line or use default."""
    return request.config.getoption("--server-url")


@pytest.fixture(scope="session")
def available_resources():
    """Get list of available resources from config."""
    from app.services.config_util import get_resources

    return get_resources()


@pytest.fixture(scope="session")
def resources_with_metadata():
    """Get list of resources that have metadata available."""
    from app.services.config_util import get_resources_with_metadata

    return get_resources_with_metadata()


@pytest.fixture(scope="session")
def example_phenotypes():
    """Get example phenotypes for testing from config."""
    import app.config.credible_sets as config

    examples = {}
    for df in config.data_files:
        if "example_pheno_or_study" in df:
            examples[df["id"]] = df["example_pheno_or_study"]
    return examples


@pytest.fixture(scope="session")
def test_gene():
    """A gene that should exist in the database."""
    return "GPT"


@pytest.fixture(scope="session")
def test_gene_large_window():
    """A gene with data across resources."""
    return "PCSK9"


@pytest.fixture(scope="session")
def test_region():
    """A genomic region that should have data."""
    return "1:1000000-1000100"


@pytest.fixture(scope="session")
def test_variant():
    """A variant that should have data."""
    return "19-44908684-T-C"


@pytest.fixture(scope="session")
def test_variant_coloc():
    """A variant known to have colocalization data."""
    return "1-55039974-G-T"


@pytest.fixture(scope="session")
def invalid_region():
    """An invalid region format for negative testing."""
    return "invalid:region"


@pytest.fixture(scope="session")
def invalid_variant():
    """An invalid variant format for negative testing."""
    return "invalid-variant"


@pytest.fixture(scope="session")
def invalid_gene():
    """A gene name that doesn't exist."""
    return "NONEXISTENTGENE123"


@pytest.fixture(scope="session")
def invalid_resource():
    """A resource name that doesn't exist."""
    return "invalid_resource"


# ============================================================================
# Pytest Hooks for Better Error Reporting
# ============================================================================

import threading

_request_context = threading.local()


def _store_last_request(url: str, params: dict | None, response: requests.Response):
    """Store the last request info for error reporting."""
    from urllib.parse import urlencode

    if params:
        full_url = f"{url}?{urlencode(params, doseq=True)}"
    else:
        full_url = url
    _request_context.last_url = full_url
    _request_context.last_status = response.status_code
    _request_context.last_response_preview = response.text[:500] if response.text else ""


def _get_last_request_info() -> str | None:
    """Get the last request URL if available."""
    return getattr(_request_context, "last_url", None)


def _get_last_response_info() -> tuple[int | None, str | None]:
    """Get the last response status and preview."""
    status = getattr(_request_context, "last_status", None)
    preview = getattr(_request_context, "last_response_preview", None)
    return status, preview


_original_requests_get = requests.get


def _tracked_requests_get(url, params=None, **kwargs):
    """Wrapper around requests.get that tracks the URL."""
    response = _original_requests_get(url, params=params, **kwargs)
    _store_last_request(url, params, response)
    return response


requests.get = _tracked_requests_get


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """
    Hook to capture test failures and add request URL information.
    """
    outcome = yield
    rep = outcome.get_result()

    if rep.when == "call" and rep.failed:
        last_url = _get_last_request_info()
        if last_url:
            status, preview = _get_last_response_info()
            section_content = f"URL: {last_url}"
            if status is not None:
                section_content += f"\nStatus: {status}"
            if preview:
                section_content += f"\nResponse preview: {preview}"
            rep.sections.append(("Failed Request Info", section_content))
        elif hasattr(item, "funcargs"):
            for arg_name, arg_value in item.funcargs.items():
                if isinstance(arg_value, requests.Response) and hasattr(
                    arg_value, "test_url"
                ):
                    rep.sections.append(("Failed Request URL", arg_value.test_url))
