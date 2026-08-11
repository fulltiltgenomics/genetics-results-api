import pytest
import requests

# Add the project root to Python path so we can import app modules
import sys
import os
import socket

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ============================================================================
# Integration Test Fixtures (for testing against live server)
# ============================================================================


def pytest_addoption(parser):
    """Add custom command line options for pytest."""
    parser.addoption(
        "--server-url",
        action="store",
        default=None,
        help=(
            "Base URL of an already-running server to test against. "
            "When omitted, the app is started in-process on a free port."
        ),
    )


def pytest_collection_modifyitems(config, items):
    """Auto-classify every test as `integration` or `offline`.

    The distinction is the fixture a test asks for, not a marker someone remembers to
    write: anything that takes `server_url` reaches real data over the network, and
    everything else does not. Applying it here means a newly added test lands in the
    `offline` set by default — the set that runs with no credentials and no network —
    rather than silently joining a pile that never executes.

    The lane only holds as long as importing an `app` module stays credential-free:
    collection imports every test module regardless of the mark expression, so anything
    that reaches the network — or merely constructs a Google client that resolves
    Application Default Credentials — at import time breaks `-m offline` for everyone. It
    breaks loudly, at collection, not silently-green, and it need not be a data client:
    `google.cloud.logging.Client()` behind `setup_logging()` took the lane down the same
    way `DatasetMapping()` did. The fix belongs in the app module (build it lazily, as
    `app.core.streams.get_dataset_mapping` does, or degrade, as `setup_logging` now does),
    not in a list of exclusions here.
    """
    for item in items:
        declared = {m.name for m in item.iter_markers()}
        if "server_url" in getattr(item, "fixturenames", ()):
            if "integration" not in declared:
                item.add_marker(pytest.mark.integration)
        elif not declared & {"integration", "offline"}:
            item.add_marker(pytest.mark.offline)


# app startup warms every configured tabix header, the gene maps, the search index and
# the gene-disease tables off GCS, then runs a cross-resource smoke query; ~25s observed
_STARTUP_TIMEOUT_SECONDS = 300


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="session")
def server_url(request):
    """Base URL of the server under test.

    With `--server-url`, yields it untouched — the deployed-server workflow is unchanged.
    Without it, boots the real ASGI app in this process on an ephemeral port and yields
    that, so a bare `pytest` exercises the routers instead of failing to connect (or, worse,
    hitting whatever unrelated service happens to hold the old default port 4000).
    """
    explicit = request.config.getoption("--server-url")
    if explicit:
        yield explicit
        return

    import threading
    import time
    import uvicorn

    # the tabix services cache fetched .tbi/.csi here; run_server.py creates it too
    os.makedirs("/tmp/tbi_cache", exist_ok=True)

    from app.server import app

    port = _free_port()
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=port,
        # asyncio, not uvloop: uvloop gives subprocesses a socket for stdin, which breaks
        # tabix's -R /dev/stdin (uvloop #532). Same reason run_server.py pins it.
        loop="asyncio",
        log_config=None,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, name="in-process-results-api", daemon=True)
    thread.start()

    deadline = time.monotonic() + _STARTUP_TIMEOUT_SECONDS
    while not server.started:
        if not thread.is_alive():
            pytest.fail(
                "in-process server failed to start (the lifespan warming or smoke query "
                "raised); it needs working Google credentials and GCS access. Run "
                "`pytest -m offline` for the subset that needs neither, or pass "
                "--server-url to test against a running server."
            )
        if time.monotonic() > deadline:
            server.should_exit = True
            thread.join(timeout=30)
            pytest.fail(f"in-process server did not start within {_STARTUP_TIMEOUT_SECONDS}s")
        time.sleep(0.25)

    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=30)


@pytest.fixture(scope="session")
def available_resources():
    """Get list of available resources from config."""
    from app.services.config_util import get_resources

    return get_resources()


@pytest.fixture(scope="session")
def cs_resources():
    """Resources that offer credible-set data (for credible_sets endpoint tests)."""
    from app.services.config_util import get_resources

    return get_resources(data_type="cs")


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
