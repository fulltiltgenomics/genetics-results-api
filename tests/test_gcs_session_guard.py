"""The lazy GCS session must be impossible to reach unopened.

`GCloudTabixBase` opens its aiohttp/GCS client on first use rather than in `__init__`,
because most instances only ever shell out to tabix and an eagerly created session would
leak. What makes that safe is that `storage` and `session` are properties which open it on
read: reading an unopened client would raise an AttributeError that the callers cannot tell
apart from a missing object, and `check_phenotype_exists` — which gates every path that
would otherwise open the session — is one of those callers, so such a 404 never heals.
"""

import asyncio
import re
from pathlib import Path

import aiohttp.client_exceptions
import pytest

from app.services import gcloud_tabix_base
from app.services.gcloud_tabix_data_access import GCloudTabixDataAccess

SERVICES_DIR = Path(__file__).resolve().parent.parent / "app" / "services"
APP_DIR = Path(__file__).resolve().parent.parent / "app"
ACCESSOR_MODULE = SERVICES_DIR / "gcloud_tabix_base.py"


class _FakeResponse:
    def __init__(self, status: int):
        self._status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def raise_for_status(self):
        if self._status >= 400:
            raise aiohttp.client_exceptions.ClientResponseError(
                request_info=None, history=(), status=self._status
            )


class _FakeSession:
    closed = False

    def __init__(self, status: int, log: list):
        self._status = status
        self._log = log

    def head(self, url, headers=None):
        self._log.append(("HEAD", url, headers))
        return _FakeResponse(self._status)


class _FakeStorage:
    async def _headers(self):
        return {"Authorization": "Bearer fake"}


def _stub_gcs(access, status: int) -> list:
    """Make the guard's one initialiser install fakes, so nothing reaches the network.

    Everything hangs off `_init_storage`: if a code path skips the guard, this never runs
    and the path sees an unopened client, which is exactly the failure under test.
    """
    log: list = []
    access._init_storage = lambda: (
        setattr(access, "_session", _FakeSession(status, log)),
        setattr(access, "_storage", _FakeStorage()),
    )
    return log


@pytest.fixture
def unwarmed_ibd_exome(monkeypatch):
    """The one resource whose `warm()` opens nothing: it has no combined file."""
    monkeypatch.setattr(gcloud_tabix_base, "ensure_gcs_token", lambda: None)
    access = GCloudTabixDataAccess("ibd_exome_2026", "exome")
    assert access._combined_file() is None
    asyncio.run(access.warm())
    assert access._session is None, "warm() must not have opened a session here"
    return access


def test_check_phenotype_exists_opens_the_session_it_was_never_given(unwarmed_ibd_exome):
    log = _stub_gcs(unwarmed_ibd_exome, status=200)

    assert asyncio.run(unwarmed_ibd_exome.check_phenotype_exists("IBD")) is True
    assert unwarmed_ibd_exome._session is not None
    assert len(log) == 1 and log[0][0] == "HEAD"
    assert log[0][1].startswith("https://storage.googleapis.com/")


def test_check_phenotype_exists_still_reports_a_genuinely_missing_file(
    unwarmed_ibd_exome,
):
    """The guard must not turn a real 404 into a hit — that is what it looked like."""
    _stub_gcs(unwarmed_ibd_exome, status=404)

    assert asyncio.run(unwarmed_ibd_exome.check_phenotype_exists("IBD")) is False


def test_cleanup_does_not_open_a_session(unwarmed_ibd_exome):
    """Shutdown reads the backing fields, so closing an unused object opens nothing."""
    asyncio.run(unwarmed_ibd_exome.cleanup())

    assert unwarmed_ibd_exome._session is None
    assert unwarmed_ibd_exome._storage is None


def test_only_the_accessor_module_touches_the_backing_fields():
    """Reintroducing the bug means reading `_session`/`_storage` outside their properties.

    Nothing else can omit the guard, so this is the one remaining way in — make it loud
    rather than a silent, permanent 404. The invariant is repo-wide, not just
    `app/services/`: routers, core, and `app/dependencies.py` all import these classes
    too, so the sweep covers all of `app/`, excluding only the accessor module itself.
    """
    backing_field = re.compile(r"(?<![A-Za-z0-9_])_(?:session|storage)\b")
    scanned = [p for p in APP_DIR.rglob("*.py") if p != ACCESSOR_MODULE]
    assert scanned, "no files were scanned — app/ was renamed, emptied, or moved"

    offenders = sorted(
        str(p.relative_to(APP_DIR))
        for p in scanned
        if backing_field.search(p.read_text())
    )

    assert offenders == [], (
        "these modules read the GCS client's backing fields directly, bypassing the "
        f"guard in gcloud_tabix_base.py: {offenders}"
    )


def test_a_tabix_only_subclass_says_so_instead_of_yielding_a_none_client(monkeypatch):
    """`GnomAD` stubs `_init_storage` out because it only shells out to tabix.

    That is the one way the guard can still run and produce nothing, so it must name the
    class rather than let an AttributeError reach a caller that reads it as "not found".
    """
    monkeypatch.setattr(gcloud_tabix_base, "ensure_gcs_token", lambda: None)

    class TabixOnly(gcloud_tabix_base.GCloudTabixBase):
        def _init_storage(self):
            pass

    with pytest.raises(RuntimeError, match="TabixOnly"):
        TabixOnly().session
