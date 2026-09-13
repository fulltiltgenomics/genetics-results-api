"""The per-phenotype existence check must not acquire a connection it never releases.

A bare ``session.get`` of a multi-hundred-MB summary-stats object, never read and never
closed, left the response streaming into a socket nobody drained. A PheWAS makes hundreds
of these per request; after a few requests 256 of them held every slot in the aiohttp
connector and every later GCS call timed out. The check is now a HEAD inside a context
manager, and the checks for one request run concurrently.
"""

import asyncio

import aiohttp.client_exceptions

from app.services.sumstats_data_access import SumstatsDataAccess


class _FakeResponse:
    def __init__(self, status, log):
        self.status = status
        self._log = log

    async def __aenter__(self):
        self._log.append("enter")
        return self

    async def __aexit__(self, *exc):
        self._log.append("exit")
        return False


class _FakeSession:
    def __init__(self, status, log):
        self._status = status
        self._log = log
        self.inflight = 0
        self.high = 0

    def head(self, url, headers=None):
        self._log.append(("HEAD", url))
        return _FakeResponse(self._status, self._log)

    def get(self, url, headers=None):
        raise AssertionError("existence check must not GET the object")


class _FakeStorage:
    async def _headers(self):
        return {"Authorization": "Bearer fake"}


def _access(status):
    log = []
    access = SumstatsDataAccess()
    access._initialized = True
    access._session = _FakeSession(status, log)
    access._storage = _FakeStorage()
    access._ensure_storage = lambda: None
    return access, log


def test_existing_file_is_a_head_request_whose_response_is_released():
    access, log = _access(200)
    assert asyncio.run(access._check_file_exists("gs://b/R14/PHENO.gz")) is True
    assert log == [
        ("HEAD", "https://storage.googleapis.com/b/R14/PHENO.gz"),
        "enter",
        "exit",
    ]


def test_missing_file_by_status_and_by_raised_404():
    access, log = _access(404)
    assert asyncio.run(access._check_file_exists("gs://b/R14/PHENO.gz")) is False
    assert log[-1] == "exit"

    access, _ = _access(200)

    class _Raising(_FakeSession):
        def head(self, url, headers=None):
            raise aiohttp.client_exceptions.ClientResponseError(
                request_info=None, history=(), status=404
            )

    access._session = _Raising(200, [])
    assert asyncio.run(access._check_file_exists("gs://b/R14/PHENO.gz")) is False


def test_local_path_does_not_touch_the_session(tmp_path):
    access, log = _access(200)
    present = tmp_path / "x.gz"
    present.write_bytes(b"")
    assert asyncio.run(access._check_file_exists(str(present))) is True
    assert asyncio.run(access._check_file_exists(str(tmp_path / "missing.gz"))) is False
    assert log == []
