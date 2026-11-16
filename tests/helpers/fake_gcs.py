"""Fakes for the aiohttp session and GCS client a GCloudTabixBase object opens lazily."""

import aiohttp.client_exceptions


class FakeGcsResponse:
    """A response context manager; with a `log` it records being entered and released."""

    def __init__(self, status: int, log: list | None = None):
        self.status = status
        self._log = log

    async def __aenter__(self):
        if self._log is not None:
            self._log.append("enter")
        return self

    async def __aexit__(self, *exc):
        if self._log is not None:
            self._log.append("exit")
        return False

    def raise_for_status(self):
        if self.status >= 400:
            raise aiohttp.client_exceptions.ClientResponseError(
                request_info=None, history=(), status=self.status
            )


class FakeGcsSession:
    """Answers every HEAD with `status` and logs it; `track_release` also logs each
    response's enter and exit, for tests about the connection being given back."""

    closed = False

    def __init__(self, status: int, log: list, track_release: bool = False):
        self._status = status
        self._log = log
        self._track_release = track_release

    def head(self, url, headers=None):
        self._log.append(("HEAD", url))
        return FakeGcsResponse(self._status, self._log if self._track_release else None)

    def get(self, url, headers=None):
        raise AssertionError("existence check must not GET the object")


class FakeGcsStorage:
    async def _headers(self):
        return {"Authorization": "Bearer fake"}
