"""Drive an ASGI app directly: httpx (and so TestClient) is not installed here.

The driver itself is tests/sandbox_contract.py's, which db-api's tests share; it is
re-exported so this repo's tests have one place to import it from.
"""

from sandbox_contract import AsgiResponse, acall_asgi, call_asgi
from starlette.requests import Request

__all__ = ["AsgiResponse", "DecodedResponse", "acall_asgi", "call_asgi", "request_with_headers"]


class DecodedResponse(AsgiResponse):
    """An AsgiResponse with its headers as a dict keyed by lower-case name and its body as text."""

    def __init__(self, messages: list[dict]):
        super().__init__(messages)
        self.headers = {k.decode().lower(): v.decode() for k, v in self.headers}

    @property
    def text(self) -> str:
        return self.content.decode()


def request_with_headers(headers: dict[str, str]) -> Request:
    """A bare GET request carrying `headers`, for calling the auth helpers directly."""
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/api/v1/gene/GPT",
            "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
        }
    )
