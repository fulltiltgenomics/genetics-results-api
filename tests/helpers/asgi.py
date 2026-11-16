"""Drive an ASGI app directly: httpx (and so TestClient) is not installed here.

The driver itself is tests/sandbox_contract.py's, which db-api's tests share; it is
re-exported so this repo's tests have one place to import it from.
"""

from sandbox_contract import AsgiResponse, call_asgi
from starlette.requests import Request

__all__ = ["AsgiResponse", "call_asgi", "request_with_headers"]


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
