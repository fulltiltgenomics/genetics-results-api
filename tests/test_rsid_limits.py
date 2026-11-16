"""The uniform bound on /api/v1/rsid/variants.

This route is ``@is_public``, so `app.core.limits` relaxes the response cap on it. With nothing
bounding the id count, a script that *omitted* its sandbox token got an unbounded response
where the same script presenting the token got 16 MiB — the weaker credential buying the looser
limit, which is exactly the invariant the design is built on. The POST is the sharp end: it
reads its ids from a request body, and `k8s/network-policies/sandbox-policy.yaml` lets the
sandbox reach results-api:4000 directly, so auth-gateway's ``client_max_body_size`` never sees
it.

The fix is a bound applied to every caller with no sandbox special case, which is what makes
the invariant hold rather than merely tightening the caller that chose to identify itself.
"""


import pytest
from fastapi import FastAPI, HTTPException
from helpers.asgi import call_asgi

from app.routers import rsid


def test_the_limit_is_the_documented_number():
    assert rsid.MAX_RSIDS == 5000


def test_a_request_at_the_limit_is_accepted():
    ids = ",".join(f"rs{i}" for i in range(rsid.MAX_RSIDS))
    assert len(rsid.parse_and_validate_rsids(ids)) == rsid.MAX_RSIDS


def test_a_request_over_the_limit_is_rejected():
    ids = ",".join(f"rs{i}" for i in range(rsid.MAX_RSIDS + 1))
    with pytest.raises(HTTPException) as exc:
        rsid.parse_and_validate_rsids(ids)

    assert exc.value.status_code == 422
    assert str(rsid.MAX_RSIDS) in exc.value.detail


def test_the_limit_does_not_regress_any_get_that_works_today():
    """The GET is incidentally bounded by h11's 16 KiB request-line-plus-headers limit, and the
    shortest possible id costs 4 bytes in the query string ("rs1,"), so 4096 is the most a GET
    can carry. The uniform bound has to sit at or above that to regress nothing."""
    assert rsid.MAX_RSIDS >= 16 * 1024 // len("rs1,")


def test_the_bad_format_message_is_not_built_from_an_oversized_list():
    """The count is checked before the format scan, so a huge list of invalid ids cannot turn
    into a huge error body."""
    ids = ",".join("nope" for _ in range(rsid.MAX_RSIDS + 1))
    with pytest.raises(HTTPException) as exc:
        rsid.parse_and_validate_rsids(ids)

    assert "Too many rsids" in exc.value.detail
    assert "nope" not in exc.value.detail


# --- the POST body itself --------------------------------------------------------------------


class _FakeRsidDb:
    async def get_variants_by_rsids(self, rsids):
        return {r.lower(): [] for r in rsids}


def _post(body: bytes):
    """Drive the ASGI app directly: httpx (and so TestClient) is not installed here."""
    api = FastAPI()
    api.include_router(rsid.router)
    api.dependency_overrides[rsid.get_rsid_db] = lambda: _FakeRsidDb()

    response = call_asgi(
        api,
        "POST",
        "/rsid/variants",
        headers=[("content-length", str(len(body)))],
        body=body,
        body_chunk_size=4096,
    )
    return response.status_code, response.json()


def test_post_accepts_a_batch_at_the_limit():
    status, payload = _post(",".join(f"rs{i}" for i in range(rsid.MAX_RSIDS)).encode())
    assert status == 200
    assert len(payload) == rsid.MAX_RSIDS


def test_post_rejects_a_batch_over_the_limit():
    status, payload = _post(",".join(f"rs{i}" for i in range(rsid.MAX_RSIDS + 1)).encode())
    assert status == 422
    assert "Too many rsids" in str(payload["detail"])


def test_post_stops_reading_an_oversized_body_instead_of_materializing_it():
    """The count check can only run once the whole body is decoded, which is too late when the
    body itself is the payload — so the read is bounded as it streams in."""
    status, payload = _post(b"a" * (rsid.MAX_RSID_BODY_BYTES + 8192))
    assert status == 413
    assert "too large" in str(payload["detail"]).lower()
