"""Validation of the per-execution sandbox tokens minted by chat-backend.

Design of record: ``docs/code-execution-security.md`` §4 in genetics-results-suite.

results-api is harder to get right than db-api because it has *other* JWT callers. Three
rules follow from that, and all three are load-bearing:

1. **Discriminate on the JOSE ``alg`` header, never on dots.** Google Identity Tokens are
   three-segment JWTs too, and they are handled at ``get_bearer_token_user``'s ``"." in
   token`` branch. Routing every JWT-shaped bearer here would reject every Google Identity
   Token results-api serves. Only ``alg == "HS256"`` is a sandbox token.

2. **Reading the unverified header is safe, and proves nothing.** It selects a validator, it
   never configures one: this module pins ``algorithms=["HS256"]`` and its own key, the
   Google path verifies against Google's RS256 certificates. A forged ``alg`` changes which
   validator rejects the token, not whether it is rejected. This is not licence to pass the
   header's ``alg`` to the decoder or to trust ``kid``/``iss`` for anything but routing.

3. **A sandbox-shaped bearer that fails is a hard 401**, never a fallthrough. It must never
   be compared against the shared secret and must never reach ``verify_oauth2_token``, where
   an unset ``GOOGLE_TOKEN_AUDIENCE`` only warns and continues.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import sys
import time
from dataclasses import dataclass

import jwt

import app.config.common as config

logger = logging.getLogger(__name__)

AUDIENCE = "results-api"
ISSUER = "chat-backend"
ALGORITHM = "HS256"

REQUIRED_CLAIMS = ["iss", "aud", "sub", "sid", "jti", "iat", "exp"]

MAX_TOKEN_AGE_SECONDS = 300

# tolerance for minter/verifier clock skew, applied by PyJWT to `exp`, `nbf` and `iat`. The
# 300s ttl already absorbs skew in the past direction; `iat` has no such slack forward, where
# PyJWT >= 2.10 raises ImmatureSignatureError the moment iat > now — and chat-backend and this
# service are separate pods on (soon) separate node pools. Deliberately not applied to the
# MAX_TOKEN_AGE_SECONDS check below, which stays exact.
LEEWAY_SECONDS = 5

# identity recorded for a sandbox execution in endpoint_access. Deliberately distinct from a
# bare email so a sandbox request is never mistaken for a verified human: it carries the
# authenticated user for attribution while staying separable in logs and in any later
# per-credential limit.
PRINCIPAL_PREFIX = "sandbox:"


@dataclass(frozen=True)
class SandboxPrincipal:
    """An authenticated sandbox execution. ``execution_id`` is the token's ``jti``."""

    user: str
    session_id: str
    execution_id: str
    scope: str
    expires_at: int

    @property
    def identity(self) -> str:
        return f"{PRINCIPAL_PREFIX}{self.user}"


class SandboxTokenError(Exception):
    """The bearer was sandbox-shaped and did not validate. Always a hard 401."""


def _b64url_decode(segment: str) -> bytes:
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def is_sandbox_shaped(token: str) -> bool:
    """True when the bearer's JOSE header declares HS256 — the sandbox token's algorithm."""
    if not token or token.count(".") != 2:
        return False
    try:
        header = json.loads(_b64url_decode(token.split(".", 1)[0]))
    except (ValueError, binascii.Error, UnicodeDecodeError):
        return False
    return isinstance(header, dict) and header.get("alg") == ALGORITHM


def verify_sandbox_token(token: str) -> SandboxPrincipal:
    """Validate a sandbox-shaped bearer, or raise :class:`SandboxTokenError`.

    Fails closed when the signing key is unset: no warning-and-continue, because the sandbox
    is the only caller of this service that runs attacker-authored code.
    """
    key = config.sandbox_token_signing_key
    if not key:
        raise SandboxTokenError("SANDBOX_TOKEN_SIGNING_KEY is not set")

    try:
        claims = jwt.decode(
            token,
            key,
            algorithms=[ALGORITHM],
            audience=AUDIENCE,
            issuer=ISSUER,
            options={"require": REQUIRED_CLAIMS},
            leeway=LEEWAY_SECONDS,
        )
    except jwt.InvalidTokenError as exc:
        raise SandboxTokenError(f"{type(exc).__name__}: {exc}") from exc

    scope = claims.get("scope")
    if not scope:
        raise SandboxTokenError("missing scope claim")

    # `options={"require": ...}` rejects only missing/null claims, so an empty string passes it
    # and yields a principal attributing the query to nobody. Attribution is the point of the
    # token, so assert the minter's invariant here rather than trusting it.
    for name in ("sub", "sid", "jti"):
        if not claims.get(name):
            raise SandboxTokenError(f"empty {name} claim")

    # PyJWT treats a list `aud` as membership, so {"aud": ["db-api", "results-api"]} would
    # validate at BOTH services — single-destination binding must be a property of the
    # validator, not merely of the minter
    if not isinstance(claims["aud"], str):
        raise SandboxTokenError("aud must be a single string")

    if int(claims["iat"]) < int(time.time()) - MAX_TOKEN_AGE_SECONDS:
        raise SandboxTokenError("iat too far in the past")

    return SandboxPrincipal(
        user=str(claims["sub"]),
        session_id=str(claims["sid"]),
        execution_id=str(claims["jti"]),
        scope=str(scope),
        expires_at=int(claims["exp"]),
    )


def require_sandbox_config() -> None:
    """Refuse to start mis-configured while the sandbox is deployed.

    ``SANDBOX_ENABLED`` tracks the sandbox Deployment, not the signing key. Every rule above
    fires on "a sandbox-shaped bearer", and nothing obliges the sandbox to send one — with
    ``INTERNAL_API_SECRET`` unset, ``is_internal_caller`` returns False and a script could
    still be served as an anonymous caller on the public endpoints. Both secrets are
    mandatory once the sandbox exists.
    """
    if not config.sandbox_enabled:
        return
    missing = [
        name
        for name, value in (
            ("INTERNAL_API_SECRET", config.internal_api_secret),
            ("SANDBOX_TOKEN_SIGNING_KEY", config.sandbox_token_signing_key),
        )
        if not value
    ]
    if missing:
        logger.error(
            "SANDBOX_ENABLED is true but %s unset: refusing to start fail-open while the "
            "sandbox can reach this service",
            " and ".join(missing),
        )
        sys.exit(1)
