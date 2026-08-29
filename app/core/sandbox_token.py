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

# Shortest SANDBOX_TOKEN_SIGNING_KEY `require_sandbox_config` will start with, measured on the
# stripped value but NEVER applied to it — see the gate for why nothing may normalise this key.
# 32 is not a feel: RFC 7518 §3.2 requires an HS256 key at least as long as the hash output, and
# PyJWT 2.12 warns `InsecureKeyLengthWarning: The HMAC key is N bytes long, which is below the
# minimum recommended length of 32 bytes for SHA256` under it — so this is the threshold the
# crypto library already complains about, moved from a warning nobody reads to a startup refusal.
# Every generator the suite ships clears it with room to spare: `openssl rand -base64 32` in
# scripts/create-secrets.sh is 44 chars and `secrets.token_urlsafe(32)` in scripts/dev-stack.sh
# is 43, so the gate rejects nothing a correct install produces. Kept byte-identical to db-api's
# `api/sandbox_auth.py` constant: the two verifiers share one deployed key, so a threshold that
# differed would let a key start one service and not the other.
MIN_SIGNING_KEY_BYTES = 32


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

    Two failures, both fatal: a missing secret, and a ``SANDBOX_TOKEN_SIGNING_KEY`` too short to
    be a real HS256 key (see ``MIN_SIGNING_KEY_BYTES``).

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

    # A truthy key is not a usable key: "   ", "\n", "x" and "0" all passed the check above and
    # became guessable HMAC keys that mint valid sandbox principals (the `kubectl create secret
    # --from-file` of a near-empty file). Length is checked on the STRIPPED value and the
    # stripped value is then thrown away, deliberately: chat-backend MINTS with its own copy of
    # the secret and this service VERIFIES with the exact configured bytes, so normalising here
    # would 401 every legitimate token whenever the deployed key carries a trailing newline.
    # `surrogateescape` because a non-UTF-8 secret reaches os.environ as surrogates, and a bare
    # .encode() would kill startup with a traceback instead of this message.
    key = config.sandbox_token_signing_key
    stripped = key.strip()
    if len(stripped.encode("utf-8", "surrogateescape")) < MIN_SIGNING_KEY_BYTES:
        logger.error(
            "SANDBOX_TOKEN_SIGNING_KEY is %d bytes (ignoring surrounding whitespace), below the "
            "%d-byte minimum for HS256 (RFC 7518 §3.2): refusing to start with a guessable "
            "signing key while the sandbox can reach this service. Generate one with "
            "`openssl rand -base64 32`.",
            len(stripped.encode("utf-8", "surrogateescape")),
            MIN_SIGNING_KEY_BYTES,
        )
        sys.exit(1)

    # Not fatal, because the value is load-bearing exactly as deployed and both sides may well
    # carry the same newline — but a secret whose whitespace is part of the key is something an
    # operator should see rather than discover from a 401 after rotating it through a different
    # path (genetics-results-suite-4h6.36).
    if key != stripped:
        logger.warning(
            "SANDBOX_TOKEN_SIGNING_KEY has leading or trailing whitespace, which is PART OF THE "
            "KEY here and at the minter: chat-backend must hold the byte-identical value or "
            "every sandbox token will 401. Likely `kubectl create secret --from-file` of a file "
            "with a trailing newline."
        )
