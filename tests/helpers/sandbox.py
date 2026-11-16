"""What the sandbox-credential tests share that is results-api's own."""

import functools

from sandbox_contract import SIGNING_KEY, mint_token

import app.config.common as config

INTERNAL_SECRET = "test-internal-secret"

mint = functools.partial(mint_token, audience="results-api")


def configure_credentials(monkeypatch) -> None:
    """Give the app the shared secret, the signing key and the one allowed domain `mint` uses."""
    monkeypatch.setattr(config, "internal_api_secret", INTERNAL_SECRET)
    monkeypatch.setattr(config, "sandbox_token_signing_key", SIGNING_KEY)
    monkeypatch.setattr(config, "allowed_email_domains", {"finngen.fi"})
    monkeypatch.setattr(config, "allowed_emails", set())
