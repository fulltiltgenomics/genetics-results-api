"""Authentication via X-Goog-Authenticated-User-Email header (set by IAP or oauth2-proxy).

Authorization is optionally checked against Google Workspace groups and a whitelist
loaded from the authentication_file when config.authorization is True.
"""

import json
import logging
import threading
from collections import defaultdict
from typing import Any, Dict

from fastapi import HTTPException, Request

from app.config.common import authorization, authorization_file

logger = logging.getLogger(__name__)

if authorization:
    with open(authorization_file) as f:
        auth_json = json.load(f)

    GROUP_NAMES = auth_json["group_auth"]["GROUPS"]
    SERVICE_ACCOUNT_FILE = auth_json["group_auth"]["SERVICE_ACCOUNT_FILE"]
    DELEGATED_ACCOUNT = auth_json["group_auth"]["DELEGATED_ACCOUNT"]
    WHITELIST = auth_json["login"].get("WHITELIST", [])

    SERVICE_ACCOUNT_SCOPES = [
        "https://www.googleapis.com/auth/admin.directory.group.readonly",
        "https://www.googleapis.com/auth/admin.directory.user.readonly",
        "https://www.googleapis.com/auth/admin.directory.group.member.readonly",
    ]

    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SERVICE_ACCOUNT_SCOPES
    )
    delegated_creds = creds.with_subject(DELEGATED_ACCOUNT)
    services: Dict[int, Any] = defaultdict(
        lambda: build("admin", "directory_v1", credentials=delegated_creds)
    )


def get_authenticated_user(request: Request) -> str | None:
    """Extract authenticated user email from the IAP/oauth2-proxy header."""
    iap_email = request.headers.get("X-Goog-Authenticated-User-Email")
    if not iap_email:
        return None
    # header format: "accounts.google.com:user@domain.com"
    return iap_email.split(":")[-1] if ":" in iap_email else iap_email


def verify_membership(username: str) -> bool:
    """Check if user is in the whitelist or a member of an authorized Google group."""
    if not authorization or username in WHITELIST:
        return True
    for name in GROUP_NAMES:
        r = (
            services[threading.get_ident()]
            .members()
            .hasMember(groupKey=name, memberKey=username)
            .execute()
        )
        if r["isMember"] is True:
            return True
    return False


def get_verified_user(request: Request) -> str | None:
    """Get authenticated user and verify group membership if authorization is enabled."""
    email = get_authenticated_user(request)
    if email is None:
        return None
    if not verify_membership(email):
        raise HTTPException(status_code=403, detail="Unauthorized")
    return email
