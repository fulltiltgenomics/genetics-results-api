from typing import Optional
from fastapi import HTTPException, Request
import json
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
import threading
from collections import defaultdict
from typing import Any, Dict

try:
    import importlib.util
    spec = importlib.util.spec_from_file_location("config", "app/config/config.py")
    _conf_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(_conf_module)
    config = {
        key: getattr(_conf_module, key)
        for key in dir(_conf_module)
        if not key.startswith("_")
    }
except Exception:
    raise Exception("Could not load config from ./app/config/config.py")

if config["authentication"]:
    with open(config["authentication_file"]) as f:
        auth_json = json.load(f)
    
    GROUP_NAMES = auth_json["group_auth"]["GROUPS"]
    SERVICE_ACCOUNT_FILE = auth_json["group_auth"]["SERVICE_ACCOUNT_FILE"]
    DELEGATED_ACCOUNT = auth_json["group_auth"]["DELEGATED_ACCOUNT"]
    WHITELIST = auth_json["login"].get("WHITELIST", [])
    
    GOOGLE_CLIENT_ID = auth_json["login"]["GOOGLE_LOGIN_CLIENT_ID"]
    GOOGLE_CLIENT_SECRET = auth_json["login"]["GOOGLE_LOGIN_CLIENT_SECRET"]
    REDIRECT_URI = auth_json["login"]["REDIRECT_URI"]

    SERVICE_ACCOUNT_SCOPES = [
        "https://www.googleapis.com/auth/admin.directory.group.readonly",
        "https://www.googleapis.com/auth/admin.directory.user.readonly",
        "https://www.googleapis.com/auth/admin.directory.group.member.readonly",
    ]

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SERVICE_ACCOUNT_SCOPES
    )
    delegated_creds = creds.with_subject(DELEGATED_ACCOUNT)
    services: Dict[int, Any] = defaultdict(
        lambda: build("admin", "directory_v1", credentials=delegated_creds)
    )

class GoogleAuth:
    def __init__(self):
        self.client_id = GOOGLE_CLIENT_ID
        self.client_secret = GOOGLE_CLIENT_SECRET
        google_params = self._get_google_info()
        self.authorize_url = google_params.get("authorization_endpoint")
        self.token_url = google_params.get("token_endpoint")
        self.userinfo_url = google_params.get("userinfo_endpoint")

    def _get_google_info(self) -> dict:
        r = requests.get("https://accounts.google.com/.well-known/openid-configuration")
        r.raise_for_status()
        return r.json()

    def get_authorization_url(self, redirect_uri: str) -> str:
        params = {
            "client_id": self.client_id,
            "response_type": "code",
            "scope": "email",
            "redirect_uri": redirect_uri,
            "prompt": "select_account",
        }
        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"{self.authorize_url}?{query}"

    async def get_user_info(self, code: str, redirect_uri: str) -> dict:
        token_data = {
            "code": code,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }
        token_response = requests.post(self.token_url, data=token_data)
        token_response.raise_for_status()
        access_token = token_response.json()["access_token"]

        headers = {"Authorization": f"Bearer {access_token}"}
        userinfo_response = requests.get(self.userinfo_url, headers=headers)
        userinfo_response.raise_for_status()
        return userinfo_response.json()

def verify_membership(username: str) -> bool:
    if not config["authentication"] or username in WHITELIST:
        return True
    else:
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

async def get_current_user(request: Request) -> Optional[str]:
    if not config["authentication"]:
        return None
    email = request.session.get("user_email")
    if not email:
        return None
    if not verify_membership(email):
        raise HTTPException(status_code=403, detail="Unauthorized")
    return email

google_auth = GoogleAuth() 