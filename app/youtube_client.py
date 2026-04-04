import json
from typing import Dict

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from app.config import settings
from app.utils import create_oauth_state, decrypt_data, encrypt_data

SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.readonly",
]

CLIENT_CONFIG = {
    "web": {
        "client_id": settings.google_client_id,
        "client_secret": settings.google_client_secret,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "redirect_uris": [str(settings.oauth_redirect_uri)],
    }
}


def create_oauth_url(telegram_id: int) -> str:
    flow = Flow.from_client_config(
        CLIENT_CONFIG,
        scopes=SCOPES,
        redirect_uri=str(settings.oauth_redirect_uri),
    )
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
        state=create_oauth_state(telegram_id),
    )
    return auth_url


def fetch_credentials(authorization_response: str) -> dict:
    flow = Flow.from_client_config(
        CLIENT_CONFIG,
        scopes=SCOPES,
        redirect_uri=str(settings.oauth_redirect_uri),
    )
    flow.fetch_token(authorization_response=authorization_response)
    credentials = flow.credentials
    return {
        "token": credentials.token,
        "refresh_token": credentials.refresh_token,
        "token_uri": credentials.token_uri,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "scopes": credentials.scopes,
    }


def build_youtube_service(credential_data: dict):
    credentials = Credentials(
        token=credential_data["token"],
        refresh_token=credential_data.get("refresh_token"),
        token_uri=credential_data["token_uri"],
        client_id=credential_data["client_id"],
        client_secret=credential_data["client_secret"],
        scopes=credential_data.get("scopes", SCOPES),
    )
    return build("youtube", "v3", credentials=credentials, cache_discovery=False)


def upload_video(youtube, filepath: str, title: str, description: str, visibility: str, on_progress=None) -> dict:
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "categoryId": "22",
        },
        "status": {
            "privacyStatus": visibility,
        },
    }
    media = MediaFileUpload(filepath, chunksize=-1, resumable=True, mimetype="video/mp4")
    request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
    response = None
    while response is None:
        status, response = request.next_chunk()
        if status and on_progress:
            on_progress(int(status.progress() * 100))
    return response


def serialize_credentials(credentials: dict) -> str:
    return encrypt_data(credentials)


def deserialize_credentials(encrypted_data: str) -> dict:
    return decrypt_data(encrypted_data)
