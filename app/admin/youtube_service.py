"""YouTube service with OAuth token refresh, quota tracking, and upload orchestration."""
from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

from app.admin import db
from app.utils import download_video, encrypt_data, remove_file, validate_youtube_url
from app.youtube_client import CLIENT_CONFIG, SCOPES, build_youtube_service

logger = logging.getLogger(__name__)

TEMP_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tmp")


# ---------------------------------------------------------------------------
# Token refresh
# ---------------------------------------------------------------------------

def refresh_credentials_if_needed(yt_id: int, credential_data: Dict[str, Any]) -> Dict[str, Any]:
    """Refresh OAuth token if expired and persist to DB. Returns fresh credentials."""
    credentials = Credentials(
        token=credential_data.get("token"),
        refresh_token=credential_data.get("refresh_token"),
        token_uri=credential_data.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=credential_data.get("client_id"),
        client_secret=credential_data.get("client_secret"),
        scopes=credential_data.get("scopes", SCOPES),
    )

    if not credentials.valid:
        if credentials.expired and credentials.refresh_token:
            logger.info(f"Refreshing OAuth token for YouTube channel {yt_id}")
            try:
                credentials.refresh(GoogleRequest())
                refreshed = {
                    "token": credentials.token,
                    "refresh_token": credentials.refresh_token,
                    "token_uri": credentials.token_uri,
                    "client_id": credentials.client_id,
                    "client_secret": credentials.client_secret,
                    "scopes": list(credentials.scopes) if credentials.scopes else SCOPES,
                }
                db.update_channel_credentials(yt_id, refreshed)
                return refreshed
            except Exception as exc:
                logger.error(f"Token refresh failed for channel {yt_id}: {exc}")
                db.update_youtube_channel(yt_id, {"status": "expired"})
                raise
        else:
            db.update_youtube_channel(yt_id, {"status": "expired"})
            raise RuntimeError("Credentials expired and no refresh token available")
    return credential_data


def get_valid_youtube_service(yt_id: int) -> Any:
    """Build a YouTube API service with valid (possibly refreshed) credentials."""
    creds = db.get_channel_credentials(yt_id)
    if not creds:
        raise RuntimeError(f"No credentials found for YouTube channel {yt_id}")
    fresh = refresh_credentials_if_needed(yt_id, creds)
    return build_youtube_service(fresh)


# ---------------------------------------------------------------------------
# Quota / rate-limit helpers
# ---------------------------------------------------------------------------

def check_channel_quota(yt_id: int) -> bool:
    channel = db.get_youtube_channel(yt_id)
    if not channel:
        return False
    if channel.get("status") != "connected":
        return False
    limit = channel.get("daily_quota_limit", 6) or 6
    used = channel.get("uploads_today", 0) or 0
    reset_at = channel.get("quota_reset_at")
    now = datetime.now(timezone.utc)
    if reset_at:
        try:
            reset_dt = datetime.fromisoformat(str(reset_at).replace("Z", "+00:00"))
            if reset_dt.date() < now.date():
                used = 0
        except Exception:
            pass
    return used < limit


def record_upload_attempt(yt_id: int) -> None:
    db.increment_uploads_today(yt_id)


# ---------------------------------------------------------------------------
# Upload orchestration
# ---------------------------------------------------------------------------

def upload_video_to_channel(
    yt_id: int,
    filepath: str,
    title: str,
    description: str,
    visibility: str,
    on_progress: Optional[Callable[[int], None]] = None,
) -> Dict[str, Any]:
    """Upload a single video to a specific YouTube channel with quota checks and logging."""
    if not check_channel_quota(yt_id):
        raise RuntimeError(f"Daily quota exceeded for YouTube channel {yt_id}")

    youtube = get_valid_youtube_service(yt_id)
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
    guessed_mime, _ = __import__("mimetypes").guess_type(filepath)
    media = MediaFileUpload(
        filepath,
        chunksize=-1,
        resumable=True,
        mimetype=guessed_mime or "application/octet-stream",
    )
    request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
    response = None
    last_progress = 0
    while response is None:
        status, response = request.next_chunk()
        if status and on_progress:
            pct = int(status.progress() * 100)
            if pct > last_progress:
                on_progress(pct)
                last_progress = pct

    video_id = response.get("id")
    result_url = f"https://youtu.be/{video_id}" if video_id else None
    record_upload_attempt(yt_id)
    return {
        "video_id": video_id,
        "result_url": result_url,
        "raw_response": response,
    }


# ---------------------------------------------------------------------------
# Bulk / queue processing
# ---------------------------------------------------------------------------

def process_upload_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single upload_queue item end-to-end. Returns result dict."""
    item_id = item["id"]
    yt_id = item.get("youtube_channel_id")
    if not yt_id:
        raise ValueError("Missing youtube_channel_id in upload item")

    video_url = item["video_url"]
    if not validate_youtube_url(video_url):
        raise ValueError(f"Invalid YouTube URL: {video_url}")

    title = item.get("title") or "Untitled video"
    description = item.get("description") or ""
    visibility = item.get("visibility", "unlisted")

    db.create_upload_log({
        "queue_id": item_id,
        "level": "info",
        "event": "download_started",
        "message": f"Downloading {video_url}",
        "context": {"youtube_channel_id": yt_id},
    })

    filepath = download_video(video_url, TEMP_DIR)
    try:
        db.update_upload_item(item_id, {"status": "uploading", "started_at": datetime.now(timezone.utc).isoformat()})
        db.create_upload_log({
            "queue_id": item_id,
            "level": "info",
            "event": "upload_started",
            "message": f"Uploading to channel {yt_id}",
            "context": {"filepath": filepath},
        })

        result = upload_video_to_channel(
            yt_id=yt_id,
            filepath=filepath,
            title=title,
            description=description,
            visibility=visibility,
        )

        db.update_upload_item(item_id, {
            "status": "done",
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "result_url": result["result_url"],
            "result_video_id": result["video_id"],
        })
        db.create_upload_log({
            "queue_id": item_id,
            "level": "info",
            "event": "upload_done",
            "message": f"Uploaded: {result['result_url']}",
            "context": {"video_id": result["video_id"]},
        })
        return {"success": True, "result_url": result["result_url"], "video_id": result["video_id"]}

    except HttpError as exc:
        error_details = exc.resp.get("content", "{}").decode() if hasattr(exc.resp, "get") else str(exc)
        db.update_upload_item(item_id, {
            "status": "failed",
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "error_message": error_details,
        })
        db.create_upload_log({
            "queue_id": item_id,
            "level": "error",
            "event": "upload_failed",
            "message": str(exc),
            "context": {"http_status": exc.resp.status if hasattr(exc.resp, "status") else None},
        })
        raise
    except Exception as exc:
        db.update_upload_item(item_id, {
            "status": "failed",
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "error_message": str(exc),
        })
        db.create_upload_log({
            "queue_id": item_id,
            "level": "error",
            "event": "upload_failed",
            "message": str(exc),
            "context": {},
        })
        raise
    finally:
        if filepath:
            remove_file(filepath)
