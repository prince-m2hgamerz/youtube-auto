import json
import logging
from typing import List, Optional

from yt_dlp import YoutubeDL

from app.config import settings
from app.supabase_client import (
    create_job,
    get_source_channel_url,
    set_source_channel_url,
    get_uploaded_shorts_ids,
    add_uploaded_short_id,
    get_bot_settings,
)
from app.utils import extract_video_info, _apply_youtube_auth_options

logger = logging.getLogger(__name__)

ADMIN_IDS = {part.strip() for part in (settings.admin_ids or "").split(",") if part.strip()}


def fetch_shorts_from_channel(channel_url: str, limit: int = 50) -> List[dict]:
    shorts_url = channel_url.rstrip("/") + "/shorts"
    options = {
        "quiet": True,
        "extract_flat": True,
        "playlistend": limit,
        "skip_download": True,
        "ignoreerrors": True,
    }
    _apply_youtube_auth_options(options)

    try:
        with YoutubeDL(options) as ydl:
            info = ydl.extract_info(shorts_url, download=False)
            if not info:
                return []
            entries = info.get("entries") or []
            results = []
            for entry in entries:
                if not entry:
                    continue
                vid = entry.get("id")
                if vid:
                    results.append({
                        "id": vid,
                        "url": f"https://www.youtube.com/shorts/{vid}",
                        "title": entry.get("title", "Untitled"),
                    })
            return results
    except Exception as exc:
        logger.error(f"Failed to fetch shorts: {exc}", exc_info=True)
        return []


def process_source_channel_uploads() -> dict:
    result = {"status": "unknown", "message": "", "job_id": None}
    channel_url = get_source_channel_url()
    if not channel_url:
        result["status"] = "no_source"
        result["message"] = "No source channel configured. Use /setsource <channel_url> first."
        logger.info("No source channel configured, skipping scheduled upload")
        return result

    logger.info(f"Processing source channel: {channel_url}")
    shorts = fetch_shorts_from_channel(channel_url, limit=50)
    if not shorts:
        result["status"] = "no_shorts"
        result["message"] = "No shorts found on the source channel."
        logger.info("No shorts found on source channel")
        return result

    uploaded_ids = get_uploaded_shorts_ids()
    admin_id = next(iter(ADMIN_IDS), None)
    if not admin_id:
        result["status"] = "error"
        result["message"] = "No admin ID configured in bot settings."
        logger.error("No admin ID configured")
        return result

    new_short = None
    for short in shorts:
        if short["id"] not in uploaded_ids:
            new_short = short
            break

    if not new_short:
        result["status"] = "all_uploaded"
        result["message"] = f"All {len(shorts)} fetched shorts have already been uploaded."
        logger.info("All fetched shorts already uploaded")
        return result

    logger.info(f"Queueing short: {new_short['id']} - {new_short['title']}")
    try:
        from app.job_worker import enqueue_job
        info = extract_video_info(new_short["url"])
        vis = get_bot_settings().get("auto_upload_visibility", "public")
        if vis not in {"public", "unlisted", "private"}:
            vis = "public"
        job = create_job({
            "telegram_id": admin_id,
            "video_url": new_short["url"],
            "title": info.get("title", new_short["title"]),
            "description": info.get("description", ""),
            "visibility": vis,
            "status": "pending",
        })
        enqueue_job(job["id"])
        add_uploaded_short_id(new_short["id"])
        logger.info(f"Job created: {job['id']}")
        result["status"] = "queued"
        result["message"] = f"✅ Queued short: {new_short['title'][:50]}\n🆔 Job ID: {job['id']}"
        result["job_id"] = job["id"]
    except Exception as exc:
        logger.error(f"Failed to create job for short {new_short['id']}: {exc}", exc_info=True)
        result["status"] = "error"
        result["message"] = f"Failed to queue short: {exc}"
    return result
