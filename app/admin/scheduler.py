"""Enhanced scheduler for automated publishing based on mapping cron schedules."""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from app.admin import db
from app.admin.queue_worker import enqueue_upload_item
from app.channel_copier import process_source_channel_uploads
from app.config import settings
from app.supabase_client import get_bot_settings, set_bot_settings
from app.utils import extract_video_info, validate_youtube_url

logger = logging.getLogger(__name__)

_scheduler_thread: threading.Thread | None = None
_shutdown_event = threading.Event()
SLEEP_INTERVAL = 60  # seconds


def _fetch_source_videos(source_url: str, content_filter: str, fetch_limit: int) -> List[Dict[str, Any]]:
    """Fetch recent videos from a source channel URL."""
    try:
        # Reuse existing channel copier logic or yt-dlp to list videos
        from yt_dlp import YoutubeDL
        ydl_opts = {
            "quiet": True,
            "skip_download": True,
            "extract_flat": True,
            "playlistend": fetch_limit,
        }
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(source_url, download=False)
        entries = info.get("entries") if info else None
        if not entries:
            return []
        videos = []
        for entry in entries:
            if not entry:
                continue
            vid = entry.get("id")
            if not vid:
                continue
            videos.append({
                "source_video_id": vid,
                "url": f"https://www.youtube.com/shorts/{vid}" if content_filter == "shorts" else f"https://www.youtube.com/watch?v={vid}",
                "title": entry.get("title", "Untitled"),
            })
        return videos
    except Exception as exc:
        logger.error(f"Failed to fetch source videos from {source_url}: {exc}")
        return []


def _process_mapping(mapping: Dict[str, Any]) -> None:
    """Evaluate a single mapping and enqueue uploads if scheduled."""
    mapping_id = mapping["id"]
    source_id = mapping.get("source_channel_id")
    yt_id = mapping.get("youtube_channel_id")
    if not source_id or not yt_id:
        return

    source = db.get_source_channel(source_id)
    yt = db.get_youtube_channel(yt_id)
    if not source or not yt:
        return
    if not source.get("is_active") or not yt.get("is_active"):
        return
    if yt.get("status") != "connected":
        return

    # Check channel quota
    limit = yt.get("daily_quota_limit", 6) or 6
    used = yt.get("uploads_today", 0) or 0
    reset_at = yt.get("quota_reset_at")
    now = datetime.now(timezone.utc)
    if reset_at:
        try:
            reset_dt = datetime.fromisoformat(str(reset_at).replace("Z", "+00:00"))
            if reset_dt.date() < now.date():
                used = 0
        except Exception:
            pass
    remaining_quota = max(limit - used, 0)
    max_per_run = mapping.get("max_per_run", 1) or 1
    allowed = min(remaining_quota, max_per_run)
    if allowed <= 0:
        logger.debug(f"Mapping {mapping_id} skipped: quota exhausted")
        return

    # Fetch source videos
    videos = _fetch_source_videos(
        source.get("source_url", ""),
        source.get("content_filter", "shorts"),
        source.get("fetch_limit", 50),
    )

    enqueued = 0
    for video in videos:
        if enqueued >= allowed:
            break
        vid = video["source_video_id"]
        if db.is_source_video_seen(source_id, vid):
            continue

        # Check dedup in queue
        existing = db.list_upload_queue(
            source_channel_id=source_id,
            youtube_channel_id=yt_id,
            limit=1000,
        )
        already_queued = any(
            e.get("source_video_id") == vid and e.get("status") not in ("failed", "cancelled")
            for e in existing
        )
        if already_queued:
            db.mark_source_video_seen(source_id, vid)
            continue

        # Build title/description from templates
        title_template = mapping.get("title_template") or "{title}"
        desc_template = mapping.get("description_template") or ""
        title = title_template.replace("{title}", video["title"])
        description = desc_template.replace("{title}", video["title"])

        queue_item = db.enqueue_upload({
            "mapping_id": mapping_id,
            "source_channel_id": source_id,
            "youtube_channel_id": yt_id,
            "source_video_id": vid,
            "video_url": video["url"],
            "title": title,
            "description": description,
            "visibility": mapping.get("visibility", "public"),
            "status": "pending",
            "scheduled_at": datetime.now(timezone.utc).isoformat(),
            "max_attempts": 5,
        })
        enqueue_upload_item(queue_item["id"])
        db.mark_source_video_seen(source_id, vid)
        enqueued += 1

    if enqueued > 0:
        db.update_mapping(mapping_id, {"last_run_at": datetime.now(timezone.utc).isoformat()})
        logger.info(f"Mapping {mapping_id}: enqueued {enqueued} videos")


def _scheduler_loop() -> None:
    """Main scheduler loop: runs every minute, processes active mappings."""
    logger.info("Admin scheduler loop started")
    while not _shutdown_event.is_set():
        try:
            # Get all active mappings
            mappings = db.list_mappings(is_active=True, limit=1000)
            for mapping in mappings:
                try:
                    cron = mapping.get("schedule_cron")
                    if cron:
                        # Simple cron evaluation: if cron is a time string like "HH:MM", check current time
                        # For full cron support, consider python-crontab. Here we support simple time list.
                        now = datetime.now(timezone.utc)
                        tz_str = mapping.get("schedule_timezone", "UTC")
                        if tz_str != "UTC":
                            import pytz
                            try:
                                tz = pytz.timezone(tz_str)
                                now = now.astimezone(tz)
                            except Exception:
                                pass
                        # parse comma-separated HH:MM
                        times = [t.strip() for t in cron.split(",") if t.strip()]
                        should_run = False
                        for t in times:
                            try:
                                h, m = t.split(":")
                                if now.hour == int(h) and now.minute == int(m):
                                    should_run = True
                                    break
                            except Exception:
                                continue
                        if not should_run:
                            continue
                    _process_mapping(mapping)
                except Exception as exc:
                    logger.error(f"Scheduler failed for mapping {mapping.get('id')}: {exc}", exc_info=True)
        except Exception as exc:
            logger.error(f"Scheduler loop error: {exc}", exc_info=True)

        # Sleep with shutdown awareness
        _shutdown_event.wait(SLEEP_INTERVAL)


def start_admin_scheduler() -> None:
    """Start the admin mapping-based scheduler."""
    global _scheduler_thread
    if _scheduler_thread and _scheduler_thread.is_alive():
        logger.info("Admin scheduler already running")
        return
    _shutdown_event.clear()
    _scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True, name="admin-scheduler")
    _scheduler_thread.start()
    logger.info("Admin scheduler thread started")


def stop_admin_scheduler() -> None:
    _shutdown_event.set()
    logger.info("Admin scheduler shutdown signaled")
