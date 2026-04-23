"""Admin system database service layer (Supabase REST wrapper)."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.supabase_client import _request, _request_with_missing_column_retry
from app.utils import decrypt_data, encrypt_data

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _headers(prefer: str = "return=representation") -> Dict[str, str]:
    return {"Prefer": prefer}


# ---------------------------------------------------------------------------
# 1. admin_users
# ---------------------------------------------------------------------------

def create_admin_user(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = _request_with_missing_column_retry(
        "POST", "admin_users", json_body=[payload], extra_headers=_headers()
    )
    return data[0] if data else {}


def get_admin_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    params = {"email": f"eq.{email}", "select": "*"}
    data = _request("GET", "admin_users", params=params)
    return data[0] if data else None


def get_admin_user_by_id(admin_id: int) -> Optional[Dict[str, Any]]:
    params = {"id": f"eq.{admin_id}", "select": "*"}
    data = _request("GET", "admin_users", params=params)
    return data[0] if data else None


def list_admin_users(limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    params = {"select": "*", "order": "created_at.desc", "limit": str(limit), "offset": str(offset)}
    return _request("GET", "admin_users", params=params) or []


def update_admin_user(admin_id: int, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    params = {"id": f"eq.{admin_id}"}
    data = _request_with_missing_column_retry(
        "PATCH", "admin_users", params=params, json_body=payload, extra_headers=_headers()
    )
    return data[0] if data else None


def delete_admin_user(admin_id: int) -> bool:
    try:
        _request("DELETE", "admin_users", params={"id": f"eq.{admin_id}"})
        return True
    except Exception as exc:
        logger.warning(f"Failed to delete admin user {admin_id}: {exc}")
        return False


# ---------------------------------------------------------------------------
# 2. source_channels
# ---------------------------------------------------------------------------

def create_source_channel(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = _request_with_missing_column_retry(
        "POST", "source_channels", json_body=[payload], extra_headers=_headers()
    )
    return data[0] if data else {}


def get_source_channel(source_id: int) -> Optional[Dict[str, Any]]:
    params = {"id": f"eq.{source_id}", "select": "*"}
    data = _request("GET", "source_channels", params=params)
    return data[0] if data else None


def list_source_channels(is_active: Optional[bool] = None, limit: int = 200, offset: int = 0) -> List[Dict[str, Any]]:
    params: Dict[str, str] = {"select": "*", "order": "created_at.desc", "limit": str(limit), "offset": str(offset)}
    if is_active is not None:
        params["is_active"] = f"eq.{str(is_active).lower()}"
    return _request("GET", "source_channels", params=params) or []


def update_source_channel(source_id: int, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    params = {"id": f"eq.{source_id}"}
    data = _request_with_missing_column_retry(
        "PATCH", "source_channels", params=params, json_body=payload, extra_headers=_headers()
    )
    return data[0] if data else None


def delete_source_channel(source_id: int) -> bool:
    try:
        _request("DELETE", "source_channels", params={"id": f"eq.{source_id}"})
        return True
    except Exception as exc:
        logger.warning(f"Failed to delete source channel {source_id}: {exc}")
        return False


# ---------------------------------------------------------------------------
# 3. youtube_channels
# ---------------------------------------------------------------------------

def create_youtube_channel(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = _request_with_missing_column_retry(
        "POST", "youtube_channels", json_body=[payload], extra_headers=_headers()
    )
    return data[0] if data else {}


def get_youtube_channel(yt_id: int) -> Optional[Dict[str, Any]]:
    params = {"id": f"eq.{yt_id}", "select": "*"}
    data = _request("GET", "youtube_channels", params=params)
    return data[0] if data else None


def get_youtube_channel_by_youtube_id(youtube_channel_id: str) -> Optional[Dict[str, Any]]:
    params = {"youtube_channel_id": f"eq.{youtube_channel_id}", "select": "*"}
    data = _request("GET", "youtube_channels", params=params)
    return data[0] if data else None


def list_youtube_channels(status: Optional[str] = None, limit: int = 200, offset: int = 0) -> List[Dict[str, Any]]:
    params: Dict[str, str] = {"select": "*", "order": "created_at.desc", "limit": str(limit), "offset": str(offset)}
    if status:
        params["status"] = f"eq.{status}"
    return _request("GET", "youtube_channels", params=params) or []


def update_youtube_channel(yt_id: int, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    params = {"id": f"eq.{yt_id}"}
    data = _request_with_missing_column_retry(
        "PATCH", "youtube_channels", params=params, json_body=payload, extra_headers=_headers()
    )
    return data[0] if data else None


def delete_youtube_channel(yt_id: int) -> bool:
    try:
        _request("DELETE", "youtube_channels", params={"id": f"eq.{yt_id}"})
        return True
    except Exception as exc:
        logger.warning(f"Failed to delete youtube channel {yt_id}: {exc}")
        return False


def increment_uploads_today(yt_id: int) -> bool:
    channel = get_youtube_channel(yt_id)
    if not channel:
        return False
    current = channel.get("uploads_today", 0) or 0
    reset_at = channel.get("quota_reset_at")
    now = datetime.now(timezone.utc)
    if reset_at:
        try:
            reset_dt = datetime.fromisoformat(str(reset_at).replace("Z", "+00:00"))
            if reset_dt.date() < now.date():
                current = 0
        except Exception:
            pass
    return bool(update_youtube_channel(yt_id, {
        "uploads_today": current + 1,
        "quota_reset_at": now.isoformat(),
        "last_used_at": now.isoformat(),
    }))


def reset_daily_quotas() -> None:
    """Reset uploads_today for all channels at midnight UTC."""
    try:
        channels = list_youtube_channels()
        now = datetime.now(timezone.utc).isoformat()
        for ch in channels:
            ch_id = ch.get("id")
            if ch_id:
                update_youtube_channel(ch_id, {"uploads_today": 0, "quota_reset_at": now})
    except Exception as exc:
        logger.error(f"Failed to reset daily quotas: {exc}")


def get_channel_credentials(yt_id: int) -> Optional[Dict[str, Any]]:
    channel = get_youtube_channel(yt_id)
    if not channel or not channel.get("oauth_credentials"):
        return None
    try:
        return decrypt_data(channel["oauth_credentials"])
    except Exception as exc:
        logger.error(f"Failed to decrypt credentials for channel {yt_id}: {exc}")
        return None


def update_channel_credentials(yt_id: int, credentials: Dict[str, Any]) -> bool:
    try:
        encrypted = encrypt_data(credentials)
        update_youtube_channel(yt_id, {"oauth_credentials": encrypted})
        return True
    except Exception as exc:
        logger.error(f"Failed to encrypt/update credentials for channel {yt_id}: {exc}")
        return False


# ---------------------------------------------------------------------------
# 4. channel_mappings (many-to-many)
# ---------------------------------------------------------------------------

def create_mapping(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = _request_with_missing_column_retry(
        "POST", "channel_mappings", json_body=[payload], extra_headers=_headers()
    )
    return data[0] if data else {}


def get_mapping(mapping_id: int) -> Optional[Dict[str, Any]]:
    params = {"id": f"eq.{mapping_id}", "select": "*"}
    data = _request("GET", "channel_mappings", params=params)
    return data[0] if data else None


def list_mappings(
    source_channel_id: Optional[int] = None,
    youtube_channel_id: Optional[int] = None,
    is_active: Optional[bool] = None,
    limit: int = 500,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    params: Dict[str, str] = {"select": "*", "order": "priority.asc,created_at.desc", "limit": str(limit), "offset": str(offset)}
    if source_channel_id is not None:
        params["source_channel_id"] = f"eq.{source_channel_id}"
    if youtube_channel_id is not None:
        params["youtube_channel_id"] = f"eq.{youtube_channel_id}"
    if is_active is not None:
        params["is_active"] = f"eq.{str(is_active).lower()}"
    return _request("GET", "channel_mappings", params=params) or []


def update_mapping(mapping_id: int, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    params = {"id": f"eq.{mapping_id}"}
    data = _request_with_missing_column_retry(
        "PATCH", "channel_mappings", params=params, json_body=payload, extra_headers=_headers()
    )
    return data[0] if data else None


def delete_mapping(mapping_id: int) -> bool:
    try:
        _request("DELETE", "channel_mappings", params={"id": f"eq.{mapping_id}"})
        return True
    except Exception as exc:
        logger.warning(f"Failed to delete mapping {mapping_id}: {exc}")
        return False


def get_mappings_with_channels(source_channel_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """Return mappings joined with source_channel and youtube_channel."""
    params: Dict[str, str] = {
        "select": "*,source_channels(*),youtube_channels(*)",
        "order": "priority.asc,created_at.desc",
        "limit": "500",
    }
    if source_channel_id is not None:
        params["source_channel_id"] = f"eq.{source_channel_id}"
    return _request("GET", "channel_mappings", params=params) or []


# ---------------------------------------------------------------------------
# 5. upload_queue
# ---------------------------------------------------------------------------

def enqueue_upload(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = _request_with_missing_column_retry(
        "POST", "upload_queue", json_body=[payload], extra_headers=_headers()
    )
    return data[0] if data else {}


def get_upload_item(item_id: int) -> Optional[Dict[str, Any]]:
    params = {"id": f"eq.{item_id}", "select": "*"}
    data = _request("GET", "upload_queue", params=params)
    return data[0] if data else None


def list_upload_queue(
    status: Optional[str] = None,
    statuses: Optional[List[str]] = None,
    scheduled_before: Optional[str] = None,
    youtube_channel_id: Optional[int] = None,
    limit: int = 500,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    params: Dict[str, str] = {"select": "*", "order": "created_at.asc", "limit": str(limit), "offset": str(offset)}
    if status:
        params["status"] = f"eq.{status}"
    if statuses:
        params["status"] = f"in.({','.join(statuses)})"
    if scheduled_before:
        params["scheduled_at"] = f"lte.{scheduled_before}"
    if youtube_channel_id is not None:
        params["youtube_channel_id"] = f"eq.{youtube_channel_id}"
    return _request("GET", "upload_queue", params=params) or []


def update_upload_item(item_id: int, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    params = {"id": f"eq.{item_id}"}
    data = _request_with_missing_column_retry(
        "PATCH", "upload_queue", params=params, json_body=payload, extra_headers=_headers()
    )
    return data[0] if data else None


def delete_upload_item(item_id: int) -> bool:
    try:
        _request("DELETE", "upload_queue", params={"id": f"eq.{item_id}"})
        return True
    except Exception as exc:
        logger.warning(f"Failed to delete upload item {item_id}: {exc}")
        return False


def get_pending_uploads(limit: int = 100) -> List[Dict[str, Any]]:
    now = _now()
    params = {
        "status": "in.(pending,retrying)",
        "scheduled_at": f"lte.{now}",
        "select": "*",
        "order": "created_at.asc",
        "limit": str(limit),
    }
    return _request("GET", "upload_queue", params=params) or []


def get_scheduled_uploads(limit: int = 500) -> List[Dict[str, Any]]:
    now = _now()
    params = {
        "status": "eq.scheduled",
        "scheduled_at": f"lte.{now}",
        "select": "*",
        "order": "scheduled_at.asc",
        "limit": str(limit),
    }
    return _request("GET", "upload_queue", params=params) or []


# ---------------------------------------------------------------------------
# 6. upload_logs
# ---------------------------------------------------------------------------

def create_upload_log(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = _request_with_missing_column_retry(
        "POST", "upload_logs", json_body=[payload], extra_headers=_headers()
    )
    return data[0] if data else {}


def list_upload_logs(queue_id: Optional[int] = None, limit: int = 200, offset: int = 0) -> List[Dict[str, Any]]:
    params: Dict[str, str] = {"select": "*", "order": "created_at.desc", "limit": str(limit), "offset": str(offset)}
    if queue_id is not None:
        params["queue_id"] = f"eq.{queue_id}"
    return _request("GET", "upload_logs", params=params) or []


# ---------------------------------------------------------------------------
# 7. admin_audit_log
# ---------------------------------------------------------------------------

def audit_log(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = _request_with_missing_column_retry(
        "POST", "admin_audit_log", json_body=[payload], extra_headers=_headers()
    )
    return data[0] if data else {}


def list_audit_logs(admin_id: Optional[int] = None, limit: int = 200, offset: int = 0) -> List[Dict[str, Any]]:
    params: Dict[str, str] = {"select": "*", "order": "created_at.desc", "limit": str(limit), "offset": str(offset)}
    if admin_id is not None:
        params["admin_id"] = f"eq.{admin_id}"
    return _request("GET", "admin_audit_log", params=params) or []


# ---------------------------------------------------------------------------
# 8. seen_source_videos (dedup)
# ---------------------------------------------------------------------------

def mark_source_video_seen(source_channel_id: int, source_video_id: str) -> Dict[str, Any]:
    payload = {"source_channel_id": source_channel_id, "source_video_id": source_video_id}
    data = _request_with_missing_column_retry(
        "POST", "seen_source_videos", json_body=[payload], extra_headers=_headers("return=representation,resolution=merge-duplicates")
    )
    return data[0] if data else {}


def is_source_video_seen(source_channel_id: int, source_video_id: str) -> bool:
    params = {
        "source_channel_id": f"eq.{source_channel_id}",
        "source_video_id": f"eq.{source_video_id}",
        "select": "id",
    }
    data = _request("GET", "seen_source_videos", params=params)
    return bool(data and len(data) > 0)


# ---------------------------------------------------------------------------
# 9. Analytics helpers
# ---------------------------------------------------------------------------

def get_queue_stats() -> Dict[str, int]:
    try:
        items = _request("GET", "upload_queue", params={"select": "status"}) or []
        stats: Dict[str, int] = {}
        for item in items:
            s = item.get("status", "unknown")
            stats[s] = stats.get(s, 0) + 1
        return stats
    except Exception as exc:
        logger.error(f"Failed to get queue stats: {exc}")
        return {}


def get_analytics_overview() -> Dict[str, Any]:
    sources = list_source_channels()
    yts = list_youtube_channels()
    maps = list_mappings()
    q_stats = get_queue_stats()

    # success rate last 7 days
    now = datetime.now(timezone.utc)
    seven_days_ago = (now - __import__("datetime").timedelta(days=7)).isoformat()
    recent = list_upload_queue(scheduled_before=now.isoformat(), limit=10000)
    done = [r for r in recent if r.get("status") == "done" and r.get("finished_at") and r["finished_at"] >= seven_days_ago]
    failed = [r for r in recent if r.get("status") == "failed" and r.get("finished_at") and r["finished_at"] >= seven_days_ago]
    total_recent = len(done) + len(failed)
    success_rate = (len(done) / total_recent * 100) if total_recent > 0 else 0.0

    yesterday = (now - __import__("datetime").timedelta(hours=24)).isoformat()
    uploads_24h = len([r for r in recent if r.get("status") == "done" and r.get("finished_at") and r["finished_at"] >= yesterday])

    return {
        "total_source_channels": len(sources),
        "active_source_channels": len([s for s in sources if s.get("is_active")]),
        "total_youtube_channels": len(yts),
        "connected_youtube_channels": len([y for y in yts if y.get("status") == "connected"]),
        "total_mappings": len(maps),
        "active_mappings": len([m for m in maps if m.get("is_active")]),
        "queue_totals": q_stats,
        "success_rate_7d": round(success_rate, 2),
        "uploads_last_24h": uploads_24h,
    }


def get_channel_performance(youtube_channel_id: int) -> Dict[str, Any]:
    channel = get_youtube_channel(youtube_channel_id)
    if not channel:
        raise ValueError("Channel not found")
    params = {
        "youtube_channel_id": f"eq.{youtube_channel_id}",
        "select": "status",
    }
    items = _request("GET", "upload_queue", params=params) or []
    done = len([i for i in items if i.get("status") == "done"])
    failed = len([i for i in items if i.get("status") == "failed"])
    pending = len([i for i in items if i.get("status") in ("pending", "scheduled", "retrying")])
    total = done + failed
    return {
        "youtube_channel_id": youtube_channel_id,
        "label": channel.get("label"),
        "uploads_done": done,
        "uploads_failed": failed,
        "uploads_pending": pending,
        "success_rate": round((done / total * 100), 2) if total > 0 else 0.0,
        "last_used_at": channel.get("last_used_at"),
    }
