"""Telegram bot commands for the admin system.

These commands wrap the admin DB layer so admins can manage the system
entirely from Telegram without using the web API.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from aiogram import types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.admin import db
from app.admin.queue_worker import enqueue_upload_item
from app.admin.schemas import (
    MappingCreate,
    SourceChannelCreate,
    SourceChannelUpdate,
    YouTubeChannelUpdate,
    MappingUpdate,
)
from app.admin.youtube_service import get_valid_youtube_service
from app.config import settings
from app.youtube_client import create_oauth_url, fetch_credentials, serialize_credentials

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_telegram_admin(telegram_id: str) -> bool:
    import os
    admin_ids = {p.strip() for p in os.getenv("ADMIN_IDS", "5798029484").split(",") if p.strip()}
    return telegram_id in admin_ids


def _require_admin(message: types.Message) -> bool:
    if not _is_telegram_admin(str(message.from_user.id)):
        return False
    return True


def _parse_args(text: str, expected: int) -> List[str]:
    parts = text.split(None, expected)
    return parts


def _format_source_channel(ch: Dict[str, Any]) -> str:
    return (
        f"📺 ID: {ch.get('id')}\n"
        f"Name: {ch.get('name')}\n"
        f"URL: {ch.get('source_url')}\n"
        f"Type: {ch.get('source_type')}\n"
        f"Filter: {ch.get('content_filter')}\n"
        f"Limit: {ch.get('fetch_limit')}\n"
        f"Active: {'✅' if ch.get('is_active') else '❌'}\n"
        f"Created: {ch.get('created_at')[:10]}"
    )


def _format_youtube_channel(ch: Dict[str, Any]) -> str:
    return (
        f"▶️ ID: {ch.get('id')}\n"
        f"Label: {ch.get('label')}\n"
        f"YT Channel: {ch.get('youtube_channel_id') or '-'}\n"
        f"Handle: {ch.get('handle') or '-'}\n"
        f"Status: {ch.get('status')}\n"
        f"Daily Quota: {ch.get('uploads_today', 0)}/{ch.get('daily_quota_limit', 6)}\n"
        f"Last Used: {ch.get('last_used_at')[:16] if ch.get('last_used_at') else '-'}"
    )


def _format_mapping(m: Dict[str, Any]) -> str:
    return (
        f"🔗 ID: {m.get('id')}\n"
        f"Source ID: {m.get('source_channel_id')}\n"
        f"YouTube ID: {m.get('youtube_channel_id')}\n"
        f"Visibility: {m.get('visibility')}\n"
        f"Schedule: {m.get('schedule_cron') or 'on-demand'}\n"
        f"Max/Run: {m.get('max_per_run')}\n"
        f"Priority: {m.get('priority')}\n"
        f"Active: {'✅' if m.get('is_active') else '❌'}"
    )


def _format_upload_item(item: Dict[str, Any]) -> str:
    return (
        f"⬆️ ID: {item.get('id')}\n"
        f"URL: {item.get('video_url')[:60]}...\n"
        f"Title: {item.get('title') or '-'}\n"
        f"Status: {item.get('status')}\n"
        f"Attempts: {item.get('attempts', 0)}/{item.get('max_attempts', 5)}\n"
        f"YT Channel: {item.get('youtube_channel_id')}\n"
        f"Result: {item.get('result_url') or '-'}"
    )


# ---------------------------------------------------------------------------
# 1. Source Channels
# ---------------------------------------------------------------------------

async def cmd_admin_create_source(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 3)
    if len(parts) < 3:
        await message.reply(
            "Usage: /admin_create_source <name> <source_url> [content_filter]\n"
            "Example: /admin_create_source \"Tech Shorts\" https://youtube.com/@tech shorts"
        )
        return
    _, name, url, *rest = parts
    content_filter = rest[0] if rest else "shorts"
    try:
        data = SourceChannelCreate(
            name=name,
            source_url=url,
            content_filter=content_filter,
        )
        created = db.create_source_channel({**data.model_dump(), "created_by": None})
        await message.reply(f"✅ Source channel created:\n\n{_format_source_channel(created)}")
    except Exception as exc:
        logger.exception("Failed to create source channel")
        await message.reply(f"❌ Error: {exc}")


async def cmd_admin_list_sources(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    try:
        sources = db.list_source_channels(limit=50)
        if not sources:
            await message.reply("No source channels found.")
            return
        lines = [f"📺 Source Channels ({len(sources)} total):"]
        for s in sources:
            lines.append(f"\n{_format_source_channel(s)}")
        await message.reply("\n---\n".join(lines))
    except Exception as exc:
        logger.exception("Failed to list sources")
        await message.reply(f"❌ Error: {exc}")


async def cmd_admin_get_source(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 1)
    if len(parts) < 2:
        await message.reply("Usage: /admin_get_source <source_id>")
        return
    try:
        sid = int(parts[1].strip())
    except ValueError:
        await message.reply("Source ID must be an integer.")
        return
    s = db.get_source_channel(sid)
    if not s:
        await message.reply("Source channel not found.")
        return
    await message.reply(f"📺 Source Channel:\n\n{_format_source_channel(s)}")


async def cmd_admin_update_source(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 3)
    if len(parts) < 3:
        await message.reply(
            "Usage: /admin_update_source <source_id> <field> <value>\n"
            "Fields: name, source_url, content_filter, fetch_limit, is_active\n"
            "Example: /admin_update_source 1 name \"New Name\""
        )
        return
    _, sid_str, field, *rest = parts
    try:
        sid = int(sid_str.strip())
    except ValueError:
        await message.reply("Source ID must be an integer.")
        return
    value = rest[0] if rest else ""
    payload: Dict[str, Any] = {}
    if field == "is_active":
        payload[field] = value.lower() in ("true", "1", "yes")
    elif field == "fetch_limit":
        payload[field] = int(value)
    else:
        payload[field] = value
    updated = db.update_source_channel(sid, payload)
    if not updated:
        await message.reply("Source channel not found or update failed.")
        return
    await message.reply(f"✅ Updated source channel:\n\n{_format_source_channel(updated)}")


async def cmd_admin_delete_source(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 1)
    if len(parts) < 2:
        await message.reply("Usage: /admin_delete_source <source_id>")
        return
    try:
        sid = int(parts[1].strip())
    except ValueError:
        await message.reply("Source ID must be an integer.")
        return
    if db.delete_source_channel(sid):
        await message.reply(f"✅ Source channel {sid} deleted.")
    else:
        await message.reply("❌ Delete failed.")


# ---------------------------------------------------------------------------
# 2. YouTube Channels
# ---------------------------------------------------------------------------

async def cmd_admin_connect_youtube(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 1)
    if len(parts) < 2:
        await message.reply("Usage: /admin_connect_youtube <label>\nExample: /admin_connect_youtube MainChannel")
        return
    label = parts[1].strip()
    try:
        import hmac, hashlib, json, base64
        state_payload = json.dumps({"admin_tg_id": str(message.from_user.id), "label": label})
        state_b64 = base64.urlsafe_b64encode(state_payload.encode()).decode().rstrip("=")
        signature = hmac.new(settings.secret_key.encode(), state_b64.encode(), hashlib.sha256).hexdigest()[:16]
        state = f"{state_b64}.{signature}"
        from google_auth_oauthlib.flow import Flow
        from app.youtube_client import CLIENT_CONFIG, SCOPES
        flow = Flow.from_client_config(CLIENT_CONFIG, scopes=SCOPES, redirect_uri=str(settings.oauth_redirect_uri))
        auth_url, _ = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
            prompt="consent",
            state=state,
        )
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Connect YouTube", url=auth_url)]
        ])
        await message.reply(
            f"Click the button to connect YouTube channel with label: *{label}*\n\n"
            f"After authorization, the channel will be added to the admin system.",
            reply_markup=keyboard,
        )
    except Exception as exc:
        logger.exception("Failed to init YouTube connect")
        await message.reply(f"❌ Error: {exc}")


async def cmd_admin_list_youtube(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    try:
        channels = db.list_youtube_channels(limit=50)
        if not channels:
            await message.reply("No YouTube channels connected.")
            return
        lines = [f"▶️ YouTube Channels ({len(channels)} total):"]
        for ch in channels:
            lines.append(f"\n{_format_youtube_channel(ch)}")
        await message.reply("\n---\n".join(lines))
    except Exception as exc:
        logger.exception("Failed to list YouTube channels")
        await message.reply(f"❌ Error: {exc}")


async def cmd_admin_get_youtube(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 1)
    if len(parts) < 2:
        await message.reply("Usage: /admin_get_youtube <yt_id>")
        return
    try:
        yid = int(parts[1].strip())
    except ValueError:
        await message.reply("YouTube channel ID must be an integer.")
        return
    ch = db.get_youtube_channel(yid)
    if not ch:
        await message.reply("YouTube channel not found.")
        return
    ch.pop("oauth_credentials", None)
    await message.reply(f"▶️ YouTube Channel:\n\n{_format_youtube_channel(ch)}")


async def cmd_admin_update_youtube(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 3)
    if len(parts) < 3:
        await message.reply(
            "Usage: /admin_update_youtube <yt_id> <field> <value>\n"
            "Fields: label, status, daily_quota_limit\n"
            "Example: /admin_update_youtube 1 daily_quota_limit 10"
        )
        return
    _, yid_str, field, *rest = parts
    try:
        yid = int(yid_str.strip())
    except ValueError:
        await message.reply("YouTube channel ID must be an integer.")
        return
    value = rest[0] if rest else ""
    payload: Dict[str, Any] = {}
    if field == "daily_quota_limit":
        payload[field] = int(value)
    else:
        payload[field] = value
    updated = db.update_youtube_channel(yid, payload)
    if not updated:
        await message.reply("Channel not found or update failed.")
        return
    updated.pop("oauth_credentials", None)
    await message.reply(f"✅ Updated channel:\n\n{_format_youtube_channel(updated)}")


async def cmd_admin_delete_youtube(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 1)
    if len(parts) < 2:
        await message.reply("Usage: /admin_delete_youtube <yt_id>")
        return
    try:
        yid = int(parts[1].strip())
    except ValueError:
        await message.reply("YouTube channel ID must be an integer.")
        return
    if db.delete_youtube_channel(yid):
        await message.reply(f"✅ YouTube channel {yid} deleted.")
    else:
        await message.reply("❌ Delete failed.")


# ---------------------------------------------------------------------------
# 3. Mappings
# ---------------------------------------------------------------------------

async def cmd_admin_create_mapping(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 4)
    if len(parts) < 3:
        await message.reply(
            "Usage: /admin_create_mapping <source_id> <youtube_id> [visibility] [cron]\n"
            "Example: /admin_create_mapping 1 1 public 07:15,19:15"
        )
        return
    _, sid_str, yid_str, *rest = parts
    try:
        sid = int(sid_str)
        yid = int(yid_str)
    except ValueError:
        await message.reply("IDs must be integers.")
        return
    visibility = rest[0] if len(rest) > 0 else "public"
    cron = rest[1] if len(rest) > 1 else None
    if not db.get_source_channel(sid):
        await message.reply("Source channel not found.")
        return
    if not db.get_youtube_channel(yid):
        await message.reply("YouTube channel not found.")
        return
    try:
        data = MappingCreate(
            source_channel_id=sid,
            youtube_channel_id=yid,
            visibility=visibility,
            schedule_cron=cron,
        )
        created = db.create_mapping(data.model_dump())
        await message.reply(f"✅ Mapping created:\n\n{_format_mapping(created)}")
    except Exception as exc:
        logger.exception("Failed to create mapping")
        await message.reply(f"❌ Error: {exc}")


async def cmd_admin_list_mappings(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    try:
        mappings = db.list_mappings(limit=100)
        if not mappings:
            await message.reply("No mappings found.")
            return
        lines = [f"🔗 Mappings ({len(mappings)} total):"]
        for m in mappings:
            lines.append(f"\n{_format_mapping(m)}")
        await message.reply("\n---\n".join(lines))
    except Exception as exc:
        logger.exception("Failed to list mappings")
        await message.reply(f"❌ Error: {exc}")


async def cmd_admin_get_mapping(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 1)
    if len(parts) < 2:
        await message.reply("Usage: /admin_get_mapping <mapping_id>")
        return
    try:
        mid = int(parts[1].strip())
    except ValueError:
        await message.reply("Mapping ID must be an integer.")
        return
    m = db.get_mapping(mid)
    if not m:
        await message.reply("Mapping not found.")
        return
    await message.reply(f"🔗 Mapping:\n\n{_format_mapping(m)}")


async def cmd_admin_update_mapping(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 3)
    if len(parts) < 3:
        await message.reply(
            "Usage: /admin_update_mapping <mapping_id> <field> <value>\n"
            "Fields: visibility, schedule_cron, max_per_run, priority, is_active\n"
            "Example: /admin_update_mapping 1 max_per_run 3"
        )
        return
    _, mid_str, field, *rest = parts
    try:
        mid = int(mid_str.strip())
    except ValueError:
        await message.reply("Mapping ID must be an integer.")
        return
    value = rest[0] if rest else ""
    payload: Dict[str, Any] = {}
    if field in ("max_per_run", "priority"):
        payload[field] = int(value)
    elif field == "is_active":
        payload[field] = value.lower() in ("true", "1", "yes")
    else:
        payload[field] = value
    updated = db.update_mapping(mid, payload)
    if not updated:
        await message.reply("Mapping not found or update failed.")
        return
    await message.reply(f"✅ Updated mapping:\n\n{_format_mapping(updated)}")


async def cmd_admin_delete_mapping(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 1)
    if len(parts) < 2:
        await message.reply("Usage: /admin_delete_mapping <mapping_id>")
        return
    try:
        mid = int(parts[1].strip())
    except ValueError:
        await message.reply("Mapping ID must be an integer.")
        return
    if db.delete_mapping(mid):
        await message.reply(f"✅ Mapping {mid} deleted.")
    else:
        await message.reply("❌ Delete failed.")


# ---------------------------------------------------------------------------
# 4. Upload Queue
# ---------------------------------------------------------------------------

async def cmd_admin_enqueue(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 3)
    if len(parts) < 3:
        await message.reply(
            "Usage: /admin_enqueue <video_url> <youtube_channel_id> [title]\n"
            "Example: /admin_enqueue https://youtube.com/watch?v=abc 1 \"My Title\""
        )
        return
    _, url, yid_str, *rest = parts
    try:
        yid = int(yid_str)
    except ValueError:
        await message.reply("YouTube channel ID must be an integer.")
        return
    title = rest[0] if rest else None
    try:
        item = db.enqueue_upload({
            "video_url": url,
            "youtube_channel_id": yid,
            "title": title,
            "status": "pending",
            "scheduled_at": datetime.now(timezone.utc).isoformat(),
            "max_attempts": 5,
        })
        enqueue_upload_item(item["id"])
        await message.reply(f"✅ Enqueued upload:\n\n{_format_upload_item(item)}")
    except Exception as exc:
        logger.exception("Failed to enqueue upload")
        await message.reply(f"❌ Error: {exc}")


async def cmd_admin_list_uploads(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 1)
    status_filter = parts[1].strip() if len(parts) > 1 else None
    try:
        items = db.list_upload_queue(status=status_filter, limit=50)
        if not items:
            await message.reply("No upload queue items found.")
            return
        lines = [f"⬆️ Upload Queue ({len(items)} shown):"]
        for item in items:
            lines.append(f"\n{_format_upload_item(item)}")
        await message.reply("\n---\n".join(lines))
    except Exception as exc:
        logger.exception("Failed to list uploads")
        await message.reply(f"❌ Error: {exc}")


async def cmd_admin_retry_upload(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 1)
    if len(parts) < 2:
        await message.reply("Usage: /admin_retry_upload <item_id>")
        return
    try:
        item_id = int(parts[1].strip())
    except ValueError:
        await message.reply("Item ID must be an integer.")
        return
    item = db.get_upload_item(item_id)
    if not item:
        await message.reply("Upload item not found.")
        return
    db.update_upload_item(item_id, {
        "status": "pending",
        "attempts": 0,
        "error_message": None,
        "next_retry_at": None,
    })
    enqueue_upload_item(item_id)
    await message.reply(f"✅ Retry scheduled for item #{item_id}.")


async def cmd_admin_cancel_upload(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 1)
    if len(parts) < 2:
        await message.reply("Usage: /admin_cancel_upload <item_id>")
        return
    try:
        item_id = int(parts[1].strip())
    except ValueError:
        await message.reply("Item ID must be an integer.")
        return
    item = db.get_upload_item(item_id)
    if not item:
        await message.reply("Upload item not found.")
        return
    db.update_upload_item(item_id, {
        "status": "cancelled",
        "finished_at": datetime.now(timezone.utc).isoformat(),
    })
    await message.reply(f"✅ Upload item #{item_id} cancelled.")


# ---------------------------------------------------------------------------
# 5. Analytics
# ---------------------------------------------------------------------------

async def cmd_admin_analytics(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    try:
        overview = db.get_analytics_overview()
        lines = [
            "📊 Admin Analytics Overview",
            "",
            f"📺 Source Channels: {overview['total_source_channels']} total / {overview['active_source_channels']} active",
            f"▶️ YouTube Channels: {overview['total_youtube_channels']} total / {overview['connected_youtube_channels']} connected",
            f"🔗 Mappings: {overview['total_mappings']} total / {overview['active_mappings']} active",
            f"📈 Success Rate (7d): {overview['success_rate_7d']}%",
            f"⬆️ Uploads Last 24h: {overview['uploads_last_24h']}",
            "",
            "📋 Queue Status:",
        ]
        for st, cnt in overview.get("queue_totals", {}).items():
            lines.append(f"  {st}: {cnt}")
        await message.reply("\n".join(lines))
    except Exception as exc:
        logger.exception("Failed to get analytics")
        await message.reply(f"❌ Error: {exc}")


async def cmd_admin_channel_perf(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 1)
    if len(parts) < 2:
        await message.reply("Usage: /admin_channel_perf <youtube_channel_id>")
        return
    try:
        yid = int(parts[1].strip())
    except ValueError:
        await message.reply("YouTube channel ID must be an integer.")
        return
    try:
        perf = db.get_channel_performance(yid)
        lines = [
            f"📈 Channel Performance (ID: {yid})",
            f"Label: {perf.get('label')}",
            f"✅ Done: {perf['uploads_done']}",
            f"❌ Failed: {perf['uploads_failed']}",
            f"⏳ Pending: {perf['uploads_pending']}",
            f"📈 Success Rate: {perf['success_rate']}%",
            f"🕐 Last Used: {perf['last_used_at'][:16] if perf['last_used_at'] else '-'}",
        ]
        await message.reply("\n".join(lines))
    except Exception as exc:
        logger.exception("Failed to get channel performance")
        await message.reply(f"❌ Error: {exc}")


async def cmd_admin_worker_status(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    from app.admin.queue_worker import get_worker_status
    status = get_worker_status()
    lines = [
        "⚙️ Worker Status",
        f"Running: {'✅' if status['running'] else '❌'}",
        f"Queue Size: {status['queue_size']}",
        f"Max Workers: {status['max_workers']}",
        f"Shutdown Requested: {'⚠️ yes' if status['shutdown_requested'] else 'no'}",
    ]
    await message.reply("\n".join(lines))


async def cmd_admin_audit_logs(message: types.Message):
    if not _require_admin(message):
        await message.reply("Access denied.")
        return
    try:
        logs = db.list_audit_logs(limit=20)
        if not logs:
            await message.reply("No audit logs found.")
            return
        lines = ["📋 Recent Audit Logs:"]
        for log in logs:
            lines.append(
                f"\nID: {log.get('id')} | Action: {log.get('action')}\n"
                f"Target: {log.get('target_type')} / {log.get('target_id')}\n"
                f"Admin: {log.get('admin_id')}\n"
                f"Time: {log.get('created_at')[:16]}"
            )
        await message.reply("\n---\n".join(lines))
    except Exception as exc:
        logger.exception("Failed to get audit logs")
        await message.reply(f"❌ Error: {exc}")
