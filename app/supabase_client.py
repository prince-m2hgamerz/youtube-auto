import json
import logging
import requests
import re
from typing import Any, Dict, List, Optional

from app.config import settings

logger = logging.getLogger(__name__)

BASE_URL = str(settings.supabase_url).rstrip("/")
REST_URL = f"{BASE_URL}/rest/v1"
HEADERS = {
    "apikey": settings.supabase_service_key,
    "Authorization": f"Bearer {settings.supabase_service_key}",
    "Content-Type": "application/json",
}
_LOCAL_APP_SETTINGS: Dict[str, Any] = {}


def _request(method: str, path: str, params: dict | None = None, json_body: Any | None = None, extra_headers: dict | None = None):
    headers = {**HEADERS, **(extra_headers or {})}
    url = f"{REST_URL}/{path}"
    try:
        response = requests.request(method, url, headers=headers, params=params, json=json_body, timeout=30)
    except requests.exceptions.RequestException as exc:
        logger.warning(f"Supabase connection failed ({exc.__class__.__name__}): {exc}")
        return None
    if not response.ok:
        raise RuntimeError(f"Supabase request failed {response.status_code}: {response.text}")
    if response.text:
        return response.json()
    return None


def _extract_missing_column(error_text: str) -> Optional[str]:
    match = re.search(r"Could not find the '([^']+)' column", error_text)
    return match.group(1) if match else None


def _is_missing_table_error(error_text: str, table_name: str) -> bool:
    marker = f"Could not find the table 'public.{table_name}'"
    return marker in error_text or '"code":"PGRST205"' in error_text


def _request_with_missing_column_retry(
    method: str,
    path: str,
    *,
    params: dict | None = None,
    json_body: Any | None = None,
    extra_headers: dict | None = None,
):
    try:
        return _request(method, path, params=params, json_body=json_body, extra_headers=extra_headers)
    except RuntimeError as exc:
        missing_column = _extract_missing_column(str(exc))
        if not missing_column:
            raise

        if isinstance(json_body, dict):
            if missing_column not in json_body:
                raise
            retry_payload = {k: v for k, v in json_body.items() if k != missing_column}
            if not retry_payload:
                return None
            return _request(method, path, params=params, json_body=retry_payload, extra_headers=extra_headers)

        if isinstance(json_body, list):
            retry_rows: list[dict] = []
            changed = False
            for row in json_body:
                if isinstance(row, dict) and missing_column in row:
                    retry_rows.append({k: v for k, v in row.items() if k != missing_column})
                    changed = True
                else:
                    retry_rows.append(row)

            if changed:
                return _request(method, path, params=params, json_body=retry_rows, extra_headers=extra_headers)

        raise


def get_user(telegram_id: str) -> Optional[Dict[str, Any]]:
    params = {"telegram_id": f"eq.{telegram_id}", "select": "*"}
    data = _request("GET", "users", params=params)
    return data[0] if data else None


def upsert_user(telegram_id: str, oauth_credentials: Optional[str] = None, is_connected: Optional[bool] = None) -> Dict[str, Any]:
    payload = {"telegram_id": telegram_id}
    if oauth_credentials is not None:
        payload["oauth_credentials"] = oauth_credentials
    if is_connected is not None:
        payload["is_connected"] = is_connected

    headers = {"Prefer": "return=representation,resolution=merge-duplicates"}
    params = {"on_conflict": "telegram_id"}
    data = _request_with_missing_column_retry("POST", "users", params=params, json_body=[payload], extra_headers=headers)
    return data[0]


def create_job(job: Dict[str, Any]) -> Dict[str, Any]:
    headers = {"Prefer": "return=representation"}
    data = _request("POST", "video_jobs", json_body=[job], extra_headers=headers)
    return data[0]


def update_job(job_id: int, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    headers = {"Prefer": "return=representation"}
    params = {"id": f"eq.{job_id}"}
    try:
        data = _request("PATCH", "video_jobs", params=params, json_body=payload, extra_headers=headers)
        return data[0] if data else None
    except RuntimeError as exc:
        missing_column = _extract_missing_column(str(exc))
        if not missing_column or missing_column not in payload:
            raise

        retry_payload = {k: v for k, v in payload.items() if k != missing_column}
        if not retry_payload:
            return None

        data = _request("PATCH", "video_jobs", params=params, json_body=retry_payload, extra_headers=headers)
        return data[0] if data else None


def get_job(job_id: int) -> Optional[Dict[str, Any]]:
    params = {"id": f"eq.{job_id}", "select": "*"}
    data = _request("GET", "video_jobs", params=params)
    return data[0] if data else None


def get_pending_jobs() -> List[Dict[str, Any]]:
    params = {"status": "in.(pending,downloading)", "select": "*"}
    return _request("GET", "video_jobs", params=params) or []


def list_user_jobs(telegram_id: str) -> List[Dict[str, Any]]:
    params = {
        "telegram_id": f"eq.{telegram_id}",
        "select": "*",
        "order": "created_at.desc",
        "limit": "5",
    }
    return _request("GET", "video_jobs", params=params) or []


def count_user_jobs(
    telegram_id: str,
    statuses: Optional[List[str]] = None,
    created_after: Optional[str] = None,
) -> int:
    params: Dict[str, str] = {
        "telegram_id": f"eq.{telegram_id}",
        "select": "id",
    }
    if statuses:
        params["status"] = f"in.({','.join(statuses)})"
    if created_after:
        params["created_at"] = f"gte.{created_after}"
    data = _request("GET", "video_jobs", params=params) or []
    return len(data)


def get_all_users() -> List[Dict[str, Any]]:
    """Get all users for admin dashboard"""
    params = {"select": "*"}
    return _request("GET", "users", params=params) or []


def get_connected_users_count() -> int:
    """Get count of users with YouTube connected"""
    params = {"is_connected": "eq.true", "select": "count"}
    data = _request("GET", "users", params=params)
    return len(data) if data else 0


def get_all_jobs() -> List[Dict[str, Any]]:
    """Get all jobs for admin statistics"""
    params = {"select": "*"}
    return _request("GET", "video_jobs", params=params) or []


def get_jobs_by_status(status: str) -> List[Dict[str, Any]]:
    """Get jobs by status"""
    params = {"status": f"eq.{status}", "select": "*"}
    return _request("GET", "video_jobs", params=params) or []


def get_recent_jobs(limit: int = 10) -> List[Dict[str, Any]]:
    """Get recent jobs for admin dashboard"""
    params = {
        "select": "*",
        "order": "created_at.desc",
        "limit": str(limit)
    }
    return _request("GET", "video_jobs", params=params) or []


def get_jobs_stats() -> Dict[str, Any]:
    """Get comprehensive job statistics"""
    all_jobs = get_all_jobs()
    
    stats = {
        "total": len(all_jobs),
        "pending": len([j for j in all_jobs if j.get("status") == "pending"]),
        "downloading": len([j for j in all_jobs if j.get("status") == "downloading"]),
        "uploading": len([j for j in all_jobs if j.get("status") == "uploading"]),
        "completed": len([j for j in all_jobs if j.get("status") == "done"]),
        "failed": len([j for j in all_jobs if j.get("status") == "failed"]),
    }
    
    # Calculate success rate
    total_completed = stats["completed"] + stats["failed"]
    stats["success_rate"] = (stats["completed"] / total_completed * 100) if total_completed > 0 else 0
    
    return stats


def get_users_stats() -> Dict[str, Any]:
    """Get comprehensive user statistics"""
    all_users = get_all_users()
    
    stats = {
        "total_users": len(all_users),
        "connected_users": len([u for u in all_users if u.get("is_connected")]),
        "unconnected_users": len([u for u in all_users if not u.get("is_connected")]),
    }
    
    return stats


def log_admin_action(admin_id: str, action: str, details: str = "") -> None:
    """Log admin actions (for future admin_logs table)"""
    # For now, just print to console. In production, create admin_logs table
    print(f"[ADMIN LOG] {admin_id}: {action} - {details}")


def update_user_settings(telegram_id: str, settings: Dict[str, Any]) -> Dict[str, Any]:
    payload = {"telegram_id": telegram_id, **settings}
    headers = {"Prefer": "return=representation,resolution=merge-duplicates"}
    params = {"on_conflict": "telegram_id"}
    data = _request_with_missing_column_retry("POST", "users", params=params, json_body=[payload], extra_headers=headers)
    return data[0]


def get_app_settings() -> Dict[str, Any]:
    # Legacy single-row mode (id=1), kept for backward compatibility.
    try:
        params = {"id": "eq.1", "select": "*"}
        data = _request("GET", "app_settings", params=params)
        if data:
            merged = {**_LOCAL_APP_SETTINGS, **data[0]}
            _LOCAL_APP_SETTINGS.update(merged)
            return merged
    except Exception as exc:
        if _is_missing_table_error(str(exc), "app_settings"):
            return dict(_LOCAL_APP_SETTINGS)
        pass

    # Key-value mode using (setting_name, setting_value), aligned with README schema.
    try:
        params = {"select": "setting_name,setting_value"}
        rows = _request("GET", "app_settings", params=params) or []
        settings_map: Dict[str, Any] = {}
        for row in rows:
            name = row.get("setting_name")
            if not name:
                continue
            raw_value = row.get("setting_value")
            if isinstance(raw_value, str):
                try:
                    settings_map[name] = json.loads(raw_value)
                except Exception:
                    settings_map[name] = raw_value
            else:
                settings_map[name] = raw_value
        merged = {**_LOCAL_APP_SETTINGS, **settings_map}
        _LOCAL_APP_SETTINGS.update(merged)
        return merged
    except Exception as exc:
        if _is_missing_table_error(str(exc), "app_settings"):
            return dict(_LOCAL_APP_SETTINGS)
        return dict(_LOCAL_APP_SETTINGS)


def update_app_settings(settings_payload: Dict[str, Any]) -> Dict[str, Any]:
    _LOCAL_APP_SETTINGS.update(settings_payload)

    # Legacy single-row mode.
    payload = {"id": 1, **settings_payload}
    headers = {"Prefer": "return=representation,resolution=merge-duplicates"}
    params = {"on_conflict": "id"}
    try:
        data = _request_with_missing_column_retry("POST", "app_settings", params=params, json_body=[payload], extra_headers=headers)
        if data:
            merged = {**_LOCAL_APP_SETTINGS, **data[0]}
            _LOCAL_APP_SETTINGS.update(merged)
            return merged
    except Exception as exc:
        if _is_missing_table_error(str(exc), "app_settings"):
            return dict(_LOCAL_APP_SETTINGS)
        logger.warning(f"app_settings write failed (legacy mode): {exc}")

    # Key-value mode fallback.
    kv_headers = {"Prefer": "return=representation,resolution=merge-duplicates"}
    kv_params = {"on_conflict": "setting_name"}
    try:
        for key, value in settings_payload.items():
            row = {"setting_name": key, "setting_value": json.dumps(value)}
            _request_with_missing_column_retry("POST", "app_settings", params=kv_params, json_body=[row], extra_headers=kv_headers)
        return get_app_settings()
    except Exception as exc:
        if _is_missing_table_error(str(exc), "app_settings"):
            return dict(_LOCAL_APP_SETTINGS)
        logger.warning(f"app_settings write failed (kv mode): {exc}")
    return dict(_LOCAL_APP_SETTINGS)


def get_broadcast_targets() -> List[Dict[str, Any]]:
    params = {"select": "telegram_id,is_connected"}
    return _request("GET", "users", params=params) or []


def create_broadcast_record(admin_id: str, content: str, sent_count: int, failed_count: int) -> Optional[Dict[str, Any]]:
    try:
        payload = {
            "admin_id": admin_id,
            "content": content,
            "sent_count": sent_count,
            "failed_count": failed_count,
        }
        headers = {"Prefer": "return=representation"}
        data = _request("POST", "broadcasts", json_body=[payload], extra_headers=headers)
        return data[0] if data else None
    except Exception:
        return None


# ── Persistent bot settings stored in admin's user row ──

_LOCAL_BOT_SETTINGS: Dict[str, Any] = {}


def _get_admin_id() -> str:
    return str(settings.admin_ids or "").split(",")[0].strip() if settings.admin_ids else ""


def get_bot_settings() -> Dict[str, Any]:
    admin_id = _get_admin_id()
    if not admin_id:
        return dict(_LOCAL_BOT_SETTINGS)
    try:
        user = get_user(admin_id)
        if user:
            for key in ("source_channel_url", "auto_upload_visibility", "auto_upload_times", "uploaded_shorts_ids"):
                val = user.get(key)
                if val is not None:
                    _LOCAL_BOT_SETTINGS[key] = val
    except Exception as exc:
        logger.warning(f"Failed to load bot settings from user row: {exc}")
    return dict(_LOCAL_BOT_SETTINGS)


def set_bot_settings(settings_payload: Dict[str, Any]) -> None:
    _LOCAL_BOT_SETTINGS.update(settings_payload)
    admin_id = _get_admin_id()
    if not admin_id:
        logger.warning("No admin ID configured; settings kept in memory only")
        return
    try:
        update_user_settings(admin_id, settings_payload)
        logger.info(f"Bot settings persisted for admin {admin_id}")
    except Exception as exc:
        logger.warning(f"Failed to persist bot settings to user row: {exc}")


def get_source_channel_url() -> Optional[str]:
    return get_bot_settings().get("source_channel_url")


def set_source_channel_url(url: str) -> None:
    set_bot_settings({"source_channel_url": url})


def get_uploaded_shorts_ids() -> List[str]:
    raw = get_bot_settings().get("uploaded_shorts_ids")
    if isinstance(raw, list):
        return [str(x) for x in raw]
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            pass
    return []


def add_uploaded_short_id(video_id: str) -> None:
    ids = get_uploaded_shorts_ids()
    if video_id not in ids:
        ids.append(video_id)
        set_bot_settings({"uploaded_shorts_ids": ids})
