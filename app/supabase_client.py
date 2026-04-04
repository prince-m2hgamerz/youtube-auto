import requests
from typing import Any, Dict, List, Optional

from app.config import settings

BASE_URL = str(settings.supabase_url).rstrip("/")
REST_URL = f"{BASE_URL}/rest/v1"
HEADERS = {
    "apikey": settings.supabase_service_key,
    "Authorization": f"Bearer {settings.supabase_service_key}",
    "Content-Type": "application/json",
}


def _request(method: str, path: str, params: dict | None = None, json_body: Any | None = None, extra_headers: dict | None = None):
    headers = {**HEADERS, **(extra_headers or {})}
    url = f"{REST_URL}/{path}"
    response = requests.request(method, url, headers=headers, params=params, json=json_body, timeout=30)
    if not response.ok:
        raise RuntimeError(f"Supabase request failed {response.status_code}: {response.text}")
    if response.text:
        return response.json()
    return None


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
    data = _request("POST", "users", params=params, json_body=[payload], extra_headers=headers)
    return data[0]


def create_job(job: Dict[str, Any]) -> Dict[str, Any]:
    headers = {"Prefer": "return=representation"}
    data = _request("POST", "video_jobs", json_body=[job], extra_headers=headers)
    return data[0]


def update_job(job_id: int, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    headers = {"Prefer": "return=representation"}
    params = {"id": f"eq.{job_id}"}
    data = _request("PATCH", "video_jobs", params=params, json_body=payload, extra_headers=headers)
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
    data = _request("POST", "users", params=params, json_body=[payload], extra_headers=headers)
    return data[0]


def get_app_settings() -> Dict[str, Any]:
    try:
        params = {"id": "eq.1", "select": "*"}
        data = _request("GET", "app_settings", params=params)
        return data[0] if data else {}
    except Exception:
        return {}


def update_app_settings(settings_payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = {"id": 1, **settings_payload}
    headers = {"Prefer": "return=representation,resolution=merge-duplicates"}
    params = {"on_conflict": "id"}
    data = _request("POST", "app_settings", params=params, json_body=[payload], extra_headers=headers)
    return data[0]


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
