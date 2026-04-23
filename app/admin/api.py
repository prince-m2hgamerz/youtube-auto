"""Admin API layer — FastAPI routers for channels, mappings, uploads, analytics, and RBAC."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from app.admin import db
from app.admin.auth import (
    audit,
    create_access_token,
    generate_api_key,
    get_current_admin,
    hash_api_key,
    hash_password,
    require_admin,
    require_operator,
    require_super_admin,
    require_viewer,
    verify_password,
)
from app.admin.queue_worker import enqueue_upload_item, get_worker_status
from app.admin.schemas import (
    AdminUserCreate,
    AdminUserOut,
    AdminUserUpdate,
    AnalyticsOverview,
    BulkMappingCreate,
    BulkUploadEnqueue,
    ChannelPerformance,
    ConnectYouTubeRequest,
    ConnectYouTubeResponse,
    LoginRequest,
    MappingCreate,
    MappingOut,
    MappingUpdate,
    SourceChannelCreate,
    SourceChannelOut,
    SourceChannelUpdate,
    TokenOut,
    UploadEnqueue,
    UploadLogOut,
    UploadOut,
    YouTubeChannelOut,
    YouTubeChannelUpdate,
)
from app.admin.youtube_service import get_valid_youtube_service
from app.config import settings
from app.utils import encrypt_data
from app.youtube_client import create_oauth_url, fetch_credentials

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

@router.post("/auth/register", response_model=AdminUserOut, status_code=status.HTTP_201_CREATED)
def register_admin(
    request: Request,
    data: AdminUserCreate,
    admin: Dict[str, Any] = Depends(require_super_admin),
):
    existing = db.get_admin_user_by_email(data.email)
    if existing:
        raise HTTPException(status_code=409, detail="Email already registered")
    payload = data.model_dump(exclude_unset=True)
    if data.password:
        payload["password_hash"] = hash_password(data.password)
    del payload["password"]
    created = db.create_admin_user(payload)
    audit(request, admin, "create_admin_user", "admin_user", str(created.get("id")), {"email": data.email})
    return created


@router.post("/auth/login", response_model=TokenOut)
def login_admin(request: Request, data: LoginRequest):
    user = db.get_admin_user_by_email(data.email)
    if not user or not user.get("is_active"):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    if not user.get("password_hash"):
        raise HTTPException(status_code=401, detail="Password not set")
    if not verify_password(data.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    db.update_admin_user(user["id"], {"last_login_at": datetime.now(timezone.utc).isoformat()})
    token = create_access_token(user["id"], user["role"], user["email"])
    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_in": 86400,
        "role": user["role"],
        "admin_id": user["id"],
    }


@router.get("/auth/me", response_model=AdminUserOut)
def me(admin: Dict[str, Any] = Depends(get_current_admin)):
    user = db.get_admin_user_by_id(admin["id"])
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


# ---------------------------------------------------------------------------
# Admin users management
# ---------------------------------------------------------------------------

@router.get("/users", response_model=List[AdminUserOut])
def list_users(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    admin: Dict[str, Any] = Depends(require_admin),
):
    return db.list_admin_users(limit=limit, offset=offset)


@router.patch("/users/{user_id}", response_model=AdminUserOut)
def update_user(
    request: Request,
    user_id: int,
    data: AdminUserUpdate,
    admin: Dict[str, Any] = Depends(require_super_admin),
):
    if user_id == admin["id"]:
        raise HTTPException(status_code=400, detail="Use /auth/me to update yourself")
    payload = data.model_dump(exclude_unset=True)
    if data.password:
        payload["password_hash"] = hash_password(data.password)
        del payload["password"]
    updated = db.update_admin_user(user_id, payload)
    if not updated:
        raise HTTPException(status_code=404, detail="User not found")
    audit(request, admin, "update_admin_user", "admin_user", str(user_id), payload)
    return updated


@router.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user(
    request: Request,
    user_id: int,
    admin: Dict[str, Any] = Depends(require_super_admin),
):
    if user_id == admin["id"]:
        raise HTTPException(status_code=400, detail="Cannot delete yourself")
    if not db.delete_admin_user(user_id):
        raise HTTPException(status_code=404, detail="User not found")
    audit(request, admin, "delete_admin_user", "admin_user", str(user_id))
    return None


# ---------------------------------------------------------------------------
# Source channels
# ---------------------------------------------------------------------------

@router.post("/source-channels", response_model=SourceChannelOut, status_code=status.HTTP_201_CREATED)
def create_source(
    request: Request,
    data: SourceChannelCreate,
    admin: Dict[str, Any] = Depends(require_operator),
):
    payload = data.model_dump()
    payload["created_by"] = admin["id"]
    created = db.create_source_channel(payload)
    audit(request, admin, "create_source_channel", "source_channel", str(created.get("id")), {"name": data.name})
    return created


@router.get("/source-channels", response_model=List[SourceChannelOut])
def list_sources(
    is_active: Optional[bool] = Query(None),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
    admin: Dict[str, Any] = Depends(require_viewer),
):
    return db.list_source_channels(is_active=is_active, limit=limit, offset=offset)


@router.get("/source-channels/{source_id}", response_model=SourceChannelOut)
def get_source(
    source_id: int,
    admin: Dict[str, Any] = Depends(require_viewer),
):
    item = db.get_source_channel(source_id)
    if not item:
        raise HTTPException(status_code=404, detail="Source channel not found")
    return item


@router.patch("/source-channels/{source_id}", response_model=SourceChannelOut)
def update_source(
    request: Request,
    source_id: int,
    data: SourceChannelUpdate,
    admin: Dict[str, Any] = Depends(require_operator),
):
    payload = data.model_dump(exclude_unset=True)
    updated = db.update_source_channel(source_id, payload)
    if not updated:
        raise HTTPException(status_code=404, detail="Source channel not found")
    audit(request, admin, "update_source_channel", "source_channel", str(source_id), payload)
    return updated


@router.delete("/source-channels/{source_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_source(
    request: Request,
    source_id: int,
    admin: Dict[str, Any] = Depends(require_admin),
):
    if not db.delete_source_channel(source_id):
        raise HTTPException(status_code=404, detail="Source channel not found")
    audit(request, admin, "delete_source_channel", "source_channel", str(source_id))
    return None


# ---------------------------------------------------------------------------
# YouTube channels
# ---------------------------------------------------------------------------

@router.post("/youtube-channels/connect", response_model=ConnectYouTubeResponse)
def connect_youtube_init(
    request: Request,
    data: ConnectYouTubeRequest,
    admin: Dict[str, Any] = Depends(require_operator),
):
    """Start OAuth flow for a new YouTube channel connection."""
    import random
    state_key = f"admin:{admin['id']}:{random.randint(100000, 999999)}"
    # Store pending connection state in a transient way (we use DB with short-lived row or in-memory).
    # For simplicity, we encode admin_id + label into a signed state.
    from app.utils import create_oauth_state
    # We'll use a fake telegram_id as seed for state signing, but store label in a temp cookie / param.
    # Simpler: generate a URL with a custom state that includes label.
    import hmac, hashlib, json, base64
    state_payload = json.dumps({"admin_id": admin["id"], "label": data.label})
    state_b64 = base64.urlsafe_b64encode(state_payload.encode()).decode().rstrip("=")
    signature = hmac.new(settings.secret_key.encode(), state_b64.encode(), hashlib.sha256).hexdigest()[:16]
    state = f"{state_b64}.{signature}"
    auth_url = create_oauth_url(0)  # create_oauth_url takes telegram_id; we ignore the state part and rebuild
    # Rebuild auth_url with our custom state
    from google_auth_oauthlib.flow import Flow
    from app.youtube_client import CLIENT_CONFIG, SCOPES
    flow = Flow.from_client_config(CLIENT_CONFIG, scopes=SCOPES, redirect_uri=str(settings.oauth_redirect_uri))
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
        state=state,
    )
    audit(request, admin, "init_youtube_connect", "youtube_channel", "", {"label": data.label})
    return {"oauth_url": auth_url, "state": state}


@router.get("/youtube-channels", response_model=List[YouTubeChannelOut])
def list_youtube(
    status: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
    admin: Dict[str, Any] = Depends(require_viewer),
):
    return db.list_youtube_channels(status=status, limit=limit, offset=offset)


@router.get("/youtube-channels/{yt_id}", response_model=YouTubeChannelOut)
def get_youtube(
    yt_id: int,
    admin: Dict[str, Any] = Depends(require_viewer),
):
    item = db.get_youtube_channel(yt_id)
    if not item:
        raise HTTPException(status_code=404, detail="YouTube channel not found")
    # Never expose raw oauth_credentials
    item.pop("oauth_credentials", None)
    return item


@router.patch("/youtube-channels/{yt_id}", response_model=YouTubeChannelOut)
def update_youtube(
    request: Request,
    yt_id: int,
    data: YouTubeChannelUpdate,
    admin: Dict[str, Any] = Depends(require_operator),
):
    payload = data.model_dump(exclude_unset=True)
    updated = db.update_youtube_channel(yt_id, payload)
    if not updated:
        raise HTTPException(status_code=404, detail="YouTube channel not found")
    audit(request, admin, "update_youtube_channel", "youtube_channel", str(yt_id), payload)
    updated.pop("oauth_credentials", None)
    return updated


@router.delete("/youtube-channels/{yt_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_youtube(
    request: Request,
    yt_id: int,
    admin: Dict[str, Any] = Depends(require_admin),
):
    if not db.delete_youtube_channel(yt_id):
        raise HTTPException(status_code=404, detail="YouTube channel not found")
    audit(request, admin, "delete_youtube_channel", "youtube_channel", str(yt_id))
    return None


# ---------------------------------------------------------------------------
# Mappings (many-to-many)
# ---------------------------------------------------------------------------

@router.post("/mappings", response_model=MappingOut, status_code=status.HTTP_201_CREATED)
def create_mapping(
    request: Request,
    data: MappingCreate,
    admin: Dict[str, Any] = Depends(require_operator),
):
    # Validate FKs exist
    if not db.get_source_channel(data.source_channel_id):
        raise HTTPException(status_code=404, detail="Source channel not found")
    if not db.get_youtube_channel(data.youtube_channel_id):
        raise HTTPException(status_code=404, detail="YouTube channel not found")
    payload = data.model_dump()
    created = db.create_mapping(payload)
    audit(request, admin, "create_mapping", "channel_mapping", str(created.get("id")), payload)
    return created


@router.post("/mappings/bulk", response_model=List[MappingOut], status_code=status.HTTP_201_CREATED)
def create_bulk_mapping(
    request: Request,
    data: BulkMappingCreate,
    admin: Dict[str, Any] = Depends(require_operator),
):
    """Assign one or more source channels to one or more YouTube channels."""
    results: List[Dict[str, Any]] = []
    for sc_id in data.source_channel_ids:
        if not db.get_source_channel(sc_id):
            raise HTTPException(status_code=404, detail=f"Source channel {sc_id} not found")
        for yt_id in data.youtube_channel_ids:
            if not db.get_youtube_channel(yt_id):
                raise HTTPException(status_code=404, detail=f"YouTube channel {yt_id} not found")
            payload = {
                "source_channel_id": sc_id,
                "youtube_channel_id": yt_id,
                "visibility": data.visibility,
                "schedule_cron": data.schedule_cron,
                "schedule_timezone": data.schedule_timezone,
                "max_per_run": data.max_per_run,
                "priority": data.priority,
                "is_active": data.is_active,
            }
            try:
                created = db.create_mapping(payload)
                results.append(created)
            except Exception as exc:
                logger.warning(f"Bulk mapping create failed for {sc_id}->{yt_id}: {exc}")
    audit(request, admin, "create_bulk_mapping", "channel_mapping", "", {
        "source_count": len(data.source_channel_ids),
        "youtube_count": len(data.youtube_channel_ids),
    })
    return results


@router.get("/mappings", response_model=List[MappingOut])
def list_mappings(
    source_channel_id: Optional[int] = Query(None),
    youtube_channel_id: Optional[int] = Query(None),
    is_active: Optional[bool] = Query(None),
    limit: int = Query(500, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    admin: Dict[str, Any] = Depends(require_viewer),
):
    return db.list_mappings(
        source_channel_id=source_channel_id,
        youtube_channel_id=youtube_channel_id,
        is_active=is_active,
        limit=limit,
        offset=offset,
    )


@router.get("/mappings/{mapping_id}", response_model=MappingOut)
def get_mapping(
    mapping_id: int,
    admin: Dict[str, Any] = Depends(require_viewer),
):
    item = db.get_mapping(mapping_id)
    if not item:
        raise HTTPException(status_code=404, detail="Mapping not found")
    return item


@router.patch("/mappings/{mapping_id}", response_model=MappingOut)
def update_mapping(
    request: Request,
    mapping_id: int,
    data: MappingUpdate,
    admin: Dict[str, Any] = Depends(require_operator),
):
    payload = data.model_dump(exclude_unset=True)
    updated = db.update_mapping(mapping_id, payload)
    if not updated:
        raise HTTPException(status_code=404, detail="Mapping not found")
    audit(request, admin, "update_mapping", "channel_mapping", str(mapping_id), payload)
    return updated


@router.delete("/mappings/{mapping_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_mapping(
    request: Request,
    mapping_id: int,
    admin: Dict[str, Any] = Depends(require_admin),
):
    if not db.delete_mapping(mapping_id):
        raise HTTPException(status_code=404, detail="Mapping not found")
    audit(request, admin, "delete_mapping", "channel_mapping", str(mapping_id))
    return None


# ---------------------------------------------------------------------------
# Upload queue
# ---------------------------------------------------------------------------

@router.post("/uploads/enqueue", response_model=UploadOut, status_code=status.HTTP_201_CREATED)
def enqueue_upload(
    request: Request,
    data: UploadEnqueue,
    admin: Dict[str, Any] = Depends(require_operator),
):
    if data.youtube_channel_id and not db.get_youtube_channel(data.youtube_channel_id):
        raise HTTPException(status_code=404, detail="YouTube channel not found")
    if data.source_channel_id and not db.get_source_channel(data.source_channel_id):
        raise HTTPException(status_code=404, detail="Source channel not found")
    payload = data.model_dump()
    payload["created_by"] = admin["id"]
    payload["priority"] = data.priority
    created = db.enqueue_upload(payload)
    enqueue_upload_item(created["id"])
    audit(request, admin, "enqueue_upload", "upload_queue", str(created.get("id")), {"video_url": data.video_url})
    return created


@router.post("/uploads/enqueue-bulk", response_model=List[UploadOut], status_code=status.HTTP_201_CREATED)
def enqueue_bulk_upload(
    request: Request,
    data: BulkUploadEnqueue,
    admin: Dict[str, Any] = Depends(require_operator),
):
    results: List[Dict[str, Any]] = []
    for item in data.items:
        payload = item.model_dump()
        payload["created_by"] = admin["id"]
        payload["priority"] = item.priority
        created = db.enqueue_upload(payload)
        enqueue_upload_item(created["id"])
        results.append(created)
    audit(request, admin, "enqueue_bulk_upload", "upload_queue", "", {"count": len(data.items)})
    return results


@router.get("/uploads", response_model=List[UploadOut])
def list_uploads(
    status: Optional[str] = Query(None),
    youtube_channel_id: Optional[int] = Query(None),
    limit: int = Query(500, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    admin: Dict[str, Any] = Depends(require_viewer),
):
    return db.list_upload_queue(status=status, youtube_channel_id=youtube_channel_id, limit=limit, offset=offset)


@router.get("/uploads/{item_id}", response_model=UploadOut)
def get_upload(
    item_id: int,
    admin: Dict[str, Any] = Depends(require_viewer),
):
    item = db.get_upload_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Upload item not found")
    return item


@router.post("/uploads/{item_id}/retry", response_model=UploadOut)
def retry_upload(
    request: Request,
    item_id: int,
    admin: Dict[str, Any] = Depends(require_operator),
):
    item = db.get_upload_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Upload item not found")
    if item.get("status") not in ("failed", "cancelled"):
        raise HTTPException(status_code=400, detail="Only failed or cancelled items can be retried")
    updated = db.update_upload_item(item_id, {
        "status": "pending",
        "attempts": 0,
        "error_message": None,
        "next_retry_at": None,
    })
    if updated:
        enqueue_upload_item(item_id)
    audit(request, admin, "retry_upload", "upload_queue", str(item_id))
    return updated or item


@router.delete("/uploads/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
def cancel_upload(
    request: Request,
    item_id: int,
    admin: Dict[str, Any] = Depends(require_admin),
):
    item = db.get_upload_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Upload item not found")
    if item.get("status") not in ("pending", "scheduled", "retrying"):
        raise HTTPException(status_code=400, detail="Only pending/scheduled/retrying items can be cancelled")
    db.update_upload_item(item_id, {"status": "cancelled", "finished_at": datetime.now(timezone.utc).isoformat()})
    audit(request, admin, "cancel_upload", "upload_queue", str(item_id))
    return None


# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------

@router.get("/uploads/{item_id}/logs", response_model=List[UploadLogOut])
def get_upload_logs(
    item_id: int,
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
    admin: Dict[str, Any] = Depends(require_viewer),
):
    return db.list_upload_logs(queue_id=item_id, limit=limit, offset=offset)


@router.get("/audit-logs")
def list_audit_logs(
    admin_id: Optional[int] = Query(None),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
    admin: Dict[str, Any] = Depends(require_admin),
):
    return db.list_audit_logs(admin_id=admin_id, limit=limit, offset=offset)


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

@router.get("/analytics/overview", response_model=AnalyticsOverview)
def analytics_overview(admin: Dict[str, Any] = Depends(require_viewer)):
    return db.get_analytics_overview()


@router.get("/analytics/channels/{yt_id}/performance", response_model=ChannelPerformance)
def channel_performance(
    yt_id: int,
    admin: Dict[str, Any] = Depends(require_viewer),
):
    return db.get_channel_performance(yt_id)


# ---------------------------------------------------------------------------
# Worker status
# ---------------------------------------------------------------------------

@router.get("/worker/status")
def worker_status(admin: Dict[str, Any] = Depends(require_viewer)):
    return get_worker_status()
