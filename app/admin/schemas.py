"""Pydantic schemas for the admin system."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, EmailStr, Field

Role = Literal["super_admin", "admin", "operator", "viewer"]
Visibility = Literal["public", "unlisted", "private"]
ContentFilter = Literal["shorts", "videos", "both"]
SourceType = Literal["youtube", "rss", "custom"]
QueueStatus = Literal[
    "pending", "scheduled", "downloading", "uploading",
    "done", "failed", "cancelled", "retrying",
]


# ---------- Admin users --------------------------------------------------

class AdminUserCreate(BaseModel):
    email: EmailStr
    password: Optional[str] = Field(None, min_length=8)
    full_name: Optional[str] = None
    telegram_id: Optional[str] = None
    role: Role = "viewer"


class AdminUserUpdate(BaseModel):
    full_name: Optional[str] = None
    role: Optional[Role] = None
    is_active: Optional[bool] = None
    password: Optional[str] = Field(None, min_length=8)
    telegram_id: Optional[str] = None


class AdminUserOut(BaseModel):
    id: int
    email: EmailStr
    full_name: Optional[str] = None
    telegram_id: Optional[str] = None
    role: Role
    is_active: bool
    last_login_at: Optional[datetime] = None
    created_at: datetime


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    role: Role
    admin_id: int


# ---------- Source channels ---------------------------------------------

class SourceChannelCreate(BaseModel):
    name: str
    source_url: str
    source_type: SourceType = "youtube"
    external_id: Optional[str] = None
    fetch_limit: int = Field(default=50, ge=1, le=500)
    content_filter: ContentFilter = "shorts"
    is_active: bool = True
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SourceChannelUpdate(BaseModel):
    name: Optional[str] = None
    source_url: Optional[str] = None
    external_id: Optional[str] = None
    fetch_limit: Optional[int] = Field(default=None, ge=1, le=500)
    content_filter: Optional[ContentFilter] = None
    is_active: Optional[bool] = None
    metadata: Optional[Dict[str, Any]] = None


class SourceChannelOut(BaseModel):
    id: int
    name: str
    source_url: str
    source_type: SourceType
    external_id: Optional[str] = None
    fetch_limit: int
    content_filter: ContentFilter
    is_active: bool
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


# ---------- YouTube channels --------------------------------------------

class YouTubeChannelOut(BaseModel):
    id: int
    label: str
    youtube_channel_id: Optional[str] = None
    handle: Optional[str] = None
    email: Optional[str] = None
    status: str
    daily_quota_limit: int
    uploads_today: int
    last_used_at: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class YouTubeChannelUpdate(BaseModel):
    label: Optional[str] = None
    status: Optional[Literal["connected", "expired", "revoked", "disabled"]] = None
    daily_quota_limit: Optional[int] = Field(default=None, ge=1, le=100)
    metadata: Optional[Dict[str, Any]] = None


class ConnectYouTubeRequest(BaseModel):
    label: str = Field(..., description="Friendly name shown in the admin UI")


class ConnectYouTubeResponse(BaseModel):
    oauth_url: str
    state: str


# ---------- Mappings ----------------------------------------------------

class MappingCreate(BaseModel):
    source_channel_id: int
    youtube_channel_id: int
    visibility: Visibility = "public"
    schedule_cron: Optional[str] = None
    schedule_timezone: str = "UTC"
    max_per_run: int = Field(default=1, ge=1, le=25)
    priority: int = 100
    title_template: Optional[str] = None
    description_template: Optional[str] = None
    tag_template: Optional[str] = None
    is_active: bool = True


class MappingUpdate(BaseModel):
    visibility: Optional[Visibility] = None
    schedule_cron: Optional[str] = None
    schedule_timezone: Optional[str] = None
    max_per_run: Optional[int] = Field(default=None, ge=1, le=25)
    priority: Optional[int] = None
    title_template: Optional[str] = None
    description_template: Optional[str] = None
    tag_template: Optional[str] = None
    is_active: Optional[bool] = None


class MappingOut(BaseModel):
    id: int
    source_channel_id: int
    youtube_channel_id: int
    visibility: Visibility
    schedule_cron: Optional[str] = None
    schedule_timezone: str
    max_per_run: int
    priority: int
    is_active: bool
    title_template: Optional[str] = None
    description_template: Optional[str] = None
    tag_template: Optional[str] = None
    last_run_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


class BulkMappingCreate(BaseModel):
    """Assign one source to many YouTube channels (or vice-versa) in one call."""
    source_channel_ids: List[int] = Field(default_factory=list)
    youtube_channel_ids: List[int] = Field(default_factory=list)
    visibility: Visibility = "public"
    schedule_cron: Optional[str] = None
    schedule_timezone: str = "UTC"
    max_per_run: int = 1
    priority: int = 100
    is_active: bool = True


# ---------- Upload queue -------------------------------------------------

class UploadEnqueue(BaseModel):
    video_url: str
    youtube_channel_id: int
    source_channel_id: Optional[int] = None
    mapping_id: Optional[int] = None
    title: Optional[str] = None
    description: Optional[str] = None
    visibility: Visibility = "public"
    scheduled_at: Optional[datetime] = None
    max_attempts: int = Field(default=5, ge=1, le=20)
    priority: int = Field(default=100, ge=0, le=10000)


class BulkUploadEnqueue(BaseModel):
    items: List[UploadEnqueue]


class UploadOut(BaseModel):
    id: int
    mapping_id: Optional[int] = None
    source_channel_id: Optional[int] = None
    youtube_channel_id: Optional[int] = None
    source_video_id: Optional[str] = None
    video_url: str
    title: Optional[str] = None
    visibility: Visibility
    status: QueueStatus
    attempts: int
    max_attempts: int
    scheduled_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    next_retry_at: Optional[datetime] = None
    result_url: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime


class UploadLogOut(BaseModel):
    id: int
    queue_id: int
    level: str
    event: str
    message: Optional[str] = None
    context: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


# ---------- Analytics ---------------------------------------------------

class AnalyticsOverview(BaseModel):
    total_source_channels: int
    active_source_channels: int
    total_youtube_channels: int
    connected_youtube_channels: int
    total_mappings: int
    active_mappings: int
    queue_totals: Dict[str, int]
    success_rate_7d: float
    uploads_last_24h: int


class ChannelPerformance(BaseModel):
    youtube_channel_id: int
    label: str
    uploads_done: int
    uploads_failed: int
    uploads_pending: int
    success_rate: float
    last_used_at: Optional[datetime] = None
