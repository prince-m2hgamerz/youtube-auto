# Scalable Admin System

A production-ready admin backend for managing multiple source channels, YouTube channels, and automated content publishing with role-based access control, queue-based uploads, scheduling, and analytics.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        FastAPI App                               │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────────────┐  │
│  │  Admin API  │  │ Queue Worker │  │   Mapping Scheduler     │  │
│  │  (/admin/*) │  │ (threadpool) │  │   (cron-based)          │  │
│  └──────┬──────┘  └──────┬───────┘  └─────────────────────────┘  │
│         │                │                                       │
│  ┌──────▼──────┐  ┌──────▼───────┐                              │
│  │  JWT Auth   │  │  OAuth       │                              │
│  │  + RBAC     │  │  Refresh     │                              │
│  └─────────────┘  └──────────────┘                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────▼──────────┐
                    │    Supabase SQL    │
                    │  (Postgres-backed) │
                    └────────────────────┘
```

---

## Database Schema

Run `sql/create_admin_system.sql` in your Supabase SQL Editor to create:

1. **`admin_users`** — RBAC users (super_admin, admin, operator, viewer)
2. **`source_channels`** — Content sources (YouTube channels, RSS, custom)
3. **`youtube_channels`** — Connected YouTube accounts with encrypted OAuth tokens
4. **`channel_mappings`** — Many-to-many relationships with per-mapping scheduling rules
5. **`upload_queue`** — Persistent job queue with retry logic and priorities
6. **`upload_logs`** — Per-job event trail for debugging and analytics
7. **`admin_audit_log`** — Who-did-what tracking
8. **`seen_source_videos`** — Deduplication table
9. **`v_upload_stats`** — Analytics materialized view

---

## Role-Based Access Control (RBAC)

| Role | Permissions |
|------|-------------|
| `super_admin` | Full access: manage users, delete anything |
| `admin` | Manage source/YouTube channels, mappings, cancel uploads, view audit logs |
| `operator` | Create/update sources, mappings, enqueue uploads, retry jobs |
| `viewer` | Read-only: view channels, mappings, queue, analytics |

All endpoints require a Bearer token in the `Authorization` header.

---

## API Endpoints

### Auth
| Method | Endpoint | Role | Description |
|--------|----------|------|-------------|
| POST | `/admin/auth/register` | super_admin | Create new admin user |
| POST | `/admin/auth/login` | — | Login, returns JWT |
| GET | `/admin/auth/me` | any | Current user profile |

### Admin Users
| Method | Endpoint | Role | Description |
|--------|----------|------|-------------|
| GET | `/admin/users` | admin | List admin users |
| PATCH | `/admin/users/{id}` | super_admin | Update user |
| DELETE | `/admin/users/{id}` | super_admin | Delete user |

### Source Channels
| Method | Endpoint | Role | Description |
|--------|----------|------|-------------|
| POST | `/admin/source-channels` | operator | Create source channel |
| GET | `/admin/source-channels` | viewer | List source channels |
| GET | `/admin/source-channels/{id}` | viewer | Get source channel |
| PATCH | `/admin/source-channels/{id}` | operator | Update source channel |
| DELETE | `/admin/source-channels/{id}` | admin | Delete source channel |

### YouTube Channels
| Method | Endpoint | Role | Description |
|--------|----------|------|-------------|
| POST | `/admin/youtube-channels/connect` | operator | Init OAuth flow |
| GET | `/admin/youtube-channels` | viewer | List connected channels |
| GET | `/admin/youtube-channels/{id}` | viewer | Get channel (credentials hidden) |
| PATCH | `/admin/youtube-channels/{id}` | operator | Update channel settings |
| DELETE | `/admin/youtube-channels/{id}` | admin | Remove channel |

### Mappings (Many-to-Many)
| Method | Endpoint | Role | Description |
|--------|----------|------|-------------|
| POST | `/admin/mappings` | operator | Create mapping |
| POST | `/admin/mappings/bulk` | operator | Bulk create mappings |
| GET | `/admin/mappings` | viewer | List mappings |
| GET | `/admin/mappings/{id}` | viewer | Get mapping |
| PATCH | `/admin/mappings/{id}` | operator | Update mapping |
| DELETE | `/admin/mappings/{id}` | admin | Delete mapping |

### Upload Queue
| Method | Endpoint | Role | Description |
|--------|----------|------|-------------|
| POST | `/admin/uploads/enqueue` | operator | Enqueue single upload |
| POST | `/admin/uploads/enqueue-bulk` | operator | Enqueue bulk uploads |
| GET | `/admin/uploads` | viewer | List queue items |
| GET | `/admin/uploads/{id}` | viewer | Get queue item |
| POST | `/admin/uploads/{id}/retry` | operator | Retry failed item |
| DELETE | `/admin/uploads/{id}` | admin | Cancel pending item |
| GET | `/admin/uploads/{id}/logs` | viewer | Get item logs |

### Analytics
| Method | Endpoint | Role | Description |
|--------|----------|------|-------------|
| GET | `/admin/analytics/overview` | viewer | System-wide stats |
| GET | `/admin/analytics/channels/{id}/performance` | viewer | Channel performance |
| GET | `/admin/audit-logs` | admin | Admin audit trail |
| GET | `/admin/worker/status` | viewer | Queue worker health |

---

## OAuth Flow (YouTube Channel Connection)

1. **Init**: `POST /admin/youtube-channels/connect` with a label
2. **Redirect**: Admin opens the returned `oauth_url` and authorizes
3. **Callback**: Google redirects to `/oauth2callback` with code + state
4. **Store**: System creates a `youtube_channels` row with **Fernet-encrypted** credentials

Token refresh happens automatically on upload. Expired tokens mark the channel as `expired`.

---

## Queue Worker

- **Thread-pool based**: Configurable via `UPLOAD_WORKERS` env (default 4)
- **Priority queue**: Lower `priority` value = higher priority
- **Exponential backoff**: `30s * 2^(attempt-1)` with jitter for retries
- **Rate limiting**: Per-channel daily quota (`daily_quota_limit`)
- **Auto-poll**: DB is polled every 15s for pending/scheduled/retrying work
- **Dedup**: In-memory queue deduplication

---

## Mapping Scheduler

- Runs every minute
- Evaluates `schedule_cron` as comma-separated `HH:MM` times
- Respects timezone (`schedule_timezone`)
- Fetches source videos, deduplicates, enqueues up to `max_per_run` per mapping
- Respects per-channel daily upload quotas

---

## Security

- **Fernet encryption** for OAuth tokens (44-byte SECRET_KEY)
- **bcrypt** for admin passwords
- **HMAC-signed JWT** tokens (24h expiry)
- **Audit logging** for all mutating admin actions
- **OAuth credentials never exposed** in API responses

---

## Environment Variables

All existing variables remain. No new required env vars.

| Variable | Description |
|----------|-------------|
| `SECRET_KEY` | 44-byte Fernet key (also used for JWT signing) |
| `UPLOAD_WORKERS` | (Optional) Queue worker thread count |
| `GOOGLE_CLIENT_ID` | OAuth client ID |
| `GOOGLE_CLIENT_SECRET` | OAuth client secret |
| `OAUTH_REDIRECT_URI` | Must point to `/oauth2callback` |

---

## Setup

1. Run `sql/create_admin_system.sql` in Supabase SQL Editor
2. Install deps: `pip install -r requirements.txt`
3. Create a super_admin via DB or API
4. Start API: `python run_api.py`
5. Queue worker and scheduler auto-start with the API

---

## Quick Start Example

```bash
# 1. Login
TOKEN=$(curl -s -X POST http://localhost:8000/admin/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@example.com","password":"secret"}' \
  | jq -r '.access_token')

# 2. Create a source channel
curl -X POST http://localhost:8000/admin/source-channels \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"Tech Shorts","source_url":"https://www.youtube.com/@techchannel","content_filter":"shorts"}'

# 3. Connect a YouTube channel (open oauth_url in browser)
curl -X POST http://localhost:8000/admin/youtube-channels/connect \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"label":"Main Channel"}'

# 4. Create mapping
curl -X POST http://localhost:8000/admin/mappings \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"source_channel_id":1,"youtube_channel_id":1,"schedule_cron":"07:15,19:15","max_per_run":2}'

# 5. Enqueue a manual upload
curl -X POST http://localhost:8000/admin/uploads/enqueue \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"video_url":"https://www.youtube.com/watch?v=xyz","youtube_channel_id":1,"title":"My Video"}'

# 6. Check analytics
curl http://localhost:8000/admin/analytics/overview \
  -H "Authorization: Bearer $TOKEN"
```
