"""JWT authentication + role-based access control for the admin system."""
from __future__ import annotations

import hashlib
import hmac
import logging
import secrets
import time
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Set

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.config import settings
from app.admin import db

logger = logging.getLogger(__name__)

security = HTTPBearer(auto_error=False)

# Role hierarchy (higher index = more permissions)
ROLE_LEVELS: Dict[str, int] = {
    "viewer": 0,
    "operator": 1,
    "admin": 2,
    "super_admin": 3,
}


def _get_role_level(role: str) -> int:
    return ROLE_LEVELS.get(role, -1)


def _jwt_sign(payload: str) -> str:
    return hmac.new(settings.secret_key.encode(), payload.encode(), hashlib.sha256).hexdigest()[:32]


def _jwt_encode(data: Dict[str, Any], expires_in: int = 86400) -> str:
    """Simple stateless JWT-like token. Format: base64(header).base64(payload).signature"""
    import base64, json
    header = base64.urlsafe_b64encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode()).decode().rstrip("=")
    now = int(time.time())
    payload_data = {**data, "iat": now, "exp": now + expires_in}
    payload_b64 = base64.urlsafe_b64encode(json.dumps(payload_data).encode()).decode().rstrip("=")
    signature = _jwt_sign(f"{header}.{payload_b64}")
    return f"{header}.{payload_b64}.{signature}"


def _jwt_decode(token: str) -> Optional[Dict[str, Any]]:
    import base64, json
    parts = token.split(".")
    if len(parts) != 3:
        return None
    header_b64, payload_b64, signature = parts
    expected = _jwt_sign(f"{header_b64}.{payload_b64}")
    if not hmac.compare_digest(expected, signature):
        return None
    try:
        payload_json = base64.urlsafe_b64decode(payload_b64 + "=" * (4 - len(payload_b64) % 4)).decode()
        payload = json.loads(payload_json)
    except Exception:
        return None
    if payload.get("exp", 0) < int(time.time()):
        return None
    return payload


def hash_password(password: str) -> str:
    import bcrypt
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=12)).decode()


def verify_password(password: str, hashed: str) -> bool:
    import bcrypt
    return bcrypt.checkpw(password.encode(), hashed.encode())


def generate_api_key() -> str:
    return "ak_" + secrets.token_urlsafe(32)


def hash_api_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def create_access_token(admin_id: int, role: str, email: str) -> str:
    return _jwt_encode({"sub": str(admin_id), "role": role, "email": email})


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

async def get_current_admin(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> Dict[str, Any]:
    if not credentials:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing authorization header")
    token = credentials.credentials
    payload = _jwt_decode(token)
    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")
    admin_id = int(payload["sub"])
    admin = db.get_admin_user_by_id(admin_id)
    if not admin or not admin.get("is_active"):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Admin user inactive or deleted")
    return {
        "id": admin["id"],
        "email": admin["email"],
        "role": admin["role"],
        "full_name": admin.get("full_name"),
        "telegram_id": admin.get("telegram_id"),
    }


class RequireRole:
    def __init__(self, min_role: str):
        self.min_level = _get_role_level(min_role)

    async def __call__(self, admin: Dict[str, Any] = Depends(get_current_admin)) -> Dict[str, Any]:
        if _get_role_level(admin["role"]) < self.min_level:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Requires role '{list(ROLE_LEVELS.keys())[self.min_level]}' or higher",
            )
        return admin


# Convenience instances
require_viewer = RequireRole("viewer")
require_operator = RequireRole("operator")
require_admin = RequireRole("admin")
require_super_admin = RequireRole("super_admin")


# ---------------------------------------------------------------------------
# Audit helper
# ---------------------------------------------------------------------------

def audit(request: Request, admin: Dict[str, Any], action: str, target_type: str = "", target_id: str = "", details: Optional[Dict[str, Any]] = None):
    try:
        db.audit_log({
            "admin_id": admin["id"],
            "action": action,
            "target_type": target_type,
            "target_id": target_id,
            "details": details or {},
            "ip_address": request.client.host if request.client else None,
            "user_agent": request.headers.get("user-agent"),
        })
    except Exception as exc:
        logger.warning(f"Audit log failed: {exc}")
