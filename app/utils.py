import base64
import hashlib
import hmac
import json
import logging
import os
import re
import tempfile
from pathlib import Path

from cryptography.fernet import Fernet
from yt_dlp import YoutubeDL

from app.config import settings

YOUTUBE_URL_PATTERN = r"^(https?://)?(www\.)?(youtube\.com/watch\?v=|youtu\.be/).+"
logger = logging.getLogger(__name__)
_COOKIE_FILE_CACHE: Path | None = None
_YOUTUBE_BOT_CHECK_MSG = "Sign in to confirm you"


def _resolve_cookie_file() -> str | None:
    global _COOKIE_FILE_CACHE

    if settings.youtube_cookies_file:
        file_path = Path(settings.youtube_cookies_file).expanduser()
        if file_path.exists():
            return str(file_path)
        logger.warning("YOUTUBE_COOKIES_FILE was set but file not found: %s", file_path)

    if _COOKIE_FILE_CACHE and _COOKIE_FILE_CACHE.exists():
        return str(_COOKIE_FILE_CACHE)

    cookie_data: str | None = None
    if settings.youtube_cookies:
        cookie_data = settings.youtube_cookies.strip()
    elif settings.youtube_cookies_base64:
        try:
            cookie_data = base64.b64decode(settings.youtube_cookies_base64).decode("utf-8").strip()
        except Exception as exc:
            logger.error("Failed to decode YOUTUBE_COOKIES_BASE64: %s", exc)
            return None

    if not cookie_data:
        return None

    cookie_dir = Path(tempfile.gettempdir()) / "youtube-auto"
    cookie_dir.mkdir(parents=True, exist_ok=True)
    cookie_file = cookie_dir / "youtube-cookies.txt"
    cookie_file.write_text(f"{cookie_data}\n", encoding="utf-8")
    _COOKIE_FILE_CACHE = cookie_file
    return str(cookie_file)


def _apply_youtube_auth_options(options: dict) -> None:
    # Prefer clients that commonly avoid stricter web anti-bot checks.
    options["extractor_args"] = {"youtube": {"player_client": ["android", "web"]}}
    cookie_file = _resolve_cookie_file()
    if cookie_file:
        options["cookiefile"] = cookie_file


def _format_yt_dlp_error(prefix: str, error: Exception) -> ValueError:
    error_text = str(error)
    if _YOUTUBE_BOT_CHECK_MSG in error_text:
        cookie_file = _resolve_cookie_file()
        if cookie_file:
            return ValueError(f"{prefix}: {error_text}")
        return ValueError(
            f"{prefix}: {error_text} Configure YOUTUBE_COOKIES_FILE or YOUTUBE_COOKIES_BASE64 in Railway."
        )
    return ValueError(f"{prefix}: {error_text}")


def get_fernet() -> Fernet:
    key_bytes = settings.secret_key.encode()
    if len(key_bytes) != 44:
        raise ValueError("SECRET_KEY must be a 44-byte URL-safe base64 string for Fernet encryption")
    return Fernet(key_bytes)


def encrypt_data(data: dict) -> str:
    payload = json.dumps(data).encode("utf-8")
    return get_fernet().encrypt(payload).decode("utf-8")


def decrypt_data(token: str) -> dict:
    payload = get_fernet().decrypt(token.encode("utf-8"))
    return json.loads(payload.decode("utf-8"))


def create_oauth_state(telegram_id: int) -> str:
    message = str(telegram_id).encode("utf-8")
    signature = hmac.new(settings.secret_key.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return f"{telegram_id}:{signature}"


def parse_oauth_state(state: str) -> int:
    try:
        telegram_id, signature = state.split(":", 1)
    except ValueError:
        raise ValueError("Invalid OAuth state")
    expected = hmac.new(settings.secret_key.encode("utf-8"), telegram_id.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise ValueError("Invalid OAuth state signature")
    return int(telegram_id)


def validate_youtube_url(url: str) -> bool:
    return re.match(YOUTUBE_URL_PATTERN, url.strip()) is not None


def extract_video_info(video_url: str) -> dict:
    options = {
        "quiet": False,  # Show errors
        "skip_download": True,
        "ignoreerrors": False,  # Raise on errors
        "no_warnings": False,
        "socket_timeout": 30,
        "http_chunk_size": 1024 * 1024,
    }
    _apply_youtube_auth_options(options)
    try:
        with YoutubeDL(options) as ydl:
            info = ydl.extract_info(video_url, download=False)
        if info is None:
            raise ValueError("Unable to fetch YouTube metadata - video not found or private")
        return {
            "title": info.get("title", "Untitled video"),
            "description": info.get("description", ""),
        }
    except Exception as e:
        raise _format_yt_dlp_error("Failed to extract video info", e)


def download_video(video_url: str, output_dir: str) -> str:
    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)
    output_template = str(output_dir_path / "%(id)s.%(ext)s")

    # Find ffmpeg executable
    ffmpeg_path = None
    possible_paths = [
        r"C:\Users\m2hga\AppData\Local\Microsoft\WinGet\Packages\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-8.1-full_build\bin\ffmpeg.exe",
        "ffmpeg",  # If in PATH
    ]
    for path in possible_paths:
        if Path(path).exists() or path == "ffmpeg":
            try:
                import subprocess
                subprocess.run([path, "-version"], capture_output=True, check=True)
                ffmpeg_path = path
                break
            except:
                continue

    ydl_opts = {
        "outtmpl": output_template,
        "noplaylist": True,
        "quiet": False,  # Show progress
        "no_warnings": False,
        "socket_timeout": 60,
        "http_chunk_size": 1024 * 1024,
    }
    _apply_youtube_auth_options(ydl_opts)

    if ffmpeg_path:
        ydl_opts["format"] = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"
        ydl_opts["merge_output_format"] = "mp4"
        ydl_opts["ffmpeg_location"] = ffmpeg_path
    else:
        # Fallback when ffmpeg is unavailable (common on minimal PaaS images):
        # download a single progressive stream that doesn't require merge.
        ydl_opts["format"] = "best[ext=mp4]/best"
        logger.warning("ffmpeg not found; using single-stream download format (lower max quality possible)")

    try:
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=True)
            if info is None:
                raise ValueError("Download failed - no video info")
            filename = ydl.prepare_filename(info)
            if not Path(filename).exists():
                raise ValueError(f"Downloaded file not found: {filename}")
        return filename
    except Exception as e:
        raise _format_yt_dlp_error("Download failed", e)


def remove_file(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass
