import base64
import hashlib
import hmac
import json
import logging
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path

from cryptography.fernet import Fernet
from yt_dlp import YoutubeDL

from app.config import settings

YOUTUBE_URL_PATTERN = r"^(https?://)?(www\.)?(youtube\.com/watch\?v=|youtu\.be/).+"
logger = logging.getLogger(__name__)
_COOKIE_FILE_CACHE: Path | None = None
_YOUTUBE_BOT_CHECK_MSG = "Sign in to confirm you"
_FORMAT_NOT_AVAILABLE_MSG = "Requested format is not available"
_RATE_LIMIT_MSGS = ("HTTP Error 429", "Too Many Requests")
_FFMPEG_MISSING_MSGS = ("ffmpeg is not installed", "ffprobe and ffmpeg not found")


def _is_format_unavailable_error(error: Exception) -> bool:
    return _FORMAT_NOT_AVAILABLE_MSG in str(error)


def _is_rate_limited_error(error: Exception) -> bool:
    text = str(error)
    return any(marker in text for marker in _RATE_LIMIT_MSGS)


def _is_ffmpeg_missing_error(error: Exception) -> bool:
    text = str(error).lower()
    return any(marker in text for marker in _FFMPEG_MISSING_MSGS)


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
    youtube_args: dict[str, list[str]] = {
        # Include formats that may be tagged as missing POT; without this, yt-dlp can
        # end up exposing only storyboard/image formats on some YouTube responses.
        "formats": ["missing_pot"],
        # Ask yt-dlp to fetch PO tokens when needed.
        "fetch_pot": ["always"],
    }
    if settings.youtube_po_token:
        youtube_args["po_token"] = [settings.youtube_po_token]

    options["extractor_args"] = {"youtube": youtube_args}
    options["js_runtimes"] = {
        "node": {"path": settings.youtube_js_runtime_path} if settings.youtube_js_runtime_path else {},
    }
    # Allow yt-dlp to fetch EJS challenge solver scripts.
    options["remote_components"] = {"ejs:github", "ejs:npm"}
    options["extractor_retries"] = 5
    options["retries"] = 5
    options["fragment_retries"] = 5

    cookie_file = _resolve_cookie_file()
    if cookie_file:
        options["cookiefile"] = cookie_file


def _format_yt_dlp_error(prefix: str, error: Exception) -> ValueError:
    error_text = str(error)
    if _is_rate_limited_error(error):
        return ValueError(
            f"{prefix}: {error_text}. YouTube is rate-limiting this server IP. Retry later and keep cookies fresh."
        )
    if _is_ffmpeg_missing_error(error):
        return ValueError(
            f"{prefix}: {error_text}. ffmpeg is missing in runtime; install ffmpeg or use non-merge formats."
        )
    if _is_format_unavailable_error(error):
        return ValueError(
            f"{prefix}: {error_text}. Tried alternate download formats but none were available for this video."
        )
    if _YOUTUBE_BOT_CHECK_MSG in error_text:
        cookie_file = _resolve_cookie_file()
        if cookie_file:
            return ValueError(f"{prefix}: {error_text}")
        return ValueError(
            f"{prefix}: {error_text} Configure YOUTUBE_COOKIES_FILE or YOUTUBE_COOKIES_BASE64 in Railway."
        )
    return ValueError(f"{prefix}: {error_text}")


def _detect_ffmpeg() -> str | None:
    candidates: list[str] = []
    env_path = os.getenv("FFMPEG_BINARY")
    if env_path:
        candidates.append(env_path)

    if os.name == "nt":
        candidates.append(
            r"C:\Users\m2hga\AppData\Local\Microsoft\WinGet\Packages\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-8.1-full_build\bin\ffmpeg.exe"
        )

    candidates.append("ffmpeg")

    for candidate in candidates:
        try:
            subprocess.run([candidate, "-version"], capture_output=True, check=True, timeout=5)
            return candidate
        except Exception:
            continue
    return None


def _extract_video_payload(info: dict | None) -> dict | None:
    if info is None:
        return None
    entries = info.get("entries")
    if entries:
        for item in entries:
            if item:
                return item
        return None
    return info


def _resolve_downloaded_file(info: dict, ydl: YoutubeDL, output_dir: Path) -> str:
    candidates: list[Path] = []

    try:
        candidates.append(Path(ydl.prepare_filename(info)))
    except Exception:
        pass

    for item in info.get("requested_downloads") or []:
        filepath = item.get("filepath")
        if filepath:
            candidates.append(Path(filepath))

    video_id = info.get("id")
    if video_id:
        candidates.extend(output_dir.glob(f"{video_id}.*"))

    for path in candidates:
        if path.exists() and path.is_file():
            return str(path)

    raise ValueError("Downloaded file not found after extraction")


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
        "ignoreconfig": True,  # Ignore host-level yt-dlp config that may enforce incompatible formats
        "no_warnings": False,
        "socket_timeout": 30,
        "http_chunk_size": 1024 * 1024,
    }
    _apply_youtube_auth_options(options)
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with YoutubeDL(options) as ydl:
                info = ydl.extract_info(video_url, download=False, process=False)
            payload = _extract_video_payload(info)
            if payload is None:
                raise ValueError("Unable to fetch YouTube metadata - video not found or private")
            return {
                "title": payload.get("title", "Untitled video"),
                "description": payload.get("description", ""),
            }
        except Exception as exc:
            last_error = exc
            if _is_rate_limited_error(exc) and attempt < 2:
                wait_seconds = 2 * (attempt + 1)
                logger.warning("YouTube metadata request hit 429; retrying in %ss", wait_seconds)
                time.sleep(wait_seconds)
                continue
            raise _format_yt_dlp_error("Failed to extract video info", exc)

    if last_error is not None:
        raise _format_yt_dlp_error("Failed to extract video info", last_error)
    raise ValueError("Failed to extract video info")


def download_video(video_url: str, output_dir: str) -> str:
    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)
    output_template = str(output_dir_path / "%(id)s.%(ext)s")

    ffmpeg_path = _detect_ffmpeg()

    ydl_opts = {
        "outtmpl": output_template,
        "noplaylist": True,
        "quiet": False,  # Show progress
        "no_warnings": False,
        "ignoreconfig": True,  # Ignore host-level yt-dlp config that may force unavailable formats
        "socket_timeout": 60,
        "http_chunk_size": 1024 * 1024,
    }
    _apply_youtube_auth_options(ydl_opts)

    progressive_candidates = [
        "best[ext=mp4][vcodec!=none][acodec!=none]/best[vcodec!=none][acodec!=none]",
        "18/best[vcodec!=none][acodec!=none]",
        "best",
    ]
    merge_candidates = [
        "bestvideo*[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best[ext=mp4]/best",
        "bestvideo+bestaudio/best[ext=mp4]/best",
    ]

    format_candidates: list[str]
    if ffmpeg_path:
        ydl_opts["merge_output_format"] = "mp4"
        ydl_opts["ffmpeg_location"] = ffmpeg_path
        # Prefer progressive formats first even when ffmpeg exists, then try merge formats.
        format_candidates = [*progressive_candidates, *merge_candidates]
    else:
        # Fallback when ffmpeg is unavailable (common on minimal PaaS images):
        # download a single progressive stream that doesn't require merge.
        logger.warning("ffmpeg not found; using single-stream download format (lower max quality possible)")
        format_candidates = progressive_candidates

    last_error: Exception | None = None
    for fmt in format_candidates:
        for attempt in range(3):
            attempt_opts = {**ydl_opts, "format": fmt}
            try:
                with YoutubeDL(attempt_opts) as ydl:
                    info = ydl.extract_info(video_url, download=True)
                    payload = _extract_video_payload(info)
                    if payload is None:
                        raise ValueError("Download failed - no video info")
                    return _resolve_downloaded_file(payload, ydl, output_dir_path)
            except Exception as exc:
                last_error = exc
                if _is_ffmpeg_missing_error(exc):
                    logger.warning("ffmpeg unavailable while processing format %s; skipping merge-required formats", fmt)
                    # Any format with '+' requires merge; move to next candidate.
                    break
                if _is_format_unavailable_error(exc):
                    logger.warning("Format selector failed, trying fallback: %s", fmt)
                    break
                if _is_rate_limited_error(exc) and attempt < 2:
                    wait_seconds = 2 * (attempt + 1)
                    logger.warning("YouTube download hit 429 for format %s; retrying in %ss", fmt, wait_seconds)
                    time.sleep(wait_seconds)
                    continue
                if _is_rate_limited_error(exc):
                    logger.warning("YouTube kept rate-limiting format %s; moving to fallback format", fmt)
                    break
                raise _format_yt_dlp_error("Download failed", exc)

    if last_error is not None:
        raise _format_yt_dlp_error("Download failed", last_error)
    raise ValueError("Download failed: no compatible format found")


def remove_file(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass
