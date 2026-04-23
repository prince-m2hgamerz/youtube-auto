import logging
import threading
import time
from datetime import datetime, timedelta

from app.channel_copier import process_source_channel_uploads
from app.supabase_client import get_app_settings

logger = logging.getLogger(__name__)
_scheduler_thread: threading.Thread | None = None

DEFAULT_TIMES = "07:15,19:15"


def _parse_schedule_times() -> list[tuple[int, int]]:
    s = get_app_settings()
    raw = s.get("auto_upload_times", DEFAULT_TIMES) if s else DEFAULT_TIMES
    if isinstance(raw, list) and len(raw) == 2:
        return [(int(raw[0][0]), int(raw[0][1])), (int(raw[1][0]), int(raw[1][1]))]
    if isinstance(raw, str):
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        result = []
        for part in parts:
            try:
                h, m = part.split(":")
                result.append((int(h), int(m)))
            except Exception:
                continue
        if result:
            return result
    return [(7, 15), (19, 15)]


def _seconds_until(hour: int, minute: int) -> float:
    now = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def _scheduler_loop():
    logger.info("Scheduler loop started")
    while True:
        times = _parse_schedule_times()
        for hour, minute in times:
            sleep_seconds = _seconds_until(hour, minute)
            logger.info(f"Scheduler sleeping {sleep_seconds / 60:.1f} minutes until {hour:02d}:{minute:02d}")
            time.sleep(sleep_seconds)
            try:
                process_source_channel_uploads()
            except Exception as exc:
                logger.error(f"Scheduled upload ({hour:02d}:{minute:02d}) failed: {exc}", exc_info=True)


def start_scheduler():
    global _scheduler_thread
    if _scheduler_thread and _scheduler_thread.is_alive():
        logger.info("Scheduler already running")
        return
    _scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True)
    _scheduler_thread.start()
    logger.info("Scheduler thread started")
