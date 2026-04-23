import json
import logging
import threading
import time
from datetime import datetime, timedelta

from app.channel_copier import process_source_channel_uploads
from app.supabase_client import get_bot_settings, set_bot_settings

logger = logging.getLogger(__name__)
_scheduler_thread: threading.Thread | None = None

DEFAULT_TIMES = "07:15,19:15"
SLEEP_INTERVAL = 60  # wake up every minute to survive Railway free-tier sleeps


def _parse_schedule_times() -> list[tuple[int, int]]:
    s = get_bot_settings()
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


def _get_last_run_dates() -> dict[str, str]:
    s = get_bot_settings()
    raw = s.get("scheduler_last_runs", "{}")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            pass
    return {}


def _set_last_run(time_key: str) -> None:
    runs = _get_last_run_dates()
    runs[time_key] = datetime.now().strftime("%Y-%m-%d")
    set_bot_settings({"scheduler_last_runs": runs})


def _should_run_now(hour: int, minute: int) -> bool:
    now = datetime.now()
    # Only run if current time is within the target minute window
    if now.hour != hour or now.minute != minute:
        return False
    time_key = f"{hour:02d}:{minute:02d}"
    last_runs = _get_last_run_dates()
    today = now.strftime("%Y-%m-%d")
    if last_runs.get(time_key) == today:
        return False  # already ran today
    return True


def _scheduler_loop():
    logger.info("Scheduler loop started")
    while True:
        times = _parse_schedule_times()
        for hour, minute in times:
            if _should_run_now(hour, minute):
                logger.info(f"Triggering scheduled upload for {hour:02d}:{minute:02d}")
                try:
                    process_source_channel_uploads()
                    _set_last_run(f"{hour:02d}:{minute:02d}")
                except Exception as exc:
                    logger.error(f"Scheduled upload ({hour:02d}:{minute:02d}) failed: {exc}", exc_info=True)
        time.sleep(SLEEP_INTERVAL)


def start_scheduler():
    global _scheduler_thread
    if _scheduler_thread and _scheduler_thread.is_alive():
        logger.info("Scheduler already running")
        return
    _scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True)
    _scheduler_thread.start()
    logger.info("Scheduler thread started")
