"""Scalable queue worker: handles bulk uploads, retries, rate limits, and concurrency."""
from __future__ import annotations

import logging
import queue
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from app.admin import db
from app.admin.youtube_service import process_upload_item

logger = logging.getLogger(__name__)

# Configurable worker settings
MAX_WORKERS = int(__import__("os").getenv("UPLOAD_WORKERS", "4"))
RETRY_BASE_SECONDS = 30
RETRY_MAX_SECONDS = 3600
MAX_RETRIES_DEFAULT = 5

# In-memory priority queue for hot-path scheduling
# Items: (priority, created_at, item_id)
_job_queue: queue.PriorityQueue[tuple[int, str, int]] = queue.PriorityQueue()
_worker_threads: List[threading.Thread] = []
_executor: Optional[ThreadPoolExecutor] = None
_shutdown_event = threading.Event()


def _calculate_backoff(attempts: int) -> int:
    """Exponential backoff with jitter."""
    import random
    base = min(RETRY_BASE_SECONDS * (2 ** (attempts - 1)), RETRY_MAX_SECONDS)
    jitter = random.randint(0, base // 2)
    return base + jitter


def _should_retry(item: Dict[str, Any]) -> bool:
    attempts = item.get("attempts", 0) or 0
    max_attempts = item.get("max_attempts", MAX_RETRIES_DEFAULT) or MAX_RETRIES_DEFAULT
    return attempts < max_attempts


def _schedule_retry(item_id: int) -> None:
    item = db.get_upload_item(item_id)
    if not item:
        return
    attempts = (item.get("attempts", 0) or 0) + 1
    if attempts >= (item.get("max_attempts", MAX_RETRIES_DEFAULT) or MAX_RETRIES_DEFAULT):
        db.update_upload_item(item_id, {
            "status": "failed",
            "attempts": attempts,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "error_message": (item.get("error_message") or "") + " | Max retries exceeded",
        })
        db.create_upload_log({
            "queue_id": item_id,
            "level": "error",
            "event": "max_retries_exceeded",
            "message": "Upload failed after max retry attempts",
            "context": {"attempts": attempts},
        })
        return

    backoff = _calculate_backoff(attempts)
    next_retry = datetime.now(timezone.utc) + timedelta(seconds=backoff)
    db.update_upload_item(item_id, {
        "status": "retrying",
        "attempts": attempts,
        "next_retry_at": next_retry.isoformat(),
    })
    db.create_upload_log({
        "queue_id": item_id,
        "level": "warning",
        "event": "scheduled_retry",
        "message": f"Retry #{attempts} scheduled at {next_retry.isoformat()}",
        "context": {"backoff_seconds": backoff},
    })


def _process_single_item(item_id: int) -> None:
    item = db.get_upload_item(item_id)
    if not item:
        logger.warning(f"Queue item {item_id} not found")
        return
    if item.get("status") not in ("pending", "scheduled", "retrying"):
        logger.debug(f"Skipping item {item_id} with status {item.get('status')}")
        return

    db.update_upload_item(item_id, {
        "status": "downloading",
        "started_at": datetime.now(timezone.utc).isoformat(),
    })

    try:
        process_upload_item(item)
    except Exception as exc:
        logger.error(f"Item {item_id} failed: {exc}", exc_info=True)
        _schedule_retry(item_id)


def _queue_poller_loop() -> None:
    """Background thread that polls the DB for pending work and feeds the in-memory queue."""
    logger.info("Queue poller started")
    while not _shutdown_event.is_set():
        try:
            # Pull pending + due scheduled/retrying items
            pending = db.get_pending_uploads(limit=200)
            scheduled = db.get_scheduled_uploads(limit=200)
            retrying = db.list_upload_queue(status="retrying", limit=200)
            now = datetime.now(timezone.utc)

            for item in pending:
                _job_queue.put((item.get("priority", 100) or 100, item.get("created_at", ""), item["id"]))

            for item in scheduled:
                _job_queue.put((item.get("priority", 100) or 100, item.get("scheduled_at", ""), item["id"]))

            for item in retrying:
                next_retry = item.get("next_retry_at")
                if next_retry:
                    try:
                        retry_dt = datetime.fromisoformat(str(next_retry).replace("Z", "+00:00"))
                        if retry_dt <= now:
                            _job_queue.put((item.get("priority", 100) or 100, item.get("created_at", ""), item["id"]))
                    except Exception:
                        pass

            # Dedup in-memory queue by draining and re-adding unique items
            seen: set[int] = set()
            deduped: List[tuple[int, str, int]] = []
            while not _job_queue.empty() and len(deduped) < 500:
                try:
                    pri, created, iid = _job_queue.get_nowait()
                    if iid not in seen:
                        seen.add(iid)
                        deduped.append((pri, created, iid))
                except queue.Empty:
                    break
            for entry in deduped:
                _job_queue.put(entry)

        except Exception as exc:
            logger.error(f"Queue poller error: {exc}", exc_info=True)

        # Sleep before next poll
        _shutdown_event.wait(15)


def _worker_loop() -> None:
    """Individual worker that pulls from the priority queue and processes items."""
    while not _shutdown_event.is_set():
        try:
            priority, created_at, item_id = _job_queue.get(timeout=5)
        except queue.Empty:
            continue
        try:
            _process_single_item(item_id)
        except Exception as exc:
            logger.error(f"Unhandled worker error for item {item_id}: {exc}", exc_info=True)
        finally:
            try:
                _job_queue.task_done()
            except ValueError:
                pass


def start_queue_worker(num_workers: int = MAX_WORKERS) -> None:
    """Start the scalable queue worker with a thread pool."""
    global _worker_threads, _executor
    if _worker_threads and any(t.is_alive() for t in _worker_threads):
        logger.info("Queue worker already running")
        return

    _shutdown_event.clear()

    # Start poller thread
    poller = threading.Thread(target=_queue_poller_loop, daemon=True, name="queue-poller")
    poller.start()
    _worker_threads.append(poller)

    # Start worker threads
    for i in range(num_workers):
        t = threading.Thread(target=_worker_loop, daemon=True, name=f"upload-worker-{i}")
        t.start()
        _worker_threads.append(t)

    logger.info(f"Queue worker started with {num_workers} workers")

    # Also start a thread-pool executor for parallel processing if needed
    _executor = ThreadPoolExecutor(max_workers=num_workers, thread_name_prefix="upload-exec-")


def stop_queue_worker() -> None:
    """Gracefully signal shutdown."""
    _shutdown_event.set()
    if _executor:
        _executor.shutdown(wait=False)
    logger.info("Queue worker shutdown signaled")


def enqueue_upload_item(item_id: int) -> None:
    """Manually enqueue an item (used by API / scheduler)."""
    item = db.get_upload_item(item_id)
    if item:
        _job_queue.put((item.get("priority", 100) or 100, item.get("created_at", ""), item_id))


def get_worker_status() -> Dict[str, Any]:
    return {
        "running": bool(_worker_threads and any(t.is_alive() for t in _worker_threads)),
        "queue_size": _job_queue.qsize(),
        "max_workers": MAX_WORKERS,
        "shutdown_requested": _shutdown_event.is_set(),
    }
