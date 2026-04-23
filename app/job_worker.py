import os
import queue
import threading
import traceback
import logging
from typing import Optional

import requests

from app.config import settings
from app.scheduler import start_scheduler
from app.supabase_client import get_job, get_pending_jobs, get_user, update_job, update_user_settings
from app.utils import download_video, remove_file, validate_youtube_url
from app.youtube_client import build_youtube_service, deserialize_credentials, upload_video

logger = logging.getLogger(__name__)

TEMP_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tmp")
job_queue: queue.Queue[int] = queue.Queue()
worker_thread: Optional[threading.Thread] = None


def send_telegram_message(chat_id: str, text: str) -> None:
    try:
        url = f"https://api.telegram.org/bot{settings.telegram_token}/sendMessage"
        response = requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=10)
        response.raise_for_status()
        logger.info(f"Message sent to {chat_id}")
    except Exception as e:
        logger.error(f"Failed to send message to {chat_id}: {e}")


def process_job(job_id: int) -> None:
    logger.info(f"Processing job {job_id}")
    job = get_job(job_id)
    if not job:
        logger.error(f"Job {job_id} not found")
        return

    try:
        update_job(job_id, {"status": "downloading"})
        send_telegram_message(job["telegram_id"], "📥 Download started for your video.")
        logger.info(f"Download started for job {job_id}: {job['video_url']}")

        video_path = None
        if not validate_youtube_url(job["video_url"]):
            raise ValueError("Invalid YouTube URL")

        video_path = download_video(job["video_url"], TEMP_DIR)
        logger.info(f"Video downloaded to {video_path}")
        
        update_job(job_id, {"status": "uploading"})
        send_telegram_message(job["telegram_id"], "📤 Uploading video to YouTube...")
        logger.info(f"Starting upload for job {job_id}")

        user = get_user(job["telegram_id"])
        if not user or not user.get("oauth_credentials"):
            raise ValueError("YouTube account not connected")

        credential_data = deserialize_credentials(user["oauth_credentials"])
        youtube = build_youtube_service(credential_data)

        def progress_callback(percent: int) -> None:
            send_telegram_message(job["telegram_id"], f"⏳ Upload progress: {percent}%")

        response = upload_video(
            youtube,
            video_path,
            job.get("title") or "Youtube Auto Upload",
            job.get("description") or "Uploaded with Telegram YouTube bot",
            job.get("visibility", "unlisted"),
            on_progress=progress_callback,
        )

        video_id = response.get("id")
        result_url = f"https://youtu.be/{video_id}" if video_id else None
        update_job(job_id, {"status": "done", "result_url": result_url})
        send_telegram_message(job["telegram_id"], f"✅ Upload complete! {result_url}")
        logger.info(f"Job {job_id} completed successfully: {result_url}")
    except Exception as exc:
        logger.error(f"Job {job_id} failed: {exc}", exc_info=True)
        error_text = str(exc)
        try:
            update_job(job_id, {"status": "failed", "error_message": error_text})
        except Exception as update_err:
            logger.error(f"Failed to persist failed status for job {job_id}: {update_err}", exc_info=True)

        # Detect expired/revoked OAuth token and give a clear message
        if "invalid_grant" in error_text or "expired or revoked" in error_text:
            try:
                update_user_settings(job["telegram_id"], {"is_connected": False, "oauth_credentials": None})
            except Exception as disconnect_err:
                logger.error(f"Failed to auto-disconnect user {job['telegram_id']}: {disconnect_err}")
            send_telegram_message(
                job["telegram_id"],
                "🔒 Your YouTube connection has expired or been revoked.\n\n"
                "Please reconnect your account using /connect",
            )
        else:
            send_telegram_message(job["telegram_id"], f"❌ Upload failed: {exc}")
    finally:
        if 'video_path' in locals() and video_path:
            try:
                remove_file(video_path)
                logger.info(f"Cleaned up video file: {video_path}")
            except Exception as e:
                logger.error(f"Failed to clean up {video_path}: {e}")


def worker_loop() -> None:
    logger.info("Worker loop started")
    while True:
        try:
            job_id = job_queue.get()
            logger.info(f"Processing queued job {job_id}")
            process_job(job_id)
        except Exception as e:
            logger.error(f"Worker loop error: {e}", exc_info=True)
        finally:
            job_queue.task_done()


def enqueue_job(job_id: int) -> None:
    logger.info(f"Enqueueing job {job_id}")
    job_queue.put(job_id)


def start_worker() -> None:
    global worker_thread
    if worker_thread and worker_thread.is_alive():
        logger.info("Worker thread already running")
        return
    worker_thread = threading.Thread(target=worker_loop, daemon=True)
    worker_thread.start()
    logger.info("Worker thread started")
    
    # Start the auto-upload scheduler for source-channel shorts
    try:
        start_scheduler()
    except Exception as exc:
        logger.error(f"Failed to start scheduler: {exc}", exc_info=True)
    
    # Requeue any pending jobs
    try:
        pending = get_pending_jobs()
        logger.info(f"Found {len(pending)} pending jobs")
        for job in pending:
            enqueue_job(job["id"])
    except Exception as exc:
        logger.error(f"Failed to load pending jobs from Supabase: {exc}")

