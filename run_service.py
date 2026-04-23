"""Combined runner for Railway deployment — starts both the API server and the Telegram bot."""
import asyncio
import threading
import logging

import uvicorn

from app.api import app as fastapi_app
from app.bot import start_bot

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import os

API_HOST = "0.0.0.0"
API_PORT = int(os.environ.get("PORT", 8000))


def _run_api():
    logger.info(f"Starting API server on {API_HOST}:{API_PORT}")
    uvicorn.run(fastapi_app, host=API_HOST, port=API_PORT, log_level="info")


def main():
    logger.info("=== Starting YouTube Auto Bot Service ===")

    # Start FastAPI in a background thread so bot + API run together
    api_thread = threading.Thread(target=_run_api, daemon=True)
    api_thread.start()

    # Start the Telegram bot (this blocks with asyncio.run)
    start_bot()


if __name__ == "__main__":
    main()
