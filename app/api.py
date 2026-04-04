from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
import requests

from app.config import settings
from app.supabase_client import upsert_user
from app.utils import parse_oauth_state
from app.youtube_client import fetch_credentials, serialize_credentials

app = FastAPI(title="YouTube Auto Bot API")


@app.on_event("startup")
def on_startup() -> None:
    return None


@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "YouTube Auto Bot API"}


@app.get("/oauth2callback", response_class=HTMLResponse)
def oauth2callback(request: Request, code: str = Query(None), state: str = Query(None), error: str = Query(None)):
    try:
        if error:
            raise HTTPException(status_code=400, detail=f"OAuth authorization failed: {error}")

        if not code or not state:
            raise HTTPException(status_code=400, detail="Missing OAuth code or state")

        telegram_id = parse_oauth_state(state)
        credentials = fetch_credentials(str(request.url))
        encrypted = serialize_credentials(credentials)
        upsert_user(str(telegram_id), oauth_credentials=encrypted, is_connected=True)
        
        # Send Telegram notification
        try:
            url = f"https://api.telegram.org/bot{settings.telegram_token}/sendMessage"
            requests.post(url, json={"chat_id": str(telegram_id), "text": "✅ YouTube channel connected! You can now use /download to upload videos."})
        except Exception as notify_err:
            print(f"Failed to send notification: {notify_err}")

        return HTMLResponse(
            content="<h1>Connected!</h1><p>Your YouTube channel is now linked to the bot. You can return to Telegram.</p>",
            status_code=200,
        )
    except Exception as e:
        # Log the error and return a user-friendly message
        import traceback
        print("OAuth callback error:", traceback.format_exc())
        return HTMLResponse(
            content=f"<h1>Error</h1><p>Failed to connect: {str(e)}</p><pre>{traceback.format_exc()}</pre>",
            status_code=500,
        )
