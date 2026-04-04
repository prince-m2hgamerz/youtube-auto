import os
import uvicorn

os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("app.api:app", host="0.0.0.0", port=port)
