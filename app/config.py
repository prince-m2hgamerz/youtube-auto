from pydantic import AnyUrl, validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    telegram_token: str
    supabase_url: AnyUrl
    supabase_service_key: str
    admin_ids: str | None = None
    secret_key: str
    google_client_id: str
    google_client_secret: str
    oauth_redirect_uri: AnyUrl
    base_url: AnyUrl
    environment: str = "development"
    youtube_cookies_file: str | None = None
    youtube_cookies_base64: str | None = None
    youtube_cookies: str | None = None
    youtube_js_runtime_path: str | None = None
    youtube_po_token: str | None = None
    payment_currency: str = "XTR"
    payment_provider_token: str | None = None
    paid_plan_price: int = 150
    paid_plan_duration_days: int = 30
    donation_price: int = 50
    paid_plan_title: str = "YouTube Auto Paid Plan"
    paid_plan_description: str = "Unlock higher limits and public visibility uploads."

    @validator("secret_key")
    def validate_secret_key(cls, value: str) -> str:
        if len(value.encode()) != 44:
            raise ValueError("SECRET_KEY must be exactly 44 url-safe base64 characters for Fernet encryption")
        return value

    class Config:
        env_file = ".env"


settings = Settings()
