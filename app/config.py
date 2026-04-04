from pydantic import AnyUrl, validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    telegram_token: str
    supabase_url: AnyUrl
    supabase_service_key: str
    secret_key: str
    google_client_id: str
    google_client_secret: str
    oauth_redirect_uri: AnyUrl
    base_url: AnyUrl
    environment: str = "development"

    @validator("secret_key")
    def validate_secret_key(cls, value: str) -> str:
        if len(value.encode()) != 44:
            raise ValueError("SECRET_KEY must be exactly 44 url-safe base64 characters for Fernet encryption")
        return value

    class Config:
        env_file = ".env"


settings = Settings()