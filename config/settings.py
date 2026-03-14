from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # === База данных (Neon) ===
    DATABASE_URL: str

    # === Vercel Blob ===
    VERCEL_BLOB_TOKEN: str = ""

    # === Telegram ===
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = ""

    # === Gorgia.ge ===
    # Пусто = все категории из GORGIA_CATEGORIES в scrapers/gorgia.py
    # Или конкретные URL через запятую
    GORGIA_CATEGORY_URLS: str = ""

    # === Поведение агента ===
    REQUEST_DELAY: float = 1.5
    MAX_RETRIES: int = 3

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
