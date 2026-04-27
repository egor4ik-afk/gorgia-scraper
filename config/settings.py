from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # === База данных (Neon) ===
    DATABASE_URL: str

    # === Vercel Blob ===
    # Dashboard → Storage → Blob → .env.local → BLOB_READ_WRITE_TOKEN
    VERCEL_BLOB_TOKEN: str = ""

    # === Telegram ===
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = ""

    # === Gorgia.ge ===
    # Пусто = полный обход через sitemap (рекомендуется)
    # Или конкретные URL категорий через запятую
    GORGIA_CATEGORY_URLS: str = ""

    # === Поведение агента ===
    REQUEST_DELAY: float = 1.5    # задержка между запросами (сек)
    MAX_RETRIES: int = 3           # повторы при ошибке
    CONCURRENT_SCRAPERS: int = 3   # параллельных воркеров

    # Порог изменения цены для уведомления (0.05 = 5%)
    PRICE_CHANGE_THRESHOLD: float = 0.05

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
