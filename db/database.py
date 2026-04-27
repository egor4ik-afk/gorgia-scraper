import logging
import asyncpg
from config.settings import settings

logger = logging.getLogger("database")


async def init_db():
    conn = await asyncpg.connect(settings.DATABASE_URL)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id              SERIAL PRIMARY KEY,
            external_id     TEXT NOT NULL,
            source          TEXT NOT NULL DEFAULT 'gorgia',
            source_url      TEXT,

            -- Основное поле (ru -> en -> ka, для совместимости)
            name            TEXT NOT NULL DEFAULT '',

            -- Трёхязычные названия
            name_ka         TEXT,
            name_ru         TEXT,
            name_en         TEXT,

            -- Описания
            description     TEXT,
            description_ka  TEXT,
            description_ru  TEXT,
            description_en  TEXT,

            -- Наличие (Georgian оригинал + перевод)
            availability_ka TEXT,
            availability_ru TEXT,

            -- Категории (все три языка)
            category_ka     TEXT,
            category_ru     TEXT,
            category_en     TEXT,
            sub_category_ka TEXT,
            sub_category_ru TEXT,
            sub_category_en TEXT,

            -- Артикул / SKU
            sku             TEXT,

            -- Коммерция
            price           NUMERIC(10,2),
            currency        TEXT DEFAULT 'GEL',
            in_stock        BOOLEAN DEFAULT TRUE,

            -- Фото в Vercel Blob (JSON-массив URL)
            images          JSONB DEFAULT '[]',

            -- Всё дополнительное: характеристики, бренд, gtin, хлебные крошки...
            other           JSONB DEFAULT '{}',

            created_at      TIMESTAMPTZ DEFAULT NOW(),
            updated_at      TIMESTAMPTZ DEFAULT NOW(),

            UNIQUE(external_id, source)
        );

        -- История изменения цен
        CREATE TABLE IF NOT EXISTS price_history (
            id          SERIAL PRIMARY KEY,
            product_id  INTEGER REFERENCES products(id) ON DELETE CASCADE,
            price_old   NUMERIC(10,2),
            price_new   NUMERIC(10,2),
            changed_at  TIMESTAMPTZ DEFAULT NOW()
        );

        -- Индексы
        CREATE INDEX IF NOT EXISTS idx_products_external    ON products(external_id, source);
        CREATE INDEX IF NOT EXISTS idx_products_sku         ON products(sku);
        CREATE INDEX IF NOT EXISTS idx_products_in_stock    ON products(in_stock);
        CREATE INDEX IF NOT EXISTS idx_products_category_ka ON products(category_ka);
        CREATE INDEX IF NOT EXISTS idx_products_category_ru ON products(category_ru);
        CREATE INDEX IF NOT EXISTS idx_products_updated_at  ON products(updated_at);

        -- Миграция: добавляем колонки если не существуют (безопасно для уже развёрнутой БД)
        ALTER TABLE products ADD COLUMN IF NOT EXISTS sku             TEXT;
        ALTER TABLE products ADD COLUMN IF NOT EXISTS availability_ka TEXT;
        ALTER TABLE products ADD COLUMN IF NOT EXISTS availability_ru TEXT;
        ALTER TABLE products ADD COLUMN IF NOT EXISTS category_ka     TEXT;
        ALTER TABLE products ADD COLUMN IF NOT EXISTS category_ru     TEXT;
        ALTER TABLE products ADD COLUMN IF NOT EXISTS category_en     TEXT;
        ALTER TABLE products ADD COLUMN IF NOT EXISTS sub_category_ka TEXT;
        ALTER TABLE products ADD COLUMN IF NOT EXISTS sub_category_ru TEXT;
        ALTER TABLE products ADD COLUMN IF NOT EXISTS sub_category_en TEXT;
        ALTER TABLE products ADD COLUMN IF NOT EXISTS other           JSONB DEFAULT '{}';
    """)
    await conn.close()
    logger.info("✅ БД инициализирована")
