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

            -- Основное поле (ru → en → ka)
            name            TEXT NOT NULL,

            -- Трёхязычные поля
            name_ka         TEXT,
            name_ru         TEXT,
            name_en         TEXT,
            description     TEXT,
            description_ka  TEXT,
            description_ru  TEXT,
            description_en  TEXT,

            price           NUMERIC(10,2),
            currency        TEXT DEFAULT 'GEL',
            in_stock        BOOLEAN DEFAULT TRUE,
            availability    TEXT,

            category        TEXT,
            sub_category    TEXT,

            -- JSON-массив URL фото в Vercel Blob
            images          JSONB DEFAULT '[]',

            created_at      TIMESTAMPTZ DEFAULT NOW(),
            updated_at      TIMESTAMPTZ DEFAULT NOW(),

            UNIQUE(external_id, source)
        );

        CREATE TABLE IF NOT EXISTS price_history (
            id          SERIAL PRIMARY KEY,
            product_id  INTEGER REFERENCES products(id) ON DELETE CASCADE,
            price_old   NUMERIC(10,2),
            price_new   NUMERIC(10,2),
            changed_at  TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_products_external ON products(external_id, source);
        CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
        CREATE INDEX IF NOT EXISTS idx_products_in_stock ON products(in_stock);
    """)
    await conn.close()
    logger.info("✅ БД инициализирована")
