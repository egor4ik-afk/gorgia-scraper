#!/usr/bin/env python3
"""
Gorgia.ge Product Scraper Agent
Парсит gorgia.ge, обновляет Neon PostgreSQL,
загружает фото в Vercel Blob, шлёт отчёт в Telegram.
"""

import asyncio
import logging
from datetime import datetime

from config.settings import settings
from scrapers.gorgia import GorgiaScraper, GORGIA_CATEGORIES, save_to_db
from notifier.telegram import TelegramNotifier
from db.database import init_db
from agents.models import UpdateReport

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/agent.log"),
    ],
)
logger = logging.getLogger("main")


async def main():
    logger.info("🚀 Запуск агента gorgia.ge")
    start = datetime.now()

    notifier = TelegramNotifier(
        token=settings.TELEGRAM_BOT_TOKEN,
        chat_id=settings.TELEGRAM_CHAT_ID,
    )

    report = UpdateReport()

    try:
        await init_db()

        # Определяем список категорий
        if settings.GORGIA_CATEGORY_URLS:
            categories = [
                (url.strip(), "", "")
                for url in settings.GORGIA_CATEGORY_URLS.split(",")
                if url.strip()
            ]
        else:
            categories = GORGIA_CATEGORIES

        logger.info(f"Категорий для парсинга: {len(categories)}")

        for cat_url, category, sub_category in categories:
            try:
                scraper = GorgiaScraper(
                    category_url=cat_url,
                    category=category,
                    sub_category=sub_category,
                )
                products = await scraper.scrape()

                if products:
                    inserted, updated = await save_to_db(products)
                    report.total_scraped  += len(products)
                    report.new_products   += inserted
                    report.updated_products += updated

                    # Считаем изменения цен и наличия
                    for p in products:
                        report.images_uploaded += len(p.image_urls_blob)

                await asyncio.sleep(2)

            except Exception as e:
                msg = f"Ошибка категории {cat_url}: {e}"
                logger.error(msg)
                report.errors.append(msg)

        elapsed = (datetime.now() - start).seconds
        await notifier.send_report(report, elapsed)
        logger.info(f"✅ Готово за {elapsed}с")

    except Exception as e:
        logger.exception(f"❌ Критическая ошибка: {e}")
        await notifier.send_error(str(e))
        raise


if __name__ == "__main__":
    asyncio.run(main())
