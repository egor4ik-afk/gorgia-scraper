#!/usr/bin/env python3
"""
main.py
Gorgia.ge Product Scraper Agent
  --full      полный парсинг всего сайта (первый запуск)
  --update    только ежедневное обновление цены/наличия
  (без флагов: полный парсинг)
"""

import asyncio
import logging
import sys
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


async def run_full_scrape():
    """Полный обход всего сайта gorgia.ge."""
    logger.info("🚀 Запуск полного парсинга gorgia.ge")
    start   = datetime.now()
    report  = UpdateReport()
    notifier = TelegramNotifier(
        token=settings.TELEGRAM_BOT_TOKEN,
        chat_id=settings.TELEGRAM_CHAT_ID,
    )

    try:
        await init_db()

        # Если заданы конкретные URL — используем их
        # Иначе full_site=True: автоматически обходим весь sitemap
        if settings.GORGIA_CATEGORY_URLS:
            for url in [u.strip() for u in settings.GORGIA_CATEGORY_URLS.split(",") if u.strip()]:
                try:
                    scraper  = GorgiaScraper(category_url=url)
                    products = await scraper.scrape()
                    if products:
                        ins, upd = await save_to_db(products)
                        report.total_scraped    += len(products)
                        report.new_products     += ins
                        report.updated_products += upd
                        report.images_uploaded  += sum(len(p.image_urls_blob) for p in products)
                    await asyncio.sleep(2)
                except Exception as e:
                    msg = f"Ошибка категории {url}: {e}"
                    logger.error(msg)
                    report.errors.append(msg)
        elif GORGIA_CATEGORIES:
            for cat_url, category_ka, sub_category_ka in GORGIA_CATEGORIES:
                try:
                    scraper  = GorgiaScraper(
                        category_url=cat_url,
                        category_ka=category_ka,
                        sub_category_ka=sub_category_ka,
                    )
                    products = await scraper.scrape()
                    if products:
                        ins, upd = await save_to_db(products)
                        report.total_scraped    += len(products)
                        report.new_products     += ins
                        report.updated_products += upd
                        report.images_uploaded  += sum(len(p.image_urls_blob) for p in products)
                    await asyncio.sleep(2)
                except Exception as e:
                    msg = f"Ошибка {cat_url}: {e}"
                    logger.error(msg)
                    report.errors.append(msg)
        else:
            # Полный обход через sitemap
            scraper  = GorgiaScraper(full_site=True)
            products = await scraper.scrape()
            if products:
                ins, upd = await save_to_db(products)
                report.total_scraped    = len(products)
                report.new_products     = ins
                report.updated_products = upd
                report.images_uploaded  = sum(len(p.image_urls_blob) for p in products)

        elapsed = int((datetime.now() - start).total_seconds())
        await notifier.send_report(report, elapsed)
        logger.info(f"✅ Парсинг завершён за {elapsed}с | +{report.new_products} ~{report.updated_products}")

    except Exception as e:
        logger.exception(f"❌ Критическая ошибка: {e}")
        await notifier.send_error(str(e))
        raise


async def run_update():
    """Только ежедневное обновление цены/наличия."""
    from updater import run_daily_update
    await run_daily_update()


if __name__ == "__main__":
    if "--update" in sys.argv:
        asyncio.run(run_update())
    else:
        asyncio.run(run_full_scrape())
