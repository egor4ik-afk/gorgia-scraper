import asyncio
import logging
from typing import Optional

from config.settings import settings
from agents.models import ProductData, UpdateReport
from scrapers.wildberries import WildberriesScraper
from scrapers.ozon import OzonScraper
from scrapers.generic import GenericScraper
from db.database import Database
from storage.r2 import R2Storage

logger = logging.getLogger("orchestrator")


class OrchestratorAgent:
    """
    Координирует все скраперы, загрузку фото и обновление БД.
    """

    def __init__(self, notifier=None):
        self.notifier = notifier
        self.db = Database()
        self.storage = R2Storage()
        self.report = UpdateReport()
        self.semaphore = asyncio.Semaphore(settings.CONCURRENT_SCRAPERS)

    async def run(self) -> UpdateReport:
        tasks = []

        # Wildberries
        if settings.WB_QUERIES:
            queries = [q.strip() for q in settings.WB_QUERIES.split(",") if q.strip()]
            for query in queries:
                tasks.append(self._run_scraper(WildberriesScraper(query=query)))

        # Ozon
        if settings.OZON_QUERIES:
            queries = [q.strip() for q in settings.OZON_QUERIES.split(",") if q.strip()]
            for query in queries:
                tasks.append(self._run_scraper(OzonScraper(query=query)))

        # Gorgia.ge — полный каталог
        from scrapers.gorgia import GorgiaScraper, GORGIA_CATEGORIES, save_to_db as gorgia_save_db
        if settings.GORGIA_CATEGORY_URLS:
            for url in [u.strip() for u in settings.GORGIA_CATEGORY_URLS.split(",") if u.strip()]:
                tasks.append(self._run_gorgia(GorgiaScraper(url), gorgia_save_db))
        else:
            for cat_url, category, sub_category in GORGIA_CATEGORIES:
                tasks.append(self._run_gorgia(
                    GorgiaScraper(cat_url, category=category, sub_category=sub_category),
                    gorgia_save_db,
                ))

        # Конкуренты
        if settings.COMPETITOR_URLS:
            urls = [u.strip() for u in settings.COMPETITOR_URLS.split(",") if u.strip()]
            for url in urls:
                tasks.append(self._run_scraper(GenericScraper(url=url, source="competitor")))

        # Поставщики
        if settings.SUPPLIER_URLS:
            urls = [u.strip() for u in settings.SUPPLIER_URLS.split(",") if u.strip()]
            for url in urls:
                tasks.append(self._run_scraper(GenericScraper(url=url, source="supplier")))

        logger.info(f"Запускаю {len(tasks)} скраперов...")
        await asyncio.gather(*tasks, return_exceptions=True)

        return self.report

    async def _run_scraper(self, scraper) -> None:
        async with self.semaphore:
            try:
                products = await scraper.scrape()
                logger.info(f"[{scraper.__class__.__name__}] Получено {len(products)} товаров")

                for product in products:
                    await self._process_product(product)

            except Exception as e:
                msg = f"Ошибка скрапера {scraper.__class__.__name__}: {e}"
                logger.error(msg)
                self.report.errors.append(msg)


    async def _run_gorgia(self, scraper, save_fn) -> None:
        """Запускает Gorgia-скрапер и сохраняет напрямую через gorgia save_to_db."""
        async with self.semaphore:
            try:
                products = await scraper.scrape()
                logger.info(f"[Gorgia] Получено {len(products)} товаров")
                if products:
                    inserted, updated = await save_fn(products)
                    self.report.total_scraped += len(products)
                    self.report.new_products += inserted
                    self.report.updated_products += updated
            except Exception as e:
                msg = f"Ошибка Gorgia скрапера: {e}"
                logger.error(msg)
                self.report.errors.append(msg)

    async def _process_product(self, product: ProductData) -> None:
        self.report.total_scraped += 1

        try:
            # Загружаем фото в R2
            if product.images:
                uploaded = await self.storage.upload_images(product.images, product.external_id)
                product.images_uploaded = uploaded
                self.report.images_uploaded += len(uploaded)

            # Проверяем существующий товар в БД
            existing = await self.db.get_product_by_external_id(
                product.external_id, product.source
            )

            if existing is None:
                # Новый товар
                await self.db.insert_product(product)
                self.report.new_products += 1
                logger.info(f"➕ Новый товар: {product.name}")

            else:
                # Обновляем существующий
                changes = self._detect_changes(existing, product)

                if changes:
                    await self.db.update_product(existing["id"], product, changes)
                    self.report.updated_products += 1

                    # Фиксируем важные изменения для отчёта
                    if "price" in changes:
                        self.report.price_changes.append({
                            "name": product.name,
                            "old": changes["price"]["old"],
                            "new": changes["price"]["new"],
                            "diff_pct": changes["price"]["diff_pct"],
                        })

                    if "in_stock" in changes:
                        self.report.stock_changes.append({
                            "name": product.name,
                            "old": changes["in_stock"]["old"],
                            "new": changes["in_stock"]["new"],
                        })

        except Exception as e:
            msg = f"Ошибка обработки '{product.name}': {e}"
            logger.error(msg)
            self.report.errors.append(msg)

    def _detect_changes(self, existing: dict, new: ProductData) -> dict:
        changes = {}

        # Цена
        if existing.get("price") and new.price:
            old_price = float(existing["price"])
            diff = abs(new.price - old_price) / old_price if old_price else 0
            if diff >= settings.PRICE_CHANGE_THRESHOLD:
                changes["price"] = {
                    "old": old_price,
                    "new": new.price,
                    "diff_pct": round(diff * 100, 1),
                }

        # Наличие
        old_stock = existing.get("in_stock", True)
        if old_stock != new.in_stock:
            changes["in_stock"] = {"old": old_stock, "new": new.in_stock}

        # Описание
        if new.description and existing.get("description") != new.description:
            changes["description"] = True

        # Характеристики
        if new.characteristics and existing.get("characteristics") != new.characteristics:
            changes["characteristics"] = True

        return changes
