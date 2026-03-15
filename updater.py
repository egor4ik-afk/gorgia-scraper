#!/usr/bin/env python3
"""
updater.py
Ежедневный апдейтер: обновляет цену, наличие и SKU (если отсутствовал).
Запускается по расписанию — не пересоздаёт товары, только UPDATE.
"""

import asyncio
import logging
import json
import re
from datetime import datetime, timezone

import aiohttp
import asyncpg
from bs4 import BeautifulSoup

from config.settings import settings
from scrapers.gorgia import (
    HEADERS, parse_price, parse_availability, parse_detail_page,
)
from notifier.telegram import TelegramNotifier
from agents.models import UpdateReport

logger = logging.getLogger("updater")


# ──────────────────────────────────────────────
# Загрузка страницы
# ──────────────────────────────────────────────
async def fetch_page(
    session: aiohttp.ClientSession, url: str
) -> BeautifulSoup | None:
    for attempt in range(settings.MAX_RETRIES):
        try:
            async with session.get(
                url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=25)
            ) as resp:
                if resp.status == 404:
                    return None
                resp.raise_for_status()
                html = await resp.text()
            return BeautifulSoup(html, "html.parser")
        except Exception as e:
            if attempt == settings.MAX_RETRIES - 1:
                logger.warning(f"Не удалось загрузить {url}: {e}")
                return None
            await asyncio.sleep(2 ** attempt)
    return None


# ──────────────────────────────────────────────
# Обновление одного товара
# ──────────────────────────────────────────────
async def update_one(
    session: aiohttp.ClientSession,
    conn: asyncpg.Connection,
    row: dict,
    report: UpdateReport,
):
    url = row["source_url"]
    if not url:
        return

    soup = await fetch_page(session, url)
    if not soup:
        return

    changes = {}

    # Цена
    new_price = parse_price(soup)
    old_price = float(row["price"]) if row["price"] else None
    if new_price is not None and new_price != old_price:
        if old_price:
            diff_pct = round(abs(new_price - old_price) / old_price * 100, 1)
            report.price_changes.append({
                "name":     row["name"] or "",
                "old":      old_price,
                "new":      new_price,
                "diff_pct": diff_pct,
            })
            # История цен
            await conn.execute(
                """
                INSERT INTO price_history (product_id, price_old, price_new)
                VALUES ($1, $2, $3)
                """,
                row["id"], old_price, new_price,
            )
        changes["price"] = new_price

    # Наличие
    new_avail_ka, new_in_stock = parse_availability(soup)
    if new_avail_ka and new_avail_ka != (row["availability_ka"] or ""):
        changes["availability_ka"] = new_avail_ka
        # Переводим наличие
        try:
            async with session.get(
                "https://translate.googleapis.com/translate_a/single",
                params={"client": "gtx", "sl": "ka", "tl": "ru", "dt": "t", "q": new_avail_ka},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    changes["availability_ru"] = "".join(t[0] for t in data[0] if t[0])
        except Exception:
            pass

    old_in_stock = row["in_stock"]
    if new_in_stock != old_in_stock:
        changes["in_stock"] = new_in_stock
        report.stock_changes.append({
            "name": row["name"] or "",
            "old":  old_in_stock,
            "new":  new_in_stock,
        })

    # SKU (заполняем если пустой)
    if not row["sku"]:
        detail = parse_detail_page(soup)
        if detail["sku"]:
            changes["sku"] = detail["sku"]
            logger.info(f"  SKU найден: {detail['sku']} для {url[-50:]}")

    # Применяем изменения
    if changes:
        set_parts = [f"{k} = ${i+2}" for i, k in enumerate(changes)]
        set_parts.append("updated_at = NOW()")
        sql = f"UPDATE products SET {', '.join(set_parts)} WHERE id = $1"
        await conn.execute(sql, row["id"], *changes.values())
        report.updated_products += 1
        logger.info(
            f"  ✏️  {(row['name'] or url)[:50]} | "
            + " | ".join(f"{k}={v}" for k, v in changes.items())
        )
    else:
        # Помечаем как проверенный (updated_at)
        await conn.execute(
            "UPDATE products SET updated_at = NOW() WHERE id = $1", row["id"]
        )


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
async def run_daily_update():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/updater.log"),
        ],
    )
    logger.info(f"=== DAILY UPDATE {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} ===")

    report   = UpdateReport()
    notifier = TelegramNotifier(
        token=settings.TELEGRAM_BOT_TOKEN,
        chat_id=settings.TELEGRAM_CHAT_ID,
    )

    conn = await asyncpg.connect(settings.DATABASE_URL)

    rows = await conn.fetch("""
        SELECT id, external_id, source_url, name,
               price, in_stock, availability_ka, sku
        FROM products
        WHERE source = 'gorgia'
          AND source_url IS NOT NULL
        ORDER BY updated_at ASC
    """)

    total = len(rows)
    logger.info(f"Товаров к проверке: {total}")
    report.total_scraped = total

    sem = asyncio.Semaphore(5)

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        async def process(row):
            async with sem:
                try:
                    await update_one(session, conn, dict(row), report)
                except Exception as e:
                    msg = f"Ошибка обновления id={row['id']}: {e}"
                    logger.error(msg)
                    report.errors.append(msg)
                await asyncio.sleep(settings.REQUEST_DELAY)

        # Батчами по 50 чтобы не перегружать сервер
        for i in range(0, len(rows), 50):
            batch = rows[i:i+50]
            await asyncio.gather(*[process(r) for r in batch])
            logger.info(
                f"  [{min(i+50, total)}/{total}] "
                f"updated={report.updated_products} errors={len(report.errors)}"
            )

    await conn.close()

    elapsed = 0  # будет вычислен в main при необходимости
    await notifier.send_report(report, elapsed)
    logger.info(
        f"=== DONE | updated={report.updated_products} "
        f"price_changes={len(report.price_changes)} "
        f"stock_changes={len(report.stock_changes)} "
        f"errors={len(report.errors)} ==="
    )


if __name__ == "__main__":
    asyncio.run(run_daily_update())
