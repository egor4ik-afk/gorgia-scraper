#!/usr/bin/env python3
"""
Scraper для gorgia.ge
- Обходит все страницы каталога
- Сохраняет оригинал на грузинском (KA) + переводы RU и EN
- Загружает фото в Vercel Blob
- Пишет результат в Neon PostgreSQL
"""

import asyncio
import json
import logging
import re
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import aiohttp
import asyncpg
from bs4 import BeautifulSoup

from config.settings import settings

logger = logging.getLogger("scraper.gorgia")

BASE_URL = "https://gorgia.ge"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}


# ──────────────────────────────────────────────
# Модель товара (трёхязычная: KA + RU + EN)
# ──────────────────────────────────────────────
@dataclass
class GorgiaProduct:
    external_id: str
    source_url: str

    # Грузинский — оригинал
    name_ka: str
    description_ka: str = ""

    # Русский — перевод
    name_ru: str = ""
    description_ru: str = ""

    # Английский — перевод
    name_en: str = ""
    description_en: str = ""

    price: Optional[float] = None
    currency: str = "GEL"
    in_stock: bool = False
    availability: str = ""

    category: str = ""
    sub_category: str = ""

    image_urls_raw:  list[str] = field(default_factory=list)   # оригинальные URL
    image_urls_blob: list[str] = field(default_factory=list)   # загруженные в Vercel Blob

    scraped_at: datetime = field(default_factory=datetime.now)


# ──────────────────────────────────────────────
# Перевод через Google Translate (бесплатный)
# ──────────────────────────────────────────────
async def translate_text(
    session: aiohttp.ClientSession,
    text: str,
    target: str,
    source: str = "ka",
) -> str:
    """Переводит text с source-языка на target через gtx endpoint."""
    if not text or not text.strip():
        return ""
    try:
        params = {"client": "gtx", "sl": source, "tl": target, "dt": "t", "q": text}
        async with session.get(
            "https://translate.googleapis.com/translate_a/single",
            params=params,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status == 200:
                data = await resp.json(content_type=None)
                return "".join([t[0] for t in data[0] if t[0]])
    except Exception as e:
        logger.warning(f"Ошибка перевода [{source}→{target}]: {e}")
    return text


# ──────────────────────────────────────────────
# Vercel Blob upload
# ──────────────────────────────────────────────
async def upload_to_vercel_blob(
    session: aiohttp.ClientSession,
    image_url: str,
    blob_filename: str,
) -> Optional[str]:
    token = settings.VERCEL_BLOB_TOKEN
    if not token:
        return image_url  # fallback — оригинальный URL

    try:
        async with session.get(
            image_url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=20)
        ) as img_resp:
            if img_resp.status != 200:
                return None
            content = await img_resp.read()
            content_type = img_resp.headers.get("Content-Type", "image/webp")

        upload_url = f"https://blob.vercel-storage.com/{blob_filename}"
        async with session.put(
            upload_url,
            data=content,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": content_type,
                "x-content-type": content_type,
            },
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status in (200, 201):
                data = await resp.json()
                return data.get("url")
            body = await resp.text()
            logger.warning(f"Blob upload error {resp.status}: {body[:200]}")
    except Exception as e:
        logger.warning(f"Ошибка загрузки фото {image_url}: {e}")
    return None


# ──────────────────────────────────────────────
# Парсинг URL фото
# ──────────────────────────────────────────────
def extract_image_urls(card: BeautifulSoup) -> list[str]:
    urls, seen = [], set()

    def add(url: str):
        if url and url not in seen:
            urls.append(url)
            seen.add(url)

    img_tag = card.select_one(".ut2-gl__image img")
    if img_tag and img_tag.get("src"):
        add(_to_webp(img_tag["src"]))

    for item in card.select(".item[data-ca-product-additional-image-src]"):
        srcset = item.get("data-ca-product-additional-image-srcset")
        if srcset:
            add(srcset.split()[0].strip())
        else:
            src = item.get("data-ca-product-additional-image-src")
            if src:
                add(src)

    return urls


def _to_webp(url: str) -> str:
    return (
        url
        .replace("/images/thumbnails/240/240/", "/images/ab__webp/thumbnails/1100/900/")
        .replace(".jpg",  "_jpg.webp")
        .replace(".JPG",  "_jpg.webp")
        .replace(".jpeg", "_jpg.webp")
        .replace(".png",  "_jpg.webp")
    )


# ──────────────────────────────────────────────
# Парсинг одной карточки
# ──────────────────────────────────────────────
def parse_card(
    card: BeautifulSoup, idx: int, category: str, sub_category: str
) -> Optional[GorgiaProduct]:
    title_tag = card.select_one(".ut2-gl__name a")
    if not title_tag:
        return None

    name_ka = title_tag.get_text(strip=True)
    link = urllib.parse.urljoin(BASE_URL, title_tag.get("href", ""))
    external_id = re.sub(r"[^a-zA-Z0-9_-]", "_", link.replace(BASE_URL, "").strip("/"))[:100]

    # Цена
    price = None
    price_tag = card.select_one(".ty-price-num")
    if price_tag:
        for sup in price_tag.find_all("sup"):
            sup.extract()
        raw = price_tag.get_text(strip=True).replace(",", ".").replace("₾", "").strip()
        try:
            price = float(re.sub(r"[^\d.]", "", raw))
        except ValueError:
            pass

    # Наличие
    availability, in_stock = "unknown", False
    stock_tag = card.select_one(".ty-qty-in-stock")
    if stock_tag:
        txt = stock_tag.get_text(strip=True)
        if "მარაგშია" in txt:
            availability, in_stock = "in_stock", True
        elif "მარაგი იწურება" in txt:
            availability, in_stock = "low_stock", True
        else:
            availability, in_stock = "out_of_stock", False

    desc_tag = card.select_one(".product-description")
    description_ka = desc_tag.get_text(strip=True) if desc_tag else ""

    return GorgiaProduct(
        external_id=external_id,
        source_url=link,
        name_ka=name_ka,
        description_ka=description_ka,
        price=price,
        currency="GEL",
        in_stock=in_stock,
        availability=availability,
        category=category,
        sub_category=sub_category,
        image_urls_raw=extract_image_urls(card),
    )


# ──────────────────────────────────────────────
# Основной класс скрапера
# ──────────────────────────────────────────────
class GorgiaScraper:
    """
    Парсит полный каталог gorgia.ge.
    1. Обходит все страницы пагинации
    2. Переводит KA → RU и KA → EN параллельно
    3. Загружает фото в Vercel Blob
    """

    def __init__(
        self,
        category_url: str,
        category: str = "",
        sub_category: str = "",
        max_pages: int = 999,
    ):
        self.category_url = category_url.rstrip("/") + "/"
        self.category    = category
        self.sub_category = sub_category
        self.max_pages   = max_pages

    async def scrape(self) -> list[GorgiaProduct]:
        all_products: list[GorgiaProduct] = []

        async with aiohttp.ClientSession(headers=HEADERS) as session:
            page = 1
            while page <= self.max_pages:
                url = (
                    self.category_url
                    if page == 1
                    else f"{self.category_url}?page={page}"
                )
                logger.info(f"[Gorgia] стр.{page}: {url}")

                cards_data = await self._fetch_page(session, url)
                if not cards_data:
                    logger.info(f"[Gorgia] стр.{page} пустая — стоп")
                    break

                products = []
                for idx, card in enumerate(cards_data, start=1 + (page - 1) * 100):
                    p = parse_card(card, idx, self.category, self.sub_category)
                    if p:
                        products.append(p)

                logger.info(f"[Gorgia] стр.{page}: {len(products)} товаров — переводим...")
                await self._translate_batch(session, products)

                logger.info(f"[Gorgia] стр.{page}: загружаем фото...")
                await self._upload_images_batch(session, products)

                for p in products:
                    flag = "✅" if p.in_stock else "❌"
                    logger.info(
                        f"  {flag} [{p.name_ka}] "
                        f"RU: {p.name_ru or '—'} | "
                        f"EN: {p.name_en or '—'} | "
                        f"{p.price} GEL"
                    )

                all_products.extend(products)
                await asyncio.sleep(settings.REQUEST_DELAY)
                page += 1

        logger.info(f"[Gorgia] Итого: {len(all_products)} товаров")
        return all_products

    # ── fetch ──────────────────────────────────
    async def _fetch_page(
        self, session: aiohttp.ClientSession, url: str
    ) -> list:
        for attempt in range(settings.MAX_RETRIES):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 404:
                        return []
                    resp.raise_for_status()
                    html = await resp.text()
                soup = BeautifulSoup(html, "html.parser")
                return soup.select(".ut2-gl__body")
            except Exception as e:
                logger.warning(f"Попытка {attempt+1}/{settings.MAX_RETRIES}: {e}")
                await asyncio.sleep(2 ** attempt)
        return []

    # ── translate ──────────────────────────────
    async def _translate_batch(
        self, session: aiohttp.ClientSession, products: list[GorgiaProduct]
    ):
        """Переводим KA→RU и KA→EN параллельно для всех товаров."""
        sem = asyncio.Semaphore(5)

        async def translate_one(p: GorgiaProduct):
            async with sem:
                # RU и EN параллельно
                results = await asyncio.gather(
                    translate_text(session, p.name_ka, target="ru"),
                    translate_text(session, p.name_ka, target="en"),
                    translate_text(session, p.description_ka, target="ru") if p.description_ka else asyncio.sleep(0, result=""),
                    translate_text(session, p.description_ka, target="en") if p.description_ka else asyncio.sleep(0, result=""),
                )
                p.name_ru, p.name_en, p.description_ru, p.description_en = results
                await asyncio.sleep(0.15)

        await asyncio.gather(*[translate_one(p) for p in products])

    # ── upload ─────────────────────────────────
    async def _upload_images_batch(
        self, session: aiohttp.ClientSession, products: list[GorgiaProduct]
    ):
        sem = asyncio.Semaphore(4)

        async def upload_for_product(p: GorgiaProduct):
            async with sem:
                if not p.image_urls_raw:
                    return
                if not p.in_stock:
                    p.image_urls_blob = p.image_urls_raw
                    return

                blob_urls = []
                for idx, raw_url in enumerate(p.image_urls_raw[:10]):
                    ext  = "webp" if "webp" in raw_url else "jpg"
                    name = f"gorgia/{p.external_id}/{idx}.{ext}"
                    uploaded = await upload_to_vercel_blob(session, raw_url, name)
                    if uploaded:
                        blob_urls.append(uploaded)
                    await asyncio.sleep(0.3)

                p.image_urls_blob = blob_urls

        await asyncio.gather(*[upload_for_product(p) for p in products])


# ──────────────────────────────────────────────
# Запись в PostgreSQL (Neon) — KA + RU + EN
# ──────────────────────────────────────────────
async def save_to_db(products: list[GorgiaProduct]):
    conn = await asyncpg.connect(settings.DATABASE_URL)

    # Добавляем колонки если отсутствуют (идемпотентно)
    await conn.execute("""
        ALTER TABLE products
            ADD COLUMN IF NOT EXISTS name_ka        TEXT,
            ADD COLUMN IF NOT EXISTS description_ka TEXT,
            ADD COLUMN IF NOT EXISTS name_ru        TEXT,
            ADD COLUMN IF NOT EXISTS description_ru TEXT,
            ADD COLUMN IF NOT EXISTS name_en        TEXT,
            ADD COLUMN IF NOT EXISTS description_en TEXT;
    """)

    inserted, updated = 0, 0

    for p in products:
        images   = p.image_urls_blob or p.image_urls_raw
        existing = await conn.fetchrow(
            "SELECT id FROM products WHERE external_id=$1 AND source='gorgia'",
            p.external_id,
        )

        if existing is None:
            await conn.execute("""
                INSERT INTO products (
                    external_id, source, source_url,
                    name,
                    name_ka, name_ru, name_en,
                    description,
                    description_ka, description_ru, description_en,
                    price, currency, in_stock,
                    category, images
                ) VALUES (
                    $1, 'gorgia', $2,
                    $3,
                    $4, $5, $6,
                    $7,
                    $8, $9, $10,
                    $11, $12, $13,
                    $14, $15
                )
            """,
                p.external_id, p.source_url,
                p.name_ru or p.name_en or p.name_ka,   # name = ru → en → ka
                p.name_ka, p.name_ru, p.name_en,
                p.description_ru or p.description_en or p.description_ka,
                p.description_ka, p.description_ru, p.description_en,
                p.price, p.currency, p.in_stock,
                p.category,
                json.dumps(images),
            )
            inserted += 1
        else:
            await conn.execute("""
                UPDATE products SET
                    name            = $1,
                    name_ka         = $2,
                    name_ru         = $3,
                    name_en         = $4,
                    description     = $5,
                    description_ka  = $6,
                    description_ru  = $7,
                    description_en  = $8,
                    price           = $9,
                    in_stock        = $10,
                    images          = $11,
                    updated_at      = NOW()
                WHERE external_id = $12 AND source = 'gorgia'
            """,
                p.name_ru or p.name_en or p.name_ka,
                p.name_ka, p.name_ru, p.name_en,
                p.description_ru or p.description_en or p.description_ka,
                p.description_ka, p.description_ru, p.description_en,
                p.price, p.in_stock,
                json.dumps(images),
                p.external_id,
            )
            updated += 1

    await conn.close()
    logger.info(f"[DB] ➕ {inserted} новых  ✏️ {updated} обновлено")
    return inserted, updated


# ──────────────────────────────────────────────
# Список категорий gorgia.ge
# Добавляй свои URL по аналогии
# ──────────────────────────────────────────────
GORGIA_CATEGORIES = [
    # (url,  category,  sub_category)
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-aveji/ikeas-magidebi-da-merxebi/", "IKEA", "Столы / Tables"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-aveji/ikeas-stulebida-skamebi/",   "IKEA", "Стулья / Chairs"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-aveji/ikeas-karebiani-satumebi/",  "IKEA", "Шкафы / Wardrobes"),
    # Добавь остальные категории:
    # ("https://gorgia.ge/ka/.../", "Категория", "Подкатегория"),
]


# ──────────────────────────────────────────────
# Точка входа для ручного запуска
# ──────────────────────────────────────────────
async def run_gorgia_full():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    total = []
    for cat_url, category, sub_category in GORGIA_CATEGORIES:
        scraper  = GorgiaScraper(cat_url, category=category, sub_category=sub_category)
        products = await scraper.scrape()
        total.extend(products)

        if products:
            ins, upd = await save_to_db(products)
            logger.info(f"[{category}/{sub_category}] ➕{ins} ✏️{upd}")

        await asyncio.sleep(2)

    logger.info(f"\n✅ Всего обработано: {len(total)} товаров")


if __name__ == "__main__":
    asyncio.run(run_gorgia_full())
