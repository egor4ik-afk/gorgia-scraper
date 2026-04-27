#!/usr/bin/env python3
"""
scrapers/gorgia.py
Парсит gorgia.ge:
  - Обходит ВСЕ категории через sitemap (+ nav fallback)
  - Парсит каждую детальную страницу: артикул/SKU, характеристики, бренд
  - Хранит оригинал на грузинском + переводы RU и EN
  - Загружает фото в Vercel Blob
  - Доп. данные в поле `other` (JSONB)
"""

import asyncio
import hashlib
import json
import logging
import re
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import aiohttp
import asyncpg
from bs4 import BeautifulSoup, NavigableString

from config.settings import settings

logger = logging.getLogger("scraper.gorgia")

BASE_URL = "https://gorgia.ge"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "ka,ru;q=0.9,en;q=0.8",
}


# ──────────────────────────────────────────────
# Модель товара
# ──────────────────────────────────────────────
@dataclass
class GorgiaProduct:
    external_id: str
    source_url: str

    # Оригинал — грузинский
    name_ka: str = ""
    description_ka: str = ""
    availability_ka: str = ""
    category_ka: str = ""
    sub_category_ka: str = ""

    # Переводы
    name_ru: str = ""
    name_en: str = ""
    description_ru: str = ""
    description_en: str = ""
    availability_ru: str = ""
    category_ru: str = ""
    category_en: str = ""
    sub_category_ru: str = ""
    sub_category_en: str = ""

    # Коммерция
    sku: Optional[str] = None
    price: Optional[float] = None
    currency: str = "GEL"
    in_stock: bool = False

    # Изображения
    image_urls_raw: list[str] = field(default_factory=list)
    image_urls_blob: list[str] = field(default_factory=list)

    # Доп. данные (характеристики, бренд, хлебные крошки и пр.)
    other: dict = field(default_factory=dict)

    scraped_at: datetime = field(default_factory=datetime.now)


# ──────────────────────────────────────────────
# Утилиты
# ──────────────────────────────────────────────
def make_external_id(url: str) -> str:
    path = urllib.parse.urlparse(url).path.strip("/")
    slug = re.sub(r"[^a-zA-Z0-9_\-]", "_", path)[:80]
    short = hashlib.md5(path.encode()).hexdigest()[:6]
    return f"{slug}_{short}"


def _to_webp(url: str) -> str:
    return (
        url
        .replace("/images/thumbnails/240/240/", "/images/ab__webp/thumbnails/1100/900/")
        .replace("/images/thumbnails/480/480/", "/images/ab__webp/thumbnails/1100/900/")
        .replace(".jpg",  "_jpg.webp")
        .replace(".JPG",  "_jpg.webp")
        .replace(".jpeg", "_jpg.webp")
        .replace(".png",  "_jpg.webp")
    )


# ──────────────────────────────────────────────
# Перевод
# ──────────────────────────────────────────────
async def translate_text(
    session: aiohttp.ClientSession,
    text: str,
    target: str,
    source: str = "ka",
) -> str:
    if not text or not text.strip():
        return ""
    try:
        params = {"client": "gtx", "sl": source, "tl": target, "dt": "t", "q": text}
        async with session.get(
            "https://translate.googleapis.com/translate_a/single",
            params=params,
            timeout=aiohttp.ClientTimeout(total=12),
        ) as resp:
            if resp.status == 200:
                data = await resp.json(content_type=None)
                return "".join(t[0] for t in data[0] if t[0])
    except Exception as e:
        logger.warning(f"Перевод [{source}->>{target}] ошибка: {e}")
    return text


# ──────────────────────────────────────────────
# Vercel Blob
# ──────────────────────────────────────────────
async def upload_to_vercel_blob(
    session: aiohttp.ClientSession,
    image_url: str,
    blob_path: str,
) -> Optional[str]:
    token = settings.VERCEL_BLOB_TOKEN
    if not token:
        return image_url

    try:
        async with session.get(
            image_url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=25)
        ) as img_resp:
            if img_resp.status != 200:
                return None
            content = await img_resp.read()
            ctype = img_resp.headers.get("Content-Type", "image/webp")

        async with session.put(
            f"https://blob.vercel-storage.com/{blob_path}",
            data=content,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": ctype,
                "x-content-type": ctype,
            },
            timeout=aiohttp.ClientTimeout(total=40),
        ) as resp:
            if resp.status in (200, 201):
                data = await resp.json()
                return data.get("url")
            body = await resp.text()
            logger.warning(f"Blob {resp.status}: {body[:150]}")
    except Exception as e:
        logger.warning(f"Blob upload error {image_url}: {e}")
    return None


# ──────────────────────────────────────────────
# Парсинг изображений
# ──────────────────────────────────────────────
def extract_images_from_detail(soup: BeautifulSoup) -> list[str]:
    urls, seen = [], set()

    def add(u: str):
        u = u.strip()
        if u and u not in seen and not u.endswith(".svg"):
            urls.append(u)
            seen.add(u)

    for img in soup.select(
        ".cm-image-previewer img, "
        ".product-image img, "
        "[class*='product-img'] img, "
        ".ty-product-block-image img"
    ):
        src = img.get("src") or img.get("data-src") or ""
        if src:
            add(_to_webp(src))

    for item in soup.select(".item[data-ca-product-additional-image-src]"):
        srcset = item.get("data-ca-product-additional-image-srcset", "")
        if srcset:
            add(srcset.split()[0])
        else:
            src = item.get("data-ca-product-additional-image-src", "")
            if src:
                add(src)

    return urls


# ──────────────────────────────────────────────
# Парсинг цены
# ──────────────────────────────────────────────
def parse_price(container: BeautifulSoup) -> Optional[float]:
    tag = container.select_one(".ty-price-num")
    if not tag:
        return None
    for sup in tag.find_all("sup"):
        sup.extract()
    raw = ""
    for child in tag.children:
        if isinstance(child, NavigableString):
            t = str(child).strip()
            if t:
                raw = t
                break
    if not raw:
        raw = tag.get_text(strip=True)
    clean = re.sub(r"[^\d.,]", "", raw).replace(",", ".")
    try:
        return float(clean)
    except ValueError:
        return None


# ──────────────────────────────────────────────
# Парсинг наличия — Georgian оригинал
# ──────────────────────────────────────────────
def parse_availability(container: BeautifulSoup) -> tuple[str, bool]:
    tag = container.select_one(".ty-qty-in-stock")
    if not tag:
        return "", False
    text = tag.get_text(strip=True)
    if "მარაგშია" in text:
        return "მარაგშია", True
    if "მარაგი იწურება" in text:
        return "მარაგი იწურება", True
    if "არ არის მარაგში" in text:
        return "არ არის მარაგში", False
    return text, False


# ──────────────────────────────────────────────
# Парсинг детальной страницы
# ──────────────────────────────────────────────
def parse_detail_page(soup: BeautifulSoup) -> dict:
    result = {
        "name_ka": None,
        "description_ka": "",
        "sku": None,
        "brand": None,
        "image_urls": [],
        "features_ka": {},
        "other": {},
    }

    # Название
    h1 = soup.select_one(
        "h1.ty-product-block-title, h1[class*='product'], "
        ".product-title h1, [itemprop='name']"
    )
    if h1:
        result["name_ka"] = h1.get_text(strip=True)

    # SKU / Артикул
    for sel in [
        ".ty-product-feature--sku .ty-product-feature__value",
        "[class*='product-code'] span",
        "[class*='product-code']",
        "#product_code",
        "[itemprop='sku']",
        "[itemprop='productID']",
    ]:
        tag = soup.select_one(sel)
        if tag:
            v = tag.get_text(strip=True)
            if v and len(v) <= 40:
                result["sku"] = v
                break

    # SKU regex fallback
    if not result["sku"]:
        page_text = soup.get_text(" ")
        m = re.search(
            r'(?:კოდი|კატ(?:ალოგ)?\.?\s*[#№]|product\s*code|SKU|Артикул)[:\s#]*([A-Za-z0-9\-\/\.]{3,30})',
            page_text, re.IGNORECASE
        )
        if m:
            result["sku"] = m.group(1).strip()

    # Бренд
    brand_tag = soup.select_one(
        ".ty-product-block__brand img, [class*='brand'] img, [itemprop='brand']"
    )
    if brand_tag:
        result["brand"] = brand_tag.get("alt") or brand_tag.get_text(strip=True)

    # Описание
    desc_tag = soup.select_one(
        ".ty-product-block-description, [class*='product-description'], "
        "#product_description, [itemprop='description']"
    )
    if desc_tag:
        result["description_ka"] = desc_tag.get_text(separator="\n", strip=True)

    # Характеристики (Georgia оригинал)
    features = {}
    for row in soup.select(
        ".ty-product-feature, .features-list li, "
        "table.product-features tr, [class*='feature-item']"
    ):
        lbl = row.select_one(".ty-product-feature__title, .feature-label, th, td:first-child")
        val = row.select_one(".ty-product-feature__value, .feature-value, td:last-child")
        if lbl and val:
            k = lbl.get_text(strip=True)
            v = val.get_text(strip=True)
            if k and v and k != v and len(k) < 120:
                features[k] = v
    result["features_ka"] = features

    # JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string or "")
            if not isinstance(ld, dict):
                continue
            if not result["sku"] and ld.get("sku"):
                result["sku"] = str(ld["sku"])
            if not result["brand"] and ld.get("brand"):
                b = ld["brand"]
                result["brand"] = b.get("name") if isinstance(b, dict) else str(b)
            for k in ("gtin", "gtin13", "gtin8", "mpn", "model", "color", "weight"):
                if ld.get(k):
                    result["other"][k] = str(ld[k])
            if ld.get("aggregateRating"):
                ar = ld["aggregateRating"]
                result["other"]["rating"] = {
                    "value": ar.get("ratingValue"),
                    "count": ar.get("reviewCount"),
                }
        except Exception:
            pass

    # Фото
    result["image_urls"] = extract_images_from_detail(soup)

    # Хлебные крошки (KA оригинал)
    crumbs = [
        b.get_text(strip=True)
        for b in soup.select(".ty-breadcrumbs a, [class*='breadcrumb'] a")
        if b.get_text(strip=True)
    ]
    if crumbs:
        result["other"]["breadcrumbs_ka"] = crumbs

    return result


# ──────────────────────────────────────────────
# Обнаружение URL через sitemap
# ──────────────────────────────────────────────
async def discover_product_urls(session: aiohttp.ClientSession) -> list[str]:
    logger.info("Обнаружение URL через sitemap...")
    urls = []

    try:
        async with session.get(
            f"{BASE_URL}/sitemap.xml",
            headers=HEADERS,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            resp.raise_for_status()
            text = await resp.text()

        soup = BeautifulSoup(text, "xml")
        all_locs = [loc.get_text(strip=True) for loc in soup.find_all("loc")]

        # Sitemap index vs single sitemap
        if any("sitemap" in loc.lower() for loc in all_locs[:5]):
            sem = asyncio.Semaphore(5)

            async def fetch_sub(sm_url: str) -> list[str]:
                async with sem:
                    try:
                        async with session.get(
                            sm_url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)
                        ) as r:
                            r.raise_for_status()
                            t = await r.text()
                        sub = BeautifulSoup(t, "xml")
                        return [
                            l.get_text(strip=True)
                            for l in sub.find_all("loc")
                            if _is_product_url(l.get_text(strip=True))
                        ]
                    except Exception as e:
                        logger.warning(f"Sub-sitemap {sm_url}: {e}")
                        return []

            results = await asyncio.gather(*[fetch_sub(u) for u in all_locs])
            for batch in results:
                urls.extend(batch)
        else:
            urls = [l for l in all_locs if _is_product_url(l)]

    except Exception as e:
        logger.warning(f"Sitemap error: {e}")

    if not urls:
        logger.warning("Sitemap пустой, fallback на навигацию")
        urls = await _discover_via_nav(session)

    urls = list(dict.fromkeys(urls))
    logger.info(f"  -> Найдено {len(urls)} URL товаров")
    return urls


def _is_product_url(url: str) -> bool:
    if "/ka/" not in url:
        return False
    parts = [p for p in urllib.parse.urlparse(url).path.split("/") if p]
    return len(parts) >= 4


async def _discover_via_nav(session: aiohttp.ClientSession) -> list[str]:
    try:
        async with session.get(
            f"{BASE_URL}/ka/", headers=HEADERS, timeout=aiohttp.ClientTimeout(total=20)
        ) as resp:
            resp.raise_for_status()
            html = await resp.text()
    except Exception as e:
        logger.error(f"Главная страница недоступна: {e}")
        return []

    soup = BeautifulSoup(html, "html.parser")
    cat_urls = set()
    for a in soup.select(".ty-menu__items a[href], nav a[href], [class*='menu'] a[href]"):
        href = a.get("href", "")
        full = urllib.parse.urljoin(BASE_URL, href)
        if full.startswith(f"{BASE_URL}/ka/") and full.endswith("/"):
            cat_urls.add(full)

    product_urls = []
    sem = asyncio.Semaphore(3)

    async def crawl(cat_url: str):
        async with sem:
            found = await crawl_category_pages(session, cat_url)
            product_urls.extend(found)
            await asyncio.sleep(1)

    await asyncio.gather(*[crawl(u) for u in list(cat_urls)[:60]])
    return product_urls


async def crawl_category_pages(
    session: aiohttp.ClientSession, category_url: str
) -> list[str]:
    urls = []
    page = 1
    while page <= 200:
        url = category_url if page == 1 else f"{category_url}?page={page}"
        try:
            async with session.get(
                url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=25)
            ) as resp:
                if resp.status == 404:
                    break
                resp.raise_for_status()
                html = await resp.text()
        except Exception as e:
            logger.warning(f"Категория {url}: {e}")
            break

        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select(".ut2-gl__body")
        if not cards:
            break

        for card in cards:
            a = card.select_one(".ut2-gl__name a")
            if a and a.get("href"):
                urls.append(urllib.parse.urljoin(BASE_URL, a["href"]))

        if not soup.select_one(
            ".ty-pagination__next:not(.ty-pagination__disabled), .ty-pagination__next a"
        ):
            break

        page += 1
        await asyncio.sleep(settings.REQUEST_DELAY)

    return urls


# ──────────────────────────────────────────────
# Основной скрапер
# ──────────────────────────────────────────────
class GorgiaScraper:
    """
    full_site=True  -> обходит весь сайт через sitemap
    category_url    -> только конкретная категория
    """

    def __init__(
        self,
        category_url: Optional[str] = None,
        category_ka: str = "",
        sub_category_ka: str = "",
        full_site: bool = False,
        max_pages: int = 999,
    ):
        self.category_url    = category_url
        self.category_ka     = category_ka
        self.sub_category_ka = sub_category_ka
        self.full_site       = full_site
        self.max_pages       = max_pages

    async def scrape(self) -> list[GorgiaProduct]:
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            if self.full_site:
                product_urls = await discover_product_urls(session)
            elif self.category_url:
                product_urls = await crawl_category_pages(session, self.category_url)
            else:
                return []

            if not product_urls:
                return []

            logger.info(f"[Gorgia] {len(product_urls)} товаров к обработке")

            sem = asyncio.Semaphore(3)

            async def process(url: str) -> Optional[GorgiaProduct]:
                async with sem:
                    p = await self._scrape_one(session, url)
                    await asyncio.sleep(settings.REQUEST_DELAY)
                    return p

            results = await asyncio.gather(
                *[process(u) for u in product_urls],
                return_exceptions=True,
            )

            products = []
            for r in results:
                if isinstance(r, GorgiaProduct):
                    products.append(r)
                elif isinstance(r, Exception):
                    logger.error(f"Ошибка: {r}")

            logger.info(f"[Gorgia] Итого: {len(products)} товаров")
            return products

    async def _scrape_one(
        self, session: aiohttp.ClientSession, url: str
    ) -> Optional[GorgiaProduct]:
        for attempt in range(settings.MAX_RETRIES):
            try:
                async with session.get(
                    url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 404:
                        return None
                    resp.raise_for_status()
                    html = await resp.text()
                break
            except Exception as e:
                if attempt == settings.MAX_RETRIES - 1:
                    logger.error(f"Не удалось: {url}: {e}")
                    return None
                await asyncio.sleep(2 ** attempt)

        soup = BeautifulSoup(html, "html.parser")
        detail = parse_detail_page(soup)

        price = parse_price(soup)
        availability_ka, in_stock = parse_availability(soup)

        # Категория из хлебных крошек если не задана
        category_ka = self.category_ka
        sub_category_ka = self.sub_category_ka
        if not category_ka:
            crumbs = detail["other"].get("breadcrumbs_ka", [])
            if len(crumbs) > 1:
                category_ka = crumbs[1]
            if len(crumbs) > 2:
                sub_category_ka = crumbs[-2]

        # Сборка other
        other = {}
        if detail.get("brand"):
            other["brand"] = detail["brand"]
        if detail["features_ka"]:
            other["features_ka"] = detail["features_ka"]
        if detail["other"]:
            other.update(detail["other"])

        p = GorgiaProduct(
            external_id     = make_external_id(url),
            source_url      = url,
            name_ka         = detail["name_ka"] or "",
            description_ka  = detail["description_ka"] or "",
            availability_ka = availability_ka,
            category_ka     = category_ka,
            sub_category_ka = sub_category_ka,
            sku             = detail["sku"],
            price           = price,
            in_stock        = in_stock,
            image_urls_raw  = detail["image_urls"],
            other           = other,
        )

        await self._translate(session, p)
        await self._upload_images(session, p)

        flag = "✅" if in_stock else "❌"
        logger.info(
            f"  {flag} {p.name_ka[:45]} | SKU:{p.sku or '—'} | "
            f"{price} GEL | {p.availability_ru}"
        )
        return p

    async def _translate(self, session: aiohttp.ClientSession, p: GorgiaProduct):
        results = await asyncio.gather(
            translate_text(session, p.name_ka, "ru"),
            translate_text(session, p.name_ka, "en"),
            translate_text(session, p.description_ka, "ru"),
            translate_text(session, p.description_ka, "en"),
            translate_text(session, p.availability_ka, "ru"),
            translate_text(session, p.category_ka, "ru"),
            translate_text(session, p.category_ka, "en"),
            translate_text(session, p.sub_category_ka, "ru"),
            translate_text(session, p.sub_category_ka, "en"),
        )
        (
            p.name_ru, p.name_en,
            p.description_ru, p.description_en,
            p.availability_ru,
            p.category_ru, p.category_en,
            p.sub_category_ru, p.sub_category_en,
        ) = results

        # Переводим характеристики
        features_ka = p.other.get("features_ka", {})
        if features_ka:
            features_ru, features_en = {}, {}
            for k_ka, v_ka in list(features_ka.items())[:30]:  # лимит 30 строк
                k_ru, k_en, v_ru, v_en = await asyncio.gather(
                    translate_text(session, k_ka, "ru"),
                    translate_text(session, k_ka, "en"),
                    translate_text(session, v_ka, "ru"),
                    translate_text(session, v_ka, "en"),
                )
                features_ru[k_ru] = v_ru
                features_en[k_en] = v_en
                await asyncio.sleep(0.1)
            p.other["features_ru"] = features_ru
            p.other["features_en"] = features_en

    async def _upload_images(self, session: aiohttp.ClientSession, p: GorgiaProduct):
        if not p.image_urls_raw:
            return

        if not p.in_stock:
            # Не в наличии — оставляем оригинальные URL, не тратим место в Blob
            p.image_urls_blob = p.image_urls_raw
            return

        blob_urls = []
        for idx, raw_url in enumerate(p.image_urls_raw[:10]):
            ext  = "webp" if "webp" in raw_url else "jpg"
            path = f"gorgia/{p.external_id}/{idx}.{ext}"
            uploaded = await upload_to_vercel_blob(session, raw_url, path)
            blob_urls.append(uploaded or raw_url)
            await asyncio.sleep(0.3)

        p.image_urls_blob = blob_urls


# ──────────────────────────────────────────────
# Сохранение в Neon PostgreSQL
# ──────────────────────────────────────────────
async def save_to_db(products: list[GorgiaProduct]) -> tuple[int, int]:
    conn = await asyncpg.connect(settings.DATABASE_URL)
    inserted, updated = 0, 0

    for p in products:
        images = p.image_urls_blob or p.image_urls_raw
        name   = p.name_ru or p.name_en or p.name_ka

        existing = await conn.fetchrow(
            "SELECT id FROM products WHERE external_id=$1 AND source='gorgia'",
            p.external_id,
        )

        if existing is None:
            await conn.execute("""
                INSERT INTO products (
                    external_id, source, source_url,
                    name, name_ka, name_ru, name_en,
                    description, description_ka, description_ru, description_en,
                    availability_ka, availability_ru,
                    category_ka, category_ru, category_en,
                    sub_category_ka, sub_category_ru, sub_category_en,
                    sku, price, currency, in_stock,
                    images, other
                ) VALUES (
                    $1, 'gorgia', $2,
                    $3, $4, $5, $6,
                    $7, $8, $9, $10,
                    $11, $12,
                    $13, $14, $15,
                    $16, $17, $18,
                    $19, $20, $21, $22,
                    $23::jsonb, $24::jsonb
                )
            """,
                p.external_id, p.source_url,
                name, p.name_ka, p.name_ru, p.name_en,
                p.description_ru or p.description_en or p.description_ka,
                p.description_ka, p.description_ru, p.description_en,
                p.availability_ka, p.availability_ru,
                p.category_ka, p.category_ru, p.category_en,
                p.sub_category_ka, p.sub_category_ru, p.sub_category_en,
                p.sku, p.price, p.currency, p.in_stock,
                json.dumps(images),
                json.dumps(p.other),
            )
            inserted += 1
        else:
            await conn.execute("""
                UPDATE products SET
                    name            = $1,
                    name_ka         = $2,  name_ru = $3,  name_en = $4,
                    description     = $5,
                    description_ka  = $6,  description_ru = $7,  description_en = $8,
                    availability_ka = $9,  availability_ru = $10,
                    category_ka     = COALESCE($11, category_ka),
                    category_ru     = COALESCE($12, category_ru),
                    category_en     = COALESCE($13, category_en),
                    sub_category_ka = COALESCE($14, sub_category_ka),
                    sub_category_ru = COALESCE($15, sub_category_ru),
                    sub_category_en = COALESCE($16, sub_category_en),
                    sku             = COALESCE($17, sku),
                    price           = $18,
                    in_stock        = $19,
                    images          = COALESCE($20::jsonb, images),
                    other           = other || $21::jsonb,
                    updated_at      = NOW()
                WHERE external_id = $22 AND source = 'gorgia'
            """,
                name, p.name_ka, p.name_ru, p.name_en,
                p.description_ru or p.description_en or p.description_ka,
                p.description_ka, p.description_ru, p.description_en,
                p.availability_ka, p.availability_ru,
                p.category_ka or None, p.category_ru or None, p.category_en or None,
                p.sub_category_ka or None, p.sub_category_ru or None, p.sub_category_en or None,
                p.sku or None,
                p.price, p.in_stock,
                json.dumps(images) if images else None,
                json.dumps(p.other),
                p.external_id,
            )
            updated += 1

    await conn.close()
    logger.info(f"[DB] +{inserted} новых  ~{updated} обновлено")
    return inserted, updated


# ──────────────────────────────────────────────
# Список категорий (fallback если sitemap пустой)
# ──────────────────────────────────────────────
GORGIA_CATEGORIES: list[tuple[str, str, str]] = [
    # (url, category_ka, sub_category_ka)
    # Оставь пустым — main.py запустит GorgiaScraper(full_site=True)
]


# ──────────────────────────────────────────────
# Ручной запуск
# ──────────────────────────────────────────────
async def run_gorgia_full():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    scraper = GorgiaScraper(full_site=True)
    products = await scraper.scrape()
    if products:
        ins, upd = await save_to_db(products)
        logger.info(f"Готово: {len(products)} | +{ins} новых | ~{upd} обновлено")


if __name__ == "__main__":
    asyncio.run(run_gorgia_full())
