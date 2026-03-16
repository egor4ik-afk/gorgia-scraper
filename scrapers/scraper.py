#!/usr/bin/env python3
"""
scraper.py — Gorgia.ge category scraper
- Обходит список категорий (все страницы пагинации)
- Переводит KA → RU и KA → EN
- Загружает фото в Vercel Blob
- Сохраняет в Neon PostgreSQL
"""

import os
import re
import json
import time
import base64
import hashlib
import urllib.parse
import requests
from pathlib import Path
from bs4 import BeautifulSoup, NavigableString
import psycopg2
import psycopg2.extras

# ─────────────────────────────────────────────
# Настройки из переменных окружения
# ─────────────────────────────────────────────
DATABASE_URL        = os.environ["DATABASE_URL"]
VERCEL_BLOB_TOKEN   = os.environ.get("VERCEL_BLOB_TOKEN", "")
REQUEST_DELAY       = float(os.environ.get("REQUEST_DELAY", "1.5"))

BASE_URL = "https://gorgia.ge"
HEADERS  = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "ka,ru;q=0.9,en;q=0.8",
}

# ─────────────────────────────────────────────
# Список категорий gorgia.ge
# Формат: (url, category_ru, sub_category_ru)
# ─────────────────────────────────────────────
CATEGORIES = [
    # IKEA
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-aveji/ikeas-magidebi-da-merxebi/",       "IKEA", "Столы"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-aveji/ikeas-stulebida-skamebi/",          "IKEA", "Стулья"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-aveji/ikeas-karebiani-satumebi/",         "IKEA", "Шкафы"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-aveji/ikeas-sadzineo-aveji/",             "IKEA", "Гостиная"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-aveji/ikeas-saZinao-aveji/",              "IKEA", "Спальня"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-ganaTeba/",                               "IKEA", "Освещение"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-samzareulosaTvis/",                       "IKEA", "Кухня"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-abazanisaTvis/",                          "IKEA", "Ванная"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-saTamaSoebi-da-bavSvTa-aveji/",           "IKEA", "Детская"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-tekstili/",                               "IKEA", "Текстиль"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-dekoracia/",                              "IKEA", "Декор"),

    # Климатическое оборудование
    ("https://gorgia.ge/ka/klimaturi-teqnika/kondicionerebi/",                             "Климатическое оборудование", "Кондиционеры"),
    ("https://gorgia.ge/ka/klimaturi-teqnika/saventilacio-sistemebi/",                     "Климатическое оборудование", "Вентиляция"),
    ("https://gorgia.ge/ka/klimaturi-teqnika/gamaTbobeli-aparatebi/",                      "Климатическое оборудование", "Обогреватели"),

    # Товары для дома
    ("https://gorgia.ge/ka/sayofacxovrebo/samzareulosaTvis/",                              "Товары для дома", "Кухня"),
    ("https://gorgia.ge/ka/sayofacxovrebo/abazanisaTvis/",                                 "Товары для дома", "Ванная"),
    ("https://gorgia.ge/ka/sayofacxovrebo/dasufTaveba/",                                   "Товары для дома", "Уборка"),
    ("https://gorgia.ge/ka/sayofacxovrebo/tekstili/",                                      "Товары для дома", "Текстиль"),

    # Сад и огород
    ("https://gorgia.ge/ka/baRi-da-aivani/",                                               "Сад и огород", ""),

    # Туризм и отдых
    ("https://gorgia.ge/ka/turizmi-da-dasveneba/",                                         "Туризм и отдых", ""),

    # Детские товары
    ("https://gorgia.ge/ka/bavSvTa-tovarebi/",                                             "Детские товары", ""),

    # Инструменты
    ("https://gorgia.ge/ka/instrumentebi/",                                                "Инструменты", ""),

    # Электроника
    ("https://gorgia.ge/ka/eleqtronika/",                                                  "Электроника", ""),
]


# ─────────────────────────────────────────────
# Утилиты
# ─────────────────────────────────────────────

def make_external_id(url: str) -> str:
    path = urllib.parse.urlparse(url).path.strip("/")
    slug = re.sub(r"[^a-zA-Z0-9_\-]", "_", path)[:80]
    short = hashlib.md5(path.encode()).hexdigest()[:6]
    return f"{slug}_{short}"


def to_webp(url: str) -> str:
    return (
        url
        .replace("/images/thumbnails/240/240/", "/images/ab__webp/thumbnails/1100/900/")
        .replace("/images/thumbnails/480/480/", "/images/ab__webp/thumbnails/1100/900/")
        .replace(".jpg",  "_jpg.webp")
        .replace(".JPG",  "_jpg.webp")
        .replace(".jpeg", "_jpg.webp")
        .replace(".png",  "_jpg.webp")
    )


def get_soup(url: str, retries=3) -> BeautifulSoup | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except Exception as e:
            wait = 3 * (attempt + 1)
            print(f"  ⚠️ [{attempt+1}/{retries}] {url}: {e}. Retry in {wait}s…")
            time.sleep(wait)
    return None


# ─────────────────────────────────────────────
# Перевод
# ─────────────────────────────────────────────

def translate(text: str, target: str = "ru") -> str:
    if not text or not text.strip():
        return ""
    try:
        r = requests.get(
            "https://translate.googleapis.com/translate_a/single",
            params={"client": "gtx", "sl": "ka", "tl": target, "dt": "t", "q": text},
            timeout=12,
        )
        if r.status_code == 200:
            return "".join(t[0] for t in r.json()[0] if t[0])
    except Exception as e:
        print(f"  ⚠️ translate({target}): {e}")
    return text


# ─────────────────────────────────────────────
# Vercel Blob
# ─────────────────────────────────────────────

def upload_to_blob(img_url: str, blob_path: str) -> str | None:
    if not VERCEL_BLOB_TOKEN:
        return img_url  # fallback — оригинальный URL
    try:
        r = requests.get(img_url, headers=HEADERS, stream=True, timeout=25)
        if r.status_code != 200:
            return None
        content = r.content
        ctype = r.headers.get("Content-Type", "image/webp")

        res = requests.put(
            f"https://blob.vercel-storage.com/{blob_path}",
            headers={
                "Authorization": f"Bearer {VERCEL_BLOB_TOKEN}",
                "Content-Type": ctype,
                "x-content-type": ctype,
            },
            data=content,
            timeout=40,
        )
        if res.status_code in (200, 201):
            return res.json().get("url")
        print(f"  ⚠️ Blob {res.status_code}: {res.text[:100]}")
    except Exception as e:
        print(f"  ❌ Blob upload: {e}")
    return None


# ─────────────────────────────────────────────
# Парсинг карточки
# ─────────────────────────────────────────────

def parse_price(card) -> float | None:
    tag = card.select_one(".ty-price-num")
    if not tag:
        return None
    for s in tag.find_all("sup"):
        s.extract()
    raw = ""
    for child in tag.children:
        if isinstance(child, NavigableString):
            t = str(child).strip()
            if t:
                raw = t
                break
    if not raw:
        raw = tag.get_text(strip=True)
    try:
        return float(re.sub(r"[^\d.,]", "", raw).replace(",", "."))
    except ValueError:
        return None


def parse_availability(card) -> tuple[str, bool]:
    tag = card.select_one(".ty-qty-in-stock")
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


def get_image_urls(card) -> list[str]:
    urls, seen = [], set()

    def add(u):
        u = u.strip()
        if u and u not in seen:
            urls.append(u)
            seen.add(u)

    img = card.select_one(".ut2-gl__image img")
    if img and img.get("src"):
        add(to_webp(img["src"]))

    for item in card.select(".item[data-ca-product-additional-image-src]"):
        srcset = item.get("data-ca-product-additional-image-srcset", "")
        if srcset:
            add(srcset.split()[0])
        else:
            src = item.get("data-ca-product-additional-image-src", "")
            if src:
                add(src)

    return urls


# ─────────────────────────────────────────────
# Обход категории (все страницы)
# ─────────────────────────────────────────────

def scrape_category(cat_url: str, category_ru: str, sub_category_ru: str) -> list[dict]:
    products = []
    page = 1

    while True:
        url = cat_url if page == 1 else f"{cat_url}?page={page}"
        print(f"\n  📄 Страница {page}: {url}")

        soup = get_soup(url)
        if not soup:
            break

        cards = soup.select(".ut2-gl__body")
        if not cards:
            print(f"  ℹ️ Товаров не найдено, стоп")
            break

        print(f"  → {len(cards)} товаров")

        for card in cards:
            a_tag = card.select_one(".ut2-gl__name a")
            if not a_tag:
                continue

            product_url = urllib.parse.urljoin(BASE_URL, a_tag.get("href", ""))
            name_ka     = a_tag.get_text(strip=True)
            price       = parse_price(card)
            avail_ka, in_stock = parse_availability(card)
            image_urls  = get_image_urls(card)
            external_id = make_external_id(product_url)

            # Переводы
            name_ru = translate(name_ka, "ru")
            name_en = translate(name_ka, "en")
            avail_ru = translate(avail_ka, "ru") if avail_ka else ""

            # Загрузка фото в Vercel Blob
            uploaded = []
            if in_stock:
                for idx, img_url in enumerate(image_urls[:10]):
                    ext  = "webp" if "webp" in img_url else "jpg"
                    path = f"gorgia/{external_id}/{idx}.{ext}"
                    result = upload_to_blob(img_url, path)
                    uploaded.append(result or img_url)
                    time.sleep(0.3)
            else:
                uploaded = image_urls  # fallback для не в наличии

            main_image = uploaded[0] if uploaded else None
            images     = uploaded

            products.append({
                "external_id":    external_id,
                "source":         "gorgia",
                "source_url":     product_url,
                "name":           name_ru or name_ka,
                "name_ka":        name_ka,
                "name_ru":        name_ru,
                "name_en":        name_en,
                "availability_ka": avail_ka,
                "availability_ru": avail_ru,
                "category":       category_ru,
                "category_ru":    category_ru,
                "sub_category":   sub_category_ru,
                "sub_category_ru": sub_category_ru,
                "price":          price,
                "currency":       "GEL",
                "in_stock":       in_stock,
                "image_url":      main_image,
                "images":         json.dumps(images),
            })

            flag = "✅" if in_stock else "❌"
            print(f"    {flag} {name_ru[:50]} | {price} ₾ | {len(images)} фото")
            time.sleep(REQUEST_DELAY)

        # Проверяем следующую страницу
        next_btn = soup.select_one(
            ".ty-pagination__next:not(.ty-pagination__disabled), .ty-pagination__next a"
        )
        if not next_btn:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    return products


# ─────────────────────────────────────────────
# База данных
# ─────────────────────────────────────────────

def get_done_urls(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT source_url FROM products WHERE source = 'gorgia' AND source_url IS NOT NULL")
        return {row[0] for row in cur.fetchall()}


def upsert_product(conn, p: dict):
    sql = """
    INSERT INTO products (
        external_id, source, source_url,
        name, name_ka, name_ru, name_en,
        availability_ka, availability_ru,
        category, category_ru,
        sub_category, sub_category_ru,
        price, currency, in_stock,
        image_url, images
    ) VALUES (
        %(external_id)s, %(source)s, %(source_url)s,
        %(name)s, %(name_ka)s, %(name_ru)s, %(name_en)s,
        %(availability_ka)s, %(availability_ru)s,
        %(category)s, %(category_ru)s,
        %(sub_category)s, %(sub_category_ru)s,
        %(price)s, %(currency)s, %(in_stock)s,
        %(image_url)s, %(images)s
    )
    ON CONFLICT (external_id, source) DO UPDATE SET
        price           = EXCLUDED.price,
        in_stock        = EXCLUDED.in_stock,
        availability_ka = EXCLUDED.availability_ka,
        availability_ru = EXCLUDED.availability_ru,
        image_url       = COALESCE(EXCLUDED.image_url, products.image_url),
        images          = COALESCE(EXCLUDED.images, products.images),
        updated_at      = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(sql, p)
    conn.commit()


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    print("🚀 Gorgia scraper запущен")
    print(f"📂 Категорий: {len(CATEGORIES)}")
    print(f"🗄  DB: {DATABASE_URL[:40]}…")
    print(f"🖼  Blob: {'✓' if VERCEL_BLOB_TOKEN else '✗ (fallback на оригинальные URL)'}\n")

    conn = psycopg2.connect(DATABASE_URL)
    done_urls = get_done_urls(conn)
    print(f"ℹ️  Уже в БД: {len(done_urls)} товаров\n")

    total_new = total_upd = 0

    for cat_url, category_ru, sub_category_ru in CATEGORIES:
        label = f"{category_ru} / {sub_category_ru}" if sub_category_ru else category_ru
        print(f"\n{'='*60}")
        print(f"📁 {label}")
        print(f"{'='*60}")

        products = scrape_category(cat_url, category_ru, sub_category_ru)

        new = upd = 0
        for p in products:
            exists = p["source_url"] in done_urls
            upsert_product(conn, p)
            done_urls.add(p["source_url"])
            if exists:
                upd += 1
            else:
                new += 1

        total_new += new
        total_upd += upd
        print(f"\n  ✓ {label}: +{new} новых, ~{upd} обновлено")
        time.sleep(2)

    conn.close()
    print(f"\n{'='*60}")
    print(f"✅ ГОТОВО: +{total_new} новых, ~{total_upd} обновлено")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()


def main_single():
    """Парсит одну категорию из переменных окружения (вызывается webhook_server.py)."""
    cat_url  = os.environ.get("SCRAPE_URL", "")
    category = os.environ.get("SCRAPE_CATEGORY", "")
    sub      = os.environ.get("SCRAPE_SUB", "")

    if not cat_url and not category:
        print("Ошибка: нужен SCRAPE_URL или SCRAPE_CATEGORY")
        return

    # Если URL не задан — ищем в списке CATEGORIES по имени
    if not cat_url:
        matches = [(u, c, s) for u, c, s in CATEGORIES if c == category and (not sub or s == sub)]
        if not matches:
            print(f"Категория не найдена: {category} / {sub}")
            return
        for url, cat, sub_cat in matches:
            print(f"\nПарсим: {cat} / {sub_cat}")
            conn = psycopg2.connect(DATABASE_URL)
            products = scrape_category(url, cat, sub_cat)
            done = get_done_urls(conn)
            new = upd = 0
            for p in products:
                exists = p["source_url"] in done
                upsert_product(conn, p)
                if exists: upd += 1
                else: new += 1
            conn.close()
            print(f"Готово: +{new} новых, ~{upd} обновлено")
        return

    # Парсим конкретный URL
    print(f"\nПарсим: {category} / {sub} | {cat_url}")
    conn = psycopg2.connect(DATABASE_URL)
    products = scrape_category(cat_url, category, sub)
    done = get_done_urls(conn)
    new = upd = 0
    for p in products:
        exists = p["source_url"] in done
        upsert_product(conn, p)
        if exists: upd += 1
        else: new += 1
    conn.close()
    print(f"Готово: +{new} новых, ~{upd} обновлено")


if __name__ == "__main__":
    import sys
    if "--single" in sys.argv:
        main_single()
    else:
        main()
