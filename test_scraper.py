#!/usr/bin/env python3
"""
Тестовый скрипт для проверки парсинга Gorgia.ge локально.
Без подключения к БД и загрузки реальных файлов в Yandex S3.
"""

import re
import json
import time
import hashlib
import urllib.parse
import requests
from bs4 import BeautifulSoup, NavigableString

# Фейковые домены для теста
S3_PREFIX = "bazariara/gorgia"
CDN_BASE  = "https://cdn.relaxdev.ru"
BASE_URL  = "https://gorgia.ge"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "ka,ru;q=0.9,en;q=0.8",
}

# Оставили только одну категорию для теста
CATEGORIES = [
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-ganateba/", "IKEA", "Освещение")
]

TRANS = {
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e','ж':'zh','з':'z',
    'и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r',
    'с':'s','т':'t','у':'u','ф':'f','х':'h','ц':'ts','ч':'ch','ш':'sh',
    'щ':'sch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya',
}

# ─── Вспомогательные функции ──────────────────────────────────────────────────

def make_external_id(url: str, category_ru: str) -> str:
    slug = category_ru.lower()
    slug = ''.join(TRANS.get(c, c) for c in slug)
    slug = re.sub(r'[^a-z0-9]', '', slug)
    num  = int(hashlib.md5(url.encode()).hexdigest()[:8], 16) % 90000 + 10000
    return f"{slug}_{num}"

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

def get_soup(url: str, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser") # Используем встроенный парсер, чтобы не зависеть от lxml
        except Exception as e:
            wait = 3 * (attempt + 1)
            print(f"  ⚠️ [{attempt+1}/{retries}] {e}. Повтор через {wait}с…")
            time.sleep(wait)
    return None

def translate_dummy(text: str, target: str = "ru") -> str:
    # Упрощенная заглушка для перевода, чтобы скрипт работал быстрее при тесте
    # Если хочешь проверять реальный перевод — можешь вернуть оригинальную функцию translate
    if not text: return ""
    return f"[{target}] {text}"

def upload_to_yandex_mock(img_url: str, s3_path: str) -> str:
    """
    Вместо реальной загрузки просто возвращаем готовую CDN ссылку.
    Так мы проверим правильность формирования путей.
    """
    cdn_url = f"{CDN_BASE}/{s3_path}"
    return cdn_url

def parse_price(card):
    tag = card.select_one(".ty-price-num")
    if not tag: return None
    for s in tag.find_all("sup"): s.extract()
    raw = ""
    for child in tag.children:
        if isinstance(child, NavigableString):
            t = str(child).strip()
            if t:
                raw = t
                break
    if not raw: raw = tag.get_text(strip=True)
    try: return float(re.sub(r"[^\d.,]", "", raw).replace(",", "."))
    except ValueError: return None

def parse_availability(card):
    tag = card.select_one(".ty-qty-in-stock")
    if not tag: return "", False
    text = tag.get_text(strip=True)
    if "მარაგშია" in text: return "მარაგშია", True
    if "მარაგი იწურება" in text: return "მარაგი იწურება", True
    if "არ არის მარაგში" in text: return "არ არის მარაგში", False
    return text, False

def get_image_urls(card):
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
        if srcset: add(srcset.split()[0])
        else:
            src = item.get("data-ca-product-additional-image-src", "")
            if src: add(src)
    return urls

# ─── Основная логика парсинга ─────────────────────────────────────────────────

def scrape_category_test(cat_url: str, category_ru: str, sub_category_ru: str):
    products = []
    page = 1

    print(f"📌 Парсинг категории: {category_ru} / {sub_category_ru}")

    # Для теста ограничимся 1 страницей, чтобы не ждать долго
    while page <= 1:
        url = cat_url if page == 1 else f"{cat_url}?page={page}"
        print(f"\n  📄 Страница {page}: {url}")

        soup = get_soup(url)
        if not soup: break

        cards = soup.select(".ut2-gl__body")
        if not cards:
            print(f"  ℹ️ Товаров не найдено, стоп")
            break

        print(f"  → Найдено товаров на странице: {len(cards)}")

        for card in cards:
            a_tag = card.select_one(".ut2-gl__name a")
            if not a_tag: continue

            product_url  = urllib.parse.urljoin(BASE_URL, a_tag.get("href", ""))
            name_ka      = a_tag.get_text(strip=True)
            price        = parse_price(card)
            avail_ka, in_stock = parse_availability(card)
            image_urls   = get_image_urls(card)
            external_id  = make_external_id(product_url, category_ru)
            
            uploaded = []
            
            # Эмулируем работу с фото
            if in_stock:
                for idx, img_url in enumerate(image_urls[:10]):
                    ext = "webp" if "webp" in img_url else "jpg"
                    s3_key = f"{S3_PREFIX}/{external_id}/{idx}.{ext}"
                    # Используем нашу функцию-заглушку
                    cdn_url = upload_to_yandex_mock(img_url, s3_key)
                    uploaded.append(cdn_url)
            else:
                uploaded = image_urls

            product_data = {
                "external_id": external_id,
                "name_ka": name_ka,
                "price": price,
                "in_stock": in_stock,
                "images_mapped": uploaded # Здесь должны быть ссылки на наш CDN
            }
            products.append(product_data)
            
            flag = "✅" if in_stock else "❌"
            print(f"    {flag} {name_ka[:40]}... | {price} ₾ | Фото: {len(uploaded)}")

        page += 1
        time.sleep(1) # Небольшая пауза, чтобы не спамить сайт

    return products

if __name__ == "__main__":
    print("🚀 Запуск локального тестирования парсера...\n")
    for cat_url, cat, sub_cat in CATEGORIES:
        results = scrape_category_test(cat_url, cat, sub_cat)
        
        print("\n=== РЕЗУЛЬТАТ (ПЕРВЫЕ 2 ТОВАРА) ===")
        # Выводим первые 2 товара в формате JSON, чтобы проверить правильность путей картинок
        print(json.dumps(results[:2], indent=4, ensure_ascii=False))
        