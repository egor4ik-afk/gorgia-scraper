#!/usr/bin/env python3
"""
scrapers/scraper.py — Gorgia.ge category scraper
"""

import os
import re
import json
import sys
import time
import hashlib
import urllib.parse
import requests
from bs4 import BeautifulSoup, NavigableString
import psycopg2

DATABASE_URL      = os.environ["DATABASE_URL"]
VERCEL_BLOB_TOKEN = os.environ.get("VERCEL_BLOB_TOKEN", "")
REQUEST_DELAY     = float(os.environ.get("REQUEST_DELAY", "1.5"))

BASE_URL = "https://gorgia.ge"
HEADERS  = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "ka,ru;q=0.9,en;q=0.8",
}

CATEGORIES = [
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-ganaTeba/", "IKEA", "Освещение"),
    # Добавляй другие категории по необходимости
]

def make_external_id(url: str, category_ru: str) -> str:
    slug = category_ru.lower()
    slug = re.sub(r'[^a-z0-9]', '', slug)  # одно слово
    num  = int(hashlib.md5(url.encode()).hexdigest()[:8], 16) % 90000 + 10000
    return f"{slug}_{num}"

def get_soup(url: str, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except Exception as e:
            wait = 3 * (attempt + 1)
            print(f"  ⚠️ [{attempt+1}/{retries}] {e}. Retry in {wait}s…")
            time.sleep(wait)
    return None

def parse_price(card):
    tag = card.select_one(".ty-price-num")
    if not tag:
        return None
    for s in tag.find_all("sup"):
        s.extract()
    raw = "".join(c.strip() for c in tag.strings if c.strip())
    try:
        return float(re.sub(r"[^\d.,]", "", raw).replace(",", "."))
    except ValueError:
        return None

def parse_availability(card):
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

def get_image_urls(card):
    urls, seen = [], set()

    def add(u):
        u = u.strip()
        if u and u not in seen:
            urls.append(u)
            seen.add(u)

    img = card.select_one(".ut2-gl__image img")
    if img and img.get("src"):
        add(img["src"])

    for item in card.select(".item[data-ca-product-additional-image-src]"):
        src = item.get("data-ca-product-additional-image-src", "")
        if src:
            add(src)

    return urls

def scrape_category(cat_url: str, category_ru: str, sub_category_ru: str) -> list:
    products = []
    page = 1

    while True:
        url = cat_url if page == 1 else f"{cat_url}?page={page}"
        print(f"\n📄 Страница {page}: {url}")

        soup = get_soup(url)
        if not soup:
            print("  ℹ️ Страница не найдена или ошибка")
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
            external_id = make_external_id(product_url, category_ru)

            products.append({
                "external_id":     external_id,
                "source":          "gorgia",
                "source_url":      product_url,
                "name":            name_ka,
                "name_ka":         name_ka,
                "name_ru":         name_ka,
                "name_en":         name_ka,
                "availability_ka": avail_ka,
                "availability_ru": avail_ka,
                "category":        category_ru,
                "category_ru":     category_ru,
                "sub_category":    sub_category_ru,
                "sub_category_ru": sub_category_ru,
                "price":           price,
                "currency":        "GEL",
                "in_stock":        in_stock,
                "image_url":       image_urls[0] if image_urls else None,
                "images":          json.dumps(image_urls),
            })

        next_btn = soup.select_one(".ty-pagination__next:not(.ty-pagination__disabled)")
        if not next_btn:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    return products

def save_products(products: list):
    if not products:
        print("Нет товаров для сохранения")
        return
    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor() as cur:
        for p in products:
            cur.execute("""
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
                    %(image_url)s, %(images)s::jsonb
                )
                ON CONFLICT (external_id) DO UPDATE SET
                    price = EXCLUDED.price,
                    in_stock = EXCLUDED.in_stock,
                    availability_ka = EXCLUDED.availability_ka,
                    availability_ru = EXCLUDED.availability_ru,
                    image_url = EXCLUDED.image_url,
                    images = EXCLUDED.images,
                    name = EXCLUDED.name,
                    updated_at = NOW()
            """, p)
    conn.commit()
    conn.close()
    print(f"Сохранено товаров: {len(products)}")

def main():
    print("🚀 Gorgia scraper запущен")
    total_products = 0
    for cat_url, category_ru, sub_category_ru in CATEGORIES:
        print(f"\n📁 Парсим категорию: {category_ru} / {sub_category_ru}")
        products = scrape_category(cat_url, category_ru, sub_category_ru)
        save_products(products)
        total_products += len(products)
        time.sleep(2)
    print(f"\n✅ Скрейп завершен, всего товаров: {total_products}")

if __name__ == "__main__":
    main()