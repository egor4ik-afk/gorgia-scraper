#!/usr/bin/env python3
"""
scrapers/scraper.py — Gorgia.ge category scraper
"""

import os
import re
import json
import sys
import time
import urllib.parse
import requests
from bs4 import BeautifulSoup, NavigableString
import psycopg2

DATABASE_URL      = os.environ["DATABASE_URL"]
VERCEL_BLOB_TOKEN = os.environ.get("VERCEL_BLOB_TOKEN", "")
REQUEST_DELAY     = float(os.environ.get("REQUEST_DELAY", "1.5"))

BASE_URL = "https://gorgia.ge"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ka,ru;q=0.9,en;q=0.8",
}

CATEGORIES = [
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-ganaTeba/", "ikea", "Освещение"),
    ("https://gorgia.ge/ka/santeknika/smesitelebi/", "santehnika", "Смесители"),
    ("https://gorgia.ge/ka/santeknika/rakovina/", "santehnika", "Раковины"),
    ("https://gorgia.ge/ka/avejis-maRazia/magidebidamerxebi/", "mebel", "Столы"),
    ("https://gorgia.ge/ka/avejis-maRazia/skrebi/", "mebel", "Стулья"),
    ("https://gorgia.ge/ka/cxovelebisTvis/", "pet", ""),
]


# ------------------------------------------------
# Получаем настоящий ID товара из URL
# ------------------------------------------------

def get_product_id(url: str) -> str:
    m = re.search(r"-(\d+)(?:/)?$", url)
    if m:
        return m.group(1)
    return "0"


# ------------------------------------------------
# external_id = category_id
# ------------------------------------------------

def make_external_id(url: str, category_slug: str) -> str:
    pid = get_product_id(url)
    return f"{category_slug}_{pid}"


# ------------------------------------------------

def to_webp(url: str) -> str:
    return (
        url
        .replace("/images/thumbnails/240/240/", "/images/ab__webp/thumbnails/1100/900/")
        .replace("/images/thumbnails/480/480/", "/images/ab__webp/thumbnails/1100/900/")
        .replace(".jpg", "_jpg.webp")
        .replace(".jpeg", "_jpg.webp")
        .replace(".png", "_jpg.webp")
    )


def get_soup(url: str):

    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")

    except Exception as e:
        print("Ошибка:", e)
        return None


def parse_price(card):

    tag = card.select_one(".ty-price-num")

    if not tag:
        return None

    for s in tag.find_all("sup"):
        s.extract()

    raw = tag.get_text(strip=True)

    try:
        return float(re.sub(r"[^\d.,]", "", raw).replace(",", "."))
    except:
        return None


def parse_availability(card):

    tag = card.select_one(".ty-qty-in-stock")

    if not tag:
        return "", False

    text = tag.get_text(strip=True)

    if "მარაგშია" in text:
        return text, True

    if "არ არის მარაგში" in text:
        return text, False

    return text, False


def get_image_urls(card):

    urls = []

    img = card.select_one(".ut2-gl__image img")

    if img and img.get("src"):
        urls.append(to_webp(img["src"]))

    return urls


# ------------------------------------------------

def scrape_category(cat_url: str, category_slug: str, sub_category: str):

    products = []

    page = 1

    while True:

        url = cat_url if page == 1 else f"{cat_url}?page={page}"

        print("Страница:", url)

        soup = get_soup(url)

        if not soup:
            break

        cards = soup.select(".ut2-gl__body")

        if not cards:
            break

        for card in cards:

            a_tag = card.select_one(".ut2-gl__name a")

            if not a_tag:
                continue

            product_url = urllib.parse.urljoin(BASE_URL, a_tag.get("href", ""))

            name = a_tag.get_text(strip=True)

            price = parse_price(card)

            avail, in_stock = parse_availability(card)

            images = get_image_urls(card)

            external_id = make_external_id(product_url, category_slug)

            products.append({

                "external_id": external_id,
                "source": "gorgia",
                "source_url": product_url,
                "name": name,
                "category": category_slug,
                "sub_category": sub_category,
                "price": price,
                "currency": "GEL",
                "in_stock": in_stock,
                "image_url": images[0] if images else None,
                "images": json.dumps(images)

            })

            print("Товар:", name, "|", price)

            time.sleep(REQUEST_DELAY)

        page += 1

    return products


# ------------------------------------------------

def save_products(products):

    conn = psycopg2.connect(DATABASE_URL)

    with conn.cursor() as cur:

        for p in products:

            cur.execute(

                """
                INSERT INTO products (
                    external_id,
                    source,
                    source_url,
                    name,
                    category,
                    sub_category,
                    price,
                    currency,
                    in_stock,
                    image_url,
                    images
                )
                VALUES (
                    %(external_id)s,
                    %(source)s,
                    %(source_url)s,
                    %(name)s,
                    %(category)s,
                    %(sub_category)s,
                    %(price)s,
                    %(currency)s,
                    %(in_stock)s,
                    %(image_url)s,
                    %(images)s::jsonb
                )
                ON CONFLICT (external_id)
                DO UPDATE SET
                    price = EXCLUDED.price,
                    in_stock = EXCLUDED.in_stock,
                    image_url = EXCLUDED.image_url,
                    images = EXCLUDED.images
                """

            , p)

    conn.commit()

    conn.close()


# ------------------------------------------------

def main():

    print("Gorgia scraper старт")

    for cat_url, category_slug, sub in CATEGORIES:

        print("\nКатегория:", category_slug, "/", sub)

        products = scrape_category(cat_url, category_slug, sub)

        save_products(products)


# ------------------------------------------------

if __name__ == "__main__":
    main()