#!/usr/bin/env python3
"""
updater.py — Ежедневное обновление цен и наличия
"""

import os
import re
import time
import requests
from bs4 import BeautifulSoup, NavigableString
import psycopg2
import psycopg2.extras

DATABASE_URL  = os.environ["DATABASE_URL"]
REQUEST_DELAY = float(os.environ.get("REQUEST_DELAY", "1.5"))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
}


def get_soup(url):
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except Exception as e:
            time.sleep(3 * (attempt + 1))
    return None


def parse_price(soup):
    tag = soup.select_one(".ty-price-num")
    if not tag:
        return None
    for s in tag.find_all("sup"):
        s.extract()
    raw = tag.get_text(strip=True)
    try:
        return float(re.sub(r"[^\d.,]", "", raw).replace(",", "."))
    except ValueError:
        return None


def parse_availability(soup):
    tag = soup.select_one(".ty-qty-in-stock")
    if not tag:
        return "", False
    text = tag.get_text(strip=True)
    if "მარაგშია" in text:
        return "მარაგშია", True
    if "მარაგი იწურება" in text:
        return "მარაგი იწურება", True
    return "არ არის მარაგში", False


def translate(text, target="ru"):
    if not text:
        return ""
    try:
        r = requests.get(
            "https://translate.googleapis.com/translate_a/single",
            params={"client": "gtx", "sl": "ka", "tl": target, "dt": "t", "q": text},
            timeout=10,
        )
        if r.status_code == 200:
            return "".join(t[0] for t in r.json()[0] if t[0])
    except Exception:
        pass
    return text


def main():
    print("Updater запущен")
    conn = psycopg2.connect(DATABASE_URL)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id, source_url, price, in_stock, availability_ka
            FROM products
            WHERE source = 'gorgia' AND source_url IS NOT NULL
            ORDER BY updated_at ASC
        """)
        rows = cur.fetchall()

    print(f"Товаров к обновлению: {len(rows)}")
    updated = errors = 0

    for i, row in enumerate(rows, 1):
        url = row["source_url"]
        print(f"[{i}/{len(rows)}] {url[-60:]}", end=" ")

        soup = get_soup(url)
        if not soup:
            print("недоступен")
            errors += 1
            time.sleep(REQUEST_DELAY)
            continue

        new_price = parse_price(soup)
        new_avail_ka, new_in_stock = parse_availability(soup)

        changes = {}
        if new_price is not None and new_price != float(row["price"] or 0):
            changes["price"] = new_price
        if new_in_stock != row["in_stock"]:
            changes["in_stock"] = new_in_stock
        if new_avail_ka and new_avail_ka != (row["availability_ka"] or ""):
            changes["availability_ka"] = new_avail_ka
            changes["availability_ru"] = translate(new_avail_ka, "ru")

        if changes:
            sets = ", ".join(f"{k} = %s" for k in changes)
            vals = list(changes.values()) + [row["id"]]
            with conn.cursor() as cur:
                cur.execute(f"UPDATE products SET {sets}, updated_at = NOW() WHERE id = %s", vals)
            conn.commit()
            updated += 1
            print(f"updated: {list(changes.keys())}")
        else:
            with conn.cursor() as cur:
                cur.execute("UPDATE products SET updated_at = NOW() WHERE id = %s", [row["id"]])
            conn.commit()
            print("ok")

        time.sleep(REQUEST_DELAY)

    conn.close()
    print(f"Done: updated={updated} errors={errors}")


if __name__ == "__main__":
    main()
