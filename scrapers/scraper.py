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
VERCEL_BLOB_TOKEN = os.environ.get("BLOB_READ_WRITE_TOKEN", "") or os.environ.get("VERCEL_BLOB_TOKEN", "")
REQUEST_DELAY     = float(os.environ.get("REQUEST_DELAY", "1.5"))
TG_TOKEN          = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT           = os.environ.get("TELEGRAM_CHAT_ID", "")

BASE_URL = "https://gorgia.ge"
HEADERS  = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "ka,ru;q=0.9,en;q=0.8",
}

CATEGORIES = [
    # ── IKEA ──────────────────────────────────────────────────────────────────
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-aveji/ikeas-magidebi-da-merxebi/",   "IKEA", "Столы"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-aveji/ikeas-stulebida-skamebi/",      "IKEA", "Стулья"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-aveji/ikeas-karebiani-satumebi/",     "IKEA", "Шкафы"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-aveji/ikeas-sadzineo-aveji/",         "IKEA", "Гостиная"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-aveji/ikeas-saZinao-aveji/",          "IKEA", "Спальня"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-aveji/ikeas-samushao-otaxi/",         "IKEA", "Рабочий кабинет"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-ganateba/",                           "IKEA", "Освещение"),        # ganaTeba → ganateba
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-samzareulo/",                         "IKEA", "Кухня"),            # samzareulosaTvis → samzareulo
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-saabazano/",                          "IKEA", "Ванная"),           # abazanisaTvis → saabazano
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-sabavshvo-otaxi/",                    "IKEA", "Детская"),          # saTamaSoebi → sabavshvo-otaxi
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-teqstili/",                           "IKEA", "Текстиль"),         # tekstili → teqstili
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-saxlis-dekori/",                      "IKEA", "Декор"),            # dekoracia → saxlis-dekori
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-saxlis-movla-da-organizeba/",         "IKEA", "Организация"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-eqsterieri/",                         "IKEA", "Экстерьер"),

    # ── Климатическое оборудование ────────────────────────────────────────────
    ("https://gorgia.ge/ka/klimaturi-teqnika/centraluri-gatbobis-sistema/",            "Климатическое оборудование", "Центральное отопление"),
    ("https://gorgia.ge/ka/klimaturi-teqnika/kondicioneri/",                           "Климатическое оборудование", "Кондиционеры"),     # kondicionerebi → kondicioneri
    ("https://gorgia.ge/ka/klimaturi-teqnika/ventilatorebi/",                          "Климатическое оборудование", "Вентиляторы"),
    ("https://gorgia.ge/ka/klimaturi-teqnika/wylis-gamacxeleblebi/",                   "Климатическое оборудование", "Водонагреватели"),  # wylis-gamaTbobeli → wylis-gamacxeleblebi
    ("https://gorgia.ge/ka/klimaturi-teqnika/gamatboblebi/",                           "Климатическое оборудование", "Обогреватели"),     # gamaTbobeli-aparatebi → gamatboblebi
    ("https://gorgia.ge/ka/klimaturi-teqnika/saventilacio-sistemebi/",                 "Климатическое оборудование", "Вентиляция"),
    ("https://gorgia.ge/ka/klimaturi-teqnika/koleqtorebi-da-boilerebi/",               "Климатическое оборудование", "Коллекторы"),       # kolektorebi → koleqtorebi-da-boilerebi

    # ── Мебель ────────────────────────────────────────────────────────────────
    ("https://gorgia.ge/ka/aveji/magidebi-da-merxebi/",                                "Мебель", "Столы"),          # avejis-maRazia → aveji
    ("https://gorgia.ge/ka/aveji/skamebi/",                                            "Мебель", "Стулья"),
    ("https://gorgia.ge/ka/aveji/rbili-aveji/",                                        "Мебель", "Мягкая мебель"),
    ("https://gorgia.ge/ka/aveji/karadebi-da-taroebi/",                                "Мебель", "Шкафы и стеллажи"),
    ("https://gorgia.ge/ka/aveji/sadzinebeli/",                                        "Мебель", "Спальня"),
    ("https://gorgia.ge/ka/aveji/samzareulos-aveji/",                                  "Мебель", "Кухонная мебель"),
    ("https://gorgia.ge/ka/aveji/komodi-da-tumbo/",                                    "Мебель", "Тумбочки"),
    ("https://gorgia.ge/ka/aveji/gare-aveji/",                                         "Мебель", "Уличная мебель"),
    ("https://gorgia.ge/ka/aveji/sarke/",                                              "Мебель", "Зеркала"),
    ("https://gorgia.ge/ka/sabavshvo/sabavshvo-aveji/",                                "Мебель", "Детская мебель"),

    # ── Сантехника ────────────────────────────────────────────────────────────
    ("https://gorgia.ge/ka/santeqnika/saabazanos-aveji/",                              "Сантехника", "Мебель для ванной"),    # santeknika → santeqnika
    ("https://gorgia.ge/ka/santeqnika/onkanebi-da-sashxape-sistemebi/",                "Сантехника", "Смесители и душевые"),  # smesitelebi → onkanebi
    ("https://gorgia.ge/ka/santeqnika/abazana-da-sashxape-kabina/",                    "Сантехника", "Ванны и душевые кабины"),
    ("https://gorgia.ge/ka/santeqnika/unitazi-da-makompleqteblebi/",                   "Сантехника", "Унитазы"),
    ("https://gorgia.ge/ka/santeqnika/wyalmomarageba-da-sakanalizacio-sistemebi/",      "Сантехника", "Водоснабжение"),
    ("https://gorgia.ge/ka/santeqnika/saabazanos-da-tualetis-aqsesuarebi/",            "Сантехника", "Аксессуары"),
    ("https://gorgia.ge/ka/santeqnika/xelsabani-da-aqsesuarebi/",                      "Сантехника", "Раковины"),             # rakovina → xelsabani
    ("https://gorgia.ge/ka/santeqnika/bide-da-pisuari/",                               "Сантехника", "Биде"),

    # ── Освещение ─────────────────────────────────────────────────────────────
    ("https://gorgia.ge/ka/ganateba/shida-ganateba/",                                  "Освещение", "Внутреннее"),
    ("https://gorgia.ge/ka/ganateba/magidis-sanatebi-da-torsherebi/",                  "Освещение", "Настольные лампы"),      # magidis-naTurebi → magidis-sanatebi-da-torsherebi
    ("https://gorgia.ge/ka/ganateba/gare-ganateba/",                                   "Освещение", "Уличное"),
    ("https://gorgia.ge/ka/ganateba/teqnikuri-ganateba/",                              "Освещение", "Техническое"),
    ("https://gorgia.ge/ka/ganateba/damagrdzeleblebi-da-gadamyvanebi/",                "Освещение", "Удлинители"),
    ("https://gorgia.ge/ka/ganateba/elementebi-da-batareebi/",                         "Освещение", "Батарейки"),

    # ── Ремонт ────────────────────────────────────────────────────────────────
    ("https://gorgia.ge/ka/remonti/keramikuli-filebi/",                                "Ремонт", "Плитка"),
    ("https://gorgia.ge/ka/remonti/kari/",                                             "Ремонт", "Двери"),
    ("https://gorgia.ge/ka/remonti/iataki/",                                           "Ремонт", "Полы"),
    ("https://gorgia.ge/ka/remonti/shpaleri-da-penoplastis-karnizebi/",                "Ремонт", "Обои"),
    ("https://gorgia.ge/ka/remonti/laq-sagebavebi/",                                   "Ремонт", "Краски"),
    ("https://gorgia.ge/ka/remonti/samontajo-cheri/",                                  "Ремонт", "Потолки"),
    ("https://gorgia.ge/ka/remonti/fanjara/",                                          "Ремонт", "Окна"),

    # ── Строительство ─────────────────────────────────────────────────────────
    ("https://gorgia.ge/ka/mshenebloba/aguri-da-bloki/",                               "Строительство", "Кирпич и блоки"),
    ("https://gorgia.ge/ka/mshenebloba/saizolacio-masalebi/",                          "Строительство", "Изоляция"),
    ("https://gorgia.ge/ka/mshenebloba/samsheneblo-fxvnilebi/",                        "Строительство", "Сухие смеси"),
    ("https://gorgia.ge/ka/mshenebloba/webo-da-sahermetizacio-masalebi/",              "Строительство", "Клеи и герметики"),
    ("https://gorgia.ge/ka/mshenebloba/saxarji-masala/",                               "Строительство", "Расходные материалы"),
    ("https://gorgia.ge/ka/mshenebloba/samsheneblo-propili-da-sxva-aqsesuarebi/",      "Строительство", "Строительный профиль"),
    ("https://gorgia.ge/ka/mshenebloba/saxuravebi-da-fasadis-sistemebi/",              "Строительство", "Кровля и фасады"),
    ("https://gorgia.ge/ka/mshenebloba/kibis-safexurebi-da-moajirebi/",                "Строительство", "Лестницы"),

    # ── Инструменты ───────────────────────────────────────────────────────────
    ("https://gorgia.ge/ka/xelsawyoebi/saxvreti-da-satex-sangrevi/",                   "Инструменты", "Сверление"),
    ("https://gorgia.ge/ka/xelsawyoebi/saxerxi-da-sachrelebi/",                        "Инструменты", "Пилы и резка"),
    ("https://gorgia.ge/ka/xelsawyoebi/kibeebi/",                                      "Инструменты", "Лестницы"),
    ("https://gorgia.ge/ka/xelsawyoebi/shesadugeblebi/",                               "Инструменты", "Сварка"),
    ("https://gorgia.ge/ka/xelsawyoebi/uniforma-da-usafrtxoeba/",                      "Инструменты", "Спецодежда"),
    ("https://gorgia.ge/ka/xelsawyoebi/mafiqsireblebi/",                               "Инструменты", "Крепёж"),
    ("https://gorgia.ge/ka/xelsawyoebi/sazomebi-da-mosanishnebi/",                     "Инструменты", "Измерительные"),
    ("https://gorgia.ge/ka/xelsawyoebi/salesi/",                                       "Инструменты", "Шлифовка"),
    ("https://gorgia.ge/ka/xelsawyoebi/saavtomobilo-aqsesuarebi/",                     "Инструменты", "Автоаксессуары"),
    ("https://gorgia.ge/ka/xelsawyoebi/energiis-da-haeris-warmomqmneli/",              "Инструменты", "Генераторы"),
    ("https://gorgia.ge/ka/xelsawyoebi/shereva-gazaveba/",                              "Инструменты", "Смешивание"),
    ("https://gorgia.ge/ka/xelsawyoebi/sawmendi-da-wnevit-sarecxi/",                   "Инструменты", "Уборка и мойка"),

    # ── Сад ───────────────────────────────────────────────────────────────────
    ("https://gorgia.ge/ka/aveji/gare-aveji/",                                         "Сад", "Садовая мебель"),
    ("https://gorgia.ge/ka/bagi/auzi-da-wylis-aqsesuarebi/",                           "Сад", "Бассейны"),
    ("https://gorgia.ge/ka/bagi/bagis-xelsawyoebi-da-inventrai/",                      "Сад", "Инструменты"),
    ("https://gorgia.ge/ka/bagi/inventari-sasmelebistvis/",                             "Сад", "Напитки и пикник"),
    ("https://gorgia.ge/ka/bagi/gobeebi-da-barierebi/",                                "Сад", "Заборы"),
    ("https://gorgia.ge/ka/bagi/sapiknike-inventari/",                                 "Сад", "Пикник"),
    ("https://gorgia.ge/ka/bagi/bagis-dekori-da-aqsesuarebi/",                         "Сад", "Декор"),
    ("https://gorgia.ge/ka/bagi/sarwyavi-sistemebi/",                                  "Сад", "Полив"),
    ("https://gorgia.ge/ka/bagi/bagis-samushao-samosi/",                               "Сад", "Рабочая одежда"),
    ("https://gorgia.ge/ka/bagi/mcenareebi/",                                          "Сад", "Растения"),

    # ── Техника ───────────────────────────────────────────────────────────────
    ("https://gorgia.ge/ka/teqnika/samzareulos-wvrili-teqnika/",                       "Техника", "Мелкая кухонная"),
    ("https://gorgia.ge/ka/teqnika/samzareulos-msxvili-teqnika/",                      "Техника", "Крупная бытовая"),
    ("https://gorgia.ge/ka/teqnika/teqnika-saxlistvis/",                               "Техника", "Для дома"),
    ("https://gorgia.ge/ka/teqnika/tavis-movla/",                                      "Техника", "Уход за собой"),

    # ── Дом и быт ─────────────────────────────────────────────────────────────
    ("https://gorgia.ge/ka/sayofacxovrebo/churcheli-da-samzareulos-aqsesuarebi/",      "Дом и быт", "Кухонная утварь"),
    ("https://gorgia.ge/ka/sayofacxovrebo/samzareulo-inventari/tafa-da-qvabi/",        "Дом и быт", "Сковороды и кастрюли"),
    ("https://gorgia.ge/ka/sayofacxovrebo/samzareulo-inventari/",                      "Дом и быт", "Кухонный инвентарь"),
    ("https://gorgia.ge/ka/sayofacxovrebo/sayofacxovrebo-movlis-sashualebebi/",        "Дом и быт", "Уход за домом"),
    ("https://gorgia.ge/ka/sayofacxovrebo/saxlis-dekori/",                             "Дом и быт", "Декор"),
    ("https://gorgia.ge/ka/sayofacxovrebo/sadgesaswaulo-nivtebi/",                     "Дом и быт", "Праздничные товары"),

    # ── Товары для животных ───────────────────────────────────────────────────
    ("https://gorgia.ge/ka/cxovelebis-movla/zoo-inventari/",                           "Товары для животных", "Инвентарь"),  # cxovelebisTvis → cxovelebis-movla
    ("https://gorgia.ge/ka/cxovelebis-movla/sakvebi/",                                 "Товары для животных", "Корм"),

    # ── Детские товары ────────────────────────────────────────────────────────
    ("https://gorgia.ge/ka/sabavshvo/sabavshvo-aveji/",                                "Детские товары", "Мебель"),
    ("https://gorgia.ge/ka/sabavshvo/sabavshvo-magidis-sanatebi/",                     "Детские товары", "Лампы"),
]

TRANS = {
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e','ж':'zh','з':'z',
    'и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r',
    'с':'s','т':'t','у':'u','ф':'f','х':'h','ц':'ts','ч':'ch','ш':'sh',
    'щ':'sch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya',
}

BATCH_SIZE  = 5    # товаров за раз
BATCH_PAUSE = 20.0 # секунд между батчами
ITEM_PAUSE  = 3.0  # секунд между товарами


# ─── Helpers ──────────────────────────────────────────────────────────────────

def make_external_id(url: str, category_ru: str) -> str:
    slug = category_ru.lower()
    slug = ''.join(TRANS.get(c, c) for c in slug)
    slug = re.sub(r'[^a-z0-9]', '', slug)
    num  = int(hashlib.md5(url.encode()).hexdigest()[:8], 16) % 90000 + 10000
    return f"{slug}_{num}"


def tg_notify(text: str):
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception as e:
        print(f"  ⚠️ Telegram: {e}")


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
            return BeautifulSoup(r.text, "html.parser")
        except Exception as e:
            wait = 3 * (attempt + 1)
            print(f"  ⚠️ [{attempt+1}/{retries}] {e}. Retry in {wait}s…")
            time.sleep(wait)
    return None


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


def upload_to_blob(img_url: str, blob_path: str) -> str:
    if not VERCEL_BLOB_TOKEN:
        return img_url
    try:
        r = requests.get(img_url, headers=HEADERS, stream=True, timeout=25)
        if r.status_code != 200:
            return img_url
        ctype = r.headers.get("Content-Type", "image/webp")
        res = requests.put(
            f"https://blob.vercel-storage.com/{blob_path}",
            headers={
                "Authorization": f"Bearer {VERCEL_BLOB_TOKEN}",
                "Content-Type": ctype,
                "x-content-type": ctype,
            },
            data=r.content,
            timeout=40,
        )
        if res.status_code in (200, 201):
            return res.json().get("url", img_url)
        print(f"  ⚠️ Blob {res.status_code}: {res.text[:100]}")
    except Exception as e:
        print(f"  ❌ Blob: {e}")
    return img_url


def parse_price(card):
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


# ─── Scraper ──────────────────────────────────────────────────────────────────

def scrape_category(cat_url: str, category_ru: str, sub_category_ru: str) -> list:
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
            external_id = make_external_id(product_url, category_ru)
            category_key = external_id.split("_")[0]

            name_ru  = translate(name_ka, "ru")
            name_en  = translate(name_ka, "en")
            avail_ru = translate(avail_ka, "ru") if avail_ka else ""

            uploaded = []
            photos_uploaded = 0
            if in_stock:
                for idx, img_url in enumerate(image_urls[:10]):
                    ext  = "webp" if "webp" in img_url else "jpg"
                    path = f"gorgia/{external_id}/{idx}.{ext}"
                    result = upload_to_blob(img_url, path)
                    uploaded.append(result)
                    if result != img_url:
                        photos_uploaded += 1
                    time.sleep(0.3)
            else:
                uploaded = image_urls

            products.append({
                "external_id":     external_id,
                "category_key":    category_key,
                "source":          "gorgia",
                "source_url":      product_url,
                "name":            name_ru or name_ka,
                "name_ka":         name_ka,
                "name_ru":         name_ru,
                "name_en":         name_en,
                "description":     "",
                "description_ru":  "",
                "description_en":  "",
                "description_ka":  "",
                "availability_ka": avail_ka,
                "availability_ru": avail_ru,
                "category":        category_ru,
                "category_ru":     category_ru,
                "sub_category":    sub_category_ru,
                "sub_category_ru": sub_category_ru,
                "price":           price,
                "currency":        "GEL",
                "in_stock":        in_stock,
                "image_url":       uploaded[0] if uploaded else None,
                "images":          json.dumps(uploaded),
                "_photos_uploaded": photos_uploaded,
            })

            flag = "✅" if in_stock else "❌"
            print(f"    {flag} {name_ru[:50]} | {price} ₾ | {len(uploaded)} фото")
            time.sleep(REQUEST_DELAY)

        next_btn = soup.select_one(
            ".ty-pagination__next:not(.ty-pagination__disabled), .ty-pagination__next a"
        )
        if not next_btn:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    return products


# ─── DB ───────────────────────────────────────────────────────────────────────

def get_done_urls(conn) -> set:
    with conn.cursor() as cur:
        cur.execute("SELECT source_url FROM products WHERE source = 'gorgia' AND source_url IS NOT NULL")
        return {row[0] for row in cur.fetchall()}


def upsert_product(conn, p: dict):
    p_clean = {k: v for k, v in p.items() if not k.startswith('_')}

    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM products WHERE external_id = %s AND source = 'gorgia'",
            [p_clean["external_id"]]
        )
        existing = cur.fetchone()

    if existing:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE products SET
                    price           = %(price)s,
                    in_stock        = %(in_stock)s,
                    availability_ka = %(availability_ka)s,
                    availability_ru = %(availability_ru)s,
                    image_url       = COALESCE(%(image_url)s, image_url),
                    images          = COALESCE(%(images)s::jsonb, images),
                    category_key    = COALESCE(%(category_key)s, category_key),
                    updated_at      = NOW()
                WHERE external_id = %(external_id)s AND source = 'gorgia'
            """, p_clean)
    else:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO products (
                    external_id, category_key, source, source_url,
                    name, name_ka, name_ru, name_en,
                    description, description_ru, description_en, description_ka,
                    availability_ka, availability_ru,
                    category, category_ru,
                    sub_category, sub_category_ru,
                    price, currency, in_stock,
                    image_url, images
                ) VALUES (
                    %(external_id)s, %(category_key)s, %(source)s, %(source_url)s,
                    %(name)s, %(name_ka)s, %(name_ru)s, %(name_en)s,
                    %(description)s, %(description_ru)s, %(description_en)s, %(description_ka)s,
                    %(availability_ka)s, %(availability_ru)s,
                    %(category)s, %(category_ru)s,
                    %(sub_category)s, %(sub_category_ru)s,
                    %(price)s, %(currency)s, %(in_stock)s,
                    %(image_url)s, %(images)s::jsonb
                )
            """, p_clean)
    conn.commit()


def save_products(products: list, done_urls: set, conn):
    new = upd = photos = 0
    for p in products:
        exists = p["source_url"] in done_urls
        upsert_product(conn, p)
        done_urls.add(p["source_url"])
        photos += p.get("_photos_uploaded", 0)
        if exists:
            upd += 1
        else:
            new += 1
    return new, upd, photos


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    start = time.time()
    print("🚀 Gorgia scraper запущен")
    print(f"📂 Категорий: {len(CATEGORIES)}")
    print(f"🗄  DB: {DATABASE_URL[:40]}…")
    print(f"🖼  Blob: {'✓' if VERCEL_BLOB_TOKEN else '✗'}\n")

    conn = psycopg2.connect(DATABASE_URL)
    done_urls = get_done_urls(conn)
    total_new = total_upd = total_photos = 0

    for cat_url, category_ru, sub_category_ru in CATEGORIES:
        label = f"{category_ru} / {sub_category_ru}" if sub_category_ru else category_ru
        print(f"\n{'='*60}\n📁 {label}\n{'='*60}")

        products = scrape_category(cat_url, category_ru, sub_category_ru)

        new, upd, photos = save_products(products, done_urls, conn)
        total_new += new
        total_upd += upd
        total_photos += photos
        time.sleep(2)

    conn.close()

    elapsed = int(time.time() - start)
    total = total_new + total_upd
    msg = (
        f"✅ *Парсинг завершён*\n\n"
        f"🕐 Время: {elapsed // 60}м {elapsed % 60}с\n"
        f"🔍 Обработано: *{total}* товаров\n"
        f"➕ Новых: *{total_new}*\n"
        f"✏️ Обновлено: *{total_upd}*\n"
        f"🖼 Фото загружено: *{total_photos}*"
    )
    print(f"\n{'='*60}\n{msg}\n{'='*60}")
    tg_notify(msg)


def main_single():
    start = time.time()
    cat_url  = os.environ.get("SCRAPE_URL", "")
    category = os.environ.get("SCRAPE_CATEGORY", "")
    sub      = os.environ.get("SCRAPE_SUB", "")

    if not cat_url and not category:
        print("Ошибка: нужен SCRAPE_URL или SCRAPE_CATEGORY")
        return

    conn = psycopg2.connect(DATABASE_URL)
    done_urls = get_done_urls(conn)
    total_new = total_upd = total_photos = 0
    label = f"{category} / {sub}" if sub else category or cat_url

    if not cat_url:
        matches = [(u, c, s) for u, c, s in CATEGORIES
                   if c == category and (not sub or s == sub)]
        if not matches:
            print(f"Категория не найдена: {category} / {sub}")
            conn.close()
            return
        for url, cat, sub_cat in matches:
            print(f"\nПарсим: {cat} / {sub_cat}")
            products = scrape_category(url, cat, sub_cat)
            new, upd, photos = save_products(products, done_urls, conn)
            total_new += new
            total_upd += upd
            total_photos += photos
    else:
        print(f"\nПарсим: {label} | {cat_url}")
        products = scrape_category(cat_url, category, sub)
        total_new, total_upd, total_photos = save_products(products, done_urls, conn)

    conn.close()

    elapsed = int(time.time() - start)
    total = total_new + total_upd
    msg = (
        f"✅ *Парсинг завершён: {label}*\n\n"
        f"🕐 Время: {elapsed // 60}м {elapsed % 60}с\n"
        f"🔍 Обработано: *{total}* товаров\n"
        f"➕ Новых: *{total_new}*\n"
        f"✏️ Обновлено: *{total_upd}*\n"
        f"🖼 Фото загружено: *{total_photos}*"
    )
    print(msg)
    tg_notify(msg)


if __name__ == "__main__":
    if "--single" in sys.argv:
        main_single()
    else:
        main()
