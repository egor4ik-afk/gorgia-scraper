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
import boto3
from botocore.client import Config

DATABASE_URL       = os.environ["DATABASE_URL"]
REQUEST_DELAY      = float(os.environ.get("REQUEST_DELAY", "1.5"))
TG_TOKEN           = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT            = os.environ.get("TELEGRAM_CHAT_ID", "")

YANDEX_ACCESS_KEY  = os.environ.get("YANDEX_ACCESS_KEY_ID", "")
YANDEX_SECRET_KEY  = os.environ.get("YANDEX_SECRET_ACCESS_KEY", "")
YANDEX_BUCKET      = os.environ.get("YANDEX_BUCKET_NAME", "izipost")
YANDEX_REGION      = os.environ.get("YANDEX_REGION", "ru-central1")
YANDEX_ENDPOINT    = "https://storage.yandexcloud.net"

# ОБНОВЛЕННЫЕ ПУТИ
S3_PREFIX          = "bazariara/gorgia"
CDN_BASE           = "https://cdn.relaxdev.ru"

# S3 клиент
s3_client = boto3.client(
    "s3",
    region_name=YANDEX_REGION,
    endpoint_url=YANDEX_ENDPOINT,
    aws_access_key_id=YANDEX_ACCESS_KEY,
    aws_secret_access_key=YANDEX_SECRET_KEY,
    config=Config(signature_version="s3v4"),
)

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
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-ganateba/",                           "IKEA", "Освещение"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-samzareulo/",                         "IKEA", "Кухня"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-saabazano/",                          "IKEA", "Ванная"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-sabavshvo-otaxi/",                    "IKEA", "Детская"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-teqstili/",                           "IKEA", "Текстиль"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-saxlis-dekori/",                      "IKEA", "Декор"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-saxlis-movla-da-organizeba/",         "IKEA", "Организация"),
    ("https://gorgia.ge/ka/ikeas-produqcia/ikeas-eqsterieri/",                         "IKEA", "Экстерьер"),

    # ── Климатическое оборудование ────────────────────────────────────────────
    ("https://gorgia.ge/ka/klimaturi-teqnika/centraluri-gatbobis-sistema/",            "Климатическое оборудование", "Центральное отопление"),
    ("https://gorgia.ge/ka/klimaturi-teqnika/kondicioneri/",                           "Климатическое оборудование", "Кондиционеры"),
    ("https://gorgia.ge/ka/klimaturi-teqnika/ventilatorebi/",                          "Климатическое оборудование", "Вентиляторы"),
    ("https://gorgia.ge/ka/klimaturi-teqnika/wylis-gamacxeleblebi/",                   "Климатическое оборудование", "Водонагреватели"),
    ("https://gorgia.ge/ka/klimaturi-teqnika/gamatboblebi/",                           "Климатическое оборудование", "Обогреватели"),
    ("https://gorgia.ge/ka/klimaturi-teqnika/saventilacio-sistemebi/",                 "Климатическое оборудование", "Вентиляция"),
    ("https://gorgia.ge/ka/klimaturi-teqnika/koleqtorebi-da-boilerebi/",               "Климатическое оборудование", "Коллекторы"),

    # ── Мебель ────────────────────────────────────────────────────────────────
    ("https://gorgia.ge/ka/aveji/magidebi-da-merxebi/",                                "Мебель", "Столы"),
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
    ("https://gorgia.ge/ka/santeqnika/saabazanos-aveji/",                              "Сантехника", "Мебель для ванной"),
    ("https://gorgia.ge/ka/santeqnika/onkanebi-da-sashxape-sistemebi/",                "Сантехника", "Смесители и душевые"),
    ("https://gorgia.ge/ka/santeqnika/abazana-da-sashxape-kabina/",                    "Сантехника", "Ванны и душевые кабины"),
    ("https://gorgia.ge/ka/santeqnika/unitazi-da-makompleqteblebi/",                   "Сантехника", "Унитазы"),
    ("https://gorgia.ge/ka/santeqnika/wyalmomarageba-da-sakanalizacio-sistemebi/",      "Сантехника", "Водоснабжение"),
    ("https://gorgia.ge/ka/santeqnika/saabazanos-da-tualetis-aqsesuarebi/",            "Сантехника", "Аксессуары"),
    ("https://gorgia.ge/ka/santeqnika/xelsabani-da-aqsesuarebi/",                      "Сантехника", "Раковины"),
    ("https://gorgia.ge/ka/santeqnika/bide-da-pisuari/",                               "Сантехника", "Биде"),

    # ── Освещение ─────────────────────────────────────────────────────────────
    ("https://gorgia.ge/ka/ganateba/shida-ganateba/",                                  "Освещение", "Внутреннее"),
    ("https://gorgia.ge/ka/ganateba/magidis-sanatebi-da-torsherebi/",                  "Освещение", "Настольные лампы"),
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
    ("https://gorgia.ge/ka/xelsawyoebi/shereva-gazaveba/",                             "Инструменты", "Смешивание"),
    ("https://gorgia.ge/ka/xelsawyoebi/sawmendi-da-wnevit-sarecxi/",                   "Инструменты", "Уборка и мойка"),

    # ── Сад ───────────────────────────────────────────────────────────────────
    ("https://gorgia.ge/ka/aveji/gare-aveji/",                                         "Сад", "Садовая мебель"),
    ("https://gorgia.ge/ka/bagi/auzi-da-wylis-aqsesuarebi/",                           "Сад", "Бассейны"),
    ("https://gorgia.ge/ka/bagi/bagis-xelsawyoebi-da-inventrai/",                      "Сад", "Инструменты"),
    ("https://gorgia.ge/ka/bagi/inventari-sasmelebistvis/",                            "Сад", "Напитки и пикник"),
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
    ("https://gorgia.ge/ka/cxovelebis-movla/zoo-inventari/",                           "Товары для животных", "Инвентарь"),
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

BATCH_SIZE  = 5
BATCH_PAUSE = 20.0
ITEM_PAUSE  = 3.0


# ─── Helpers ──────────────────────────────────────────────────────────────────

def make_external_id(url: str, category_key: str = "") -> str:
    # ВАЖНО: category_key сюда больше не подмешивается. Раньше external_id включал
    # префикс категории, из-за чего смена category_key (мердж/переименование) меняла
    # external_id для того же самого товара -> upsert_product не находил "старую" строку
    # по external_id и вставлял дубликат вместо обновления. source_url — единственный
    # стабильный идентификатор товара на gorgia.ge, category_key участвовать не должен.
    h = hashlib.md5(url.encode()).hexdigest()[:12]
    return f"gorgia_{h}"


def slugify_ru(category_ru: str) -> str:
    """Транслитерация RU → ключ (латиница). Используется ТОЛЬКО для НОВЫХ категорий,
    которых ещё нет в БД (существующие category_key вроде climate/furniture — англ. слова)."""
    slug = category_ru.lower()
    slug = ''.join(TRANS.get(c, c) for c in slug)
    return re.sub(r'[^a-z0-9]', '', slug)


def slugify_sub(sub_category_ru: str) -> str:
    """Ключ подкатегории — НЕ транслитерация, а кириллица в нижнем регистре с дефисами
    (см. существующие 'коллекторы-и-бойлеры', 'центральное-отопление'). Если сгенерить
    иначе — получится дубль с другим ключом на то же самое имя (как было с konditsionery)."""
    return sub_category_ru.strip().lower().replace(" ", "-")


def resolve_category_keys(conn, category_names) -> dict:
    """
    Приоритет при определении category_key:
      1. Каноническая таблица categories (единственный источник правды для меню/фильтров сайта) —
         точное совпадение по названию.
      2. Если категории там нет — самый частый существующий category_key в products
         (на случай если товары уже есть, а строка в categories почему-то не создана).
      3. Если категории нет вообще нигде — новый ключ транслитерацией.
    Так не плодим дубли вроде climate / klimaticheskoeoborudovanie и не подхватываем
    случайно расплодившийся "неправильный" ключ, даже если его в products вдруг стало больше.
    """
    category_names = {c for c in category_names if c}
    result = {}

    if category_names:
        with conn.cursor() as cur:
            # 1. Каноническая таблица
            cur.execute("""
                SELECT name, category_key FROM categories WHERE name = ANY(%s)
            """, [list(category_names)])
            for name, key in cur.fetchall():
                result[name] = key

            # 2. Фолбэк на products — только для того, чего не нашли в categories
            remaining = category_names - set(result.keys())
            if remaining:
                cur.execute("""
                    SELECT category, category_key, COUNT(*) AS cnt
                    FROM products
                    WHERE category = ANY(%s) AND category_key IS NOT NULL
                    GROUP BY category, category_key
                    ORDER BY category, cnt DESC
                """, [list(remaining)])
                for category, key, _cnt in cur.fetchall():
                    if category not in result:
                        result[category] = key
                        print(f"  ⚠️ «{category}» есть в products, но не в таблице categories — "
                              f"беру ключ '{key}' по частоте (стоит завести строку в categories)", flush=True)

    # 3. Реально новые категории
    for name in category_names:
        if name not in result:
            slug = slugify_ru(name)
            result[name] = slug
            print(f"  🆕 Категории «{name}» ещё нет в БД → создаю новый ключ '{slug}'", flush=True)
        else:
            print(f"  🔗 «{name}» → существующий ключ '{result[name]}'", flush=True)

    return result


def tg_notify(text: str) -> bool:
    if not TG_TOKEN or not TG_CHAT:
        print("  ⚠️ Telegram: TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID не заданы — уведомление не отправлено", flush=True)
        return False
    try:
        res = requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
        if res.status_code == 200:
            print("  ✅ Telegram: уведомление отправлено", flush=True)
            return True
        print(f"  ✕ Telegram: HTTP {res.status_code}: {res.text[:300]}", flush=True)
        return False
    except Exception as e:
        print(f"  ✕ Telegram: {e}", flush=True)
        return False


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


def translate_from_ru(text: str, target: str = "en") -> str:
    if not text or not text.strip():
        return ""
    try:
        r = requests.get(
            "https://translate.googleapis.com/translate_a/single",
            params={"client": "gtx", "sl": "ru", "tl": target, "dt": "t", "q": text},
            timeout=12,
        )
        if r.status_code == 200:
            return "".join(t[0] for t in r.json()[0] if t[0])
    except Exception as e:
        print(f"  ⚠️ translate_from_ru({target}): {e}")
    return text


def upload_to_yandex(img_url: str, s3_path: str) -> str:
    """
    Скачивает изображение и загружает в Yandex S3.
    s3_path — путь внутри бакета, например: bazariara/gorgia/external_id/0.webp
    Возвращает CDN url или оригинальный img_url при ошибке.
    """
    if not YANDEX_ACCESS_KEY or not YANDEX_SECRET_KEY:
        return img_url
    try:
        r = requests.get(img_url, headers=HEADERS, stream=True, timeout=25)
        if r.status_code != 200:
            print(f"  ⚠️ Не удалось скачать изображение: {r.status_code} {img_url}")
            return img_url

        content_type = r.headers.get("Content-Type", "image/webp")

        s3_client.put_object(
            Bucket=YANDEX_BUCKET,
            Key=s3_path,
            Body=r.content,
            ContentType=content_type,
            ACL="public-read",
        )

        # Теперь cdn_url формируется просто и надежно:
        # CDN_BASE: https://cdn.relaxdev.ru
        # s3_path: bazariara/gorgia/external_id/0.webp
        cdn_url = f"{CDN_BASE}/{s3_path}"
        return cdn_url

    except Exception as e:
        print(f"  ❌ Yandex S3 upload error: {e}")
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
    if img:
        # Предпочитаем 2x-вариант из srcset — он уже в нужном формате (webp/jpg),
        # каким его отдаёт сам сайт. Строковых догадок про путь больше не делаем.
        srcset = img.get("srcset", "")
        if srcset:
            add(srcset.split(",")[0].strip().split(" ")[0])
        elif img.get("src"):
            add(img["src"])

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

STALL_SECONDS = 20  # если один товар обрабатывается дольше — печатаем предупреждение


def ensure_category_registered(category_key: str, category_ru: str, category_en: str, category_ka: str,
                                sub_category_ru: str, sub_category_en: str, sub_category_ka: str):
    """
    Регистрирует категорию/подкатегорию в канонических таблицах categories/subcategories,
    если их там ещё нет — иначе они не появятся в меню и фильтрах сайта, даже если у товаров
    поля category/sub_category уже заполнены правильно.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO categories (category_key, name, name_en, name_ka)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (category_key) DO NOTHING
                RETURNING category_key
            """, [category_key, category_ru, category_en, category_ka])
            if cur.fetchone():
                print(f"  🆕 Зарегистрирована категория в таблице categories: {category_ru} [{category_key}]", flush=True)

            if sub_category_ru:
                cur.execute(
                    "SELECT key FROM subcategories WHERE category_key = %s AND name = %s LIMIT 1",
                    [category_key, sub_category_ru]
                )
                already = cur.fetchone()
                if not already:
                    sub_key = slugify_sub(sub_category_ru)
                    cur.execute("""
                        INSERT INTO subcategories (category_key, key, name, name_en, name_ka)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (key) DO NOTHING
                        RETURNING key
                    """, [category_key, sub_key, sub_category_ru, sub_category_en, sub_category_ka])
                    if cur.fetchone():
                        print(f"  🆕 Зарегистрирована подкатегория в таблице subcategories: {sub_category_ru} [{sub_key}]", flush=True)
        conn.commit()
    except Exception as e:
        print(f"  ⚠️ Не удалось зарегистрировать категорию/подкатегорию в БД: {e}", flush=True)
        conn.rollback()
    finally:
        conn.close()


def scrape_category(cat_url: str, category_ru: str, sub_category_ru: str, category_key: str) -> list:
    products = []
    page = 1
    processed = 0
    t_start = time.time()

    print(f"  🌐 Перевод категории: {category_ru}", flush=True)
    category_en = translate_from_ru(category_ru, "en")
    category_ka = translate_from_ru(category_ru, "ka")
    time.sleep(0.5)

    sub_category_en = ""
    sub_category_ka = ""
    if sub_category_ru:
        print(f"  🌐 Перевод подкатегории: {sub_category_ru}", flush=True)
        sub_category_en = translate_from_ru(sub_category_ru, "en")
        sub_category_ka = translate_from_ru(sub_category_ru, "ka")
        time.sleep(0.5)

    print(f"  📌 {category_ru} → en: {category_en} | ka: {category_ka}", flush=True)
    if sub_category_ru:
        print(f"  📌 {sub_category_ru} → en: {sub_category_en} | ka: {sub_category_ka}", flush=True)

    ensure_category_registered(category_key, category_ru, category_en, category_ka,
                                sub_category_ru, sub_category_en, sub_category_ka)

    while True:
        url = cat_url if page == 1 else f"{cat_url}?page={page}"
        print(f"\n  📄 Страница {page}: {url}", flush=True)

        soup = get_soup(url)
        if not soup:
            break

        cards = soup.select(".ut2-gl__body")
        if not cards:
            print(f"  ℹ️ Товаров не найдено, стоп", flush=True)
            break

        print(f"  → {len(cards)} товаров", flush=True)

        for card in cards:
            a_tag = card.select_one(".ut2-gl__name a")
            if not a_tag:
                continue

            t_item = time.time()

            product_url  = urllib.parse.urljoin(BASE_URL, a_tag.get("href", ""))
            name_ka      = a_tag.get_text(strip=True)
            price        = parse_price(card)
            avail_ka, in_stock = parse_availability(card)
            image_urls   = get_image_urls(card)
            external_id  = make_external_id(product_url, category_key)

            name_ru  = translate(name_ka, "ru")
            name_en  = translate(name_ka, "en")
            avail_ru = translate(avail_ka, "ru") if avail_ka else ""

            uploaded = []
            photos_uploaded = 0

            if in_stock:
                for idx, img_url in enumerate(image_urls[:10]):
                    ext = "webp" if "webp" in img_url else "jpg"
                    
                    # ОПРЕДЕЛЯЕМ ПУТЬ ДЛЯ БАКЕТА
                    # s3_key будет: bazariara/gorgia/айди_товара/0.webp
                    s3_key = f"{S3_PREFIX}/{external_id}/{idx}.{ext}"
                    
                    cdn_url = upload_to_yandex(img_url, s3_key)
                    uploaded.append(cdn_url)
                    
                    if cdn_url != img_url:
                        photos_uploaded += 1
                    time.sleep(0.3)
            else:
                uploaded = image_urls

            products.append({
                "external_id":      external_id,
                "category_key":     category_key,
                "source":           "gorgia",
                "source_url":       product_url,
                "name":             name_ru or name_ka,
                "name_ka":          name_ka,
                "name_ru":          name_ru,
                "name_en":          name_en,
                "description":      "",
                "description_ru":   "",
                "description_en":   "",
                "description_ka":   "",
                "availability":     avail_ru or avail_ka,
                "category":         category_ru,
                "category_en":      category_en,
                "category_ka":      category_ka,
                "sub_category":     sub_category_ru,
                "sub_category_en":  sub_category_en,
                "sub_category_ka":  sub_category_ka,
                "price":            price,
                "currency":         "GEL",
                "in_stock":         in_stock,
                "image_url":        uploaded[0] if uploaded else None,
                "images":           json.dumps(uploaded),
                "_photos_uploaded": photos_uploaded,
            })

            item_elapsed = time.time() - t_item
            processed += 1

            flag = "✅" if in_stock else "❌"
            print(f"    {flag} {name_ru[:50]} | {price} ₾ | {len(uploaded)} фото | "
                  f"{photos_uploaded} загружено | {item_elapsed:.1f}с", flush=True)

            if item_elapsed > STALL_SECONDS:
                print(f"    ⚠️ Долго обрабатывался ({item_elapsed:.0f}с > {STALL_SECONDS}с): "
                      f"{name_ru[:60]} — проверь сеть/картинки/перевод", flush=True)

            if processed % 5 == 0:
                total_elapsed = time.time() - t_start
                rate = total_elapsed / processed
                print(f"  📊 Прогресс: {processed} товаров обработано | "
                      f"прошло {int(total_elapsed // 60)}м {int(total_elapsed % 60)}с | "
                      f"~{rate:.1f}с/товар", flush=True)

            time.sleep(REQUEST_DELAY)

        next_btn = soup.select_one(
            ".ty-pagination__next:not(.ty-pagination__disabled), .ty-pagination__next a"
        )
        if not next_btn:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    # Если у подкатегории в канонической таблице ещё нет превью — подставляем
    # фото первого же спарсенного товара, чтобы не было битой картинки на сайте.
    if sub_category_ru and products:
        first_image = next((p["image_url"] for p in products if p.get("image_url")), None)
        if first_image:
            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE subcategories SET image_url = %s
                        WHERE category_key = %s AND name = %s AND image_url IS NULL
                    """, [first_image, category_key, sub_category_ru])
                conn.commit()
            except Exception as e:
                print(f"  ⚠️ Не удалось подставить фото подкатегории: {e}", flush=True)
                conn.rollback()
            finally:
                conn.close()

    return products


# ─── DB ───────────────────────────────────────────────────────────────────────

def get_db_connection():
    """Соединение с keepalive — чтобы NAT/фаервол/Neon не рвали простаивающий сокет молча."""
    return psycopg2.connect(
        DATABASE_URL,
        connect_timeout=15,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


def get_done_urls(conn) -> set:
    with conn.cursor() as cur:
        cur.execute("SELECT source_url FROM products WHERE source = 'gorgia' AND source_url IS NOT NULL")
        return {row[0] for row in cur.fetchall()}


def upsert_product(conn, p: dict):
    p_clean = {k: v for k, v in p.items() if not k.startswith('_')}

    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM products WHERE source_url = %s AND source = 'gorgia'",
            [p_clean["source_url"]]
        )
        existing = cur.fetchone()

    if existing:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE products SET
                    external_id      = %(external_id)s,
                    price            = %(price)s,
                    in_stock         = %(in_stock)s,
                    availability     = %(availability)s,
                    image_url        = COALESCE(%(image_url)s, image_url),
                    images           = COALESCE(%(images)s::jsonb, images),
                    category_key     = %(category_key)s,
                    category         = %(category)s,
                    category_en      = COALESCE(%(category_en)s, category_en),
                    category_ka      = COALESCE(%(category_ka)s, category_ka),
                    sub_category     = %(sub_category)s,
                    sub_category_en  = COALESCE(%(sub_category_en)s, sub_category_en),
                    sub_category_ka  = COALESCE(%(sub_category_ka)s, sub_category_ka),
                    updated_at       = NOW()
                WHERE source_url = %(source_url)s AND source = 'gorgia'
            """, p_clean)
    else:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO products (
                    external_id, category_key, source, source_url,
                    name, name_ka, name_ru, name_en,
                    description, description_ru, description_en, description_ka,
                    availability,
                    category, category_en, category_ka,
                    sub_category, sub_category_en, sub_category_ka,
                    price, currency, in_stock,
                    image_url, images
                ) VALUES (
                    %(external_id)s, %(category_key)s, %(source)s, %(source_url)s,
                    %(name)s, %(name_ka)s, %(name_ru)s, %(name_en)s,
                    %(description)s, %(description_ru)s, %(description_en)s, %(description_ka)s,
                    %(availability)s,
                    %(category)s, %(category_en)s, %(category_ka)s,
                    %(sub_category)s, %(sub_category_en)s, %(sub_category_ka)s,
                    %(price)s, %(currency)s, %(in_stock)s,
                    %(image_url)s, %(images)s::jsonb
                )
            """, p_clean)
    conn.commit()


def save_products(products: list, done_urls: set, conn):
    new = upd = photos = 0
    total = len(products)
    for i, p in enumerate(products, start=1):
        exists = p["source_url"] in done_urls

        for attempt in range(3):
            try:
                upsert_product(conn, p)
                break
            except psycopg2.OperationalError as e:
                print(f"  ⚠️ БД оборвалась на {i}/{total} ({p.get('name','?')[:40]}): {e}. "
                      f"Переподключаюсь ({attempt + 1}/3)…")
                try:
                    conn.close()
                except Exception:
                    pass
                time.sleep(2 * (attempt + 1))
                conn = get_db_connection()
        else:
            print(f"  ❌ Не удалось сохранить после 3 попыток: {p.get('source_url')}")
            continue

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
    print(f"🖼  Yandex S3: {'✓' if YANDEX_ACCESS_KEY else '✗'} | бакет: {YANDEX_BUCKET}\n")

    # 1. Открыли БД, получили ссылки + канонические category_key, СРАЗУ ЗАКРЫЛИ
    conn = get_db_connection()
    done_urls = get_done_urls(conn)
    category_keys = resolve_category_keys(conn, {c for _, c, _ in CATEGORIES})
    conn.close()
    
    total_new = total_upd = total_photos = 0

    for cat_url, category_ru, sub_category_ru in CATEGORIES:
        label = f"{category_ru} / {sub_category_ru}" if sub_category_ru else category_ru
        print(f"\n{'='*60}\n📁 {label}\n{'='*60}")

        # Парсинг занимает много времени, БД пока отдыхает
        products = scrape_category(cat_url, category_ru, sub_category_ru, category_keys[category_ru])

        # 2. Открыли БД заново ТОЛЬКО для сохранения партии товаров
        conn = get_db_connection()
        new, upd, photos = save_products(products, done_urls, conn)
        conn.close() # Сохранили и снова закрыли

        total_new += new
        total_upd += upd
        total_photos += photos
        time.sleep(2)

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
    print(f"\n{'='*60}\n{msg}\n{'='*60}", flush=True)
    tg_notify(msg)


def main_single():
    start = time.time()
    cat_url  = os.environ.get("SCRAPE_URL", "")
    category = os.environ.get("SCRAPE_CATEGORY", "")
    sub      = os.environ.get("SCRAPE_SUB", "")

    if not cat_url and not category:
        print("Ошибка: нужен SCRAPE_URL или SCRAPE_CATEGORY")
        return

    # 1. Открыли БД, получили ссылки + канонические category_key
    conn = get_db_connection()
    done_urls = get_done_urls(conn)

    total_new = total_upd = total_photos = 0
    label = f"{category} / {sub}" if sub else category or cat_url

    if not cat_url:
        matches = [(u, c, s) for u, c, s in CATEGORIES
                   if c == category and (not sub or s == sub)]
        if not matches:
            conn.close()
            print(f"Категория не найдена: {category} / {sub}")
            return

        category_keys = resolve_category_keys(conn, {c for _, c, _ in matches})
        conn.close()

        for url, cat, sub_cat in matches:
            print(f"\nПарсим: {cat} / {sub_cat}")
            products = scrape_category(url, cat, sub_cat, category_keys[cat])
            
            # 2. Снова открываем только для сохранения
            conn = get_db_connection()
            new, upd, photos = save_products(products, done_urls, conn)
            conn.close()
            
            total_new += new
            total_upd += upd
            total_photos += photos
    else:
        category_keys = resolve_category_keys(conn, {category} if category else set())
        conn.close()
        resolved_key = category_keys.get(category) if category else slugify_ru(cat_url)

        print(f"\nПарсим: {label} | {cat_url}")
        products = scrape_category(cat_url, category, sub, resolved_key)
        
        # 2. Снова открываем только для сохранения
        conn = get_db_connection()
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
    print(msg, flush=True)
    tg_notify(msg)


if __name__ == "__main__":
    if "--single" in sys.argv:
        main_single()
    else:
        main()