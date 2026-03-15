# Gorgia.ge Product Scraper Agent

Полный агент для автоматического парсинга gorgia.ge:
- Обходит весь каталог через sitemap.xml (+ fallback на навигацию)
- Извлекает артикул/SKU с каждой детальной страницы
- Хранит оригинал на грузинском + переводы RU и EN (имя, описание, наличие, категория, характеристики)
- Загружает фото в Vercel Blob
- Хранит всё в Neon PostgreSQL
- Ежедневно обновляет цену и наличие
- Шлёт отчёты в Telegram

---

## Архитектура

```
main.py
  ├── python main.py           → полный парсинг (sitemap → detail pages)
  └── python main.py --update  → только цена/наличие (updater.py)

scrapers/gorgia.py
  ├── discover_product_urls()  → sitemap.xml или обход навигации
  ├── parse_detail_page()      → SKU, название KA, описание, характеристики, JSON-LD
  ├── _translate()             → Google Translate KA → RU + EN (параллельно)
  ├── _upload_images()         → Vercel Blob
  └── save_to_db()             → asyncpg → Neon

updater.py
  └── update_one()             → цена + наличие + SKU + история цен → Telegram
```

---

## Быстрый старт

### 1. Настрой .env

```bash
cp .env.example .env
nano .env
```

Обязательные поля:
- DATABASE_URL   — Neon: console.neon.tech → Connection string
- VERCEL_BLOB_TOKEN — Vercel Dashboard → Storage → Blob → .env.local
- TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID

### 2. Docker (рекомендуется)

```bash
docker compose build

# Первый полный парсинг
docker compose run --rm scraper

# Ежедневный апдейт
docker compose run --rm updater

# Запуск планировщика
docker compose up -d scheduler
```

### 3. Без Docker

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

python main.py           # полный парсинг
python main.py --update  # ежедневное обновление

# Crontab
0 2 * * 0 cd /opt/gorgia-agent && venv/bin/python main.py >> logs/cron.log 2>&1
0 6 * * * cd /opt/gorgia-agent && venv/bin/python main.py --update >> logs/updater.log 2>&1
```

---

## Расписание

| Задача | Когда | Что делает |
|---|---|---|
| main.py | Воскресенье 02:00 | Полный обход sitemap, новые товары, переводы, фото |
| main.py --update | Каждый день 06:00 | Обновляет цену, наличие, SKU (если пустой) |

---

## База данных

```sql
products (
    id, external_id, source, source_url,
    name,                        -- основное поле: ru → en → ka
    name_ka, name_ru, name_en,
    description, description_ka, description_ru, description_en,
    availability_ka,             -- Georgian: "მარაგშია" / "მარაგი იწურება"
    availability_ru,             -- Russian: "В наличии" / "Заканчивается"
    category_ka, category_ru, category_en,
    sub_category_ka, sub_category_ru, sub_category_en,
    sku,                         -- артикул/артикул номер
    price, currency, in_stock,
    images JSONB,                -- массив URL в Vercel Blob
    other  JSONB,                -- brand, features_ka/ru/en, breadcrumbs, gtin, rating...
    created_at, updated_at
)

price_history (id, product_id, price_old, price_new, changed_at)
```

### Поле other (JSONB)

```json
{
  "brand": "Samsung",
  "features_ka": { "სიმძლავრე": "2000W" },
  "features_ru": { "Мощность": "2000W" },
  "features_en": { "Power": "2000W" },
  "breadcrumbs_ka": ["მთავარი", "კლიმატური ტექნიკა", "კონდიციონერები"],
  "gtin": "1234567890123",
  "rating": { "value": "4.5", "count": "12" }
}
```

---

## Next.js API

```
GET /api/products           — список (фильтры: lang, in_stock, category, search, sku, price)
GET /api/products/[id]      — товар по id или external_id
GET /api/products/categories — дерево категорий на всех трёх языках
GET /api/products/stats     — статистика + покрытие SKU
```

### Параметры /api/products

| Параметр | Пример | Описание |
|---|---|---|
| lang | ru / en / ka | Язык ответа (default: ru) |
| in_stock | true | Фильтр наличия |
| category | IKEA | Поиск по категории (все языки, ILIKE) |
| search | диван | Поиск по имени + описанию + SKU |
| sku | BM-001 | Поиск по артикулу |
| min_price / max_price | 100 / 500 | Диапазон цены |
| sort | price_asc | price_asc / price_desc / updated_desc / name_asc |
| page / per_page | 1 / 20 | Пагинация |

---

## Переменные окружения

| Переменная | Обяз. | Описание |
|---|---|---|
| DATABASE_URL | YES | Neon PostgreSQL connection string |
| VERCEL_BLOB_TOKEN | YES | Токен Vercel Blob (read-write) |
| TELEGRAM_BOT_TOKEN | YES | Токен бота |
| TELEGRAM_CHAT_ID | YES | ID чата |
| GORGIA_CATEGORY_URLS | — | URL категорий через запятую (пусто = весь сайт) |
| REQUEST_DELAY | — | Задержка между запросами сек (default: 1.5) |
| MAX_RETRIES | — | Повторов при ошибке (default: 3) |
| CONCURRENT_SCRAPERS | — | Параллельных воркеров (default: 3) |
| PRICE_CHANGE_THRESHOLD | — | Порог уведомления о цене (default: 0.05 = 5%) |
