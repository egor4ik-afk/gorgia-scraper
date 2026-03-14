# 🤖 Product Scraper Agent

Агент автоматически парсит товары с Wildberries, Ozon, сайтов конкурентов и поставщиков,
обновляет базу данных (Neon PostgreSQL) и отправляет отчёты в Telegram.

## Архитектура

```
main.py
  └── OrchestratorAgent
        ├── WildberriesScraper   ← HTTP API, без браузера
        ├── OzonScraper          ← Playwright (headless Chrome)
        ├── GenericScraper       ← Playwright, универсальный
        ├── R2Storage            ← Загрузка фото в Cloudflare R2
        ├── Database             ← asyncpg → Neon PostgreSQL
        └── TelegramNotifier     ← Отчёт после каждого прогона
```

## Быстрый старт

### 1. Клонируй / скопируй проект на сервер

```bash
scp -r ./scraper-agent user@relaxdev.ru:/opt/scraper-agent
ssh user@relaxdev.ru
cd /opt/scraper-agent
```

### 2. Настрой переменные окружения

```bash
cp .env.example .env
nano .env
```

Обязательно заполни:
- `DATABASE_URL` — строка подключения к Neon
- `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID`
- `WB_QUERIES` / `OZON_QUERIES` / `COMPETITOR_URLS` — источники

### 3. Запусти через Docker Compose

```bash
# Сборка образа
docker compose build

# Тестовый запуск прямо сейчас
docker compose run --rm scraper

# Запуск планировщика (будет стартовать scraper каждый день в 03:00)
docker compose up -d scheduler
```

### 4. Проверь логи

```bash
# Логи последнего запуска
tail -f logs/agent.log

# Логи планировщика
docker compose logs -f scheduler
```

---

## Деплой без Docker (на VM/VPS напрямую)

```bash
# Установка зависимостей
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install chromium
playwright install-deps chromium

# Тестовый запуск
python main.py

# Добавить в crontab (каждый день в 03:00)
crontab -e
# Добавь строку:
0 3 * * * cd /opt/scraper-agent && /opt/scraper-agent/venv/bin/python main.py >> logs/cron.log 2>&1
```

---

## Добавление нового сайта конкурента/поставщика

В файле `scrapers/generic.py` добавь конфиг CSS-селекторов для своего сайта:

```python
SITE_CONFIGS = {
    "my-supplier.ge": {
        "product_links":  "a.product-link",
        "name":           "h1.product-name",
        "price":          "span.price-value",
        "description":    "div.product-description",
        "images":         ".product-gallery img",
        "in_stock":       ".badge-available",
        "out_of_stock":   ".badge-out-of-stock",
    },
    # ...
}
```

Затем добавь URL в `.env`:
```
SUPPLIER_URLS=https://my-supplier.ge/catalog/home
```

---

## Структура таблиц в БД

```sql
-- Основная таблица товаров
products (
  id, external_id, source, source_url,
  name, price, price_old, currency,
  in_stock, stock_qty,
  description, characteristics (JSONB),
  category, brand,
  images (JSONB),  -- массив URL в Cloudflare R2
  created_at, updated_at
)

-- История изменения цен
price_history (
  id, product_id, price_old, price_new, changed_at
)
```

---

## Пример уведомления в Telegram

```
📦 Обновление товаров завершено

🕐 Время: 4м 23с
🔍 Обработано: 1 247 товаров
➕ Новых: 34
✏️ Обновлено: 156
🖼 Фото загружено: 312

💰 Изменения цен (8):
📉 Корзина плетёная 45л: 890 → 750 (-15.7%)
📈 Палатка туристическая 2м: 4200 → 4800 (+14.3%)
...

📦 Изменения наличия (3):
❌ Гамак подвесной — нет в наличии
✅ Термос 1л нержавейка — в наличии
```

---

## Переменные окружения

| Переменная | Описание | Обязательно |
|---|---|---|
| `DATABASE_URL` | PostgreSQL connection string | ✅ |
| `TELEGRAM_BOT_TOKEN` | Токен Telegram-бота | ✅ |
| `TELEGRAM_CHAT_ID` | ID чата для уведомлений | ✅ |
| `WB_QUERIES` | Поисковые запросы WB (через запятую) | — |
| `OZON_QUERIES` | Поисковые запросы Ozon (через запятую) | — |
| `COMPETITOR_URLS` | URL каталогов конкурентов | — |
| `SUPPLIER_URLS` | URL каталогов поставщиков | — |
| `R2_ACCOUNT_ID` | Cloudflare R2 Account ID | — |
| `R2_ACCESS_KEY` | Cloudflare R2 Access Key | — |
| `R2_SECRET_KEY` | Cloudflare R2 Secret Key | — |
| `R2_PUBLIC_URL` | Публичный URL бакета R2 | — |
| `PROXY_URL` | HTTP прокси (если нужен) | — |
| `CONCURRENT_SCRAPERS` | Параллельных скраперов (default: 3) | — |
| `REQUEST_DELAY` | Задержка между запросами сек (default: 1.5) | — |
| `PRICE_CHANGE_THRESHOLD` | Порог изменения цены (default: 0.05 = 5%) | — |
