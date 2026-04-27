#!/usr/bin/env python3
"""
webhook_server.py
Принимает команды от админки bazariara.ge и запускает нужные скрипты.

Endpoints:
  POST /webhook/update           → python main.py --update
  POST /webhook/scrape           → python main.py
  POST /webhook/scrape-category  → парсинг конкретной категории
  GET  /webhook/status           → статус запущенных задач
  GET  /health                   → health check
"""

import asyncio
import hmac
import json
import logging
import os
import subprocess
from datetime import datetime
from aiohttp import web

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("webhook")

SECRET = os.environ.get("SCRAPER_WEBHOOK_SECRET", "")
PORT   = int(os.environ.get("WEBHOOK_PORT", "8080"))

status = {
    "update":          {"last_run": None, "status": "idle", "pid": None},
    "scrape":          {"last_run": None, "status": "idle", "pid": None},
    "scrape_category": {"last_run": None, "status": "idle", "pid": None, "label": None},
}


def verify(request: web.Request) -> bool:
    if not SECRET:
        return True
    return hmac.compare_digest(request.headers.get("X-Secret", ""), SECRET)


async def run_cmd(action: str, cmd: list, label: str = ""):
    status[action]["status"]   = "running"
    status[action]["last_run"] = datetime.now().isoformat()
    if label:
        status[action]["label"] = label
    logger.info(f"[{action}] {' '.join(cmd)}" + (f" | {label}" if label else ""))

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd="/app",
        )
        status[action]["pid"] = proc.pid
        stdout, _ = await proc.communicate()
        if proc.returncode == 0:
            status[action]["status"] = "done"
            logger.info(f"[{action}] Завершено успешно")
        else:
            status[action]["status"] = "error"
            logger.error(f"[{action}] Ошибка (код {proc.returncode}): {stdout.decode()[-500:]}")
    except Exception as e:
        status[action]["status"] = "error"
        logger.exception(f"[{action}] Исключение: {e}")
    finally:
        status[action]["pid"] = None


# ── Handlers ──────────────────────────────────────────────────────────────────

async def handle_update(req: web.Request) -> web.Response:
    if not verify(req):
        return web.json_response({"error": "Unauthorized"}, status=401)
    if status["update"]["status"] == "running":
        return web.json_response({"ok": False, "message": "Уже запущен"}, status=409)
    asyncio.create_task(run_cmd("update", ["python", "main.py", "--update"]))
    return web.json_response({"ok": True, "message": "Апдейт запущен"})


async def handle_scrape(req: web.Request) -> web.Response:
    if not verify(req):
        return web.json_response({"error": "Unauthorized"}, status=401)
    if status["scrape"]["status"] == "running":
        return web.json_response({"ok": False, "message": "Уже запущен"}, status=409)
    asyncio.create_task(run_cmd("scrape", ["python", "main.py"]))
    return web.json_response({"ok": True, "message": "Полный парсинг запущен"})


async def handle_scrape_category(req: web.Request) -> web.Response:
    if not verify(req):
        return web.json_response({"error": "Unauthorized"}, status=401)
    if status["scrape_category"]["status"] == "running":
        return web.json_response({"ok": False, "message": "Уже запущена другая категория"}, status=409)

    try:
        body = await req.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    cat_url     = body.get("url", "")
    category    = body.get("category", "")
    sub_category = body.get("sub_category", "")

    # Передаём через переменные окружения чтобы не экранировать в аргументах
    env_extra = {
        "SCRAPE_URL":      cat_url,
        "SCRAPE_CATEGORY": category,
        "SCRAPE_SUB":      sub_category,
    }

    label = f"{category} / {sub_category}" if sub_category else category or cat_url

    # Запускаем отдельный скрипт для одной категории
    asyncio.create_task(
        run_cmd_with_env(
            "scrape_category",
            ["python", "scrapers/scraper.py", "--single"],
            env_extra,
            label=label,
        )
    )
    return web.json_response({"ok": True, "message": f"Парсинг запущен: {label}"})


async def run_cmd_with_env(action: str, cmd: list, extra_env: dict, label: str = ""):
    """Запускает команду с дополнительными переменными окружения."""
    import os as _os
    env = _os.environ.copy()
    env.update(extra_env)

    status[action]["status"]   = "running"
    status[action]["last_run"] = datetime.now().isoformat()
    status[action]["label"]    = label
    logger.info(f"[{action}] {' '.join(cmd)} | {label}")

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd="/app",
            env=env,
        )
        status[action]["pid"] = proc.pid
        stdout, _ = await proc.communicate()
        if proc.returncode == 0:
            status[action]["status"] = "done"
            logger.info(f"[{action}] Готово: {label}")
        else:
            status[action]["status"] = "error"
            logger.error(f"[{action}] Ошибка: {stdout.decode()[-500:]}")
    except Exception as e:
        status[action]["status"] = "error"
        logger.exception(f"[{action}] {e}")
    finally:
        status[action]["pid"] = None


async def handle_status(req: web.Request) -> web.Response:
    if not verify(req):
        return web.json_response({"error": "Unauthorized"}, status=401)
    return web.json_response(status)


async def handle_health(_: web.Request) -> web.Response:
    return web.json_response({"ok": True})


# ── App ───────────────────────────────────────────────────────────────────────

def make_app():
    app = web.Application()
    app.router.add_post("/webhook/update",           handle_update)
    app.router.add_post("/webhook/scrape",           handle_scrape)
    app.router.add_post("/webhook/scrape-category",  handle_scrape_category)
    app.router.add_get( "/webhook/status",           handle_status)
    app.router.add_get( "/health",                   handle_health)
    return app


if __name__ == "__main__":
    logger.info(f"Webhook сервер запускается на порту {PORT}")
    logger.info(f"Секрет: {'задан' if SECRET else 'НЕ ЗАДАН'}")
    web.run_app(make_app(), host="0.0.0.0", port=PORT)
