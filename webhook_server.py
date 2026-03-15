#!/usr/bin/env python3
"""
webhook_server.py
Простой HTTP-сервер, который принимает команды от админки bazariara.ge
и запускает нужный скрипт в фоне.

Запуск:
    python webhook_server.py

По умолчанию слушает на порту 8080.
Добавь в docker-compose как отдельный сервис или запусти вручную.
"""

import asyncio
import hmac
import json
import logging
import os
import subprocess
from datetime import datetime

from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("webhook")

SECRET = os.environ.get("SCRAPER_WEBHOOK_SECRET", "")
PORT   = int(os.environ.get("WEBHOOK_PORT", "8080"))

# Храним статус последних запусков
status: dict = {
    "update": {"last_run": None, "status": "idle", "pid": None},
    "scrape": {"last_run": None, "status": "idle", "pid": None},
}


def verify_secret(request: web.Request) -> bool:
    if not SECRET:
        return True  # если секрет не задан — пропускаем (только для локалки)
    incoming = request.headers.get("X-Secret", "")
    return hmac.compare_digest(incoming, SECRET)


async def run_command(action: str, cmd: list[str]):
    """Запускает команду в фоне и обновляет статус."""
    status[action]["status"] = "running"
    status[action]["last_run"] = datetime.now().isoformat()
    logger.info(f"[{action}] Запускаю: {' '.join(cmd)}")

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd="/app",
        )
        status[action]["pid"] = proc.pid

        stdout, _ = await proc.communicate()
        returncode = proc.returncode

        if returncode == 0:
            status[action]["status"] = "done"
            logger.info(f"[{action}] Завершено успешно")
        else:
            status[action]["status"] = "error"
            logger.error(f"[{action}] Ошибка (код {returncode}): {stdout.decode()[-500:]}")

    except Exception as e:
        status[action]["status"] = "error"
        logger.exception(f"[{action}] Исключение: {e}")
    finally:
        status[action]["pid"] = None


# ── Handlers ──────────────────────────────────────────────────────────────────

async def handle_update(request: web.Request) -> web.Response:
    if not verify_secret(request):
        return web.json_response({"error": "Unauthorized"}, status=401)

    if status["update"]["status"] == "running":
        return web.json_response({"ok": False, "message": "Уже запущен"}, status=409)

    asyncio.create_task(run_command("update", ["python", "main.py", "--update"]))
    return web.json_response({"ok": True, "message": "Апдейт запущен"})


async def handle_scrape(request: web.Request) -> web.Response:
    if not verify_secret(request):
        return web.json_response({"error": "Unauthorized"}, status=401)

    if status["scrape"]["status"] == "running":
        return web.json_response({"ok": False, "message": "Уже запущен"}, status=409)

    asyncio.create_task(run_command("scrape", ["python", "main.py"]))
    return web.json_response({"ok": True, "message": "Полный парсинг запущен"})


async def handle_status(request: web.Request) -> web.Response:
    if not verify_secret(request):
        return web.json_response({"error": "Unauthorized"}, status=401)
    return web.json_response(status)


async def handle_health(_request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


# ── App ───────────────────────────────────────────────────────────────────────

def make_app() -> web.Application:
    app = web.Application()
    app.router.add_post("/webhook/update",  handle_update)
    app.router.add_post("/webhook/scrape",  handle_scrape)
    app.router.add_get("/webhook/status",   handle_status)
    app.router.add_get("/health",           handle_health)
    return app


if __name__ == "__main__":
    logger.info(f"Webhook сервер запускается на порту {PORT}")
    logger.info(f"Секрет: {'задан' if SECRET else 'НЕ ЗАДАН (опасно в проде!)'}")
    web.run_app(make_app(), host="0.0.0.0", port=PORT)
