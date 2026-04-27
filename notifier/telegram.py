import logging
import aiohttp
from agents.models import UpdateReport

logger = logging.getLogger("notifier.telegram")

TG_API = "https://api.telegram.org/bot{token}/{method}"


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id

    async def send_report(self, report: UpdateReport, elapsed_sec: int):
        lines = [
            "📦 *Обновление товаров завершено*",
            "",
            f"🕐 Время: {elapsed_sec // 60}м {elapsed_sec % 60}с",
            f"🔍 Обработано: *{report.total_scraped}* товаров",
            f"➕ Новых: *{report.new_products}*",
            f"✏️ Обновлено: *{report.updated_products}*",
            f"🖼 Фото загружено: *{report.images_uploaded}*",
        ]

        # Изменения цен
        if report.price_changes:
            lines.append("")
            lines.append(f"💰 *Изменения цен ({len(report.price_changes)}):*")
            for ch in report.price_changes[:10]:  # показываем топ-10
                emoji = "📈" if ch["new"] > ch["old"] else "📉"
                lines.append(
                    f"{emoji} {ch['name'][:40]}: {ch['old']:.0f} → {ch['new']:.0f} "
                    f"({'+' if ch['diff_pct'] > 0 else ''}{ch['diff_pct']}%)"
                )
            if len(report.price_changes) > 10:
                lines.append(f"  _...и ещё {len(report.price_changes) - 10}_")

        # Изменения наличия
        if report.stock_changes:
            lines.append("")
            lines.append(f"📦 *Изменения наличия ({len(report.stock_changes)}):*")
            for ch in report.stock_changes[:10]:
                emoji = "✅" if ch["new"] else "❌"
                status = "в наличии" if ch["new"] else "нет в наличии"
                lines.append(f"{emoji} {ch['name'][:40]} — {status}")

        # Ошибки
        if report.errors:
            lines.append("")
            lines.append(f"⚠️ *Ошибок: {len(report.errors)}*")
            for err in report.errors[:5]:
                lines.append(f"  • {err[:80]}")

        await self._send("\n".join(lines))

    async def send_error(self, error: str):
        await self._send(f"🚨 *Критическая ошибка агента:*\n```{error[:500]}```")

    async def _send(self, text: str):
        if not self.token or not self.chat_id:
            logger.warning("Telegram не настроен, пропускаем уведомление")
            return

        url = TG_API.format(token=self.token, method="sendMessage")
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.error(f"Telegram API ошибка: {resp.status} {body}")
        except Exception as e:
            logger.error(f"Ошибка отправки в Telegram: {e}")
