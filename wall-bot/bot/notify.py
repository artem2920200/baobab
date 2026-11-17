"""Telegram-уведомления (плюс все события всегда видны в веб-интерфейсе)."""
from __future__ import annotations

import asyncio
import logging
import os

import aiohttp

from .models import Event
from .state import AppState

log = logging.getLogger("notify")

LEVEL_EMOJI = {
    "signal": "🎯", "success": "✅", "warn": "⚠️", "error": "🛑", "info": "ℹ️",
}


class Notifier:
    def __init__(self, cfg: dict, state: AppState):
        t = cfg["telegram"]
        self.state = state
        self.token = os.environ.get("TG_BOT_TOKEN") or t.get("token", "")
        self.chat_id = os.environ.get("TG_CHAT_ID") or str(t.get("chat_id", ""))
        self.types = set(t.get("events", []))
        self.enabled = bool(t.get("enabled")) and bool(self.token) and bool(self.chat_id)
        self.queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=500)
        self._fails = 0
        if self.enabled:
            state.event_sink = self._sink
            state.telegram_active = True

    def _sink(self, ev: Event) -> None:
        if ev.type in self.types:
            try:
                self.queue.put_nowait(ev)
            except asyncio.QueueFull:
                pass

    async def loop(self) -> None:
        if not self.enabled:
            self.state.add_event(
                "info", "Telegram не настроен",
                "Уведомления идут только в веб-интерфейс. Чтобы включить Telegram: "
                "создай бота у @BotFather, впиши telegram.token и telegram.chat_id в config.yaml.")
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)) as session:
            await self._send(session, url,
                             f"🤖 <b>Wall Bot запущен</b>\n"
                             f"Режим: {self.state.mode.upper()} | данные: {self.state.data_source.upper()}\n"
                             f"Уведомления о подходе цены к плотностям включены.")
            while True:
                ev = await self.queue.get()
                emoji = LEVEL_EMOJI.get(ev.level, "•")
                text = f"{emoji} <b>{ev.title}</b>"
                if ev.text:
                    text += f"\n{ev.text}"
                await self._send(session, url, text)
                await asyncio.sleep(1.1)  # бережём лимиты Telegram

    async def _send(self, session: aiohttp.ClientSession, url: str, text: str) -> None:
        try:
            async with session.post(url, json={"chat_id": self.chat_id, "text": text,
                                               "parse_mode": "HTML",
                                               "disable_web_page_preview": True}) as r:
                if r.status != 200:
                    body = await r.text()
                    raise RuntimeError(f"HTTP {r.status}: {body[:120]}")
            self._fails = 0
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._fails += 1
            log.warning("telegram: %s", e)
            if self._fails == 5:
                self.state.telegram_active = False
                self.state.add_event("error", "Telegram недоступен",
                                     f"5 ошибок подряд ({e}). Проверь token/chat_id.",
                                     level="warn")
