"""Mock scheduler that records pretend monitoring pulses."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import Optional

from aiogram import Bot

from storage import db


class WatcherScheduler:
    """Lightweight stub that simulates background monitoring."""

    def __init__(self) -> None:
        self._bot: Optional[Bot] = None
        self._interval = 10

    async def start(self, bot: Bot, interval_minutes: int) -> None:  # pragma: no cover - lifecycle helper
        self._bot = bot
        self._interval = max(1, interval_minutes)
        await asyncio.to_thread(db.settings_set, "fake:monitor_interval", str(self._interval))

    async def stop(self) -> None:  # pragma: no cover - lifecycle helper
        self._bot = None

    async def update_interval(self, interval_minutes: int) -> None:
        self._interval = max(1, interval_minutes)
        await asyncio.to_thread(db.settings_set, "fake:monitor_interval", str(self._interval))

    async def record_pulse(self, text: str) -> None:
        """Persist a pretend monitoring event."""

        raw = await asyncio.to_thread(db.settings_get, "fake:events", "[]")
        try:
            events = json.loads(raw)
        except json.JSONDecodeError:  # pragma: no cover - defensive
            events = []
        events.append({"ts": datetime.utcnow().isoformat(), "text": text})
        events = events[-12:]
        await asyncio.to_thread(db.settings_set, "fake:events", json.dumps(events, ensure_ascii=False))

    @property
    def interval(self) -> int:
        return self._interval


scheduler = WatcherScheduler()

__all__ = ["scheduler", "WatcherScheduler"]
