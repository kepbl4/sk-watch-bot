"""Entry point for the SK Watch Bot."""
from __future__ import annotations

import asyncio
import os
from contextlib import suppress

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from dotenv import load_dotenv

from bot import menu
from watcher.scheduler import scheduler
from storage.db import init_db, settings_set
from utils.logging import logger, setup_logging


async def main() -> None:
    load_dotenv()
    setup_logging()

    init_db()

    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is not set")

    interval = int(os.getenv("CHECK_INTERVAL_MIN", "10") or 10)
    settings_set("CHECK_INTERVAL_MIN", str(interval))
    owner_id = int(os.getenv("OWNER_ID", "0") or 0)
    menu.configure(interval, owner_id if owner_id else None)

    bot = Bot(token=bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(menu.router)

    @dp.startup()
    async def _on_startup(bot: Bot) -> None:  # pragma: no cover - lifecycle
        await scheduler.start(bot, interval)

    @dp.shutdown()
    async def _on_shutdown(bot: Bot) -> None:  # pragma: no cover - lifecycle
        await scheduler.stop()

    logger.info("Starting SK Watch Bot")
    with suppress(asyncio.CancelledError):
        await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
