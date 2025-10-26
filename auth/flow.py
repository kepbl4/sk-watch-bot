from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Optional

from storage import db
from utils.logging import logger

CAPTCHA_READY = "auth:captcha_done"
CAPTCHA_CANCEL = "auth:captcha_cancel"
CAPTCHA_MANUAL = "auth:captcha_manual"


class AuthManager:
    """Lightweight mock of the authentication flow."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._state = "OK"
        self._last_reason = "Автоматический запуск"
        self._last_update = datetime.utcnow()

    async def ensure_auth(self, bot=None, manual: bool = False, force: bool = False) -> str:
        """Return the current fake authentication state."""

        await self._ensure_defaults()
        return self._state

    async def mark_refreshing(self, reason: str) -> None:
        """Switch the fake auth state into an updating mode."""

        async with self._lock:
            self._state = "UPDATING"
            self._last_reason = reason
            await asyncio.to_thread(db.settings_set, "fake:auth_state", self._state)

    async def mark_ok(self, reason: str) -> str:
        """Mark the fake authentication as successfully refreshed."""

        async with self._lock:
            self._state = "OK"
            self._last_reason = reason
            self._last_update = datetime.utcnow()
            await asyncio.to_thread(db.settings_set, "fake:auth_state", self._state)
            await asyncio.to_thread(
                db.settings_set,
                "fake:last_auth",
                self._last_update.isoformat(),
            )
            await asyncio.to_thread(db.settings_set, "fake:last_auth_reason", reason)
        return self._state

    async def get_state(self) -> str:
        await self._ensure_defaults()
        return self._state

    async def get_context(self):  # pragma: no cover - compatibility stub
        return None

    async def try_handle_owner_message(self, message) -> bool:  # pragma: no cover - compatibility stub
        return False

    async def resolve_captcha(self, success: bool) -> None:  # pragma: no cover - compatibility stub
        return None

    async def request_manual_captcha(self) -> None:  # pragma: no cover - compatibility stub
        return None

    async def capture_category_screenshot(self, cat_key: str):  # pragma: no cover - compatibility stub
        return None

    async def capture_portal_error(self, url: str, *, description: str = "") -> None:
        logger.debug("Fake portal error capture for %s (%s)", url, description)

    async def handle_portal_interstitial(self, page) -> None:  # pragma: no cover - compatibility stub
        return None

    async def capture_page_screenshot(self, page, *, name: Optional[str] = None):  # pragma: no cover - compatibility stub
        return None

    async def resolve_sms(self, code: str) -> None:  # pragma: no cover - compatibility stub
        return None

    async def _ensure_defaults(self) -> None:
        raw_state = await asyncio.to_thread(db.settings_get, "fake:auth_state", None)
        if raw_state:
            self._state = raw_state
            last_auth = await asyncio.to_thread(db.settings_get, "fake:last_auth", None)
            if last_auth:
                try:
                    self._last_update = datetime.fromisoformat(last_auth)
                except ValueError:  # pragma: no cover - defensive
                    self._last_update = datetime.utcnow()
            reason = await asyncio.to_thread(db.settings_get, "fake:last_auth_reason", None)
            if reason:
                self._last_reason = reason
            return

        await asyncio.to_thread(db.settings_set, "fake:auth_state", self._state)
        await asyncio.to_thread(db.settings_set, "fake:last_auth", self._last_update.isoformat())
        await asyncio.to_thread(db.settings_set, "fake:last_auth_reason", self._last_reason)


auth_manager = AuthManager()

__all__ = ["auth_manager", "AuthManager", "CAPTCHA_READY", "CAPTCHA_CANCEL", "CAPTCHA_MANUAL"]
