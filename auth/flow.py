"""Authentication flow handled via Playwright."""
from __future__ import annotations

import asyncio
import os
import re
from datetime import datetime, timedelta
from typing import Optional

import aiohttp
from aiogram import Bot
from aiogram.types import BufferedInputFile, Message
from playwright.async_api import (
    BrowserContext,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

from storage import db
from utils.logging import logger


CAPTCHA_READY = "auth:captcha_done"
CAPTCHA_CANCEL = "auth:captcha_cancel"
CAPTCHA_MANUAL = "auth:captcha_manual"


class AuthManager:
    """Stateful helper that encapsulates portal authentication."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._playwright: Optional[Playwright] = None
        self._context: Optional[BrowserContext] = None
        self._captcha_future: Optional[asyncio.Future[bool]] = None
        self._manual_future: Optional[asyncio.Future[bool]] = None
        self._sms_future: Optional[asyncio.Future[str]] = None
        self._owner_id = int(os.getenv("OWNER_ID", "0") or 0)
        self._login_url = os.getenv("LOGIN_URL", "")
        self._profile_dir = os.getenv("BROWSER_PROFILE_DIR", "./browser_profile")
        self._headless = os.getenv("HEADLESS", "true").lower() == "true"
        self._ignore_https = os.getenv("IGNORE_HTTPS_ERRORS", "true").lower() == "true"
        self._captcha_provider = os.getenv("CAPTCHA_PROVIDER", "auto").lower()
        self._captcha_key = os.getenv("CAPTCHA_API_KEY")
        self._timezone = os.getenv("BOT_TIMEZONE", "Europe/Bratislava")
        self._auth_valid_hours = int(os.getenv("AUTH_VALID_HOURS", "6") or 6)

    async def ensure_auth(self, bot: Bot, *, manual: bool = False, force: bool = False) -> str:
        """Ensure the session is authorised; return state string."""

        if not self._login_url:
            logger.error("LOGIN_URL is not configured")
            return "ERROR"

        async with self._lock:
            context = await self._ensure_context()
            if not context:
                return "ERROR"

            if not force:
                cached_state = db.settings_get("auth_state")
                auth_exp = db.settings_get("auth_exp")
                if cached_state == "OK" and auth_exp:
                    try:
                        exp = datetime.fromisoformat(auth_exp)
                    except ValueError:
                        exp = datetime.min
                    if exp > datetime.utcnow():
                        state = await self._preflight(context)
                        if state == "OK":
                            logger.info("Auth preflight OK, session valid until %s", exp)
                            db.settings_set("auth_state", "OK")
                            return "OK"

            result = await self._perform_login(context, bot, manual=manual)
            db.settings_set("auth_state", result)
            if result == "OK":
                expiry = datetime.utcnow() + timedelta(hours=self._auth_valid_hours)
                db.settings_set("auth_exp", expiry.isoformat())
            else:
                db.settings_set("auth_exp", "")
            logger.info("Auth finished with state %s", result)
            return result

    async def capture_category_screenshot(self, cat_key: str) -> Optional[BufferedInputFile]:
        async with self._lock:
            context = await self._ensure_context()
            if not context:
                return None

            category = db.get_category(cat_key)
            if not category:
                return None

            url = category.get("url") or self._login_url
            page = await context.new_page()
            try:
                await page.goto(url, wait_until="networkidle", timeout=45000)
                await asyncio.sleep(1)
                data = await page.screenshot(full_page=True)
                filename = f"{cat_key}.png"
                return BufferedInputFile(data, filename=filename)
            except PlaywrightTimeoutError:
                logger.warning("Screenshot timeout for %s", cat_key)
                return None
            finally:
                await page.close()

    async def get_context(self) -> Optional[BrowserContext]:
        async with self._lock:
            return await self._ensure_context()

    async def resolve_captcha(self, ok: bool) -> None:
        if self._captcha_future and not self._captcha_future.done():
            self._captcha_future.set_result(ok)

    async def submit_sms_code(self, code: str) -> None:
        if self._sms_future and not self._sms_future.done():
            self._sms_future.set_result(code)

    async def try_handle_owner_message(self, message: Message) -> bool:
        text = (message.text or "").strip()
        if self._manual_future and not self._manual_future.done():
            if text.lower() in {"готово", "done"}:
                self._manual_future.set_result(True)
                return True
            if text.lower() in {"отмена", "cancel"}:
                self._manual_future.set_result(False)
                return True
        if self._sms_future and not self._sms_future.done():
            if not re.fullmatch(r"\d{6}", text):
                await message.answer("Код должен состоять из 6 цифр. Попробуйте ещё раз.")
                return True
            self._sms_future.set_result(text)
            return True
        return False

    async def request_manual_captcha(self) -> None:
        if self._manual_future and not self._manual_future.done():
            self._manual_future.set_result(True)

    async def _ensure_context(self) -> Optional[BrowserContext]:
        if self._context:
            return self._context

        os.makedirs(self._profile_dir, exist_ok=True)
        try:
            self._playwright = await async_playwright().start()
            self._context = await self._playwright.chromium.launch_persistent_context(
                self._profile_dir,
                headless=self._headless,
                timezone_id=self._timezone,
                locale="sk-SK",
                accept_downloads=False,
                ignore_https_errors=self._ignore_https,
                args=["--lang=sk-SK,sk;q=0.9,en;q=0.8"],
            )
            return self._context
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Failed to launch browser: %s", exc)
            return None

    async def _preflight(self, context: BrowserContext) -> str:
        page = await context.new_page()
        try:
            await page.goto(self._login_url, wait_until="domcontentloaded", timeout=30000)
            if "login" in page.url.lower():
                return "NEED_AUTH"
            if await page.locator("form[id*='login']").count() > 0:
                return "NEED_AUTH"
            return "OK"
        except PlaywrightTimeoutError:
            return "NEED_VPN"
        except Exception as exc:  # pragma: no cover - network issues
            logger.exception("Preflight failed: %s", exc)
            return "ERROR"
        finally:
            await page.close()

    async def _perform_login(self, context: BrowserContext, bot: Bot, *, manual: bool) -> str:
        page = await context.new_page()
        try:
            await page.goto(self._login_url, wait_until="domcontentloaded", timeout=45000)
            await self._accept_cookies(page)

            if await self._handle_recaptcha(page, bot, manual=manual) is False:
                return "NEED_CAPTCHA"

            await self._submit_credentials(page)

            sms_needed = await self._await_sms_prompt(page)
            if sms_needed:
                code = await self._prompt_sms_code(bot)
                if not code:
                    return "NEED_SMS"
                await self._enter_sms_code(page, code)

            await page.wait_for_load_state("networkidle")
            state = await self._preflight(context)
            if state == "OK":
                return "OK"
            return state
        except PlaywrightTimeoutError:
            return "NEED_VPN"
        except Exception as exc:  # pragma: no cover - login errors
            logger.exception("Auth flow error: %s", exc)
            return "ERROR"
        finally:
            await page.close()

    async def _accept_cookies(self, page: Page) -> None:
        try:
            button = page.get_by_role("button", name=re.compile("Súhlasím|Akceptujem", re.I))
            if await button.count():
                await button.first.click()
        except PlaywrightTimeoutError:
            logger.debug("Cookie banner not shown")

    async def _handle_recaptcha(self, page: Page, bot: Bot, *, manual: bool) -> Optional[bool]:
        captcha_locator = page.locator(".g-recaptcha")
        try:
            await captcha_locator.wait_for(timeout=15000)
        except PlaywrightTimeoutError:
            return True

        if self._captcha_provider == "auto" and self._captcha_key:
            for attempt in range(1, 3):
                logger.info("Attempting automatic captcha solve (try %s)", attempt)
                token = await self._solve_recaptcha_auto(page)
                if token:
                    await self._inject_recaptcha_token(page, token)
                    logger.info("Captcha solved automatically")
                    return True
                logger.warning("AUTO captcha attempt %s failed", attempt)
            await self._prompt_manual_help(bot)

        if manual or self._captcha_provider in {"manual", "semi"}:
            return await self._await_manual_captcha(bot)
        if self._manual_future:
            approved = await self._wait_for_manual_start()
            if approved:
                return await self._await_manual_captcha(bot)
        return False

    async def _solve_recaptcha_auto(self, page: Page) -> Optional[str]:
        sitekey = await page.get_attribute(".g-recaptcha", "data-sitekey")
        if not sitekey:
            return None
        payload = {
            "key": self._captcha_key,
            "method": "userrecaptcha",
            "googlekey": sitekey,
            "pageurl": page.url,
            "json": 1,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post("https://2captcha.com/in.php", data=payload) as resp:
                data = await resp.json()
        if data.get("status") != 1:
            logger.error("2captcha request failed: %s", data)
            return None
        request_id = data.get("request")
        for attempt in range(24):
            await asyncio.sleep(5)
            params = {
                "key": self._captcha_key,
                "action": "get",
                "id": request_id,
                "json": 1,
            }
            async with aiohttp.ClientSession() as session:
                async with session.get("https://2captcha.com/res.php", params=params) as resp:
                    result = await resp.json()
            if result.get("status") == 1:
                return result.get("request")
            if result.get("request") != "CAPCHA_NOT_READY":
                logger.error("2captcha returned error: %s", result)
                break
        logger.error("2captcha did not return a solution")
        return None

    async def _inject_recaptcha_token(self, page: Page, token: str) -> None:
        await page.evaluate(
            "token => {"
            "const area = document.querySelector('textarea#g-recaptcha-response');"
            "if (area) { area.value = token; area.dispatchEvent(new Event('change')); }"
            "}",
            token,
        )

    async def _prompt_manual_help(self, bot: Bot) -> None:
        if not self._owner_id:
            return
        loop = asyncio.get_running_loop()
        self._manual_future = loop.create_future()
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="Помочь с капчей (ручной)", callback_data=CAPTCHA_MANUAL
                    )
                ]
            ]
        )
        await bot.send_message(
            self._owner_id,
            "Авто-решение reCAPTCHA не удалось. Нажмите «Помочь с капчей (ручной)», чтобы перейти в ручной режим.",
            reply_markup=keyboard,
        )

    async def _wait_for_manual_start(self) -> bool:
        if not self._manual_future:
            return False
        try:
            return await asyncio.wait_for(self._manual_future, timeout=180)
        except asyncio.TimeoutError:
            return False
        finally:
            self._manual_future = None

    async def _await_manual_captcha(self, bot: Bot) -> Optional[bool]:
        if not self._owner_id:
            return False
        if self._captcha_future and not self._captcha_future.done():
            self._captcha_future.cancel()
        loop = asyncio.get_running_loop()
        self._captcha_future = loop.create_future()
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Готово", callback_data=CAPTCHA_READY)],
                [InlineKeyboardButton(text="Отмена", callback_data=CAPTCHA_CANCEL)],
            ]
        )
        await bot.send_message(
            self._owner_id,
            "Нужно решить reCAPTCHA на портале. Нажмите «Готово», когда закончите.",
            reply_markup=keyboard,
        )
        try:
            return await asyncio.wait_for(self._captcha_future, timeout=300)
        except asyncio.TimeoutError:
            return False
        finally:
            self._captcha_future = None

    async def _submit_credentials(self, page: Page) -> None:
        username = os.getenv("PORTAL_USERNAME")
        password = os.getenv("PORTAL_PASSWORD")
        try:
            if username:
                await page.fill("input[name*='user']", username)
            if password:
                await page.fill("input[type='password']", password)
            await page.click("button[type='submit']")
        except PlaywrightTimeoutError:
            logger.warning("Credential fields not found, waiting for manual input")

    async def _await_sms_prompt(self, page: Page) -> bool:
        try:
            await page.wait_for_selector("input[type='tel']", timeout=5000)
            return True
        except PlaywrightTimeoutError:
            return False

    async def _prompt_sms_code(self, bot: Bot) -> Optional[str]:
        if not self._owner_id:
            return None
        attempts = 3
        for remaining in range(attempts, 0, -1):
            loop = asyncio.get_running_loop()
            self._sms_future = loop.create_future()
            await bot.send_message(
                self._owner_id,
                f"Введите SMS-код из 6 цифр (осталось попыток: {remaining}).",
            )
            try:
                code = await asyncio.wait_for(self._sms_future, timeout=120)
            except asyncio.TimeoutError:
                await bot.send_message(self._owner_id, "Таймаут ожидания SMS-кода.")
                return None
            finally:
                self._sms_future = None

            if re.fullmatch(r"\d{6}", code):
                return code
            await bot.send_message(self._owner_id, "Код должен состоять из 6 цифр. Попробуйте ещё раз.")
        return None

    async def _enter_sms_code(self, page: Page, code: str) -> None:
        await page.fill("input[type='tel']", code)
        await page.click("button[type='submit']")


auth_manager = AuthManager()

__all__ = ["auth_manager", "CAPTCHA_READY", "CAPTCHA_CANCEL", "CAPTCHA_MANUAL"]
