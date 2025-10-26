"""Authentication flow handled via Playwright."""
from __future__ import annotations

import asyncio
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode, urlsplit, urlunsplit

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
        self._screen_dir = Path(os.getenv("SCREEN_DIR", "/opt/bot/logs/screens"))
        self._screen_dir.mkdir(parents=True, exist_ok=True)
        self._manual_session_active = False

    async def handle_portal_interstitial(self, page: Page) -> None:
        """Dismiss intermediate confirmation screens on the portal."""

        await self._click_continue(page)
        await self._select_language(page)

    async def ensure_auth(self, bot: Bot, *, manual: bool = False, force: bool = False) -> str:
        """Ensure the session is authorised; return state string."""

        if not self._login_url:
            logger.error("LOGIN_URL is not configured")
            return "ERROR"

        async with self._lock:
            await self._run_system_checks()
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
                if self._manual_session_active:
                    await self._capture_context_screenshot(
                        context,
                        prefix="AuthManualDone",
                        description="Ручная авторизация завершена",
                    )
                    self._manual_session_active = False
            else:
                db.settings_set("auth_exp", "")
                self._manual_session_active = False
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
                await asyncio.to_thread(
                    self._store_screenshot,
                    data,
                    f"Category_{cat_key}",
                    f"Скриншот категории {cat_key}",
                )
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
        install_attempted = False
        while True:
            try:
                if not self._playwright:
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
                if not install_attempted and self._should_install_browser(exc):
                    install_attempted = True
                    logger.warning("Playwright browser missing, attempting installation…")
                    success = await self._install_playwright_browsers()
                    await self._shutdown_browser()
                    if success:
                        logger.info("Playwright browser installation finished successfully")
                        continue
                    logger.error("Playwright browser installation failed")
                    return None
                logger.exception("Failed to launch browser: %s", exc)
                await self._shutdown_browser()
                return None

    def _should_install_browser(self, exc: Exception) -> bool:
        message = str(exc)
        return "Executable doesn't exist" in message or "was just installed" in message

    async def _shutdown_browser(self) -> None:
        if self._context:
            try:
                await self._context.close()
            except Exception as close_exc:  # pragma: no cover - defensive cleanup
                logger.debug("Failed to close browser context: %s", close_exc)
            self._context = None
        if self._playwright:
            try:
                result = self._playwright.stop()
                if asyncio.iscoroutine(result):
                    await result
            except Exception as stop_exc:  # pragma: no cover - defensive cleanup
                logger.debug("Failed to stop Playwright: %s", stop_exc)
            self._playwright = None

    async def _install_playwright_browsers(self) -> bool:
        command = [sys.executable, "-m", "playwright", "install", "chromium"]
        try:
            proc = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as exc:  # pragma: no cover - runtime environment
            logger.error("playwright install command not found: %s", exc)
            return False

        stdout, stderr = await proc.communicate()
        if stdout:
            logger.debug("playwright install stdout: %s", stdout.decode(errors="ignore").strip())
        if stderr:
            logger.debug("playwright install stderr: %s", stderr.decode(errors="ignore").strip())

        if proc.returncode == 0:
            return True

        logger.error("playwright install exited with code %s", proc.returncode)
        return False

    async def _preflight(self, context: BrowserContext) -> str:
        page = await context.new_page()
        try:
            await page.goto(self._login_url, wait_until="domcontentloaded", timeout=30000)
            await self.handle_portal_interstitial(page)
            if await page.locator("form[id*='login']").count() > 0:
                return "NEED_AUTH"
            if await page.locator("input[type='password']").count() > 0:
                return "NEED_AUTH"
            if "login" in page.url.lower():
                return "NEED_AUTH"
            try:
                await page.wait_for_selector("text=Pracoviská", timeout=5000)
                logger.debug("Preflight located schedule marker")
            except PlaywrightTimeoutError:
                logger.debug("Preflight did not see schedule marker, assuming session ok")
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
        self._manual_session_active = False
        db.settings_set("auth_sms_pending", "0")
        try:
            await page.goto(self._login_url, wait_until="domcontentloaded", timeout=45000)
            await self._accept_cookies(page)
            await self.handle_portal_interstitial(page)

            if await self._handle_recaptcha(page, bot, manual=manual) is False:
                return "NEED_CAPTCHA"

            await self._submit_credentials(page)
            await self.handle_portal_interstitial(page)

            sms_needed = await self._await_sms_prompt(page)
            if sms_needed:
                code = await self._prompt_sms_code(bot)
                if not code:
                    return "NEED_SMS"
                await self._enter_sms_code(page, code)
                await self.handle_portal_interstitial(page)

            await page.wait_for_load_state("networkidle")
            await self.handle_portal_interstitial(page)
            state = await self._preflight(context)
            if state == "OK":
                await self.capture_page_screenshot(
                    page,
                    prefix="LoginDone",
                    description="Успешный вход в портал",
                )
                return "OK"
            await self.capture_page_screenshot(
                page,
                prefix="LoginState",
                description=f"Авторизация завершена с состоянием {state}",
            )
            return state
        except PlaywrightTimeoutError:
            await self.capture_page_screenshot(
                page,
                prefix="LoginTimeout",
                description="Таймаут авторизации",
            )
            return "NEED_VPN"
        except Exception as exc:  # pragma: no cover - login errors
            logger.exception("Auth flow error: %s", exc)
            await self.capture_page_screenshot(
                page,
                prefix="LoginError",
                description=f"Ошибка авторизации: {exc}",
            )
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
        logger.info("2captcha request created: id=%s", request_id)
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
                logger.info("2captcha solved: id=%s", request_id)
                return result.get("request")
            if result.get("request") != "CAPCHA_NOT_READY":
                logger.error("2captcha returned error: %s", result)
                break
            logger.debug("2captcha pending: id=%s status=%s", request_id, result.get("request"))
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
        manual_link = self._build_manual_link()
        await bot.send_message(
            self._owner_id,
            "Нужно решить reCAPTCHA на портале. Нажмите «Готово», когда закончите."
            "\nРазовая ссылка: " + manual_link,
            reply_markup=keyboard,
        )
        try:
            result = await asyncio.wait_for(self._captcha_future, timeout=300)
            if result:
                self._manual_session_active = True
            return result
        except asyncio.TimeoutError:
            return False
        finally:
            self._captcha_future = None

    async def _submit_credentials(self, page: Page) -> None:
        username = os.getenv("PORTAL_USERNAME")
        password = os.getenv("PORTAL_PASSWORD")

        async def _fill_first(selectors: list[str], value: str, field: str) -> bool:
            for selector in selectors:
                try:
                    locator = page.locator(selector)
                    if await locator.count():
                        await locator.first.fill(value)
                        logger.info("Filled %s via selector %s", field, selector)
                        return True
                except PlaywrightTimeoutError:
                    continue
                except Exception as exc:  # pragma: no cover - selector edge cases
                    logger.debug("Selector %s failed for %s: %s", selector, field, exc)
                    continue
            return False

        if username:
            username_selectors = [
                "input[name*='user']",
                "input[name*='login']",
                "input[id*='user']",
                "input[id*='login']",
                "input#username",
                "input[name='username']",
            ]
            if not await _fill_first(username_selectors, username, "username"):
                logger.warning("Username field not found, expecting manual entry")

        if password:
            password_selectors = [
                "input[type='password']",
                "input[name*='pass']",
                "input[id*='pass']",
                "input#password",
            ]
            if not await _fill_first(password_selectors, password, "password"):
                logger.warning("Password field not found, expecting manual entry")

        submit_selectors = [
            "button[type='submit']",
            "input[type='submit']",
            "button:has-text('Prihlásiť')",
            "button:has-text('Login')",
        ]
        submitted = False
        for selector in submit_selectors:
            try:
                locator = page.locator(selector)
                if await locator.count():
                    await locator.first.click()
                    submitted = True
                    logger.info("Clicked submit via selector %s", selector)
                    break
            except PlaywrightTimeoutError:
                continue
            except Exception as exc:  # pragma: no cover - selector edge cases
                logger.debug("Submit selector %s failed: %s", selector, exc)
                continue

        if not submitted:
            try:
                await page.keyboard.press("Enter")
                logger.info("Triggered form submit via Enter key")
            except Exception as exc:  # pragma: no cover - keyboard edge cases
                logger.warning("Unable to submit credentials automatically: %s", exc)

    async def _await_sms_prompt(self, page: Page) -> bool:
        selectors = [
            "input[type='tel']",
            "input[name*='sms']",
            "input[id*='sms']",
            "input[name*='otp']",
            "input[id*='otp']",
        ]
        for selector in selectors:
            try:
                await page.wait_for_selector(selector, timeout=5000)
                logger.info("SMS prompt detected via selector %s", selector)
                return True
            except PlaywrightTimeoutError:
                continue
        return False

    async def _prompt_sms_code(self, bot: Bot) -> Optional[str]:
        if not self._owner_id:
            return None
        attempts = 3
        db.settings_set("auth_sms_pending", "1")
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
                db.settings_set("auth_sms_pending", "0")
                return None
            finally:
                self._sms_future = None

            if re.fullmatch(r"\d{6}", code):
                db.settings_set("auth_sms_pending", "0")
                return code
            await bot.send_message(self._owner_id, "Код должен состоять из 6 цифр. Попробуйте ещё раз.")
        db.settings_set("auth_sms_pending", "0")
        return None

    async def _enter_sms_code(self, page: Page, code: str) -> None:
        selectors = [
            "input[type='tel']",
            "input[name*='sms']",
            "input[id*='sms']",
            "input[name*='otp']",
            "input[id*='otp']",
        ]
        for selector in selectors:
            try:
                locator = page.locator(selector)
                if await locator.count():
                    await locator.first.fill(code)
                    logger.info("Filled SMS code using selector %s", selector)
                    break
            except Exception as exc:  # pragma: no cover - selector edge cases
                logger.debug("Failed to fill SMS selector %s: %s", selector, exc)
        try:
            submit = page.locator("button[type='submit']")
            if await submit.count():
                await submit.first.click()
            else:
                await page.keyboard.press("Enter")
        except Exception as exc:  # pragma: no cover - selector edge cases
            logger.debug("Failed to submit SMS form: %s", exc)


    async def _run_system_checks(self) -> None:
        warnings = []
        hint_parts = []
        timedatectl_path = shutil.which("timedatectl")
        if not timedatectl_path:
            warnings.append("нет timedatectl")
        else:
            try:
                proc = await asyncio.create_subprocess_exec(
                    timedatectl_path,
                    "status",
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                try:
                    await asyncio.wait_for(proc.communicate(), timeout=5)
                except asyncio.TimeoutError:
                    proc.kill()
                    warnings.append("timedatectl не отвечает")
                    hint_parts.append("timeout")
                else:
                    if proc.returncode != 0:
                        warnings.append("timedatectl вернул ошибку")
                        hint_parts.append(f"rc={proc.returncode}")
            except Exception as exc:  # pragma: no cover - system specific
                warnings.append("timedatectl не отвечает")
                hint_parts.append(str(exc))

        if not Path("/etc/ssl/certs/ca-certificates.crt").exists():
            warnings.append("нет ca-certificates")
            hint_parts.append("apt install ca-certificates")

        if not Path("/usr/share/zoneinfo").exists():
            warnings.append("нет tzdata")
            hint_parts.append("apt install tzdata")

        if warnings:
            message = "; ".join(warnings)
            if hint_parts:
                message = f"{message} ({'; '.join(hint_parts)})"
            db.settings_set("auth_system_state", "WARN")
            db.settings_set("auth_system_hint", message)
        else:
            db.settings_set("auth_system_state", "OK")
            db.settings_set("auth_system_hint", "")

    def _build_manual_link(self) -> str:
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        parts = urlsplit(self._login_url)
        try:
            from urllib.parse import parse_qsl

            query = dict(parse_qsl(parts.query))
        except Exception:
            query = {}
        query["manual_token"] = timestamp
        new_query = urlencode(query)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))

    async def _click_continue(self, page: Page) -> None:
        selectors = [
            page.get_by_role("button", name=re.compile("Pokračovať", re.I)),
            page.get_by_role("button", name=re.compile("Continue", re.I)),
            page.locator("button:has-text('Pokračovať')"),
            page.locator("button:has-text('Continue')"),
        ]
        for locator in selectors:
            try:
                if await locator.count():
                    await locator.first.click()
                    await page.wait_for_timeout(300)
                    logger.info("Confirmed portal continue dialog")
                    return
            except Exception as exc:  # pragma: no cover - selector edge cases
                logger.debug("Continue selector failed: %s", exc)

    async def _select_language(self, page: Page) -> None:
        language_patterns = [
            re.compile("Sloven", re.I),
            re.compile("English", re.I),
            re.compile("Rus", re.I),
            re.compile("Укра", re.I),
        ]
        for pattern in language_patterns:
            try:
                button = page.get_by_role("button", name=pattern)
                if await button.count():
                    await button.first.click()
                    await page.wait_for_timeout(300)
                    logger.info("Selected portal language via %s", pattern.pattern)
                    return
                link = page.get_by_role("link", name=pattern)
                if await link.count():
                    await link.first.click()
                    await page.wait_for_timeout(300)
                    logger.info("Selected portal language via link %s", pattern.pattern)
                    return
            except Exception as exc:  # pragma: no cover - selector edge cases
                logger.debug("Language selector failed for %s: %s", pattern.pattern, exc)

    async def capture_page_screenshot(
        self,
        page: Page,
        *,
        prefix: str,
        description: str,
    ) -> Optional[str]:
        try:
            data = await page.screenshot(full_page=True)
        except Exception as exc:  # pragma: no cover - playwright edge
            logger.warning("Failed to capture %s screenshot: %s", prefix, exc)
            return None
        return await asyncio.to_thread(self._store_screenshot, data, prefix, description)

    async def _capture_context_screenshot(
        self,
        context: BrowserContext,
        *,
        prefix: str,
        description: str,
    ) -> Optional[str]:
        page = await context.new_page()
        try:
            await page.goto(self._login_url, wait_until="domcontentloaded", timeout=30000)
            return await self.capture_page_screenshot(page, prefix=prefix, description=description)
        except Exception as exc:  # pragma: no cover - navigation issues
            logger.warning("Failed to capture context screenshot: %s", exc)
            return None
        finally:
            await page.close()

    async def capture_portal_error(
        self,
        url: str,
        *,
        description: str,
        prefix: str = "PortalError",
    ) -> Optional[str]:
        context = await self.get_context()
        if not context:
            return None
        page = await context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        except Exception as exc:  # pragma: no cover - portal issues
            logger.warning("Portal error navigation failed: %s", exc)
        finally:
            try:
                path = await self.capture_page_screenshot(
                    page,
                    prefix=prefix,
                    description=description,
                )
            finally:
                await page.close()
        return path

    def _store_screenshot(self, data: bytes, prefix: str, description: str) -> Optional[str]:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        name = f"{prefix}_{timestamp}.png"
        path = self._screen_dir / name
        try:
            path.write_bytes(data)
        except Exception as exc:  # pragma: no cover - filesystem issues
            logger.error("Failed to persist screenshot %s: %s", name, exc)
            return None
        db.record_screenshot(name, str(path), description)
        logger.info("Screenshot saved: %s", path)
        return str(path)


    async def _run_system_checks(self) -> None:
        warnings = []
        hint_parts = []
        timedatectl_path = shutil.which("timedatectl")
        if not timedatectl_path:
            warnings.append("нет timedatectl")
        else:
            try:
                proc = await asyncio.create_subprocess_exec(
                    timedatectl_path,
                    "status",
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                await asyncio.wait_for(proc.communicate(), timeout=5)
                if proc.returncode != 0:
                    warnings.append("timedatectl вернул ошибку")
                    hint_parts.append(f"rc={proc.returncode}")
            except Exception as exc:  # pragma: no cover - system specific
                warnings.append("timedatectl не отвечает")
                hint_parts.append(str(exc))

        if not Path("/etc/ssl/certs/ca-certificates.crt").exists():
            warnings.append("нет ca-certificates")
            hint_parts.append("apt install ca-certificates")

        if not Path("/usr/share/zoneinfo").exists():
            warnings.append("нет tzdata")
            hint_parts.append("apt install tzdata")

        if warnings:
            message = "; ".join(warnings)
            if hint_parts:
                message = f"{message} ({'; '.join(hint_parts)})"
            db.settings_set("auth_system_state", "WARN")
            db.settings_set("auth_system_hint", message)
        else:
            db.settings_set("auth_system_state", "OK")
            db.settings_set("auth_system_hint", "")

    def _build_manual_link(self) -> str:
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        parts = urlsplit(self._login_url)
        try:
            from urllib.parse import parse_qsl

            query = dict(parse_qsl(parts.query))
        except Exception:
            query = {}
        query["manual_token"] = timestamp
        new_query = urlencode(query)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))

    async def capture_page_screenshot(
        self,
        page: Page,
        *,
        prefix: str,
        description: str,
    ) -> Optional[str]:
        try:
            data = await page.screenshot(full_page=True)
        except Exception as exc:  # pragma: no cover - playwright edge
            logger.warning("Failed to capture %s screenshot: %s", prefix, exc)
            return None
        return await asyncio.to_thread(self._store_screenshot, data, prefix, description)

    async def _capture_context_screenshot(
        self,
        context: BrowserContext,
        *,
        prefix: str,
        description: str,
    ) -> Optional[str]:
        page = await context.new_page()
        try:
            await page.goto(self._login_url, wait_until="domcontentloaded", timeout=30000)
            return await self.capture_page_screenshot(page, prefix=prefix, description=description)
        except Exception as exc:  # pragma: no cover - navigation issues
            logger.warning("Failed to capture context screenshot: %s", exc)
            return None
        finally:
            await page.close()

    async def capture_portal_error(
        self,
        url: str,
        *,
        description: str,
        prefix: str = "PortalError",
    ) -> Optional[str]:
        context = await self.get_context()
        if not context:
            return None
        page = await context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        except Exception as exc:  # pragma: no cover - portal issues
            logger.warning("Portal error navigation failed: %s", exc)
        finally:
            try:
                path = await self.capture_page_screenshot(
                    page,
                    prefix=prefix,
                    description=description,
                )
            finally:
                await page.close()
        return path

    def _store_screenshot(self, data: bytes, prefix: str, description: str) -> Optional[str]:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        name = f"{prefix}_{timestamp}.png"
        path = self._screen_dir / name
        try:
            path.write_bytes(data)
        except Exception as exc:  # pragma: no cover - filesystem issues
            logger.error("Failed to persist screenshot %s: %s", name, exc)
            return None
        db.record_screenshot(name, str(path), description)
        logger.info("Screenshot saved: %s", path)
        return str(path)


auth_manager = AuthManager()

__all__ = ["auth_manager", "CAPTCHA_READY", "CAPTCHA_CANCEL", "CAPTCHA_MANUAL"]
