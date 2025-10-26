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

from storage import db
from utils.logging import logger

CAPTCHA_READY = "auth:captcha_done"
CAPTCHA_CANCEL = "auth:captcha_cancel"
CAPTCHA_MANUAL = "auth:captcha_manual"


class AuthManager:
    """Lightweight mock of the authentication flow."""

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

            url = category.get("url") or self._login_url
            page = await context.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                await self.handle_portal_interstitial(page)
                await self._advance_identity_wizard(page)
                try:
                    await page.wait_for_load_state("networkidle")
                except PlaywrightTimeoutError:
                    logger.debug("Screenshot networkidle wait timed out")
                try:
                    await page.wait_for_selector("text=Pracoviská", timeout=5000)
                except PlaywrightTimeoutError:
                    logger.debug("Screenshot did not detect schedule marker")
                await asyncio.sleep(0.5)
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

    async def _advance_identity_wizard(self, page: Page) -> None:
        for _ in range(3):
            progressed = await self._complete_identity_form(page)
            if not progressed:
                break
            await self.handle_portal_interstitial(page)

    async def _complete_identity_form(self, page: Page) -> bool:
        if not await self._is_identity_wizard(page):
            return False

        details = {
            "Meno": os.getenv("PORTAL_FIRST_NAME", "Ion"),
            "Priezvisko": os.getenv("PORTAL_LAST_NAME", "Huzo"),
            "Dátum narodenia": os.getenv("PORTAL_BIRTHDATE", "15.10.2003"),
            "Číslo cestovného dokladu": os.getenv("PORTAL_PASSPORT", "GB039802"),
            "SMS kontakt": os.getenv("PORTAL_PHONE", "+421944813597"),
            "Email kontakt": os.getenv("PORTAL_EMAIL", "mifania0586@gmail.com"),
        }

        async def _fill_field(label: str, value: str, extra_selectors: list[str]) -> bool:
            if not value:
                return False
            try:
                locator = page.get_by_label(re.compile(label, re.I))
                if await locator.count():
                    await locator.first.fill(value)
                    logger.info("Filled %s via label", label)
                    return True
            except Exception as exc:  # pragma: no cover - selector edge cases
                logger.debug("Label fill failed for %s: %s", label, exc)
            for selector in extra_selectors:
                try:
                    locator = page.locator(selector)
                    if await locator.count():
                        await locator.first.fill(value)
                        logger.info("Filled %s via selector %s", label, selector)
                        return True
                except Exception as exc:  # pragma: no cover - selector edge cases
                    logger.debug("Selector %s failed for %s: %s", selector, label, exc)
            logger.warning("Не удалось автоматически заполнить поле %s", label)
            return False

        field_selectors = {
            "Meno": [
                "input[name*='meno']",
                "input[id*='meno']",
            ],
            "Priezvisko": [
                "input[name*='priez']",
                "input[id*='priez']",
            ],
            "Dátum narodenia": [
                "input[type='date']",
                "input[name*='narod']",
                "input[id*='narod']",
            ],
            "Číslo cestovného dokladu": [
                "input[name*='cest']",
                "input[name*='passport']",
                "input[id*='cest']",
            ],
            "SMS kontakt": [
                "input[type='tel']",
                "input[name*='sms']",
                "input[id*='sms']",
                "input[name*='phone']",
            ],
            "Email kontakt": [
                "input[type='email']",
                "input[name*='mail']",
                "input[id*='mail']",
            ],
        }

        filled_any = False
        for label, value in details.items():
            selectors = field_selectors.get(label, [])
            if await _fill_field(label, value, selectors):
                filled_any = True

        if not filled_any:
            logger.debug("Identity wizard detected but no fields filled")
            return False

        submit_selectors = [
            "button[type='submit']",
            "input[type='submit']",
            "button:has-text('Pokračovať')",
            "button:has-text('Continue')",
        ]
        for selector in submit_selectors:
            try:
                locator = page.locator(selector)
                if await locator.count():
                    await locator.first.click()
                    await page.wait_for_load_state("networkidle")
                    logger.info("Submitted identity wizard via %s", selector)
                    return True
            except PlaywrightTimeoutError:
                continue
            except Exception as exc:  # pragma: no cover - selector edge cases
                logger.debug("Submit selector %s failed for identity wizard: %s", selector, exc)

        try:
            await page.keyboard.press("Enter")
            await page.wait_for_load_state("networkidle")
            logger.info("Submitted identity wizard via Enter key")
            return True
        except Exception as exc:  # pragma: no cover - keyboard edge cases
            logger.warning("Не удалось отправить форму идентификации автоматически: %s", exc)
            return False

    async def _is_identity_wizard(self, page: Page) -> bool:
        try:
            if await page.locator("text=/Krok\\s+\\d+\\s+z\\s+/i").count():
                return True
            keywords = [
                "Submission of application",
                "Všetky aktuálne informácie",
                "Vyplňte nasledovné údaje",
                "Enter the name and permanent address",
            ]
            for keyword in keywords:
                if await page.locator(f"text={keyword}").count():
                    return True
            if await page.locator("input[name*='meno']").count():
                return True
            if await page.locator("input[name*='priez']").count():
                return True
        except Exception as exc:  # pragma: no cover - selector edge cases
            logger.debug("Identity wizard detection failed: %s", exc)
        return False

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

        await asyncio.to_thread(db.settings_set, "fake:auth_state", self._state)
        await asyncio.to_thread(db.settings_set, "fake:last_auth", self._last_update.isoformat())
        await asyncio.to_thread(db.settings_set, "fake:last_auth_reason", self._last_reason)


auth_manager = AuthManager()

__all__ = ["auth_manager", "AuthManager", "CAPTCHA_READY", "CAPTCHA_CANCEL", "CAPTCHA_MANUAL"]
