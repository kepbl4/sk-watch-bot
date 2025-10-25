"""Scheduler for periodic portal checks."""
from __future__ import annotations

import asyncio
import hashlib
import os
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from auth.flow import auth_manager
from storage import db
from utils.logging import logger


@dataclass(order=True)
class _Job:
    priority: int
    created_at: datetime = field(compare=False, default_factory=datetime.utcnow)
    kind: str = field(compare=False, default="full")
    category: Optional[str] = field(compare=False, default=None)
    reason: str = field(compare=False, default="manual")


def _slugify_city(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", name)
    ascii_name = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    ascii_name = ascii_name.replace("OCP", "")
    ascii_name = re.sub(r"[\s/]+", "_", ascii_name.strip().lower())
    ascii_name = ascii_name.replace("-", "_")
    return ascii_name


class WatcherScheduler:
    """Periodically checks portal categories and sends notifications."""

    def __init__(self) -> None:
        self._queue: asyncio.PriorityQueue[_Job] = asyncio.PriorityQueue()
        self._bot: Optional[Bot] = None
        self._interval = 10
        self._task: Optional[asyncio.Task] = None
        self._periodic_task: Optional[asyncio.Task] = None
        self._running = False
        self._lock = asyncio.Lock()
        self._owner_id = int(os.getenv("OWNER_ID", "0") or 0)
        self._heartbeat_path = Path(os.getenv("HEARTBEAT_PATH", "/opt/bot/run/heartbeat.ts"))

    async def start(self, bot: Bot, interval_minutes: int) -> None:
        if self._task and not self._task.done():
            return
        self._bot = bot
        self._interval = max(1, interval_minutes)
        self._running = True
        self._task = asyncio.create_task(self._runner())
        self._periodic_task = asyncio.create_task(self._periodic())
        await self.enqueue_full_check(priority=True, reason="startup")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
        if self._periodic_task:
            self._periodic_task.cancel()

    async def update_interval(self, interval_minutes: int) -> None:
        self._interval = max(1, interval_minutes)
        logger.info("Scheduler interval updated to %s minutes", self._interval)

    async def enqueue_full_check(self, *, priority: bool, reason: str) -> None:
        await self._queue.put(_Job(0 if priority else 1, kind="full", reason=reason))

    async def enqueue_category_check(self, cat_key: str, *, priority: bool, reason: str) -> None:
        await self._queue.put(
            _Job(0 if priority else 1, kind="category", category=cat_key, reason=reason)
        )

    async def _periodic(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(self._interval * 60)
                await self.enqueue_full_check(priority=False, reason="interval")
        except asyncio.CancelledError:  # pragma: no cover - lifecycle
            pass

    async def _runner(self) -> None:
        try:
            while self._running:
                job = await self._queue.get()
                async with self._lock:
                    if job.kind == "full":
                        await self._process_full_run(job)
                    elif job.kind == "category" and job.category:
                        await self._process_single_category(job.category, job.reason, record_run=True)
                        await self._send_notifications()
                        await self._refresh_views()
                self._queue.task_done()
        except asyncio.CancelledError:  # pragma: no cover - lifecycle
            pass

    async def _process_full_run(self, job: _Job) -> None:
        if not self._bot:
            return
        run_id = await asyncio.to_thread(db.create_run, None, "all")
        categories = await asyncio.to_thread(db.get_categories)
        enabled_categories = [cat for cat in categories if cat.get("enabled")]
        ok = 0
        errors = 0
        findings = 0
        if not enabled_categories:
            logger.info("No enabled categories for full run")
        for cat in enabled_categories:
            res, new_findings = await self._process_single_category(cat["key"], job.reason)
            if res == "OK":
                ok += 1
            else:
                errors += 1
            findings += new_findings
        notifications = await self._send_notifications()
        if notifications:
            logger.info("Sent %s notifications after full run", notifications)
        await asyncio.to_thread(db.finish_run, run_id, ok=ok, errors=errors, findings=findings)
        self._write_heartbeat()
        await self._refresh_views()

    async def _process_single_category(
        self,
        cat_key: str,
        reason: str,
        record_run: bool = False,
    ) -> Tuple[str, int]:
        if not self._bot:
            return "ERROR", 0
        logger.info("Checking category %s (%s)", cat_key, reason)
        state = await auth_manager.ensure_auth(self._bot)
        if state != "OK":
            await asyncio.to_thread(db.settings_set, "auth_state", state)
        run_id: Optional[int] = None
        if record_run:
            run_id = await asyncio.to_thread(db.create_run, None, f"category:{cat_key}")
        if state != "OK":
            await asyncio.to_thread(
                db.set_category_status,
                cat_key,
                state,
                last_error=f"auth-state={state}",
            )
            await asyncio.to_thread(db.reset_watches_for_category, cat_key, state, None)
            await self._finalise_run(run_id, state, 0)
            return state, 0

        context = await auth_manager.get_context()
        if not context:
            await self._finalise_run(run_id, "ERROR", 0)
            return "ERROR", 0

        watches = await asyncio.to_thread(db.get_watches_by_category, cat_key)
        city_map = {w["city_key"]: w for w in watches}
        slug_map = {w["city_key"]: _slugify_city(w["city_title"]) for w in watches}
        reverse_slug = {v: k for k, v in slug_map.items()}
        updates: Dict[str, Tuple[str, Optional[str]]] = {}
        now_iso = datetime.utcnow().isoformat()
        new_findings = 0

        category = await asyncio.to_thread(db.get_category, cat_key)
        url = (category or {}).get("url") or os.getenv("LOGIN_URL", "")
        page = await context.new_page()
        try:
            response = await page.goto(url, wait_until="networkidle", timeout=60000)
            http_status = response.status if response else None
            page_url = page.url
            await self._wait_for_schedule(page)
            entries = await self._parse_city_rows(page)
            raw_map: Dict[str, str] = {}
            for name, date_str, raw_text in entries:
                slug = _slugify_city(name)
                city_key = reverse_slug.get(slug)
                if not city_key:
                    logger.warning("Unknown city slug %s (%s)", slug, name)
                    continue
                updates[city_key] = ("OK" if date_str else "NO_DATE", date_str)
                raw_map[city_key] = raw_text

            for city_key, watch in city_map.items():
                status, date_str = updates.get(city_key, ("NO_DATE", None))
                last_seen_at = now_iso if date_str else None
                error_msg = None if status != "ERROR" else ""
                await asyncio.to_thread(
                    db.update_watch_result,
                    watch["id"],
                    status,
                    last_seen_value=date_str,
                    last_seen_at=last_seen_at,
                    error_msg=error_msg,
                )
                if status == "OK" and date_str and watch.get("last_seen_value") != date_str:
                    finding_id = await asyncio.to_thread(
                        db.record_finding, watch["id"], date_str, now_iso
                    )
                    if finding_id:
                        new_findings += 1
                        logger.info(
                            "New finding for %s/%s: %s",
                            cat_key,
                            city_key,
                            date_str,
                        )

            await asyncio.to_thread(db.set_category_status, cat_key, "OK", last_error=None)
            await self._record_diagnostics(
                cat_key,
                city_map,
                page_url,
                updates,
                raw_map,
                http_status,
                default_comment="",
            )
            await auth_manager.capture_page_screenshot(
                page,
                prefix=f"CategoryCheck_{cat_key}",
                description=f"Проверка категории {cat_key}",
            )
            await asyncio.to_thread(db.settings_set, "auth_state", "OK")
            await self._finalise_run(run_id, "OK", new_findings)
            return "OK", new_findings
        except PlaywrightTimeoutError:
            logger.warning("Timeout while checking %s", cat_key)
            await asyncio.to_thread(
                db.set_category_status,
                cat_key,
                "ERROR",
                last_error="timeout",
            )
            await asyncio.to_thread(db.reset_watches_for_category, cat_key, "ERROR", "timeout")
            await self._record_diagnostics(
                cat_key,
                city_map,
                url,
                {key: ("ERROR", None) for key in city_map},
                {},
                None,
                default_comment="timeout",
            )
            await self._finalise_run(run_id, "ERROR", new_findings)
            return "ERROR", new_findings
        except Exception as exc:  # pragma: no cover - network issues
            logger.exception("Failed to parse category %s: %s", cat_key, exc)
            await asyncio.to_thread(
                db.set_category_status,
                cat_key,
                "ERROR",
                last_error=str(exc),
            )
            await asyncio.to_thread(db.reset_watches_for_category, cat_key, "ERROR", str(exc))
            await self._record_diagnostics(
                cat_key,
                city_map,
                url,
                {key: ("ERROR", None) for key in city_map},
                {},
                None,
                default_comment=str(exc),
            )
            await self._finalise_run(run_id, "ERROR", new_findings)
            return "ERROR", new_findings
        finally:
            await page.close()

    async def _finalise_run(self, run_id: Optional[int], status: str, findings: int) -> None:
        if run_id is None:
            return
        ok = 1 if status == "OK" else 0
        errors = 0 if status == "OK" else 1
        await asyncio.to_thread(db.finish_run, run_id, ok=ok, errors=errors, findings=findings)

    async def _wait_for_schedule(self, page: Page) -> None:
        await page.wait_for_selector("text=Pracoviská", timeout=30000)

    async def _parse_city_rows(self, page: Page) -> List[Tuple[str, Optional[str], str]]:
        results: List[Tuple[str, Optional[str], str]] = []
        labels = await page.locator("label").all_text_contents()
        pattern = re.compile(r"^OCP\s+(.+?)(?:\s*[–-]\s*(\d{1,2}\.\d{1,2}\.\d{4}))?$")
        for raw in labels:
            text = raw.strip()
            match = pattern.match(text)
            if not match:
                continue
            city_name = match.group(1).strip()
            date_str = match.group(2)
            if date_str:
                day, month, year = date_str.split(".")
                iso_value = f"{year}-{int(month):02d}-{int(day):02d}"
            else:
                iso_value = None
            results.append((city_name, iso_value, text))
        return results

    async def _record_diagnostics(
        self,
        cat_key: str,
        city_map: Dict[str, Dict[str, str]],
        page_url: str,
        updates: Dict[str, Tuple[str, Optional[str]]],
        raw_map: Dict[str, str],
        http_status: Optional[int],
        *,
        default_comment: str,
    ) -> None:
        now_iso = datetime.utcnow().isoformat()
        for city_key, watch in city_map.items():
            status, date_str = updates.get(city_key, ("NO_DATE", None))
            anchor_text = raw_map.get(city_key, "")
            anchor_hash = hashlib.sha1(anchor_text.encode("utf-8")).hexdigest()
            comment = date_str or default_comment or status
            previous = await asyncio.to_thread(db.get_last_diagnostic, cat_key, city_key)
            prev_len = int(previous.get("content_len") or 0) if previous else 0
            diff_len = len(anchor_text) - prev_len
            if not previous:
                diff_anchor = "new"
            else:
                prev_hash = previous.get("anchor_hash") or ""
                diff_anchor = "changed" if prev_hash != anchor_hash else "same"
            await asyncio.to_thread(
                db.record_diagnostic,
                recorded_at=now_iso,
                category_code=cat_key,
                city_key=city_key,
                url=page_url,
                status=status,
                http_status=http_status,
                content_len=len(anchor_text),
                anchor_hash=anchor_hash,
                diff_len=diff_len,
                diff_anchor=diff_anchor,
                comment=comment,
            )

    async def _send_notifications(self) -> int:
        pending = await asyncio.to_thread(db.get_pending_findings)
        if not pending or not self._bot or not self._owner_id:
            return 0
        sent = 0
        for item in pending:
            category = await asyncio.to_thread(db.get_category, item["category_key"])
            url = (category or {}).get("url") or os.getenv("LOGIN_URL", "")
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Открыть страницу", url=url)],
                    [
                        InlineKeyboardButton(
                            text="Проверить категорию",
                            callback_data=f"cat:check:{item['category_key']}",
                        ),
                        InlineKeyboardButton(
                            text=f"Пауза {item['city_title']}",
                            callback_data=f"city:toggle:{item['category_key']}:{item['city_key']}",
                        ),
                    ],
                ]
            )
            display_date = item["found_value"]
            if display_date and re.match(r"\d{4}-\d{2}-\d{2}", display_date):
                year, month, day = display_date.split("-")
                display_date = f"{day}.{month}.{year}"
            text = (
                f"<b>{item['category_title']} — {item['city_title']}</b>\n"
                f"Найдена дата: <b>{display_date}</b>"
            )
            await self._bot.send_message(
                self._owner_id,
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
            await asyncio.to_thread(db.mark_finding_notified, item["id"])
            sent += 1
        return sent

    async def _refresh_views(self) -> None:
        if not self._bot:
            return
        from bot import menu

        await menu.refresh_summary(self._bot)

    def _write_heartbeat(self) -> None:
        try:
            self._heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
            self._heartbeat_path.write_text(str(int(datetime.utcnow().timestamp())), encoding="utf-8")
        except Exception as exc:  # pragma: no cover - filesystem issues
            logger.warning("Failed to write heartbeat: %s", exc)


scheduler = WatcherScheduler()

__all__ = ["scheduler"]
