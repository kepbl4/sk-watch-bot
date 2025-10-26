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
            await auth_manager.handle_portal_interstitial(page)
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
        await auth_manager.handle_portal_interstitial(page)
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

__all__ = ["scheduler", "WatcherScheduler"]
