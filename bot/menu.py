"""User-facing inline menu for the SK Watch Bot."""
from __future__ import annotations

import asyncio
import html
import os
import re
import time
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from aiogram import F, Router
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from auth.flow import CAPTCHA_CANCEL, CAPTCHA_MANUAL, CAPTCHA_READY, auth_manager
from storage import db
from utils.logging import logger
from watcher.scheduler import scheduler

router = Router(name="menu")

SUMMARY_ANCHOR = "summary"
CATEGORIES_ANCHOR = "categories"
TRACKED_ANCHOR = "tracked"
ADMIN_ANCHOR = "admin"
DIAGNOSTIC_ANCHOR = "diagnostics"

STATUS_ICONS = {
    None: "⏸",
    "": "⏸",
    "PAUSED": "⏸",
    "IDLE": "⏸",
    "OK": "✅",
    "NO_DATE": "⭕",
    "ERROR": "⚠️",
    "NEED_AUTH": "🔒",
    "NEED_VPN": "🌐",
    "SLOW": "🟡",
}

AUTH_STATUS_ICONS = {
    "OK": "✅",
    "NEED_AUTH": "🔒",
    "NEED_VPN": "🌐",
    "NEED_CAPTCHA": "🧩",
    "NEED_SMS": "🔒",
    "ERROR": "⚠️",
    "WARN": "⚠️",
}

INTERVAL_MINUTES = 10
OWNER_ID: Optional[int] = None
PENDING_URL_UPDATES: Dict[int, str] = {}
PENDING_SETTING_UPDATES: Dict[int, str] = {}

CONNECTIVITY_TTL = 120


def configure(interval: int, owner_id: Optional[int]) -> None:
    """Configure runtime options for the menu module."""

    global INTERVAL_MINUTES, OWNER_ID
    INTERVAL_MINUTES = max(1, interval)
    OWNER_ID = owner_id


async def run_in_thread(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


async def _read_connectivity_snapshot() -> Dict[str, Any]:
    keys = [
        "vpn_state",
        "vpn_country_code",
        "vpn_ip",
        "vpn_latency_ms",
        "vpn_error",
        "portal_state",
        "portal_code",
        "portal_latency_ms",
        "portal_error",
        "connectivity_checked_at",
        "vpn_status",
        "portal_status",
    ]
    result: Dict[str, Any] = {}
    for key in keys:
        result[key] = await run_in_thread(db.settings_get, key, "")
    return result


async def ensure_connectivity_status(force: bool = False) -> Dict[str, Any]:
    snapshot = await _read_connectivity_snapshot()
    last_checked = snapshot.get("connectivity_checked_at")
    if not force and last_checked:
        try:
            last_dt = datetime.fromisoformat(last_checked)
            if datetime.utcnow() - last_dt < timedelta(seconds=CONNECTIVITY_TTL):
                return snapshot
        except ValueError:
            pass

    vpn_state = "ERR"
    vpn_country = ""
    vpn_ip = ""
    vpn_latency = ""
    vpn_error = ""
    portal_state = "ERR"
    portal_code = ""
    portal_latency = ""
    portal_error = ""
    portal_status = "ERR ⚠️"

    timeout = aiohttp.ClientTimeout(total=10)
    headers = {"User-Agent": "sk-watch-bot/1.0", "Accept": "application/json"}
    login_url = os.getenv("LOGIN_URL", "")
    ignore_https = os.getenv("IGNORE_HTTPS_ERRORS", "false").lower() == "true"
    connector = aiohttp.TCPConnector(ssl=False) if ignore_https else None
    latency_threshold = int(os.getenv("PORTAL_SLOW_THRESHOLD_MS", "4000") or 4000)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        # VPN / geo check
        try:
            start = time.monotonic()
            async with session.get("https://ifconfig.co/json", headers=headers) as resp:
                elapsed = int((time.monotonic() - start) * 1000)
                vpn_latency = str(elapsed)
                if resp.status == 200:
                    data = await resp.json()
                    vpn_country = (data.get("country_iso") or data.get("country_iso_code") or "").upper()
                    vpn_ip = data.get("ip") or ""
                    vpn_state = "OK" if vpn_country == "SK" else "NEED_VPN"
                else:
                    vpn_state = "ERR"
                    vpn_error = f"HTTP {resp.status}"
        except Exception as exc:  # pragma: no cover - network issues
            vpn_state = "ERR"
            vpn_error = str(exc)

        # Portal availability
        if login_url:
            try:
                start = time.monotonic()
                status_code = None
                elapsed = 0
                try:
                    async with session.head(login_url, allow_redirects=False) as resp:
                        status_code = resp.status
                        elapsed = int((time.monotonic() - start) * 1000)
                except Exception:
                    status_code = None

                if status_code == 405:
                    logger.debug("Portal HEAD returned 405, retrying with GET")
                if status_code == 405 or status_code is None:
                    start = time.monotonic()
                    async with session.get(login_url, allow_redirects=False) as resp:
                        status_code = resp.status
                        await resp.read()
                        elapsed = int((time.monotonic() - start) * 1000)
                portal_latency = str(elapsed)
                portal_code = str(status_code)
                method_note = None
                if status_code == 405:
                    portal_state = "OK"
                    method_note = "method not allowed"
                elif status_code in {200, 301, 302}:
                if status_code in {200, 301, 302}:
                    portal_state = "OK"
                    if elapsed > latency_threshold:
                        portal_state = "SLOW"
                        portal_error = f"latency {elapsed} ms"
                else:
                    portal_state = "ERR"
                    portal_error = f"HTTP {status_code}"
                await asyncio.to_thread(
                    db.record_portal_pulse,
                    recorded_at=datetime.utcnow().isoformat(),
                    status=portal_state,
                    latency_ms=elapsed,
                    http_status=status_code,
                    error=portal_error or method_note,
                    error=portal_error,
                )
                if portal_state == "ERR":
                    logger.warning("Portal sensor error: %s", portal_error)
                    await auth_manager.capture_portal_error(
                        login_url, description=portal_error or "portal error"
                )
                if method_note and not portal_error:
                    portal_error = method_note
                    )
            except Exception as exc:  # pragma: no cover - network issues
                portal_state = "ERR"
                portal_error = str(exc)
                await asyncio.to_thread(
                    db.record_portal_pulse,
                    recorded_at=datetime.utcnow().isoformat(),
                    status=portal_state,
                    latency_ms=None,
                    http_status=None,
                    error=portal_error,
                )
                await auth_manager.capture_portal_error(login_url or "about:blank", description=portal_error)
        else:
            portal_state = "ERR"
            portal_error = "LOGIN_URL not configured"
            await asyncio.to_thread(
                db.record_portal_pulse,
                recorded_at=datetime.utcnow().isoformat(),
                status=portal_state,
                latency_ms=None,
                http_status=None,
                error=portal_error,
            )

    display_vpn = "ERR"
    if vpn_state == "OK":
        display_vpn = f"{vpn_country or 'SK'} ✅"
    elif vpn_state == "NEED_VPN":
        display_vpn = f"{vpn_country or '??'} ❌"
    else:
        display_vpn = "ERR"

    if portal_state == "OK":
        portal_status = "OK ✅"
    elif portal_state == "SLOW":
        portal_status = "SLOW 🟡"
    else:
        portal_status = "ERR ⚠️"

    now_iso = datetime.utcnow().isoformat()
    await run_in_thread(db.settings_set, "vpn_state", vpn_state)
    await run_in_thread(db.settings_set, "vpn_country_code", vpn_country)
    await run_in_thread(db.settings_set, "vpn_ip", vpn_ip)
    await run_in_thread(db.settings_set, "vpn_latency_ms", vpn_latency)
    await run_in_thread(db.settings_set, "vpn_error", vpn_error)
    await run_in_thread(db.settings_set, "portal_state", portal_state)
    await run_in_thread(db.settings_set, "portal_code", portal_code)
    await run_in_thread(db.settings_set, "portal_latency_ms", portal_latency)
    await run_in_thread(db.settings_set, "portal_error", portal_error)
    await run_in_thread(db.settings_set, "vpn_status", display_vpn)
    await run_in_thread(db.settings_set, "portal_status", portal_status)
    await run_in_thread(db.settings_set, "connectivity_checked_at", now_iso)

    snapshot.update(
        {
            "vpn_state": vpn_state,
            "vpn_country_code": vpn_country,
            "vpn_ip": vpn_ip,
            "vpn_latency_ms": vpn_latency,
            "vpn_error": vpn_error,
            "portal_state": portal_state,
            "portal_code": portal_code,
            "portal_latency_ms": portal_latency,
            "portal_error": portal_error,
            "vpn_status": display_vpn,
            "portal_status": portal_status,
            "connectivity_checked_at": now_iso,
        }
    )
    return snapshot


def _format_datetime(value: Optional[str], fmt: str) -> str:
    if not value:
        return "—"
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return value
    return dt.strftime(fmt)


def _status_icon(status: Optional[str]) -> str:
    return STATUS_ICONS.get(status, "⏸")


def _format_date_value(value: Optional[str]) -> str:
    if not value:
        return "—"
    if re.match(r"\d{4}-\d{2}-\d{2}", value):
        year, month, day = value.split("-")
        return f"{day}.{month}.{year}"
    return value


async def _recent_events() -> List[str]:
    findings = await run_in_thread(db.get_recent_findings, 5)
    lines = []
    for item in findings:
        timestamp = _format_datetime(item.get("found_at"), "%d.%m.%Y %H:%M")
        value = _format_date_value(item.get("found_value"))
        lines.append(
            f"{timestamp} — {item['category_title']} / {item['city_title']} • {value}"
        )
    return lines or ["—"]


async def _pending_findings_count() -> Counter:
    pending = await run_in_thread(db.get_pending_findings)
    counter: Counter = Counter()
    for item in pending:
        counter[item["category_key"]] += 1
    return counter


async def build_summary_text(force_status: bool = False) -> Tuple[str, bool]:
    snapshot = await ensure_connectivity_status(force=force_status)
    categories = await run_in_thread(db.get_categories)
    pending_per_category = await _pending_findings_count()
    lines: List[str] = []
    total_active = 0
    for cat in categories:
        watches = await run_in_thread(db.get_watches_by_category, cat["key"])
        active = sum(1 for w in watches if w["enabled"] and cat["enabled"])
        total = len(watches)
        total_active += active if cat["enabled"] else 0
        icon = _status_icon(cat.get("status"))
        new_count = pending_per_category.get(cat["key"], 0)
        lines.append(
            f"{icon} {cat['title']} — вкл {active}/{total} • новые: {new_count}"
        )

    vpn_status = snapshot.get("vpn_status") or "ERR"
    portal_status = snapshot.get("portal_status") or "ERR"
    system_state = await run_in_thread(db.settings_get, "auth_system_state", "OK")
    system_hint = await run_in_thread(db.settings_get, "auth_system_hint", "")
    sms_pending = await run_in_thread(db.settings_get, "auth_sms_pending", "0")

    auth_state = await run_in_thread(db.settings_get, "auth_state", "NEED_AUTH")
    auth_until = await run_in_thread(db.settings_get, "auth_exp", "")
    display_state = auth_state
    if system_state == "WARN" and auth_state in {"OK", ""}:
        display_state = "WARN"

    if display_state == "OK" and auth_until:
        auth_label = f"OK до {_format_datetime(auth_until, '%H:%M')}"
    elif display_state == "WARN" and auth_until:
        auth_label = f"WARN до {_format_datetime(auth_until, '%H:%M')}"
    else:
        auth_label = display_state

    icon = AUTH_STATUS_ICONS.get(display_state)
    if icon and not auth_label.startswith(icon):
        auth_label = f"{icon} {auth_label}"

    summary_lines = [
        "<b>Сводка</b>",
        "",
        f"VPN: {vpn_status}",
        f"Портал: {portal_status}",
        f"Авторизация: {auth_label}",
    ]
    if system_state == "WARN" and system_hint:
        summary_lines.append(f"⚠️ Система: {system_hint}")
    summary_lines.append("")
    if sms_pending == "1":
        summary_lines.append("SMS-код: ждём ввод в ответ на сообщение")
    summary_lines.extend(
        [
            f"Интервал: {INTERVAL_MINUTES} минут",
            "",
            "Категории:",
        ]
    )
    summary_lines.extend(lines or ["—"])
    summary_lines.extend(["", "Последние события:"])
    summary_lines.extend(await _recent_events())
    summary_lines.extend(["", f"Активных целей: {total_active}"])
    return "\n".join(summary_lines), sms_pending == "1"


def summary_keyboard(*, sms_pending: bool) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(
                text="Проверить всё сейчас", callback_data="summary:check_all"
            ),
            InlineKeyboardButton(text="Обновить", callback_data="summary:refresh"),
        ],
        [
            InlineKeyboardButton(text="Категории", callback_data="summary:categories"),
            InlineKeyboardButton(text="Диагностика", callback_data="summary:diagnostics"),
        ],
        [
            InlineKeyboardButton(text="Отслеживаемое", callback_data="summary:tracked"),
            InlineKeyboardButton(text="Панель", callback_data="summary:admin"),
        ],
    ]
    if sms_pending:
        keyboard.append(
            [
                InlineKeyboardButton(
                    text="Отправь код в ответ", callback_data="auth:sms_help"
                )
            ]
        )
    keyboard.append(
        [InlineKeyboardButton(text="Состояние VPN", callback_data="summary:vpn")]
    )
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


async def ensure_summary_message(message: Message, *, force_status: bool = False) -> None:
    text, sms_pending = await build_summary_text(force_status=force_status)
    keyboard = summary_keyboard(sms_pending=sms_pending)
    anchor = await run_in_thread(db.get_anchor, SUMMARY_ANCHOR)
    bot = message.bot

    if anchor and anchor.get("chat_id") == message.chat.id:
        try:
            await bot.edit_message_text(
                text=text,
                chat_id=anchor["chat_id"],
                message_id=anchor["message_id"],
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
            )
            return
        except TelegramBadRequest as exc:
            logger.warning("Failed to edit summary message: %s", exc)

    sent = await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
    await run_in_thread(db.save_anchor, SUMMARY_ANCHOR, message.chat.id, sent.message_id)
    await run_in_thread(db.save_anchor, CATEGORIES_ANCHOR, message.chat.id, sent.message_id)
    await run_in_thread(db.save_anchor, TRACKED_ANCHOR, message.chat.id, sent.message_id)
    await run_in_thread(db.save_anchor, ADMIN_ANCHOR, message.chat.id, sent.message_id)
    await run_in_thread(db.save_anchor, DIAGNOSTIC_ANCHOR, message.chat.id, sent.message_id)


async def edit_summary_message(bot, chat_id: int, message_id: int, *, force_status: bool = False) -> None:
    text, sms_pending = await build_summary_text(force_status=force_status)
    try:
        await bot.edit_message_text(
            text=text,
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=summary_keyboard(sms_pending=sms_pending),
            parse_mode=ParseMode.HTML,
        )
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return
        logger.warning("Unable to edit summary message: %s", exc)
        target_chat = fallback_chat_id or chat_id
        sent = await bot.send_message(
            target_chat,
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )
        await _save_anchor_bundle(sent.chat.id, sent.message_id)


async def ensure_summary_message(message: Message, *, force_status: bool = False) -> None:
    bot = message.bot
    anchor = await run_in_thread(db.get_anchor, SUMMARY_ANCHOR)

    if not anchor or anchor.get("chat_id") != message.chat.id:
        placeholder = await message.answer("Готовлю сводку…")
        await _save_anchor_bundle(placeholder.chat.id, placeholder.message_id)
        anchor = {"chat_id": placeholder.chat.id, "message_id": placeholder.message_id}

    await _render_summary(
        bot,
        anchor["chat_id"],
        anchor["message_id"],
        force_status=force_status,
        fallback_chat_id=message.chat.id,
    )


async def refresh_summary(bot, *, force_status: bool = False) -> None:
    anchor = await run_in_thread(db.get_anchor, SUMMARY_ANCHOR)
    if not anchor:
        return
    await _render_summary(
        bot,
        anchor["chat_id"],
        anchor["message_id"],
        force_status=force_status,
    )


async def build_categories_view() -> Tuple[str, InlineKeyboardMarkup]:
    categories = await run_in_thread(db.get_categories)
    pending_per_category = await _pending_findings_count()
    lines = ["<b>Категории</b>", "Переключайте категории и переходите к городам.", ""]
    keyboard_rows: List[List[InlineKeyboardButton]] = []
    for cat in categories:
        watches = await run_in_thread(db.get_watches_by_category, cat["key"])
        active = sum(1 for w in watches if w["enabled"] and cat["enabled"])
        total = len(watches)
        icon = "✅" if cat["enabled"] else "⏸"
        new_count = pending_per_category.get(cat["key"], 0)
        lines.append(
            f"{_status_icon(cat['status'])} {cat['title']} — активных {active}/{total} • новые: {new_count}"
        )
        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    text=f"{icon} {cat['title']}",
                    callback_data=f"cat:toggle:{cat['key']}",
                ),
                InlineKeyboardButton(
                    text="Города", callback_data=f"cat:cities:{cat['key']}"
                ),
            ]
        )
    keyboard_rows.append([InlineKeyboardButton(text="Отслеживаемое", callback_data="summary:tracked")])
    keyboard_rows.append([InlineKeyboardButton(text="Панель", callback_data="summary:admin")])
    keyboard_rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="summary:back")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


async def build_cities_view(cat_key: str) -> Tuple[str, InlineKeyboardMarkup]:
    watches = await run_in_thread(db.get_watches_by_category, cat_key)
    if not watches:
        return "Категория не найдена", InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="summary:categories")]]
        )
    category_title = watches[0]["category_title"]
    category_enabled = bool(watches[0]["category_enabled"])
    lines = [f"<b>{category_title}</b>", "Настройте города категории.", ""]
    keyboard_rows: List[List[InlineKeyboardButton]] = []
    for watch in watches:
        icon = _status_icon(watch.get("status"))
        if not watch["enabled"] or not category_enabled:
            icon = "⏸"
        last_date = _format_date_value(watch.get("last_seen_value"))
        lines.append(f"{icon} {watch['city_title']} — последняя дата: {last_date}")
        toggle_text = "Вкл" if not watch["enabled"] else "Выкл"
        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    text=f"{icon} {watch['city_title']}",
                    callback_data="noop",
                ),
                InlineKeyboardButton(
                    text=toggle_text,
                    callback_data=f"city:toggle:{cat_key}:{watch['city_key']}",
                ),
            ]
        )
    category_toggle_text = "Выкл категорию" if category_enabled else "Вкл категорию"
    keyboard_rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="Проверить категорию", callback_data=f"cat:check:{cat_key}"
                )
            ],
            [
                InlineKeyboardButton(
                    text=category_toggle_text, callback_data=f"cat:toggle:{cat_key}"
                )
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="summary:categories")],
        ]
    )
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


async def build_tracked_view() -> Tuple[str, InlineKeyboardMarkup]:
    watches = await run_in_thread(db.list_tracked_watches)
    categories = await run_in_thread(db.get_categories)
    category_enabled_map = {cat["id"]: cat["enabled"] for cat in categories}
    rows: List[str] = []
    enabled_targets = 0
    error_targets = 0
    last_checks: List[str] = []
    for watch in watches:
        category_enabled = category_enabled_map.get(watch["category_id"], 0)
        if not category_enabled:
            continue
        if watch.get("last_check_at"):
            last_checks.append(watch["last_check_at"])
        if watch.get("status") == "ERROR":
            error_targets += 1
        if watch["enabled"] and category_enabled:
            enabled_targets += 1
            icon = _status_icon(watch.get("status"))
            last_date = _format_date_value(watch.get("last_seen_value"))
            rows.append(
                f"{icon} {watch['category_title']} — {watch['city_title']} • дата: {last_date}"
            )

    total_targets = len([w for w in watches if category_enabled_map.get(w["category_id"], 0)])
    rows = rows or ["—"]
    last_check = None
    if last_checks:
        last_check = max(last_checks)
    header = (
        "<b>Отслеживаемое</b>\n"
        f"Целей всего {total_targets} • Включено {enabled_targets} • Ошибок {error_targets} • "
        f"Последняя проверка {_format_datetime(last_check, '%H:%M')}"
    )
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Проверить всё сейчас", callback_data="summary:check_all"
                ),
                InlineKeyboardButton(text="В категории", callback_data="summary:categories"),
            ],
            [
                InlineKeyboardButton(text="Пауза все", callback_data="tracked:pause_all"),
                InlineKeyboardButton(text="Возобновить все", callback_data="tracked:resume_all"),
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="summary:back")],
        ]
    )
    text = "\n".join([header, "", *rows])
    return text, keyboard


async def build_diagnostics_view() -> Tuple[str, InlineKeyboardMarkup]:
    records = await run_in_thread(db.get_latest_diagnostics, 60)
    lines = ["<b>Диагностика</b>", "Последние проверки по целям:", ""]
    if not records:
        lines.append("—")
    else:
        for item in records:
            recorded = _format_datetime(item.get("recorded_at"), "%d.%m %H:%M")
            http_code = item.get("http_status") or "—"
            length = item.get("content_len") or 0
            diff_len = int(item.get("diff_len") or 0)
            if diff_len > 0:
                trend = "↑"
            elif diff_len < 0:
                trend = "↓"
            else:
                trend = "≡"
            anchor_state = (item.get("diff_anchor") or "").lower()
            if anchor_state == "changed":
                anchor_flag = "⚠️"
            elif anchor_state == "new":
                anchor_flag = "🆕"
            else:
                anchor_flag = ""
            comment = item.get("comment") or item.get("status") or ""
            lines.append(
                f"{recorded} • {item.get('category_code')}/{item.get('city_key')} • HTTP {http_code} • len {length} {trend} {anchor_flag}".strip()
            )
            if comment:
                lines.append(f"↳ {comment}")
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Проверить всё сейчас", callback_data="summary:check_all")],
            [InlineKeyboardButton(text="Обновить", callback_data="diagnostics:refresh")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="summary:back")],
        ]
    )
    return "\n".join(lines), keyboard


async def build_admin_view() -> Tuple[str, InlineKeyboardMarkup]:
    categories = await run_in_thread(db.get_categories)
    interval = await run_in_thread(db.settings_get, "CHECK_INTERVAL_MIN", str(INTERVAL_MINUTES))
    notify_lang = await run_in_thread(db.settings_get, "notify_lang", "ru")
    portal_pulses = await run_in_thread(db.get_recent_portal_pulses, 5)
    screenshots = await run_in_thread(db.get_recent_screenshots, 5)
    lines = ["<b>Панель управления</b>", "URL категорий:"]
    for cat in categories:
        url = cat.get("url") or "—"
        lines.append(f"{cat['title']}: {url}")
    lines.extend(
        [
            "",
            "Настройки:",
            f"Интервал проверок: {interval} мин",
            f"Язык уведомлений: {notify_lang}",
            "Автоперезапуск в 05:00: включен",
        ]
    )
    lines.append("")
    lines.append("Датчик портала:")
    if portal_pulses:
        for pulse in portal_pulses:
            checked = _format_datetime(pulse.get("recorded_at"), "%d.%m %H:%M:%S")
            state = pulse.get("status")
            latency = pulse.get("latency_ms") or "—"
            code = pulse.get("http_status") or "—"
            error = pulse.get("error") or ""
            lines.append(f"{checked} • {state} • {latency} мс • HTTP {code} {error}")
    else:
        lines.append("—")
    lines.append("")
    lines.append("Последние скрины:")
    if screenshots:
        for shot in screenshots:
            created = _format_datetime(shot.get("created_at"), "%d.%m %H:%M:%S")
            lines.append(f"{created} — {shot.get('name')}")
    else:
        lines.append("—")

    keyboard_rows = [
        [
            InlineKeyboardButton(text="Изменить интервал", callback_data="admin:interval"),
            InlineKeyboardButton(text="Язык уведомлений", callback_data="admin:lang"),
        ]
    ]
    keyboard_rows.append(
        [InlineKeyboardButton(text="Обновить авторизацию", callback_data="admin:auth")]
    )
    for cat in categories:
        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    text=f"Сохранить {cat['title']}", callback_data=f"admin:save:{cat['key']}"
                ),
                InlineKeyboardButton(
                    text=f"Проверить {cat['title']}", callback_data=f"cat:check:{cat['key']}"
                ),
                InlineKeyboardButton(
                    text=f"Скрин {cat['title']}", callback_data=f"admin:screenshot:{cat['key']}"
                ),
            ]
        )
    keyboard_rows.append(
        [
            InlineKeyboardButton(text="Логи (50)", callback_data="admin:logs:50"),
            InlineKeyboardButton(text="Логи (100)", callback_data="admin:logs:100"),
        ]
    )
    if screenshots:
        for shot in screenshots:
            created = _format_datetime(shot.get("created_at"), "%d.%m %H:%M:%S")
            keyboard_rows.append(
                [
                    InlineKeyboardButton(
                        text=f"{created} • {shot.get('name')}",
                        callback_data=f"admin:screen:{shot.get('name')}",
                    )
                ]
            )
        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    text="Последние скрины",
                    callback_data="admin:screenshots",
                )
            ]
        )
    keyboard_rows.append(
        [InlineKeyboardButton(text="Отчёт об ошибке", callback_data="admin:failure_report")]
    )
    if screenshots:
        for shot in screenshots:
            created = _format_datetime(shot.get("created_at"), "%d.%m %H:%M:%S")
            keyboard_rows.append(
                [
                    InlineKeyboardButton(
                        text=f"{created} • {shot.get('name')}",
                        callback_data=f"admin:screen:{shot.get('name')}",
                    )
                ]
            )
    keyboard_rows.append(
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data="summary:back"),
        ]
    )
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


async def _edit_message(callback: CallbackQuery, text: str, keyboard: InlineKeyboardMarkup) -> None:
    try:
        await callback.message.edit_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
        )
    except TelegramBadRequest as exc:
        logger.warning("Failed to edit message: %s", exc)


@router.message(CommandStart())
async def handle_start(message: Message) -> None:
    await ensure_summary_message(message)


@router.callback_query(F.data == "summary:refresh")
async def handle_summary_refresh(callback: CallbackQuery) -> None:
    await callback.answer("Обновляю сводку…", show_alert=False)
    await ensure_summary_message(callback.message, force_status=True)


@router.callback_query(F.data == "summary:categories")
async def handle_show_categories(callback: CallbackQuery) -> None:
    text, keyboard = await build_categories_view()
    await _edit_message(callback, text, keyboard)
    await run_in_thread(db.save_anchor, CATEGORIES_ANCHOR, callback.message.chat.id, callback.message.message_id)
    await callback.answer()


@router.callback_query(F.data == "summary:tracked")
async def handle_show_tracked(callback: CallbackQuery) -> None:
    text, keyboard = await build_tracked_view()
    await _edit_message(callback, text, keyboard)
    await run_in_thread(db.save_anchor, TRACKED_ANCHOR, callback.message.chat.id, callback.message.message_id)
    await callback.answer()


@router.callback_query(F.data == "summary:admin")
async def handle_show_admin(callback: CallbackQuery) -> None:
    text, keyboard = await build_admin_view()
    await _edit_message(callback, text, keyboard)
    await run_in_thread(db.save_anchor, ADMIN_ANCHOR, callback.message.chat.id, callback.message.message_id)
    await callback.answer()


@router.callback_query(F.data == "summary:diagnostics")
async def handle_show_diagnostics(callback: CallbackQuery) -> None:
    text, keyboard = await build_diagnostics_view()
    await _edit_message(callback, text, keyboard)
    await run_in_thread(db.save_anchor, DIAGNOSTIC_ANCHOR, callback.message.chat.id, callback.message.message_id)
    await callback.answer()


@router.callback_query(F.data == "diagnostics:refresh")
async def handle_diagnostics_refresh(callback: CallbackQuery) -> None:
    text, keyboard = await build_diagnostics_view()
    await _edit_message(callback, text, keyboard)
    await callback.answer("Обновлено")


@router.callback_query(F.data == "summary:back")
async def handle_back(callback: CallbackQuery) -> None:
    await ensure_summary_message(callback.message)
    await callback.answer()


@router.callback_query(F.data.startswith("cat:toggle:"))
async def handle_toggle_category(callback: CallbackQuery) -> None:
    _, _, cat_key = callback.data.partition("cat:toggle:")
    category = await run_in_thread(db.get_category, cat_key)
    if not category:
        await callback.answer("Категория не найдена", show_alert=True)
        return
    new_state = not bool(category["enabled"])
    await run_in_thread(db.set_category_enabled, cat_key, new_state)
    text, keyboard = await build_categories_view()
    await _edit_message(callback, text, keyboard)
    await refresh_summary(callback.message.bot)
    await callback.answer("Категория обновлена")


@router.callback_query(F.data.startswith("cat:cities:"))
async def handle_show_cities(callback: CallbackQuery) -> None:
    _, _, cat_key = callback.data.partition("cat:cities:")
    text, keyboard = await build_cities_view(cat_key)
    await _edit_message(callback, text, keyboard)
    await callback.answer()


@router.callback_query(F.data.startswith("city:toggle:"))
async def handle_toggle_city(callback: CallbackQuery) -> None:
    _, _, rest = callback.data.partition("city:toggle:")
    cat_key, _, city_key = rest.partition(":")
    watch = await run_in_thread(db.get_watch, cat_key, city_key)
    if not watch:
        await callback.answer("Город не найден", show_alert=True)
        return
    new_state = not bool(watch["enabled"])
    await run_in_thread(db.enable_watch, cat_key, city_key, new_state)
    text, keyboard = await build_cities_view(cat_key)
    await _edit_message(callback, text, keyboard)
    await refresh_summary(callback.message.bot)
    await callback.answer("Настройки обновлены")


@router.callback_query(F.data == "summary:check_all")
async def handle_check_all(callback: CallbackQuery) -> None:
    await scheduler.enqueue_full_check(priority=True, reason="manual")
    await callback.answer("Проверка добавлена в очередь", show_alert=False)
    await asyncio.to_thread(db.record_pulse, "summary_check", "queued", "manual")


@router.callback_query(F.data.startswith("cat:check:"))
async def handle_category_check(callback: CallbackQuery) -> None:
    _, _, cat_key = callback.data.partition("cat:check:")
    await scheduler.enqueue_category_check(cat_key, priority=True, reason="manual")
    await callback.answer("Категория поставлена на проверку")
    await asyncio.to_thread(db.record_pulse, "category_check", "queued", cat_key)


@router.callback_query(F.data == "tracked:pause_all")
async def handle_pause_all(callback: CallbackQuery) -> None:
    categories = await run_in_thread(db.get_categories)
    for cat in categories:
        await run_in_thread(db.enable_all_watches, cat["key"], False)
    await refresh_summary(callback.message.bot)
    text, keyboard = await build_tracked_view()
    await _edit_message(callback, text, keyboard)
    await callback.answer("Все цели поставлены на паузу")


@router.callback_query(F.data == "tracked:resume_all")
async def handle_resume_all(callback: CallbackQuery) -> None:
    categories = await run_in_thread(db.get_categories)
    for cat in categories:
        await run_in_thread(db.enable_all_watches, cat["key"], True)
    await refresh_summary(callback.message.bot)
    text, keyboard = await build_tracked_view()
    await _edit_message(callback, text, keyboard)
    await callback.answer("Все цели возобновлены")


@router.callback_query(F.data == "summary:vpn")
async def handle_vpn_status(callback: CallbackQuery) -> None:
    snapshot = await ensure_connectivity_status(force=True)
    vpn_line = snapshot.get("vpn_status") or "ERR"
    ip = snapshot.get("vpn_ip") or "—"
    country = snapshot.get("vpn_country_code") or "??"
    latency = snapshot.get("vpn_latency_ms") or "—"
    portal = snapshot.get("portal_status") or "ERR"
    portal_latency = snapshot.get("portal_latency_ms") or "—"
    portal_error = snapshot.get("portal_error") or ""
    lines = [
        f"IP: {ip}",
        f"Страна: {country}",
        f"VPN: {vpn_line} (lat {latency} мс)",
        f"Портал: {portal} (lat {portal_latency} мс)",
    ]
    if portal_error and portal != "OK":
        lines.append(portal_error[:60])
    await refresh_summary(callback.message.bot, force_status=True)
    await callback.answer("\n".join(lines), show_alert=True)


@router.callback_query(F.data == CAPTCHA_READY)
async def handle_captcha_ready(callback: CallbackQuery) -> None:
    await auth_manager.resolve_captcha(True)
    await callback.answer("Продолжаем")


@router.callback_query(F.data == CAPTCHA_CANCEL)
async def handle_captcha_cancel(callback: CallbackQuery) -> None:
    await auth_manager.resolve_captcha(False)
    await callback.answer("Остановлено", show_alert=True)


@router.callback_query(F.data == "auth:sms_help")
async def handle_sms_help(callback: CallbackQuery) -> None:
    await callback.answer("Отправьте SMS-код ответом на сообщение в чате", show_alert=True)


@router.callback_query(F.data == CAPTCHA_MANUAL)
async def handle_captcha_manual(callback: CallbackQuery) -> None:
    await auth_manager.request_manual_captcha()
    await callback.answer("Переключаюсь в ручной режим", show_alert=True)


def _collect_error_snippet(log_path: str) -> str:
    try:
        with open(log_path, "r", encoding="utf-8", errors="ignore") as fh:
            lines = fh.readlines()
    except FileNotFoundError:
        return "Лог-файл не найден"

    if not lines:
        return "Лог пуст"

    error_indices = [
        idx for idx, line in enumerate(lines) if "ERROR" in line or "Traceback" in line
    ]
    if error_indices:
        idx = error_indices[-1]
        start = max(0, idx - 10)
        end = min(len(lines), idx + 20)
    else:
        start = max(0, len(lines) - 50)
        end = len(lines)

    snippet = "".join(lines[start:end]).strip()
    return snippet or "Лог пуст"


async def build_failure_report() -> str:
    parts: List[str] = []
    parts.append(f"Snapshot: {datetime.utcnow().isoformat()}Z")

    auth_state = await run_in_thread(db.settings_get, "auth_state", "")
    auth_exp = await run_in_thread(db.settings_get, "auth_exp", "")
    system_state = await run_in_thread(db.settings_get, "auth_system_state", "")
    system_hint = await run_in_thread(db.settings_get, "auth_system_hint", "")
    sms_pending = await run_in_thread(db.settings_get, "auth_sms_pending", "0")

    parts.append(f"Auth state: {auth_state or '—'}")
    if auth_exp:
        parts.append(f"Auth valid until: {auth_exp}")
    if system_state:
        line = f"System check: {system_state}"
        if system_hint:
            line += f" ({system_hint})"
        parts.append(line)
    if sms_pending == "1":
        parts.append("SMS pending: yes")

    portal_state = await run_in_thread(db.settings_get, "portal_state", "")
    portal_error = await run_in_thread(db.settings_get, "portal_error", "")
    portal_code = await run_in_thread(db.settings_get, "portal_code", "")
    portal_latency = await run_in_thread(db.settings_get, "portal_latency_ms", "")
    vpn_state = await run_in_thread(db.settings_get, "vpn_state", "")
    vpn_error = await run_in_thread(db.settings_get, "vpn_error", "")

    parts.append(
        f"Portal: {portal_state or '—'} (HTTP {portal_code or '—'}, {portal_latency or '—'} ms)"
    )
    if portal_error:
        parts.append(f"Portal note: {portal_error}")
    parts.append(f"VPN: {vpn_state or '—'}")
    if vpn_error:
        parts.append(f"VPN note: {vpn_error}")

    pulses = await run_in_thread(db.get_recent_portal_pulses, 3)
    if pulses:
        parts.append("Portal pulses:")
        for pulse in pulses:
            parts.append(
                "  - "
                f"{pulse.get('recorded_at', '—')} • {pulse.get('status', '—')} "
                f"lat {pulse.get('latency_ms') or '—'} ms • HTTP {pulse.get('http_status') or '—'} "
                f"{pulse.get('error') or ''}".strip()
            )

    diag_entries = await run_in_thread(db.get_latest_diagnostics, 20)
    failed = [d for d in diag_entries if (d.get("status") or "").upper() != "OK"]
    if failed:
        parts.append("Diagnostics issues:")
        for item in failed[:5]:
            parts.append(
                "  - "
                f"{item.get('recorded_at', '—')} • {item.get('category_code', '—')}/{item.get('city_key', '—')} "
                f"status {item.get('status', '—')} • HTTP {item.get('http_status') or '—'} "
                f"len {item.get('content_len') or '—'} • diff {item.get('diff_len') or '—'}"
            )
            comment = item.get("comment") or item.get("diff_anchor") or item.get("anchor_hash")
            if comment:
                parts.append(f"      note: {comment}")

    pulses_log = await run_in_thread(db.get_recent_pulses, 5)
    if pulses_log:
        parts.append("Pulses:")
        for pulse in pulses_log:
            parts.append(
                "  - "
                f"{pulse.get('created_at', '—')} • {pulse.get('kind', '—')} "
                f"{pulse.get('status', '—')} • {pulse.get('note', '')}".strip()
            )

    logs_path = os.getenv("LOG_FILE", "/opt/bot/logs/bot.log")
    snippet = await run_in_thread(_collect_error_snippet, logs_path)
    if snippet:
        parts.append("--- Log snippet ---")
        parts.append(snippet)

    return "\n".join(parts)


@router.callback_query(F.data.startswith("admin:logs:"))
async def handle_logs(callback: CallbackQuery) -> None:
    if OWNER_ID and callback.from_user.id != OWNER_ID:
        await callback.answer("Недостаточно прав", show_alert=True)
        return
    _, _, limit_str = callback.data.partition("admin:logs:")
    limit = int(limit_str or "50")
    logs_path = os.getenv("LOG_FILE", "/opt/bot/logs/bot.log")
    try:
        with open(logs_path, "r", encoding="utf-8", errors="ignore") as fh:
            lines = fh.readlines()[-limit:]
    except FileNotFoundError:
        await callback.answer("Лог-файл не найден", show_alert=True)
        return
    text = "<pre>" + "".join(lines)[-3500:] + "</pre>"
    await callback.message.answer(text, parse_mode=ParseMode.HTML)
    await callback.answer()


@router.callback_query(F.data == "admin:failure_report")
async def handle_failure_report(callback: CallbackQuery) -> None:
    if OWNER_ID and callback.from_user.id != OWNER_ID:
        await callback.answer("Недостаточно прав", show_alert=True)
        return

    report = await build_failure_report()
    escaped = html.escape(report)
    await callback.message.answer(f"<pre>{escaped}</pre>", parse_mode=ParseMode.HTML)
    await callback.answer("Отчёт отправлен")


@router.callback_query(F.data == "admin:interval")
async def handle_admin_interval(callback: CallbackQuery) -> None:
    if OWNER_ID and callback.from_user.id != OWNER_ID:
        await callback.answer("Недостаточно прав", show_alert=True)
        return
    PENDING_SETTING_UPDATES[callback.from_user.id] = "interval"
    await callback.answer("Введите новый интервал (в минутах) сообщением", show_alert=True)


@router.callback_query(F.data == "admin:lang")
async def handle_admin_language(callback: CallbackQuery) -> None:
    if OWNER_ID and callback.from_user.id != OWNER_ID:
        await callback.answer("Недостаточно прав", show_alert=True)
        return
    PENDING_SETTING_UPDATES[callback.from_user.id] = "lang"
    await callback.answer("Введите язык уведомлений (ru)", show_alert=True)


@router.callback_query(F.data.startswith("admin:save:"))
async def handle_save_url(callback: CallbackQuery) -> None:
    if OWNER_ID and callback.from_user.id != OWNER_ID:
        await callback.answer("Недостаточно прав", show_alert=True)
        return
    _, _, cat_key = callback.data.partition("admin:save:")
    PENDING_URL_UPDATES[callback.from_user.id] = cat_key
    await callback.answer("Отправьте URL отдельным сообщением", show_alert=True)


@router.callback_query(F.data.startswith("admin:screenshot:"))
async def handle_admin_screenshot(callback: CallbackQuery) -> None:
    if OWNER_ID and callback.from_user.id != OWNER_ID:
        await callback.answer("Недостаточно прав", show_alert=True)
        return
    _, _, cat_key = callback.data.partition("admin:screenshot:")
    photo = await auth_manager.capture_category_screenshot(cat_key)
    if not photo:
        await callback.answer("Не удалось сделать скрин", show_alert=True)
        return
    await callback.message.answer_photo(photo)
    await callback.answer()


@router.callback_query(F.data == "admin:auth")
async def handle_admin_auth(callback: CallbackQuery) -> None:
    if OWNER_ID and callback.from_user.id != OWNER_ID:
        await callback.answer("Недостаточно прав", show_alert=True)
        return
    await callback.answer("Запускаю авторизацию…")
    state = await auth_manager.ensure_auth(callback.message.bot, manual=True, force=True)
    await run_in_thread(db.settings_set, "auth_state", state)
    await refresh_summary(callback.message.bot)
    text = "Авторизация: OK" if state == "OK" else f"Авторизация: {state}"
    await callback.message.answer(text)


@router.callback_query(F.data == "admin:screenshots")
async def handle_admin_screenshots(callback: CallbackQuery) -> None:
    if OWNER_ID and callback.from_user.id != OWNER_ID:
        await callback.answer("Недостаточно прав", show_alert=True)
        return
    shots = await run_in_thread(db.get_recent_screenshots, 5)
    if not shots:
        await callback.answer("Скриншоты не найдены", show_alert=True)
        return
    buttons = [
        [
            InlineKeyboardButton(
                text=f"{_format_datetime(s['created_at'], '%d.%m %H:%M:%S')} — {s['name']}",
                callback_data=f"admin:screen:{s['name']}",
            )
        ]
        for s in shots
    ]
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.answer("Последние скриншоты:", reply_markup=keyboard)
    await callback.answer()


@router.callback_query(F.data.startswith("admin:screen:"))
async def handle_admin_screen(callback: CallbackQuery) -> None:
    if OWNER_ID and callback.from_user.id != OWNER_ID:
        await callback.answer("Недостаточно прав", show_alert=True)
        return
    _, _, name = callback.data.partition("admin:screen:")
    shot = await run_in_thread(db.get_screenshot, name)
    if not shot:
        await callback.answer("Скриншот не найден", show_alert=True)
        return
    path = shot.get("path")
    if not path or not os.path.exists(path):
        await callback.answer("Файл недоступен", show_alert=True)
        return
    file = FSInputFile(path)
    caption = shot.get("description") or name
    await callback.message.answer_photo(file, caption=caption)
    await callback.answer()


@router.message(F.text)
async def handle_owner_messages(message: Message) -> None:
    if OWNER_ID and message.from_user.id != OWNER_ID:
        return
    text = (message.text or "").strip()
    text_lower = text.lower()
    if await auth_manager.try_handle_owner_message(message):
        if re.fullmatch(r"\d{6}", text):
            await message.answer("Код получен.")
        elif text_lower in {"готово", "done"}:
            await message.answer("Продолжаю авторизацию.")
        elif text_lower in {"отмена", "cancel"}:
            await message.answer("Авторизация остановлена.")
        return
    user_id = message.from_user.id

    pending_setting = PENDING_SETTING_UPDATES.get(user_id)
    if pending_setting == "interval":
        try:
            value = int(text)
        except ValueError:
            await message.answer("Введите целое число от 1 до 180.")
            return
        if value < 1 or value > 180:
            await message.answer("Интервал должен быть от 1 до 180 минут.")
            return
        PENDING_SETTING_UPDATES.pop(user_id, None)
        global INTERVAL_MINUTES
        INTERVAL_MINUTES = value
        await run_in_thread(db.settings_set, "CHECK_INTERVAL_MIN", str(value))
        await scheduler.update_interval(value)
        await message.answer(f"Интервал обновлён: {value} мин.")
        await refresh_summary(message.bot)
        return
    if pending_setting == "lang":
        lang = text.lower()
        if lang not in {"ru"}:
            await message.answer("Пока доступен только язык ru.")
            return
        PENDING_SETTING_UPDATES.pop(user_id, None)
        await run_in_thread(db.settings_set, "notify_lang", lang)
        await message.answer("Язык уведомлений обновлён.")
        await refresh_summary(message.bot)
        return

    pending = PENDING_URL_UPDATES.pop(user_id, None)
    if not pending:
        return
    url = text
    await run_in_thread(db.update_category_url, pending, url)
    await message.answer(f"URL для категории {pending} сохранён.")
    await refresh_summary(message.bot)


@router.callback_query(F.data == "noop")
async def handle_noop(callback: CallbackQuery) -> None:
    await callback.answer()

