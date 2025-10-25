"""User-facing inline menu for the SK Watch Bot."""
from __future__ import annotations

import asyncio
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
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from auth.flow import CAPTCHA_CANCEL, CAPTCHA_MANUAL, CAPTCHA_READY, auth_manager
from storage import db
from utils.logging import logger
from watcher.scheduler import scheduler

router = Router(name="menu")

SUMMARY_ANCHOR = "summary"
CATEGORIES_ANCHOR = "categories"
TRACKED_ANCHOR = "tracked"
ADMIN_ANCHOR = "admin"

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
}

AUTH_STATUS_ICONS = {
    "OK": "✅",
    "NEED_AUTH": "🔒",
    "NEED_VPN": "🌐",
    "NEED_CAPTCHA": "⚠️",
    "NEED_SMS": "🔒",
    "ERROR": "⚠️",
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


async def run_in_thread(func, *args):
    return await asyncio.to_thread(func, *args)


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

    timeout = aiohttp.ClientTimeout(total=10)
    headers = {"User-Agent": "sk-watch-bot/1.0", "Accept": "application/json"}
    login_url = os.getenv("LOGIN_URL", "")

    async with aiohttp.ClientSession(timeout=timeout) as session:
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
                async with session.get(login_url, allow_redirects=False) as resp:
                    elapsed = int((time.monotonic() - start) * 1000)
                    portal_latency = str(elapsed)
                    portal_code = str(resp.status)
                    if resp.status in {200, 301, 302}:
                        portal_state = "OK"
                    else:
                        portal_state = "ERR"
                        portal_error = f"HTTP {resp.status}"
            except Exception as exc:  # pragma: no cover - network issues
                portal_state = "ERR"
                portal_error = str(exc)
        else:
            portal_state = "ERR"
            portal_error = "LOGIN_URL not configured"

    display_vpn = "ERR"
    if vpn_state == "OK":
        display_vpn = f"{vpn_country or 'SK'} ✅"
    elif vpn_state == "NEED_VPN":
        display_vpn = f"{vpn_country or '??'} ❌"
    else:
        display_vpn = "ERR"

    display_portal = "OK ✅" if portal_state == "OK" else "ERR ⚠️"

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
    await run_in_thread(db.settings_set, "portal_status", display_portal)
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
            "portal_status": display_portal,
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


async def build_summary_text(force_status: bool = False) -> str:
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
    auth_state = await run_in_thread(db.settings_get, "auth_state", "NEED_AUTH")
    auth_until = await run_in_thread(db.settings_get, "auth_exp", "")
    if auth_state == "OK" and auth_until:
        auth_label = f"OK до {_format_datetime(auth_until, '%H:%M')}"
    elif auth_state == "OK":
        auth_label = "OK"
    else:
        auth_label = auth_state
    icon = AUTH_STATUS_ICONS.get(auth_state)
    if icon and not auth_label.startswith(icon):
        auth_label = f"{icon} {auth_label}"

    summary_lines = [
        "<b>Сводка</b>",
        "",
        f"VPN: {vpn_status}",
        f"Портал: {portal_status}",
        f"Авторизация: {auth_label}",
        "",
        f"Интервал: {INTERVAL_MINUTES} минут",
        "",
        "Категории:",
    ]
    summary_lines.extend(lines or ["—"])
    summary_lines.extend(["", "Последние события:"])
    summary_lines.extend(await _recent_events())
    summary_lines.extend(["", f"Активных целей: {total_active}"])
    return "\n".join(summary_lines)


def summary_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(
                text="Проверить всё сейчас", callback_data="summary:check_all"
            ),
            InlineKeyboardButton(text="Обновить", callback_data="summary:refresh"),
        ],
        [
            InlineKeyboardButton(text="Категории", callback_data="summary:categories"),
            InlineKeyboardButton(text="Состояние VPN", callback_data="summary:vpn"),
        ],
        [
            InlineKeyboardButton(text="Отслеживаемое", callback_data="summary:tracked"),
            InlineKeyboardButton(text="Авторизация", callback_data="summary:auth"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


async def ensure_summary_message(message: Message, *, force_status: bool = False) -> None:
    text = await build_summary_text(force_status=force_status)
    keyboard = summary_keyboard()
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


async def edit_summary_message(bot, chat_id: int, message_id: int, *, force_status: bool = False) -> None:
    text = await build_summary_text(force_status=force_status)
    try:
        await bot.edit_message_text(
            text=text,
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=summary_keyboard(),
            parse_mode=ParseMode.HTML,
        )
    except TelegramBadRequest as exc:
        logger.warning("Unable to edit summary message: %s", exc)


async def refresh_summary(bot, *, force_status: bool = False) -> None:
    anchor = await run_in_thread(db.get_anchor, SUMMARY_ANCHOR)
    if not anchor:
        return
    await edit_summary_message(
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
    keyboard_rows.append(
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data="summary:back"),
            InlineKeyboardButton(text="Отслеживаемое", callback_data="summary:tracked"),
            InlineKeyboardButton(text="Панель", callback_data="summary:admin"),
        ]
    )
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


async def build_admin_view() -> Tuple[str, InlineKeyboardMarkup]:
    categories = await run_in_thread(db.get_categories)
    interval = await run_in_thread(db.settings_get, "CHECK_INTERVAL_MIN", str(INTERVAL_MINUTES))
    notify_lang = await run_in_thread(db.settings_get, "notify_lang", "ru")
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

    keyboard_rows = [
        [
            InlineKeyboardButton(text="Изменить интервал", callback_data="admin:interval"),
            InlineKeyboardButton(text="Язык уведомлений", callback_data="admin:lang"),
        ]
    ]
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
    keyboard_rows.append(
        [
            InlineKeyboardButton(text="⬅️ Назад", callback_data="summary:categories"),
            InlineKeyboardButton(text="Сводка", callback_data="summary:back"),
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


@router.callback_query(F.data.startswith("cat:check:"))
async def handle_category_check(callback: CallbackQuery) -> None:
    _, _, cat_key = callback.data.partition("cat:check:")
    await scheduler.enqueue_category_check(cat_key, priority=True, reason="manual")
    await callback.answer("Категория поставлена на проверку")


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


@router.callback_query(F.data == "summary:auth")
async def handle_auth(callback: CallbackQuery) -> None:
    await callback.answer("Проверяю авторизацию…", show_alert=False)
    state = await auth_manager.ensure_auth(callback.message.bot, manual=True, force=True)
    await run_in_thread(db.settings_set, "auth_state", state)
    await refresh_summary(callback.message.bot)
    text = "Авторизация: OK" if state == "OK" else f"Авторизация: {state}"
    await callback.answer(text, show_alert=True)


@router.callback_query(F.data == CAPTCHA_READY)
async def handle_captcha_ready(callback: CallbackQuery) -> None:
    await auth_manager.resolve_captcha(True)
    await callback.answer("Продолжаем")


@router.callback_query(F.data == CAPTCHA_CANCEL)
async def handle_captcha_cancel(callback: CallbackQuery) -> None:
    await auth_manager.resolve_captcha(False)
    await callback.answer("Остановлено", show_alert=True)


@router.callback_query(F.data == CAPTCHA_MANUAL)
async def handle_captcha_manual(callback: CallbackQuery) -> None:
    await auth_manager.request_manual_captcha()
    await callback.answer("Переключаюсь в ручной режим", show_alert=True)


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

