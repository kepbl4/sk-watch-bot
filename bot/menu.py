"""Fake dashboard and interactions for the SK Watch Bot."""
from __future__ import annotations

import asyncio
import html
import json
import random
import re
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from storage import db
from utils.logging import logger
from watcher.scheduler import scheduler

router = Router(name="menu")

SUMMARY_ANCHOR = "summary"
CATEGORIES_ANCHOR = "categories"
TRACKED_ANCHOR = "tracked"
ADMIN_ANCHOR = "admin"
DIAGNOSTIC_ANCHOR = "diagnostics"

ANCHOR_KEYS = (
    SUMMARY_ANCHOR,
    CATEGORIES_ANCHOR,
    TRACKED_ANCHOR,
    ADMIN_ANCHOR,
    DIAGNOSTIC_ANCHOR,
)

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

PENDING_ACTIONS: Dict[int, str] = {}

_CATEGORY_STATUS = [
    ("🟢", "свежих дат нет, мониторим в реальном времени"),
    ("🟡", "отмечаем движения очереди, реагируем моментально"),
    ("🔵", "расписание синхронизировано, уведомим при изменении"),
    ("🟣", "включен углублённый анализ свободных слотов"),
]

_CITY_STATUS = [
    ("📍", "канал связи стабилен, проверяем каждые 2 мин"),
    ("🛰", "сенсоры в норме, отслеживаем свежие окна"),
    ("🕒", "следующая сверка чуть позже, держим руку на пульсе"),
    ("🌟", "подхватили очередь, ничего не пропустим"),
]

_AUTH_STATES = {
    "OK": "Авторизация активна",
    "UPDATING": "Выполняется обновление",
}


def configure(interval: int, owner_id: Optional[int]) -> None:
    """Configure the pretend monitoring interval and owner."""

    global INTERVAL_MINUTES, OWNER_ID
    INTERVAL_MINUTES = max(1, interval)
    OWNER_ID = owner_id


async def run_in_thread(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


async def _save_anchor_bundle(chat_id: int, message_id: int) -> None:
    for anchor in ANCHOR_KEYS:
        await run_in_thread(db.save_anchor, anchor, chat_id, message_id)


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


async def _save_list(key: str, data: List[Dict[str, Any]]) -> None:
    await run_in_thread(db.settings_set, key, json.dumps(data, ensure_ascii=False))

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

    expected_countries_raw = os.getenv("VPN_EXPECTED_COUNTRY", "SK")
    expected_countries = {
        item.strip().upper()
        for item in expected_countries_raw.split(",")
        if item.strip()
    }

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
                    if expected_countries and vpn_country not in expected_countries:
                        vpn_state = "NEED_VPN"
                        if not vpn_error:
                            vpn_error = f"expected {','.join(sorted(expected_countries))} got {vpn_country or '??'}"
                    else:
                        vpn_state = "OK"
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
                )
                if portal_state == "ERR":
                    logger.warning("Portal sensor error: %s", portal_error)
                    await auth_manager.capture_portal_error(
                        login_url, description=portal_error or "portal error"
                )
                if method_note and not portal_error:
                    portal_error = method_note
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

async def _append_event(text: str) -> None:
    await scheduler.record_pulse(text)


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _format_relative(value: Optional[str]) -> str:
    dt = _parse_dt(value)
    if not dt:
        return "только что"
    delta = datetime.utcnow() - dt
    if delta < timedelta(minutes=1):
        seconds = max(1, int(delta.total_seconds()))
        return f"{seconds} сек назад"
    if delta < timedelta(hours=1):
        minutes = int(delta.total_seconds() // 60)
        return f"{minutes} мин назад"
    if delta < timedelta(days=1):
        hours = int(delta.total_seconds() // 3600)
        return f"{hours} ч назад"
    days = delta.days
    return f"{days} дн назад"


def _derive_title(url: str, kind: str, existing: List[Dict[str, Any]]) -> str:
    if "|" in url:
        parts = [part.strip() for part in url.split("|", 1)]
        if len(parts) == 2:
            title, link = parts
            if _looks_like_url(link):
                return title or _derive_title(link, kind, existing)
    parsed = re.sub(r"https?://", "", url).strip()
    parsed = parsed.split("?")[0]
    slug = parsed.strip("/").split("/")[-1] or parsed
    slug = re.sub(r"[-_]+", " ", slug).strip()
    if not slug:
        slug = parsed or ("категория" if kind == "category" else "город")
    base = slug.title()
    prefix = "Категория" if kind == "category" else "Город"
    candidate = f"{prefix} {base}".strip()
    existing_titles = {item.get("title") for item in existing}
    if candidate not in existing_titles:
        return candidate
    counter = 2
    while f"{candidate} #{counter}" in existing_titles:
        counter += 1
    return f"{candidate} #{counter}"


def _looks_like_url(value: str) -> bool:
    return bool(re.match(r"https?://", value, re.IGNORECASE))


def _make_entry(link: str, title: str, kind: str) -> Dict[str, Any]:
    return {
        "id": uuid.uuid4().hex,
        "url": link,
        "title": title,
        "created_at": datetime.utcnow().isoformat(),
        "kind": kind,
    }


def _status_for(kind: str, seed: str) -> str:
    bucket = _CATEGORY_STATUS if kind == "category" else _CITY_STATUS
    idx = abs(hash(seed)) % len(bucket)
    icon, text = bucket[idx]
    return f"{icon} {text}"


async def _ensure_auto_event() -> None:
    raw_last = await run_in_thread(db.settings_get, FAKE_LAST_TICK_KEY, None)
    now = datetime.utcnow()
    try:
        last = datetime.fromisoformat(raw_last) if raw_last else None
    except ValueError:
        last = None
    if not last or now - last >= timedelta(minutes=3):
        await _append_event("Плановая проверка расписания завершена — новых дат пока нет")
        await run_in_thread(db.settings_set, FAKE_LAST_TICK_KEY, now.isoformat())
        await _touch_portal_snapshot()
        await _touch_vpn_snapshot()

    lines = [
        "<b>🤖 SK Watch Bot · Панель мониторинга</b>",
        "",
    ]

async def _touch_vpn_snapshot(update_latency: bool = False) -> Dict[str, Any]:
    raw = await run_in_thread(db.settings_get, FAKE_VPN_KEY, None)
    if raw:
        try:
            snapshot = json.loads(raw)
        except json.JSONDecodeError:
            snapshot = _generate_vpn_snapshot()
    else:
        snapshot = _generate_vpn_snapshot()
    if update_latency:
        rng = random.Random()
        snapshot["latency"] = rng.randint(70, 190)
    snapshot["checked_at"] = datetime.utcnow().isoformat()
    await run_in_thread(db.settings_set, FAKE_VPN_KEY, json.dumps(snapshot, ensure_ascii=False))
    return snapshot

    lines.append(
        f"🌐 VPN: ✅ {html.escape(vpn_data.get('country', 'SK'))} • IP {vpn_data.get('ip', '—')} "
        f"• пинг {vpn_data.get('latency', 0)} мс • {_format_relative(vpn_data.get('checked_at'))}"
    )
    lines.append(
        f"🛰 Портал: ✅ HTTP {portal_data.get('http_status', 200)} • {portal_data.get('latency', 0)} мс "
        f"• {_format_relative(portal_data.get('checked_at'))}"
    )
    total_targets = len(categories) + len(cities)
    lines.append(
        f"📡 Мониторинг: {total_targets} направлений • обновление каждые {monitor_interval} мин"
    )
    lines.append("")

async def _touch_portal_snapshot() -> Dict[str, Any]:
    raw = await run_in_thread(db.settings_get, FAKE_PORTAL_KEY, None)
    if raw:
        try:
            snapshot = json.loads(raw)
        except json.JSONDecodeError:
            snapshot = _generate_portal_snapshot()
    else:
        snapshot = _generate_portal_snapshot()
    rng = random.Random()
    snapshot["latency"] = rng.randint(110, 340)
    snapshot["checked_at"] = datetime.utcnow().isoformat()
    await run_in_thread(db.settings_set, FAKE_PORTAL_KEY, json.dumps(snapshot, ensure_ascii=False))
    return snapshot


def _format_event_line(event: Dict[str, Any]) -> str:
    dt = _parse_dt(event.get("ts"))
    timestamp = dt.strftime("%H:%M") if dt else "--:--"
    text = html.escape(event.get("text", ""))
    return f"• {timestamp} — {text}"


async def build_dashboard_text() -> str:
    await _ensure_defaults()
    await _ensure_auto_event()
    categories = await _load_list(FAKE_CATEGORY_KEY)
    cities = await _load_list(FAKE_CITY_KEY)
    events = await _load_list(FAKE_EVENTS_KEY)
    events = sorted(events, key=lambda item: item.get("ts", ""))[-6:]
    monitor_interval = await run_in_thread(db.settings_get, FAKE_MONITOR_INTERVAL_KEY, str(INTERVAL_MINUTES))
    auth_state = await run_in_thread(db.settings_get, FAKE_AUTH_STATE_KEY, "OK")
    last_auth = await run_in_thread(db.settings_get, FAKE_AUTH_UPDATED_KEY, None)
    auth_reason = await run_in_thread(db.settings_get, FAKE_AUTH_REASON_KEY, "Ручное обновление")
    vpn_snapshot = await run_in_thread(db.settings_get, FAKE_VPN_KEY, None)
    portal_snapshot = await run_in_thread(db.settings_get, FAKE_PORTAL_KEY, None)
    try:
        vpn_data = json.loads(vpn_snapshot) if vpn_snapshot else _generate_vpn_snapshot()
    except json.JSONDecodeError:
        vpn_data = _generate_vpn_snapshot()
    try:
        portal_data = json.loads(portal_snapshot) if portal_snapshot else _generate_portal_snapshot()
    except json.JSONDecodeError:
        portal_data = _generate_portal_snapshot()

    lines = [
        "<b>🤖 SK Watch Bot · Панель мониторинга</b>",
        "",
    ]

    auth_icon = "✅" if auth_state == "OK" else "⏳"
    auth_human = _AUTH_STATES.get(auth_state, "Авторизация")
    lines.append(
        f"{auth_icon} Авторизация: {auth_human} • {_format_relative(last_auth)}"  # type: ignore[arg-type]
    )
    lines.append(f"Причина: {html.escape(auth_reason or '—')}")

    lines.append(
        f"🌐 VPN: ✅ {html.escape(vpn_data.get('country', 'SK'))} • IP {vpn_data.get('ip', '—')} "
        f"• пинг {vpn_data.get('latency', 0)} мс • {_format_relative(vpn_data.get('checked_at'))}"
    )
    lines.append(
        f"🛰 Портал: ✅ HTTP {portal_data.get('http_status', 200)} • {portal_data.get('latency', 0)} мс "
        f"• {_format_relative(portal_data.get('checked_at'))}"
    )
    total_targets = len(categories) + len(cities)
    lines.append(
        f"📡 Мониторинг: {total_targets} направлений • обновление каждые {monitor_interval} мин"
    )
    lines.append("")

    lines.append("<b>Категории</b>")
    if not categories:
        lines.append("Добавьте первую категорию кнопкой ниже.")
    for idx, entry in enumerate(categories, start=1):
        status = _status_for("category", entry.get("url", ""))
        title = html.escape(entry.get("title", f"Категория #{idx}"))
        url = html.escape(entry.get("url", ""))
        lines.append(
            f"{idx}. <a href=\"{url}\">{title}</a> — {status} • {_format_relative(entry.get('created_at'))}"
        )
    lines.append("")

    lines.append("<b>Города</b>")
    if not cities:
        lines.append("Добавьте города для полноты мониторинга.")
    for idx, entry in enumerate(cities, start=1):
        status = _status_for("city", entry.get("url", "") + entry.get("title", ""))
        title = html.escape(entry.get("title", f"Город #{idx}"))
        url = html.escape(entry.get("url", ""))
        lines.append(
            f"{idx}. <a href=\"{url}\">{title}</a> — {status} • {_format_relative(entry.get('created_at'))}"
        )
    lines.append("")

    if events:
        lines.append("<b>Последние события</b>")
        for event in events[::-1]:
            lines.append(_format_event_line(event))
    else:
        lines.append("<i>Событий пока нет — мониторинг ждёт вашего сигнала.</i>")

    return "\n".join(lines)


def _dashboard_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(text="+ Категорию", callback_data="dashboard:add_category"),
            InlineKeyboardButton(text="+ Город", callback_data="dashboard:add_city"),
        ],
        [InlineKeyboardButton(text="Обновить авторизацию", callback_data="dashboard:refresh_auth")],
        [
            InlineKeyboardButton(text="Статус VPN", callback_data="dashboard:vpn"),
            InlineKeyboardButton(text="Обновить панель", callback_data="dashboard:refresh"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


async def _render_summary(
    bot,
    chat_id: int,
    message_id: int,
    *,
    force_status: bool = False,
    fallback_chat_id: Optional[int] = None,
) -> None:
    text, sms_pending = await build_summary_text(force_status=force_status)
    keyboard = summary_keyboard(sms_pending=sms_pending)
    try:
        await bot.edit_message_text(
            text=text,
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML,
        )
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return
        logger.debug("Не удалось обновить панель: %s", exc)
        sent = await bot.send_message(
            chat_id,
            text,
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
        await run_in_thread(db.save_anchor, DASHBOARD_ANCHOR, sent.chat.id, sent.message_id)


async def _send_dashboard(bot, chat_id: int) -> None:
    anchor = await run_in_thread(db.get_anchor, DASHBOARD_ANCHOR)
    text = await build_dashboard_text()
    keyboard = _dashboard_keyboard()
    if anchor and anchor.get("chat_id") == chat_id:
        try:
            await bot.edit_message_text(
                text=text,
                chat_id=anchor["chat_id"],
                message_id=anchor["message_id"],
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )
            return
        except TelegramBadRequest:
            pass
    sent = await bot.send_message(
        chat_id,
        text,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )
    await run_in_thread(db.save_anchor, DASHBOARD_ANCHOR, sent.chat.id, sent.message_id)


async def _refresh_dashboard(bot) -> None:
    anchor = await run_in_thread(db.get_anchor, DASHBOARD_ANCHOR)
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
    try:
        await ensure_summary_message(message)
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        logger.exception("Failed to render summary on /start: %s", exc)
        await message.answer(
            "Не удалось подготовить сводку. Попробуйте ещё раз позже или обратитесь к администратору."
        )



@router.callback_query(F.data == "dashboard:add_category")
async def handle_add_category(callback: CallbackQuery) -> None:
    PENDING_ACTIONS[callback.from_user.id] = "category"
    await callback.message.answer(
        "Пришлите ссылку на категорию, которую нужно отслеживать."
    )
    await callback.answer()


@router.callback_query(F.data == "dashboard:add_city")
async def handle_add_city(callback: CallbackQuery) -> None:
    PENDING_ACTIONS[callback.from_user.id] = "city"
    await callback.message.answer("Пришлите ссылку на город для мониторинга.")
    await callback.answer()


@router.callback_query(F.data == "dashboard:refresh_auth")
async def handle_refresh_auth(callback: CallbackQuery) -> None:
    await run_in_thread(db.settings_set, FAKE_AUTH_STATE_KEY, "UPDATING")
    await callback.message.answer("Обновляем авторизацию…")
    await _append_event("Запущено обновление авторизации, подтверждаем сеанс")
    await _refresh_dashboard(callback.message.bot)
    await callback.answer("Обновление запущено")

    async def _complete() -> None:
        await asyncio.sleep(7)
        now = datetime.utcnow().isoformat()
        await run_in_thread(db.settings_set, FAKE_AUTH_STATE_KEY, "OK")
        await run_in_thread(db.settings_set, FAKE_AUTH_UPDATED_KEY, now)
        await run_in_thread(db.settings_set, FAKE_AUTH_REASON_KEY, "Ручное обновление из панели")
        await _append_event("Авторизация успешно обновлена — защищённый канал активен")
        await callback.message.answer("Авторизация обновлена ✅")
        await _refresh_dashboard(callback.message.bot)

    asyncio.create_task(_complete())


@router.callback_query(F.data == "dashboard:refresh")
async def handle_refresh(callback: CallbackQuery) -> None:
    await _append_event("Ручное обновление панели — изменений не обнаружено")
    await _refresh_dashboard(callback.message.bot)
    await callback.answer("Панель обновлена")


@router.callback_query(F.data == "dashboard:vpn")
async def handle_vpn_status(callback: CallbackQuery) -> None:
    await callback.answer("Обновляю диагностику…")
    try:
        snapshot = await asyncio.wait_for(
            ensure_connectivity_status(force=True), timeout=6
        )
    except asyncio.TimeoutError:
        logger.warning("Connectivity refresh timed out, using cached snapshot")
        snapshot = await ensure_connectivity_status(force=False)
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.exception("Connectivity refresh failed: %s", exc)
        snapshot = await _read_connectivity_snapshot()

    vpn_line = snapshot.get("vpn_status") or "ERR"
    ip = snapshot.get("vpn_ip") or "—"
    country = snapshot.get("vpn_country_code") or "??"
    latency = snapshot.get("vpn_latency_ms") or "—"
    portal = snapshot.get("portal_status") or "ERR"
    portal_latency = snapshot.get("portal_latency_ms") or "—"
    portal_error = snapshot.get("portal_error") or ""
    lines = [
        "<b>VPN диагностика</b>",
        f"IP: {ip}",
        f"Страна: {country}",
        f"VPN: {vpn_line} (lat {latency} мс)",
        f"Портал: {portal} (lat {portal_latency} мс)",
    ]
    if portal_error and portal.startswith("ERR"):
        lines.append(f"Ошибка: {portal_error[:120]}")

    await callback.message.answer("\n".join(lines))
    await refresh_summary(callback.message.bot)


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
    await _append_event("Проверен VPN-туннель — соединение стабильно")
    await callback.message.answer(text)
    await _refresh_dashboard(callback.message.bot)
    await callback.answer()


@router.message(F.text)
async def handle_text(message: Message) -> None:
    action = PENDING_ACTIONS.pop(message.from_user.id, None)
    if not action:
        return
    text = (message.text or "").strip()
    if "|" in text:
        maybe_title, maybe_url = [part.strip() for part in text.split("|", 1)]
    else:
        maybe_title, maybe_url = "", text
    url = maybe_url
    if not _looks_like_url(url):
        await message.answer("Пожалуйста, отправьте ссылку, начинающуюся с http:// или https://.")
        PENDING_ACTIONS[message.from_user.id] = action
        return
    if maybe_title:
        title = maybe_title
    else:
        entries = await _load_list(FAKE_CATEGORY_KEY if action == "category" else FAKE_CITY_KEY)
        title = _derive_title(url, action, entries)
    entries = await _load_list(FAKE_CATEGORY_KEY if action == "category" else FAKE_CITY_KEY)
    for entry in entries:
        if entry.get("url") == url:
            await message.answer("Эта ссылка уже отслеживается, панель обновлена.")
            await _refresh_dashboard(message.bot)
            return
    new_entry = _make_entry(url, title, action)
    entries.append(new_entry)
    await _save_list(FAKE_CATEGORY_KEY if action == "category" else FAKE_CITY_KEY, entries)
    await _append_event(
        f"Добавлена цель '{title}' — следим за расписанием без задержек"
    )
    await message.answer(
        f"Отлично! {title} добавлен в мониторинг. "
        "Если появятся новые даты, бот сразу сообщит."
    )
    await _refresh_dashboard(message.bot)


__all__ = ["router", "configure", "build_dashboard_text"]
