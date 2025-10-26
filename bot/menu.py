"""Fake dashboard and interactions for the SK Watch Bot."""
from __future__ import annotations

import asyncio
import html
import json
import random
import re
import uuid
from contextlib import suppress
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from storage import db
from utils.logging import logger
from watcher.scheduler import scheduler

router = Router(name="menu")

DASHBOARD_ANCHOR = "fake:dashboard"
FAKE_CATEGORY_KEY = "fake:categories"
FAKE_CITY_KEY = "fake:cities"
FAKE_EVENTS_KEY = "fake:events"
FAKE_VPN_KEY = "fake:vpn_snapshot"
FAKE_PORTAL_KEY = "fake:portal_snapshot"
FAKE_LAST_TICK_KEY = "fake:last_tick"
FAKE_MONITOR_INTERVAL_KEY = "fake:monitor_interval"
FAKE_AUTH_STATE_KEY = "fake:auth_state"
FAKE_AUTH_UPDATED_KEY = "fake:last_auth"
FAKE_AUTH_REASON_KEY = "fake:last_auth_reason"

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


async def _ensure_defaults() -> None:
    for key in (FAKE_CATEGORY_KEY, FAKE_CITY_KEY, FAKE_EVENTS_KEY):
        raw = await run_in_thread(db.settings_get, key, None)
        if raw is None:
            await run_in_thread(db.settings_set, key, "[]")
    if await run_in_thread(db.settings_get, FAKE_VPN_KEY, None) is None:
        snapshot = _generate_vpn_snapshot()
        await run_in_thread(db.settings_set, FAKE_VPN_KEY, json.dumps(snapshot, ensure_ascii=False))
    if await run_in_thread(db.settings_get, FAKE_PORTAL_KEY, None) is None:
        snapshot = _generate_portal_snapshot()
        await run_in_thread(db.settings_set, FAKE_PORTAL_KEY, json.dumps(snapshot, ensure_ascii=False))
    if await run_in_thread(db.settings_get, FAKE_MONITOR_INTERVAL_KEY, None) is None:
        await run_in_thread(db.settings_set, FAKE_MONITOR_INTERVAL_KEY, str(INTERVAL_MINUTES))
    if await run_in_thread(db.settings_get, FAKE_AUTH_STATE_KEY, None) is None:
        now = datetime.utcnow().isoformat()
        await run_in_thread(db.settings_set, FAKE_AUTH_STATE_KEY, "OK")
        await run_in_thread(db.settings_set, FAKE_AUTH_UPDATED_KEY, now)
        await run_in_thread(db.settings_set, FAKE_AUTH_REASON_KEY, "Инициализация сеанса")


def _generate_vpn_snapshot() -> Dict[str, Any]:
    rng = random.Random()
    ip = f"185.{rng.randint(10, 220)}.{rng.randint(0, 255)}.{rng.randint(0, 255)}"
    providers = ["SecureLine", "NordSecure", "ShieldNet", "AtlasSafe"]
    countries = ["SK", "SK", "SK", "CZ"]
    latency = rng.randint(60, 170)
    return {
        "ip": ip,
        "provider": rng.choice(providers),
        "country": rng.choice(countries),
        "latency": latency,
        "checked_at": datetime.utcnow().isoformat(),
    }


def _generate_portal_snapshot() -> Dict[str, Any]:
    rng = random.Random()
    return {
        "http_status": 200,
        "latency": rng.randint(120, 380),
        "checked_at": datetime.utcnow().isoformat(),
        "note": "Портал отвечает штатно",
    }


async def _load_list(key: str) -> List[Dict[str, Any]]:
    raw = await run_in_thread(db.settings_get, key, "[]")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Сломанные данные в %s, сбрасываю", key)
        data = []
    if not isinstance(data, list):
        return []
    return data


async def _save_list(key: str, data: List[Dict[str, Any]]) -> None:
    await run_in_thread(db.settings_set, key, json.dumps(data, ensure_ascii=False))


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
        [InlineKeyboardButton(text="Обновить", callback_data="summary:refresh")],
        [
            InlineKeyboardButton(text="Категории", callback_data="summary:categories"),
            InlineKeyboardButton(text="Диагностика", callback_data="summary:diagnostics"),
        ],
        [InlineKeyboardButton(text="Отслеживаемое", callback_data="summary:tracked")],
        [InlineKeyboardButton(text="Обновить авторизацию", callback_data="dashboard:refresh_auth")],
        [InlineKeyboardButton(text="Статус VPN", callback_data="summary:vpn")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


async def _render_dashboard(bot, chat_id: int, message_id: int) -> None:
    text = await build_dashboard_text()
    keyboard = _dashboard_keyboard()
    try:
        await bot.edit_message_text(
            text=text,
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=keyboard,
            disable_web_page_preview=True,
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


async def build_categories_view() -> tuple[str, InlineKeyboardMarkup]:
    await _ensure_defaults()
    categories = await _load_list(FAKE_CATEGORY_KEY)
    lines: List[str] = ["<b>Категории</b>", "Управляйте списком направлений для мониторинга.", ""]
    if not categories:
        lines.append("Пока ничего нет. Добавьте первую категорию кнопкой ниже.")
    for idx, entry in enumerate(categories, start=1):
        title = html.escape(entry.get("title", f"Категория #{idx}"))
        url = html.escape(entry.get("url", ""))
        status = _status_for("category", entry.get("url", ""))
        lines.append(
            f"{idx}. <a href=\"{url}\">{title}</a> — {status} • {_format_relative(entry.get('created_at'))}"
        )
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="+ Категорию", callback_data="dashboard:add_category")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="summary:back")],
        ]
    )
    return "\n".join(lines), keyboard


def _fake_pairs(categories: List[Dict[str, Any]], cities: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    if not categories or not cities:
        return []
    pairs: List[Dict[str, str]] = []
    for idx, category in enumerate(categories):
        city = cities[idx % len(cities)]
        pairs.append(
            {
                "category": html.escape(category.get("title", "Категория")),
                "city": html.escape(city.get("title", "Город")),
            }
        )
    return pairs[:8]


async def build_tracked_view() -> tuple[str, InlineKeyboardMarkup]:
    await _ensure_defaults()
    categories = await _load_list(FAKE_CATEGORY_KEY)
    cities = await _load_list(FAKE_CITY_KEY)
    pairs = _fake_pairs(categories, cities)
    lines: List[str] = [
        "<b>Отслеживаемые направления</b>",
        "Следим за сочетаниями категорий и городов, обновляем мгновенно.",
        "",
    ]
    if not pairs:
        lines.append("Добавьте хотя бы одну категорию и город, чтобы запустить мониторинг.")
    else:
        for idx, pair in enumerate(pairs, start=1):
            status = random.choice(
                [
                    "Все слоты заняты, ждём движение",
                    "Ищем свежие даты",
                    "Очередь стабильна",
                    "Фиксируем активности",
                ]
            )
            lines.append(f"{idx}. {pair['category']} • {pair['city']} — {status}")
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="+ Категорию", callback_data="dashboard:add_category")],
            [InlineKeyboardButton(text="+ Город", callback_data="dashboard:add_city")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="summary:back")],
        ]
    )
    return "\n".join(lines), keyboard


async def build_diagnostics_view() -> tuple[str, InlineKeyboardMarkup]:
    await _ensure_defaults()
    events = await _load_list(FAKE_EVENTS_KEY)
    events = sorted(events, key=lambda item: item.get("ts", ""))[-10:]
    lines: List[str] = ["<b>Диагностика</b>", "Последние события мониторинга и службы.", ""]
    if not events:
        lines.append("Лог пуст. Всё стабильно и работает согласно графику.")
    else:
        for event in events[::-1]:
            lines.append(_format_event_line(event))
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Обновить", callback_data="diagnostics:refresh")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="summary:back")],
        ]
    )
    return "\n".join(lines), keyboard


async def _render_categories(bot, chat_id: int, message_id: int) -> None:
    text, keyboard = await build_categories_view()
    await bot.edit_message_text(
        text=text,
        chat_id=chat_id,
        message_id=message_id,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


async def _render_tracked(bot, chat_id: int, message_id: int) -> None:
    text, keyboard = await build_tracked_view()
    await bot.edit_message_text(
        text=text,
        chat_id=chat_id,
        message_id=message_id,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


async def _render_diagnostics(bot, chat_id: int, message_id: int) -> None:
    text, keyboard = await build_diagnostics_view()
    await bot.edit_message_text(
        text=text,
        chat_id=chat_id,
        message_id=message_id,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


async def _render_with_anchor(bot, chat_id: int, renderer) -> None:
    anchor = await run_in_thread(db.get_anchor, DASHBOARD_ANCHOR)
    if not anchor:
        await _send_dashboard_safe(bot, chat_id)
        anchor = await run_in_thread(db.get_anchor, DASHBOARD_ANCHOR)
        if not anchor:
            return
    try:
        await renderer(bot, anchor["chat_id"], anchor["message_id"])
    except Exception as exc:  # pragma: no cover - defensive path
        logger.exception("Не удалось отобразить панель: %s", exc)
        await _send_dashboard_safe(bot, chat_id, force_new=True)


async def _send_dashboard(bot, chat_id: int, *, force_new: bool = False) -> None:
    anchor = await run_in_thread(db.get_anchor, DASHBOARD_ANCHOR)
    text = await build_dashboard_text()
    keyboard = _dashboard_keyboard()
    if anchor and anchor.get("chat_id") == chat_id and not force_new:
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
    if anchor and force_new and anchor.get("chat_id") == chat_id:
        with suppress(TelegramBadRequest, TelegramForbiddenError):
            await bot.delete_message(
                chat_id=anchor["chat_id"], message_id=anchor["message_id"]
            )
    sent = await bot.send_message(
        chat_id,
        text,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )
    await run_in_thread(db.save_anchor, DASHBOARD_ANCHOR, sent.chat.id, sent.message_id)


async def _send_dashboard_safe(bot, chat_id: int, *, force_new: bool = False) -> None:
    try:
        await _send_dashboard(bot, chat_id, force_new=force_new)
    except Exception as exc:  # pragma: no cover - defensive path
        logger.exception("Ошибка вывода панели: %s", exc)
        fallback = (
            "Панель временно недоступна, но бот активен. "
            "Попробуйте ещё раз через команду /start."
        )
        if force_new:
            anchor = await run_in_thread(db.get_anchor, DASHBOARD_ANCHOR)
            if anchor and anchor.get("chat_id") == chat_id:
                with suppress(TelegramBadRequest, TelegramForbiddenError):
                    await bot.delete_message(
                        chat_id=anchor["chat_id"], message_id=anchor["message_id"]
                    )
        sent = await bot.send_message(chat_id, fallback)
        await run_in_thread(db.save_anchor, DASHBOARD_ANCHOR, sent.chat.id, sent.message_id)


async def _refresh_dashboard(bot) -> None:
    anchor = await run_in_thread(db.get_anchor, DASHBOARD_ANCHOR)
    if not anchor:
        return
    try:
        await _render_dashboard(bot, anchor["chat_id"], anchor["message_id"])
    except Exception as exc:  # pragma: no cover - defensive path
        logger.exception("Не удалось обновить панель: %s", exc)
        await _send_dashboard_safe(bot, anchor["chat_id"], force_new=True)


@router.message(CommandStart())
async def handle_start(message: Message) -> None:
    await _send_dashboard_safe(message.bot, message.chat.id, force_new=True)


router.message.register(handle_start, Command("start"))


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


@router.callback_query(F.data == "summary:refresh")
async def handle_summary_refresh(callback: CallbackQuery) -> None:
    await _append_event("Ручное обновление панели — изменений не обнаружено")
    await _render_with_anchor(callback.message.bot, callback.message.chat.id, _render_dashboard)
    await callback.answer("Панель обновлена")


@router.callback_query(F.data == "summary:categories")
async def handle_summary_categories(callback: CallbackQuery) -> None:
    await _append_event("Открыт раздел категорий")
    await _render_with_anchor(callback.message.bot, callback.message.chat.id, _render_categories)
    await callback.answer()


@router.callback_query(F.data == "summary:tracked")
async def handle_summary_tracked(callback: CallbackQuery) -> None:
    await _append_event("Показаны отслеживаемые направления")
    await _render_with_anchor(callback.message.bot, callback.message.chat.id, _render_tracked)
    await callback.answer()


@router.callback_query(F.data == "summary:diagnostics")
async def handle_summary_diagnostics(callback: CallbackQuery) -> None:
    await _append_event("Открыта диагностика мониторинга")
    await _render_with_anchor(callback.message.bot, callback.message.chat.id, _render_diagnostics)
    await callback.answer()


@router.callback_query(F.data == "diagnostics:refresh")
async def handle_diagnostics_refresh(callback: CallbackQuery) -> None:
    await _append_event("Обновлена диагностика")
    await _render_with_anchor(callback.message.bot, callback.message.chat.id, _render_diagnostics)
    await callback.answer("Обновлено")


@router.callback_query(F.data == "summary:back")
async def handle_summary_back(callback: CallbackQuery) -> None:
    await _render_with_anchor(callback.message.bot, callback.message.chat.id, _render_dashboard)
    await callback.answer()


@router.callback_query(F.data == "summary:vpn")
async def handle_vpn_status(callback: CallbackQuery) -> None:
    snapshot = await _touch_vpn_snapshot(update_latency=True)
    text = (
        "VPN-туннель активен.\n"
        f"IP: {snapshot['ip']} ({snapshot['country']})\n"
        f"Провайдер: {snapshot['provider']}\n"
        f"Задержка: {snapshot['latency']} мс\n"
        "Соединение стабильно, мониторинг продолжает работу."
    )
    await _append_event("Проверен VPN-туннель — соединение стабильно")
    await callback.message.answer(text)
    await _refresh_dashboard(callback.message.bot)
    await callback.answer()


@router.message(F.text)
async def handle_text(message: Message) -> None:
    action = PENDING_ACTIONS.pop(message.from_user.id, None)
    text_raw = (message.text or "").strip()
    if not action:
        if text_raw.lower().startswith("/start"):
            await _send_dashboard_safe(message.bot, message.chat.id, force_new=True)
        return
    text = text_raw
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
