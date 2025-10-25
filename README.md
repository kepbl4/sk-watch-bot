# SK Watch Bot

## Деплой
Проект деплоится автоматически через GitHub Actions workflow `.github/workflows/deploy.yml`. Достаточно запушить изменения в ветку `main`, и скрипт доставит код и зависимости на сервер. В процессе деплоя создаются systemd-таймеры `bot-health.timer` (smoke-check каждые 2 минуты) и `bot-dump.timer` (ночной бэкап базы).

## Размещение на сервере
Сервис установлен на VPS в каталоге `/opt/bot`. Systemd unit называется `bot.service`. Таймеры и сервисы можно проверить командой:

```
systemctl list-timers | grep bot-
```

## Логи
Для просмотра логов используйте команду:

```
journalctl -u bot -n 100 -f
```

## Утилиты

В директории `scripts/` доступны сервисные скрипты:

- `scripts/smoke-check` — быстрый self-test (чтение конфигурации, статистика по БД и HTTP-проверка портала). Используется таймером `bot-health.timer`.
- `scripts/db-inspect` — выводит состояние категорий/городов/включённых целей.
- `scripts/backup-db.sh` — резервное копирование SQLite с ретеншеном 14 дней (используется таймером `bot-dump.timer`).
