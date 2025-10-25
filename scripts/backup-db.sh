#!/usr/bin/env bash
set -euo pipefail

DB_PATH=${DB_PATH:-/opt/bot/data/bot.db}
BACKUP_DIR=${BACKUP_DIR:-/opt/bot/backups}
RETENTION_DAYS=${RETENTION_DAYS:-14}

mkdir -p "$BACKUP_DIR"
STAMP=$(date +%Y%m%d-%H%M%S)
TARGET="$BACKUP_DIR/bot-$STAMP.db"

if [ ! -f "$DB_PATH" ]; then
  echo "Database file $DB_PATH not found" >&2
  exit 1
fi

sqlite3 "$DB_PATH" ".backup '$TARGET'"
find "$BACKUP_DIR" -name 'bot-*.db' -type f -mtime +$RETENTION_DAYS -delete
