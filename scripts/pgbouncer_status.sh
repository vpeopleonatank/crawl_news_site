#!/bin/sh
set -euo pipefail

SERVICE_NAME=${PGBOUNCER_SERVICE:-pgbouncer}
USER=${PGBOUNCER_USER:-${POSTGRES_USER:-crawl_user}}
PORT=${PGBOUNCER_PORT:-6432}
PASSWORD=${PGBOUNCER_PASSWORD:-${POSTGRES_PASSWORD:-}}

if [ -z "$PASSWORD" ]; then
  echo "PGBOUNCER_PASSWORD or POSTGRES_PASSWORD must be set for authentication" >&2
  exit 1
fi

docker compose exec -T "$SERVICE_NAME" \
  -e PGPASSWORD="$PASSWORD" \
  psql -h localhost -p "$PORT" -U "$USER" pgbouncer \
  -c "SHOW POOLS;"
