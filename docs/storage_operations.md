## Storage Operations

The ingestion pipeline now supports multiple storage volumes with automatic pause
handling when a disk approaches capacity.

### Configure Volumes via `.env`

Add the following keys to your `.env` file (the same file Docker Compose
already consumes):

```
STORAGE_VOLUMES=primary:/app/storage;hdd02:/app/storage/storage02
STORAGE_ACTIVE_VOLUME=primary
STORAGE_WARN_THRESHOLD=0.9   # 90% usage
STORAGE_PAUSE_FILE=/app/storage/.pause_ingest
STORAGE_NOTIFY_TELEGRAM_BOT_TOKEN=123456789:ABCDEF
STORAGE_NOTIFY_TELEGRAM_CHAT_ID=-10011223344
# Optional: thread/topic for forum-style chats
STORAGE_NOTIFY_TELEGRAM_THREAD_ID=42
```

- `STORAGE_VOLUMES` is a semicolon-separated list of `name:path` pairs. Paths may be
  absolute or relative to the container.
- `STORAGE_ACTIVE_VOLUME` selects the mount that new downloads use.
- `STORAGE_WARN_THRESHOLD` accepts either a decimal (0.9) or percentage (90). When
  the active partition exceeds the threshold, ingestion drops a pause sentinel file.
- `STORAGE_PAUSE_FILE` controls where the sentinel is created. Defaults to
  `<active_volume>/.pause_ingest`.
- `STORAGE_NOTIFY_TELEGRAM_BOT_TOKEN` and `STORAGE_NOTIFY_TELEGRAM_CHAT_ID` enable
  Telegram notifications when storage crosses the warn threshold. `STORAGE_NOTIFY_TELEGRAM_THREAD_ID`
  is optional and targets a specific topic in a forum-style group.

When no volumes are defined, the CLI `--storage-root` flag behaves exactly as
before. Because the `.env` file is mounted into the `test_app` container at
`/app/.env`, you can invoke the helper CLI via Docker Compose:

```bash
docker compose run --rm test_app \
  python -m crawler.storage usage --env /app/.env
```

### Runtime Behaviour

- Each run checks the pause sentinel before fetching jobs. If the file exists or
  the active volume exceeds the warn threshold, ingestion stops after the current
  article and logs a warning.
- When Telegram notifications are configured, the system sends a message the first
  time the threshold is exceeded and the pause sentinel is created.
- In Celery tasks, downloaded asset paths now include the volume identifier
  (`volume:relative/path`). Existing records without a prefix continue to work.

### Add a New Disk Without Rebuilding Containers

1. Format and mount the disk on the host (e.g. `/mnt/storage02`).
2. Bind mount the disk into the existing bind (this keeps `docker-compose.yml`
   unchanged):
   ```bash
   sudo mkdir -p storage/storage02
   sudo mount --bind /mnt/storage02 storage/storage02
   ```
3. Register the mount in `.env` by appending to `STORAGE_VOLUMES`.
4. Switch the active volume from the container:
   ```bash
   docker compose run --rm test_app \
     python -m crawler.storage set-active hdd02 --env /app/.env
   ```
5. Resume ingestion once the pause sentinel is cleared and restart any
   `docker compose run --rm test_app python -m crawler.ingest ...` processes.

### Pause and Resume Manually

```bash
docker compose run --rm test_app python -m crawler.storage pause --env /app/.env
docker compose run --rm test_app python -m crawler.storage resume --env /app/.env
docker compose run --rm test_app python -m crawler.storage usage --env /app/.env
```

These commands default to `./.env` but accept `--env` to target a different file.

### Operational Runbook (Docker Compose)

1. **Pause ingestion** for each running `docker compose run --rm test_app python -m crawler.ingest …` terminal with `Ctrl+C`, then create the pause sentinel:
   ```bash
   docker compose run --rm test_app python -m crawler.storage pause --env /app/.env
   ```
2. **Mount the new disk** on the host and bind it into the existing `storage/` directory (`sudo mount --bind /mnt/storage02 storage/storage02`).
3. **Edit `.env`** on the host, adding the new volume entry to `STORAGE_VOLUMES`.
4. **Switch the active volume** from inside the container:
   ```bash
   docker compose run --rm test_app python -m crawler.storage set-active hdd02 --env /app/.env
   ```
5. **Resume ingestion** by clearing the sentinel and restarting CLI runs:
   ```bash
   docker compose run --rm test_app python -m crawler.storage resume --env /app/.env
   ```
   Relaunch each ingest command after this step. Existing assets remain on their original volume; new downloads land on the active mount.
