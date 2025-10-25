Now let me create the Dockerfile:

Now let me create the requirements.txt:

Now let me create a test script:

Finally, let me create an optional init.sql file:

Perfect! I've created a complete Docker Compose setup for testing your models. Here's what you need:

**File structure:**
```
project/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── init.sql
├── models.py          # Your SQLAlchemy models
└── test_models.py     # Test script
```

**To run the tests:**

```bash
# Copy the environment template (edit .env as needed)
cp .env.sample .env

# Start all services
docker compose up

# Or run in detached mode
docker compose up -d

# View logs
docker compose logs -f test_app

# Stop everything
docker compose down

# Clean up volumes (removes all data)
docker compose down -v
```

**What's included:**

1. **PgBouncer** - Connection pooler on port 6432 (default DSN target)
2. **PostgreSQL 16** - Database server on port 5432 (direct maintenance access)
3. **RabbitMQ 3 management** - Message broker on port 5672 with UI at http://localhost:15672 (crawler / crawler_password)
4. **pgAdmin** - Web UI at http://localhost:5050 (admin@admin.com / admin)
5. **Test App** - Automatically runs tests when started
6. **Flower** - Celery monitoring at http://localhost:5555 (configurable via `FLOWER_PORT`)

**The test script validates:**
- ✅ UUIDv7 generation
- ✅ Unicode support (Chinese, Vietnamese, emojis)
- ✅ Image and video relationships
- ✅ JSONB comments field
- ✅ Cascade delete
- ✅ Various queries (filter, search, ordering)

**Celery configuration:**
- Broker: `amqp://crawler:crawler_password@rabbitmq:5672//`
- Result backend: `db+postgresql://crawl_user:crawl_password@pgbouncer:6432/crawl_db`
- Toggle eager execution by setting `CRAWLER_CELERY_TASK_ALWAYS_EAGER=false` (defaults to false in Docker)
- Storage volume is bind-mounted (`./storage:/app/storage`) so downloaded assets persist on the host
- Clear pending tasks with `docker compose exec rabbitmq rabbitmqctl purge_queue celery` if you need a clean queue
- All of the above are parametrised via `.env`; copy `.env.sample` and adjust values before running

**PgBouncer monitoring:**
- `scripts/pgbouncer_status.sh` runs `SHOW POOLS` through the PgBouncer admin console for a quick view of active/backlogged connections.

RabbitMQ queues stay durable, and task results persist in PostgreSQL so outstanding work resumes after restarts.

Monitor Celery workers and task states with Flower:

```bash
# Launch Flower alongside the rest of the stack
docker compose up flower

# Or start everything (including Flower) in the background
docker compose up -d
```

Then visit http://localhost:5555 (or the port defined in `FLOWER_PORT`) to inspect queues, tasks, and worker heartbeats in real time.

Requeue failed Celery tasks that are persisted in the SQLAlchemy backend with the helper script in `scripts/requeue_failed_tasks.py`:

```bash
docker compose run --rm test_app \
  python scripts/requeue_failed_tasks.py --dry-run
```

Remove `--dry-run` after inspecting the log output to actually requeue the candidates. When `CRAWLER_CELERY_RESULT_BACKEND` is set in `.env`, the script picks it up automatically; otherwise pass `--backend-url db+postgresql://crawl_user:crawl_password@pgbouncer:6432/crawl_db` explicitly. Use flags like `--task-name crawler.download_assets` or `--since 2024-03-01T00:00:00Z` to narrow the selection before resubmitting.

**To connect to pgAdmin:**
1. Open http://localhost:5050
2. Add server: Host=pgbouncer, Port=6432, User=crawl_user, Password=crawl_password

You can also connect directly using any PostgreSQL client:
```
Host: localhost
Port: 6432 (PgBouncer) or 5432 (direct Postgres)
Database: crawl_db
User: crawl_user
Password: crawl_password
```

The test will show detailed output with all operations and confirm everything works correctly!

## Storage Management

Multi-volume storage management, automatic pause handling, and the helper CLI are
documented in `docs/storage_operations.md`. Review that runbook before adding a
new disk or switching the active storage mount.
