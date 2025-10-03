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

1. **PostgreSQL 16** - Database server on port 5432
2. **pgAdmin** - Web UI at http://localhost:5050 (admin@admin.com / admin)
3. **Test App** - Automatically runs tests when started

**The test script validates:**
- ✅ UUIDv7 generation
- ✅ Unicode support (Chinese, Vietnamese, emojis)
- ✅ Image and video relationships
- ✅ JSONB comments field
- ✅ Cascade delete
- ✅ Various queries (filter, search, ordering)

**To connect to pgAdmin:**
1. Open http://localhost:5050
2. Add server: Host=postgres, Port=5432, User=crawl_user, Password=crawl_password

You can also connect directly using any PostgreSQL client:
```
Host: localhost
Port: 5432
Database: crawl_db
User: crawl_user
Password: crawl_password
```

The test will show detailed output with all operations and confirm everything works correctly!
