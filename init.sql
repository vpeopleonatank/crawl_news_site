-- Initialize database with UTF-8 encoding
-- This file runs automatically when the container starts

-- Enable UUID extension (though not strictly needed for UUIDv7)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Set timezone
SET timezone = 'UTC';

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE crawl_db TO crawl_user;
