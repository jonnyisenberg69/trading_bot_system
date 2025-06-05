-- Trading Bot PostgreSQL Initialization Script
-- This script sets up the database schema and initial data

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Create indexes for performance
-- These will be created by SQLAlchemy migrations, but we can prepare for them

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE trading_bot TO trading_user;

-- Create custom types (if needed)
-- SQLAlchemy will handle most of this, but we can add custom types here

-- Performance tuning settings
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
ALTER SYSTEM SET max_connections = 200;
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET default_statistics_target = 100;

-- Log configuration for debugging
ALTER SYSTEM SET log_min_duration_statement = 1000;  -- Log slow queries (>1s)
ALTER SYSTEM SET log_statement = 'mod';  -- Log all DDL/DML
ALTER SYSTEM SET log_line_prefix = '%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h ';

-- Create a schema for trading bot if needed
-- SQLAlchemy will use the default 'public' schema

-- Initial notification
DO $$
BEGIN
    RAISE NOTICE 'Trading Bot PostgreSQL database initialized successfully';
    RAISE NOTICE 'Database: trading_bot';
    RAISE NOTICE 'User: trading_user';
    RAISE NOTICE 'Extensions: uuid-ossp, pg_stat_statements';
END $$;
