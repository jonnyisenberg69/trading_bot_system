#!/bin/bash

# Setup trading_bot database in existing PostgreSQL installation

echo "========================================="
echo "Setting up Trading Bot Database"
echo "========================================="

# Create the database and user
sudo -u postgres psql << EOF
-- Create user if it doesn't exist
DO
\$\$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_user
      WHERE usename = 'jonnyisenberg') THEN
      CREATE USER jonnyisenberg WITH PASSWORD 'hello';
   END IF;
END
\$\$;

-- Create database if it doesn't exist
SELECT 'CREATE DATABASE trading_bot OWNER jonnyisenberg'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'trading_bot')\gexec

-- Grant all privileges
GRANT ALL PRIVILEGES ON DATABASE trading_bot TO jonnyisenberg;

-- Connect to trading_bot and set up extensions
\c trading_bot

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Grant schema permissions
GRANT ALL ON SCHEMA public TO jonnyisenberg;

\q
EOF

echo ""
echo "========================================="
echo "Database Setup Complete!"
echo "========================================="
echo ""
echo "Created database: trading_bot"
echo "Created user: jonnyisenberg"
echo "Password: hello"
echo ""
echo "To test the connection, update your .env file or environment:"
echo "  export DATABASE_URL='postgresql+asyncpg://jonnyisenberg:hello@localhost:5432/trading_bot'"
echo ""
echo "Or update database/connection.py to use these credentials" 
