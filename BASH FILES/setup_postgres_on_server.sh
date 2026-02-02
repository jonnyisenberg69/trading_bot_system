#!/bin/bash

# Setup PostgreSQL on the server for Trading Bot System

echo "========================================="
echo "Trading Bot PostgreSQL Setup Script"
echo "========================================="

# 1. Install Docker and Docker Compose if not already installed
echo "Step 1: Checking Docker installation..."
if ! command -v docker &> /dev/null; then
    echo "Docker not found. Installing Docker..."
    curl -fsSL https://get.docker.com -o get-docker.sh
    sudo sh get-docker.sh
    sudo usermod -aG docker $USER
    echo "Docker installed. You may need to log out and back in for group changes to take effect."
fi

if ! command -v docker-compose &> /dev/null; then
    echo "Docker Compose not found. Installing..."
    sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
fi

# 2. Navigate to the docker directory
echo "Step 2: Setting up PostgreSQL with Docker..."
cd docker

# 3. Start PostgreSQL and Redis
echo "Step 3: Starting PostgreSQL and Redis services..."
docker-compose up -d postgres redis

# 4. Wait for PostgreSQL to be ready
echo "Step 4: Waiting for PostgreSQL to be ready..."
sleep 10

# 5. Check if services are running
echo "Step 5: Checking service status..."
docker-compose ps

# 6. Test PostgreSQL connection
echo "Step 6: Testing PostgreSQL connection..."
docker exec trading_bot_postgres pg_isready -U postgres -d trading_bot

echo ""
echo "========================================="
echo "Setup Complete!"
echo "========================================="
echo ""
echo "PostgreSQL is now running on port 5432"
echo "Database: trading_bot"
echo "User: trading_user"
echo "Password: trading_password_2024"
echo ""
echo "Redis is running on port 6379"
echo ""
echo "To verify the connection, run:"
echo "  python -c \"import asyncio; from database.connection import test_connection; asyncio.run(test_connection())\""
echo ""
echo "To stop services:"
echo "  cd docker && docker-compose down" 
