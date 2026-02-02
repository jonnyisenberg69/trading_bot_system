#!/bin/bash

# Start Frontend Server Script
# This script starts the React frontend server accessible from external IPs

echo "Starting Trading Bot Frontend Server..."
echo "Server will be accessible at: http://20.210.196.131:3000"
echo ""

# Navigate to frontend directory
cd frontend

# Install dependencies if needed
if [ ! -d "node_modules" ]; then
    echo "Installing dependencies..."
    npm install
fi

# Build the production version for better performance
echo "Building production version..."
npm run build

# Install serve globally if not already installed
if ! command -v serve &> /dev/null; then
    echo "Installing serve..."
    npm install -g serve
fi

# Start the server
echo "Starting server on port 3000..."
serve -s build -l 3000 --no-clipboard

# Alternative: Use the development server (uncomment if preferred)
# npm start 