#!/bin/bash

# Start Frontend Development Server Script
# This script starts the React frontend in development mode

echo "Starting Trading Bot Frontend in Development Mode..."
echo "Server will be accessible at: http://20.210.196.131:3000"
echo ""
echo "Note: This is development mode. For production, use start_frontend_server.sh"
echo ""

# Navigate to frontend directory
cd frontend

# Install dependencies if needed
if [ ! -d "node_modules" ]; then
    echo "Installing dependencies..."
    npm install
fi

# Export environment variables
export HOST=0.0.0.0
export PORT=3000
export REACT_APP_API_URL=http://20.210.196.131:8080

# Start the development server
echo "Starting development server on 0.0.0.0:3000..."
npm start 