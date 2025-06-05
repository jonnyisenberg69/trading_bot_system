#!/bin/bash

# Trading Bot System Startup Script
echo "🚀 Starting Trading Bot System..."

# Check if we're in the correct directory
if [ ! -f "main.py" ]; then
    echo "❌ Error: Please run this script from the trading_bot_system directory"
    exit 1
fi

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check dependencies
echo "🔍 Checking dependencies..."

if ! command_exists python3; then
    echo "❌ Python 3 is required"
    exit 1
fi

if ! command_exists node; then
    echo "❌ Node.js is required"
    exit 1
fi

if ! command_exists npm; then
    echo "❌ npm is required"
    exit 1
fi

# Install Python dependencies if needed
if [ ! -d ".venv" ]; then
    echo "📦 Creating Python virtual environment..."
    python3 -m venv .venv
fi

echo "📦 Installing Python dependencies..."
source .venv/bin/activate
pip install -r requirements.txt

# Install frontend dependencies if needed
if [ ! -d "frontend/node_modules" ]; then
    echo "📦 Installing frontend dependencies..."
    cd frontend
    npm install
    cd ..
fi

# Start backend API in background
echo "🔧 Starting backend API server..."
source .venv/bin/activate
cd api
python main.py &
BACKEND_PID=$!
cd ..

# Wait for backend to start
echo "⏳ Waiting for backend to start..."
sleep 5

# Start frontend development server
echo "🎨 Starting frontend development server..."
cd frontend
npm start &
FRONTEND_PID=$!
cd ..

echo "✅ Trading Bot System started!"
echo ""
echo "🔗 Frontend: http://localhost:3000"
echo "🔗 API Backend: http://localhost:8000"
echo "🔗 API Docs: http://localhost:8000/docs"
echo ""
echo "To stop the system, press Ctrl+C"

# Wait for user to stop
trap "echo '🛑 Stopping servers...'; kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit" INT

# Keep script running
wait 