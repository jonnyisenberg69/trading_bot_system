#!/usr/bin/env python3
"""
Quick startup script for testing the passive quoting system.

This script will:
1. Test the system components
2. Start the API server
3. Provide instructions for frontend setup
"""

import asyncio
import subprocess
import sys
import os
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))


def print_banner():
    """Print startup banner."""
    print("""
╔═══════════════════════════════════════════════════════════════╗
║                 PASSIVE QUOTING TRADING BOT                  ║
║                     SYSTEM STARTUP                           ║
╚═══════════════════════════════════════════════════════════════╝
    """)


async def run_tests():
    """Run system tests."""
    print("🧪 Running system tests...")
    
    try:
        # Import and run the test
        from test_passive_quoting_system import main as test_main
        test_success = await test_main()
        
        if test_success:
            print("✅ All tests passed!")
            return True
        else:
            print("❌ Tests failed!")
            return False
    except Exception as e:
        print(f"❌ Test execution failed: {e}")
        return False


def start_api_server():
    """Start the API server."""
    print("\n🚀 Starting API server...")
    
    try:
        # Change to project directory
        os.chdir(PROJECT_ROOT)
        
        # Start the API server
        print("Starting FastAPI server on http://localhost:8000")
        print("API Documentation will be available at: http://localhost:8000/docs")
        
        # Run the API server
        subprocess.run([
            sys.executable, "-m", "uvicorn", 
            "api.main:app", 
            "--host", "0.0.0.0", 
            "--port", "8000", 
            "--reload"
        ])
        
    except KeyboardInterrupt:
        print("\n🛑 API server stopped by user")
    except Exception as e:
        print(f"❌ Failed to start API server: {e}")


def print_frontend_instructions():
    """Print frontend setup instructions."""
    print("""
╔═══════════════════════════════════════════════════════════════╗
║                    FRONTEND SETUP                            ║
╚═══════════════════════════════════════════════════════════════╝

To start the frontend (in a new terminal):

1. Navigate to the frontend directory:
   cd frontend

2. Install dependencies (if not already installed):
   npm install

3. Start the development server:
   npm start

4. Open your browser to:
   http://localhost:3000

5. Navigate to the Passive Quoting page:
   http://localhost:3000/passive-quoting

╔═══════════════════════════════════════════════════════════════╗
║                    USAGE INSTRUCTIONS                        ║
╚═══════════════════════════════════════════════════════════════╝

1. Configure Exchanges:
   - Go to http://localhost:3000/exchanges
   - Add your exchange API credentials
   - Test connections

2. Create Passive Quoting Bot:
   - Go to http://localhost:3000/passive-quoting
   - Configure your quote lines
   - Select exchanges
   - Create bot

3. Monitor Performance:
   - Go to http://localhost:3000/bots
   - View bot status and performance
   - Start/stop bots as needed

4. Quick Config Updates:
   - Use the "Quick Update" feature for sub-5-second updates
   - Change parameters without manually stopping/starting

╔═══════════════════════════════════════════════════════════════╗
║                 PASSIVE QUOTING FEATURES                     ║
╚═══════════════════════════════════════════════════════════════╝

✅ Multiple quote lines with different parameters
✅ Timeout-based order cancellation
✅ Drift-based order cancellation  
✅ Quantity randomization
✅ Multi-exchange support
✅ Client order ID format: pass_quote_{SIDE}_{timestamp}
✅ Base or quote currency quantities
✅ Configurable spreads in basis points
✅ Bid-only, ask-only, or both sides per line
✅ Quick configuration updates (<5 seconds)
✅ Configuration presets
✅ Real-time monitoring

Press Ctrl+C to stop the API server when done.
    """)


async def main():
    """Main startup function."""
    print_banner()
    
    # Run tests first
    test_success = await run_tests()
    
    if not test_success:
        print("\n❌ System tests failed. Please check configuration and try again.")
        sys.exit(1)
    
    # Print frontend instructions
    print_frontend_instructions()
    
    # Start API server (this will block until Ctrl+C)
    start_api_server()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
        sys.exit(0) 