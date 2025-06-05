#!/usr/bin/env python
"""
Test database connection with new credentials
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from database.connection import init_db, test_connection, get_database_info, close_db

async def test_pg():
    try:
        print("🔧 Testing PostgreSQL connection with new credentials...")
        await init_db()
        success = await test_connection()
        info = await get_database_info()
        
        print(f"📊 Database info: {info}")
        print(f"✅ Connection test result: {success}")
        
        if info.get('type') == 'postgresql':
            print(f"🎉 Successfully connected to PostgreSQL as user: {info.get('user')}")
        else:
            print(f"⚠️  Using {info.get('type')} instead of PostgreSQL")
        
        await close_db()
        return success
    except Exception as e:
        print(f'❌ Error: {e}')
        return False

if __name__ == "__main__":
    result = asyncio.run(test_pg())
    print(f'🏁 Overall result: {result}') 