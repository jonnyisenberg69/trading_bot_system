"""
Database connection management module.

Provides functions for creating and managing database connections.
"""

import os
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy import text
import structlog

from .models import Base

logger = structlog.get_logger(__name__)

# Database configuration with environment variable support
DEFAULT_POSTGRES_URL = "postgresql+asyncpg://jonnyisenberg:hello@localhost:5432/trading_bot"
DEFAULT_SQLITE_PATH = os.path.join(os.getcwd(), "data", "trading_bot.db")

# SQLAlchemy async engine
engine = None


def get_database_url():
    """Get database URL from environment or use PostgreSQL default."""
    # Check for environment variable first
    db_url = os.getenv('DATABASE_URL')
    if db_url:
        return db_url
    
    # Check for individual PostgreSQL components
    pg_host = os.getenv('POSTGRES_HOST', 'localhost')
    pg_port = os.getenv('POSTGRES_PORT', '5432')
    pg_db = os.getenv('POSTGRES_DB', 'trading_bot')
    pg_user = os.getenv('POSTGRES_USER', 'jonnyisenberg')
    pg_password = os.getenv('POSTGRES_PASSWORD', 'hello')
    
    # Try PostgreSQL first
    postgres_url = f"postgresql+asyncpg://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    
    # Check if we should force SQLite (for development/testing)
    if os.getenv('USE_SQLITE', '').lower() in ('true', '1', 'yes'):
        data_dir = os.path.dirname(DEFAULT_SQLITE_PATH)
        os.makedirs(data_dir, exist_ok=True)
        return f"sqlite+aiosqlite:///{DEFAULT_SQLITE_PATH}"
    
    return postgres_url


async def init_db(db_url=None):
    """
    Initialize the database connection and return a sessionmaker.
    Args:
        db_url: Database URL (default: PostgreSQL from environment)
    Returns:
        sessionmaker: The async session maker for the DB
    """
    global engine
    if not db_url:
        db_url = get_database_url()
    logger.info(f"Initializing database connection to {db_url}")
    # Configure engine based on database type
    if 'sqlite' in db_url:
        engine = create_async_engine(
            db_url,
            echo=False,
            future=True,
            poolclass=NullPool,
        )
    else:
        engine = create_async_engine(
            db_url,
            echo=False,
            future=True,
            pool_size=20,
            max_overflow=30,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
    async_session_maker = sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        if 'postgresql' in db_url:
            logger.warning("PostgreSQL connection failed, falling back to SQLite")
            await close_db()
            sqlite_url = f"sqlite+aiosqlite:///{DEFAULT_SQLITE_PATH}"
            data_dir = os.path.dirname(DEFAULT_SQLITE_PATH)
            os.makedirs(data_dir, exist_ok=True)
            engine = create_async_engine(
                sqlite_url,
                echo=False,
                future=True,
                poolclass=NullPool,
            )
            async_session_maker = sessionmaker(
                bind=engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("SQLite fallback database initialized successfully")
        else:
            raise
    return async_session_maker


async def get_session():
    """
    Get an async database session.
    
    Yields:
        AsyncSession: Database session
    """
    # Create a temporary session maker if needed
    global engine
    if not engine:
        await init_db()
    
    async_session_maker = sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    
    async with async_session_maker() as session:
        yield session


async def close_db():
    """Close the database connection."""
    global engine
    
    if engine:
        await engine.dispose()
        logger.info("Database connection closed")


async def test_connection():
    """Test the database connection."""
    try:
        async for session in get_session():
            # Simple test query using text() for proper SQL
            if 'postgresql' in str(engine.url):
                result = await session.execute(text("SELECT version()"))
                row = result.fetchone()
                if row:
                    logger.info(f"PostgreSQL connection test successful: {row[0]}")
                    return True
            else:
                result = await session.execute(text("SELECT 1"))
                row = result.fetchone()
                if row and row[0] == 1:
                    logger.info("SQLite connection test successful")
                    return True
                    
            logger.error("Database connection test failed")
            return False
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False


async def get_database_info():
    """Get information about the current database connection."""
    if not engine:
        return {"status": "not_connected"}
    
    try:
        async for session in get_session():
            db_type = "postgresql" if "postgresql" in str(engine.url) else "sqlite"
            
            if db_type == "postgresql":
                result = await session.execute(text("SELECT version(), current_database(), current_user"))
                row = result.fetchone()
                return {
                    "status": "connected",
                    "type": "postgresql",
                    "version": row[0] if row else "unknown",
                    "database": row[1] if row else "unknown",
                    "user": row[2] if row else "unknown",
                    "url": str(engine.url).replace(engine.url.password, "***") if engine.url.password else str(engine.url)
                }
            else:
                result = await session.execute(text("SELECT sqlite_version()"))
                row = result.fetchone()
                return {
                    "status": "connected",
                    "type": "sqlite",
                    "version": row[0] if row else "unknown",
                    "database": str(engine.url.database),
                    "url": str(engine.url)
                }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }
