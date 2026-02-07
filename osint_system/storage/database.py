"""
Database Manager
================
Centralized database connection and session management using SQLAlchemy Async.
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text

from ..core.config import get_config
from ..core.logging_config import get_logger

logger = get_logger(__name__)

class DatabaseManager:
    """
    Manages database connection and session creation.
    """
    
    def __init__(self):
        self._engine = None
        self._sessionmaker = None
        
    async def connect(self) -> None:
        """Initialize database connection pool."""
        if self._engine:
            return
            
        config = get_config().storage
        # Ensure using async driver
        url = config.database_url.replace("postgresql://", "postgresql+asyncpg://")
        
        self._engine = create_async_engine(
            url,
            echo=get_config().debug,
            pool_size=config.database_pool_size,
            max_overflow=config.database_max_overflow,
            pool_pre_ping=True
        )
        
        self._sessionmaker = async_sessionmaker(
            bind=self._engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        
        logger.info("Database connection established")
        
    async def disconnect(self) -> None:
        """Close database connection pool."""
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._sessionmaker = None
            logger.info("Database connection closed")
            
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Provides a database session (for dependency injection)."""
        if not self._sessionmaker:
            await self.connect()
            
        async with self._sessionmaker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
                
    async def create_tables(self) -> None:
        """Create all tables defined in metadata."""
        from .models import Base
        
        if not self._engine:
            await self.connect()
            
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables created/verified")

# Global DB manager instance
_db_manager = DatabaseManager()

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency for getting DB session."""
    async for session in _db_manager.get_session():
        yield session
