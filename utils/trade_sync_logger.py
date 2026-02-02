"""
Trade Sync Logging Utility

Dedicated logging for trade fetching and synchronization operations.
Creates separate log files for trade sync operations with detailed tracking.
"""

import logging
import structlog
from pathlib import Path
from datetime import datetime, timezone
import os


class TradeSyncLogger:
    """
    Trade sync logging class that provides structured logging for trade operations.
    """
    
    def __init__(self, log_dir: str = "logs/trade_sync"):
        """
        Initialize the trade sync logger.
        
        Args:
            log_dir: Directory for trade sync log files
        """
        self.log_dir = log_dir
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> structlog.BoundLogger:
        """Set up the structured logger."""
        # Create log directory
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)
        
        # Create log file with timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        log_file = Path(self.log_dir) / f"trade_sync_{timestamp}.log"
        
        # Set up file handler
        file_handler = logging.FileHandler(str(log_file))
        file_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        
        # Create logger
        logger = logging.getLogger('trade_sync')
        logger.setLevel(logging.INFO)
        
        # Clear existing handlers to avoid duplicates
        logger.handlers.clear()
        
        logger.addHandler(file_handler)
        
        # Also add console handler for immediate feedback
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # Create structlog logger
        structlog_logger = structlog.get_logger('trade_sync')
        
        return structlog_logger
    
    def log_trade_fetch_start(self, exchange: str, symbol: str, source: str = 'api', since: datetime = None):
        """Log the start of trade fetching operation."""
        self.logger.info(
            "Starting trade fetch",
            exchange=exchange,
            symbol=symbol,
            source=source,
            since=since.isoformat() if since else None,
            operation="trade_fetch_start"
        )
    
    def log_trade_fetch_result(self, exchange: str, symbol: str, trades_count: int, 
                              time_taken_ms: int = 0, success: bool = True, error: str = None):
        """Log the result of trade fetching operation."""
        self.logger.info(
            "Trade fetch completed",
            exchange=exchange,
            symbol=symbol,
            trades_count=trades_count,
            time_taken_ms=time_taken_ms,
            success=success,
            error=error,
            operation="trade_fetch_result"
        )
    
    def log_trade_storage(self, exchange: str, symbol: str, new_trades: int, 
                         duplicates: int, source: str = 'api', total_stored: int = None):
        """Log trade storage operations."""
        if total_stored is None:
            total_stored = new_trades
            
        self.logger.info(
            "Trade storage completed",
            exchange=exchange,
            symbol=symbol,
            new_trades=new_trades,
            duplicates=duplicates,
            total_stored=total_stored,
            source=source,
            operation="trade_storage"
        )
    
    def log_position_update(self, exchange: str, symbol: str, position_size: float, 
                           position_value: float, avg_price: float = None):
        """Log position updates from trade data."""
        self.logger.info(
            "Position updated from trades",
            exchange=exchange,
            symbol=symbol,
            position_size=position_size,
            position_value=position_value,
            avg_price=avg_price,
            operation="position_update"
        )
    
    def log_sync_summary(self, total_exchanges: int, successful: int, failed: int, 
                        total_trades: int, total_time_ms: int):
        """Log overall sync operation summary."""
        self.logger.info(
            "Sync operation summary",
            total_exchanges=total_exchanges,
            successful=successful,
            failed=failed,
            total_trades=total_trades,
            total_time_ms=total_time_ms,
            success_rate=f"{(successful/total_exchanges*100):.1f}%" if total_exchanges > 0 else "0%",
            operation="sync_summary"
        )


def setup_trade_sync_logger(log_dir: str = "logs/trade_sync") -> structlog.BoundLogger:
    """
    Set up dedicated logger for trade sync operations.
    
    Args:
        log_dir: Directory for trade sync log files
        
    Returns:
        Configured structlog logger
    """
    # Create log directory
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Create log file with timestamp
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_file = Path(log_dir) / f"trade_sync_{timestamp}.log"
    
    # Set up file handler
    file_handler = logging.FileHandler(str(log_file))
    file_handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    
    # Create logger
    logger = logging.getLogger('trade_sync')
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    
    # Also add console handler for immediate feedback
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Create structlog logger
    structlog_logger = structlog.get_logger('trade_sync')
    
    return structlog_logger


def log_trade_fetch_start(logger: structlog.BoundLogger, exchange: str, symbol: str, since: datetime = None):
    """Log the start of trade fetching operation."""
    logger.info(
        "Starting trade fetch",
        exchange=exchange,
        symbol=symbol,
        since=since.isoformat() if since else None,
        operation="trade_fetch_start"
    )


def log_trade_fetch_result(logger: structlog.BoundLogger, exchange: str, symbol: str, 
                          trades_count: int, time_taken_ms: int, success: bool = True, error: str = None):
    """Log the result of trade fetching operation."""
    logger.info(
        "Trade fetch completed",
        exchange=exchange,
        symbol=symbol,
        trades_count=trades_count,
        time_taken_ms=time_taken_ms,
        success=success,
        error=error,
        operation="trade_fetch_result"
    )


def log_trade_storage(logger: structlog.BoundLogger, exchange: str, symbol: str,
                     new_trades: int, duplicates: int, total_stored: int):
    """Log trade storage operations."""
    logger.info(
        "Trade storage completed",
        exchange=exchange,
        symbol=symbol,
        new_trades=new_trades,
        duplicates=duplicates,
        total_stored=total_stored,
        operation="trade_storage"
    )


def log_position_update(logger: structlog.BoundLogger, exchange: str, symbol: str,
                       position_size: float, position_value: float, avg_price: float = None):
    """Log position updates from trade data."""
    logger.info(
        "Position updated from trades",
        exchange=exchange,
        symbol=symbol,
        position_size=position_size,
        position_value=position_value,
        avg_price=avg_price,
        operation="position_update"
    )


def log_sync_summary(logger: structlog.BoundLogger, total_exchanges: int, successful: int,
                    failed: int, total_trades: int, total_time_ms: int):
    """Log overall sync operation summary."""
    logger.info(
        "Sync operation summary",
        total_exchanges=total_exchanges,
        successful=successful,
        failed=failed,
        total_trades=total_trades,
        total_time_ms=total_time_ms,
        success_rate=f"{(successful/total_exchanges*100):.1f}%" if total_exchanges > 0 else "0%",
        operation="sync_summary"
    ) 