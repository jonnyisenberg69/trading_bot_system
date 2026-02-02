#!/usr/bin/env python
"""check_ws_missed_trades.py

Fetches fresh trade history from each exchange connector for BERA/USDT,
compares it to what is already stored in the database since the strategy
start time, and reports any trades that appear to be missing (i.e. not
recorded by the WebSocket / initial sync).

Nothing is written to the DB – we only compare and print a summary.
"""

import asyncio
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Any
import structlog

# Ensure repo root on path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from database.connection import init_db, get_session
from database.models import Exchange
from database.repositories.trade_repository import TradeRepository

from api.services.exchange_manager import ExchangeManager
import json
CONFIG_PATH = os.path.join(REPO_ROOT, 'config', 'config.json')

logger = structlog.get_logger("check_ws_missed_trades")

SYMBOL = "BERA/USDT"
START_TIME_UTC = datetime(2025, 6, 7, 19, 12, 0, tzinfo=timezone.utc)


async def main():
    # ------------------ DB ------------------ #
    await init_db()

    # ---------------- Exchanges ------------- #
    with open(CONFIG_PATH, 'r') as f:
        cfg = json.load(f)

    manager = ExchangeManager(cfg)
    await manager.start()
    connectors: Dict[str, Any] = manager.get_exchange_connectors()
    logger.info("Connected exchanges", count=len(connectors))

    start_ts_ms = int(START_TIME_UTC.timestamp() * 1000)
    start_time_naive = START_TIME_UTC.replace(tzinfo=None)

    async for session in get_session():
        repo = TradeRepository(session)

        total_missing = 0
        for conn_id, connector in connectors.items():
            # Resolve exchange_id once
            result = await session.execute(
                Exchange.__table__.select().where(Exchange.name == conn_id)
            )
            row = result.fetchone()
            exchange_id = row[0] if row else None
            if exchange_id is None:
                logger.warning("Exchange not in DB", exchange=conn_id)
                continue

            logger.info("Fetching live trades", exchange=conn_id)
            try:
                trades = await connector.get_trade_history(SYMBOL)
            except Exception as e:
                logger.error("Fetch error", exchange=conn_id, error=str(e))
                continue

            # Filter trades that are >= strategy start time
            recent_trades = [t for t in trades if t.get("timestamp", 0) >= start_ts_ms]
            missing = []
            for t in recent_trades:
                trade_id = str(t.get("id"))
                existing = await repo.get_trade_by_exchange_id(exchange_id, trade_id)
                if existing is None:
                    missing.append(trade_id)
            logger.info(
                "Exchange checked",
                exchange=conn_id,
                fetched=len(recent_trades),
                missing=len(missing),
            )
            if missing:
                total_missing += len(missing)
                for tid in missing[:10]:  # show up to 10 ids
                    logger.warning("Missing trade", exchange=conn_id, trade_id=tid)

        logger.info("Check complete", total_missing=total_missing)

    # Clean up connectors
    await manager.stop()

if __name__ == "__main__":
    asyncio.run(main()) 