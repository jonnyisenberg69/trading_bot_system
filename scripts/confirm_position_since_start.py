#!/usr/bin/env python
"""confirm_position_since_start.py

Utility script to confirm the net position for a symbol across all exchanges
since a given UTC start time using the existing TradeRepository.

Usage (from repo root):
    python scripts/confirm_position_since_start.py

Adjust SYMBOL, EXCHANGES, and START_TIME_UTC as needed.
"""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
import os, sys

import structlog

# Ensure project root is on PYTHONPATH so that `database` package resolves
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from database.connection import init_db, get_session
from database.models import Exchange
from database.repositories.trade_repository import TradeRepository

# -------------------- CONFIGURE HERE -------------------- #
SYMBOL = "BERA/USDT"
# Exchanges used by the aggressive_twap_BERAUSDT strategy
EXCHANGES = [
    "binance_spot",
    "binance_perp",
    "bybit_spot",
    "bybit_perp",
    "hyperliquid_perp",
    "mexc_spot",
    "gateio_spot",
    "bitget_spot",
]
# Strategy configured UTC start time from logs
START_TIME_UTC = datetime(2025, 6, 7, 19, 12, 0, tzinfo=timezone.utc)
# ------------------------------------------------------- #

logger = structlog.get_logger("confirm_position")


async def main():
    # Initialise DB (uses env var or default PostgreSQL URL)
    await init_db()

    async for session in get_session():
        repo = TradeRepository(session)
        total_position = Decimal("0")
        total_trades = 0
        start_time_naive = START_TIME_UTC.replace(tzinfo=None)

        logger.info(
            "Starting position confirmation",
            symbol=SYMBOL,
            start_time=str(START_TIME_UTC),
        )

        for exchange_name in EXCHANGES:
            # lookup exchange_id per exchange
            result = await session.execute(
                Exchange.__table__.select().where(Exchange.name == exchange_name)
            )
            row = result.fetchone()
            exchange_id = row[0] if row else None
            if not exchange_id:
                logger.warning("Exchange not found in DB – skipping", exchange=exchange_name)
                continue

            trades = await repo.get_trades_by_symbol(
                symbol=SYMBOL,
                exchange_id=exchange_id,
                start_time=start_time_naive,
                limit=10000,
            )

            pos = Decimal("0")
            for t in trades:
                if (t.side or "").lower() == "buy":
                    pos += Decimal(str(t.amount))
                elif (t.side or "").lower() == "sell":
                    pos -= Decimal(str(t.amount))

            logger.info(
                "Exchange summary",
                exchange=exchange_name,
                trades=len(trades),
                position=float(pos),
            )
            total_position += pos
            total_trades += len(trades)

        logger.info(
            "Total position confirmed",
            trades=total_trades,
            position=float(total_position),
            symbol=SYMBOL,
        )

if __name__ == "__main__":
    asyncio.run(main()) 