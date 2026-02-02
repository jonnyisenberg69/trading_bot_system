"""
Database models for the trading system.

This module defines SQLAlchemy models for database tables.
"""

from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, JSON, Text, Enum, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
from enum import Enum as PyEnum

Base = declarative_base()


class BotInstance(Base):
    """Bot instance model."""
    
    __tablename__ = 'bot_instances'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    strategy = Column(String(255), nullable=False)
    status = Column(String(50), nullable=False, default='inactive')
    config = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_active = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    trades = relationship("Trade", back_populates="bot_instance")
    positions = relationship("Position", back_populates="bot_instance")


class Exchange(Base):
    """Exchange model."""
    
    __tablename__ = 'exchanges'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    type = Column(String(50), nullable=False)  # spot, perp, etc.
    api_key_id = Column(String(255), nullable=True)  # Reference to encrypted API key
    is_active = Column(Boolean, default=True)
    
    # Relationships
    trades = relationship("Trade", back_populates="exchange")
    positions = relationship("Position", back_populates="exchange")


class Trade(Base):
    """Trade model for storing trade details."""
    
    __tablename__ = 'trades'
    
    id = Column(Integer, primary_key=True)
    exchange_id = Column(Integer, ForeignKey('exchanges.id'), nullable=False)
    bot_instance_id = Column(Integer, ForeignKey('bot_instances.id'), nullable=True)
    exchange_trade_id = Column(String(255), nullable=False)
    symbol = Column(String(50), nullable=False)
    side = Column(String(10), nullable=False)  # buy, sell
    amount = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    cost = Column(Float, nullable=False)
    fee_cost = Column(Float, nullable=True)
    fee_currency = Column(String(50), nullable=True)
    timestamp = Column(DateTime, nullable=False)
    order_id = Column(String(255), nullable=True)
    trade_type = Column(String(50), nullable=True)  # market, limit, etc.
    is_maker = Column(Boolean, default=False)
    is_liquidation = Column(Boolean, default=False)
    p1_delta = Column(Float, nullable=True)  # Change in base coin
    p2_delta = Column(Float, nullable=True)  # Change in quote coin
    raw_data = Column(JSON, nullable=True)  # Store original trade data
    processed = Column(Boolean, default=False)  # Flag for position calculation
    created_at = Column(DateTime, default=datetime.utcnow)
    # New: Optional client order ID for tracking (not required, not unique)
    client_order_id = Column(String(255), nullable=True)
    
    # Relationships
    exchange = relationship("Exchange", back_populates="trades")
    bot_instance = relationship("BotInstance", back_populates="trades")
    
    def __repr__(self):
        return f"<Trade(id={self.id}, exchange={self.exchange_id}, symbol={self.symbol}, side={self.side})>"


class Position(Base):
    """Position model for tracking positions."""
    
    __tablename__ = 'positions'
    
    id = Column(Integer, primary_key=True)
    exchange_id = Column(Integer, ForeignKey('exchanges.id'), nullable=False)
    bot_instance_id = Column(Integer, ForeignKey('bot_instances.id'), nullable=True)
    symbol = Column(String(50), nullable=False)
    p1 = Column(Float, nullable=False, default=0)  # Base coin amount
    p2 = Column(Float, nullable=False, default=0)  # Quote coin amount
    p1_fee = Column(Float, nullable=False, default=0)  # Fee in base coin
    p2_fee = Column(Float, nullable=False, default=0)  # Fee in quote coin
    avg_price = Column(Float, nullable=True)  # Average price
    size = Column(Float, nullable=False, default=0)  # Position size
    side = Column(String(10), nullable=True)  # long, short, or null
    is_open = Column(Boolean, default=False)
    entry_time = Column(DateTime, nullable=True)
    last_update_time = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    exchange = relationship("Exchange", back_populates="positions")
    bot_instance = relationship("BotInstance", back_populates="positions")
    
    def __repr__(self):
        return f"<Position(id={self.id}, exchange={self.exchange_id}, symbol={self.symbol}, size={self.size})>"


class TradeSync(Base):
    """Track the last trade synchronization time for each exchange and symbol."""
    
    __tablename__ = 'trade_syncs'
    
    id = Column(Integer, primary_key=True)
    exchange_id = Column(Integer, ForeignKey('exchanges.id'), nullable=False)
    symbol = Column(String(50), nullable=False)
    last_sync_time = Column(DateTime, nullable=False)
    last_trade_id = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<TradeSync(exchange={self.exchange_id}, symbol={self.symbol}, last_sync={self.last_sync_time})>"


class RealizedPNL(Base):
    """Model for tracking realized PNL events."""
    __tablename__ = 'realized_pnl'

    id = Column(Integer, primary_key=True)
    exchange_id = Column(Integer, ForeignKey('exchanges.id'), nullable=False)
    bot_instance_id = Column(Integer, ForeignKey('bot_instances.id'), nullable=True)
    symbol = Column(String(50), nullable=False)
    realized_pnl = Column(Float, nullable=False)
    event_type = Column(String(50), nullable=True)  # close, flip, etc.
    trade_id = Column(Integer, ForeignKey('trades.id'), nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)

    exchange = relationship("Exchange")
    bot_instance = relationship("BotInstance")
    trade = relationship("Trade")

    def __repr__(self):
        return f"<RealizedPNL(id={self.id}, exchange={self.exchange_id}, symbol={self.symbol}, pnl={self.realized_pnl})>"


class BalanceHistory(Base):
    """Model for tracking historical balances over time."""
    __tablename__ = 'balance_history'

    id = Column(Integer, primary_key=True)
    exchange_id = Column(Integer, ForeignKey('exchanges.id'), nullable=False)
    bot_instance_id = Column(Integer, ForeignKey('bot_instances.id'), nullable=True)
    currency = Column(String(50), nullable=False)
    total = Column(Float, nullable=False)
    free = Column(Float, nullable=True)
    used = Column(Float, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)

    exchange = relationship("Exchange")
    bot_instance = relationship("BotInstance")

    def __repr__(self):
        return f"<BalanceHistory(id={self.id}, exchange={self.exchange_id}, currency={self.currency}, total={self.total})>"


class InventoryPriceHistory(Base):
    """Model for tracking inventory price (avg entry price) changes over time."""
    __tablename__ = 'inventory_price_history'

    id = Column(Integer, primary_key=True)
    exchange_id = Column(Integer, ForeignKey('exchanges.id'), nullable=False)
    bot_instance_id = Column(Integer, ForeignKey('bot_instances.id'), nullable=True)
    symbol = Column(String(50), nullable=False)
    avg_price = Column(Float, nullable=True)
    size = Column(Float, nullable=False)
    unrealized_pnl = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)

    exchange = relationship("Exchange")
    bot_instance = relationship("BotInstance")

    def __repr__(self):
        return f"<InventoryPriceHistory(id={self.id}, exchange={self.exchange_id}, symbol={self.symbol}, avg_price={self.avg_price}, size={self.size})>"
