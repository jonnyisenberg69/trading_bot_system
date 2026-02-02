"""
Data structures for market data management.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple, Set
from decimal import Decimal
from datetime import datetime


@dataclass
class MarketDataConfig:
    """Configuration for market data services."""
    update_interval: float = 1.0
    snapshot_interval: float = 60.0
    log_interval: float = 5.0
    max_depth: int = 20
    market_types: List[str] = field(default_factory=lambda: ["spot", "futures", "all"])


@dataclass
class OrderbookSnapshot:
    """Snapshot of an orderbook at a specific time."""
    exchange: str
    symbol: str
    timestamp: float
    bids: List[Tuple[Decimal, Decimal]]
    asks: List[Tuple[Decimal, Decimal]]
    sequence: int
    market_type: str = "spot"
    
    @property
    def mid_price(self) -> Optional[Decimal]:
        """Calculate mid price from best bid and best ask."""
        if not self.bids or not self.asks:
            return None
        
        best_bid = self.bids[0][0] if self.bids else None
        best_ask = self.asks[0][0] if self.asks else None
        
        if best_bid and best_ask:
            return (best_bid + best_ask) / Decimal('2')
        
        return None
    
    @property
    def spread(self) -> Optional[Decimal]:
        """Calculate spread from best bid and best ask."""
        if not self.bids or not self.asks:
            return None
        
        best_bid = self.bids[0][0] if self.bids else None
        best_ask = self.asks[0][0] if self.asks else None
        
        if best_bid and best_ask:
            return best_ask - best_bid
        
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "timestamp": self.timestamp,
            "datetime": datetime.fromtimestamp(self.timestamp).isoformat(),
            "bids": [[float(p), float(a)] for p, a in self.bids],
            "asks": [[float(p), float(a)] for p, a in self.asks],
            "sequence": self.sequence,
            "market_type": self.market_type,
            "mid_price": float(self.mid_price) if self.mid_price else None,
            "spread": float(self.spread) if self.spread else None
        }


@dataclass
class MarketSummary:
    """Summary of market data across exchanges."""
    symbol: str
    timestamp: float
    exchanges: Set[str]
    best_bid: Optional[Decimal] = None
    best_ask: Optional[Decimal] = None
    bid_exchange: Optional[str] = None
    ask_exchange: Optional[str] = None
    mid_price: Optional[Decimal] = None
    spread: Optional[Decimal] = None
    spread_pct: Optional[Decimal] = None
    exchange_data: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp,
            "datetime": datetime.fromtimestamp(self.timestamp).isoformat(),
            "exchanges": list(self.exchanges),
            "best_bid": float(self.best_bid) if self.best_bid else None,
            "best_ask": float(self.best_ask) if self.best_ask else None,
            "bid_exchange": self.bid_exchange,
            "ask_exchange": self.ask_exchange,
            "mid_price": float(self.mid_price) if self.mid_price else None,
            "spread": float(self.spread) if self.spread else None,
            "spread_pct": float(self.spread_pct) if self.spread_pct else None,
            "exchange_data": {
                ex: {
                    k: float(v) if isinstance(v, Decimal) else v
                    for k, v in data.items()
                }
                for ex, data in self.exchange_data.items()
            }
        }
