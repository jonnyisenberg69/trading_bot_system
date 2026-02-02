"""
Redis Orderbook Manager - Subscribes to Redis orderbook updates and provides data to strategies
"""
import asyncio
import json
import logging
import inspect
from typing import Dict, Optional, Callable, Any
from decimal import Decimal
from datetime import datetime, timezone
import redis.asyncio as redis


class RedisOrderbookManager:
    """Manages orderbook data from Redis pub/sub for strategies"""
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.logger = logging.getLogger(__name__)
        self.redis_url = redis_url
        self.redis_client: Optional[redis.Redis] = None
        self.pubsub: Optional[redis.client.PubSub] = None
        
        # Store latest orderbook data
        self.orderbooks: Dict[str, Dict[str, Any]] = {}
        
        # Callbacks for orderbook updates
        self.update_callbacks: list[Callable] = []
        
        # Task for listening to Redis
        self.listen_task: Optional[asyncio.Task] = None
        self.running = False
    
    async def start(self):
        """Start the Redis orderbook manager"""
        self.logger.info("Starting Redis Orderbook Manager...")
        
        # Connect to Redis
        self.redis_client = redis.from_url(self.redis_url)
        await self.redis_client.ping()
        
        # Create pubsub instance
        self.pubsub = self.redis_client.pubsub()
        
        # Subscribe to orderbook updates
        await self.pubsub.subscribe('orderbook_updates')
        
        # Start listening task
        self.running = True
        self.listen_task = asyncio.create_task(self._listen_for_updates())
        
        self.logger.info("Redis Orderbook Manager started")
    
    async def stop(self):
        """Stop the Redis orderbook manager"""
        self.logger.info("Stopping Redis Orderbook Manager...")
        
        self.running = False
        
        # Cancel listen task
        if self.listen_task and not self.listen_task.done():
            self.listen_task.cancel()
            try:
                await self.listen_task
            except asyncio.CancelledError:
                pass
        
        # Close pubsub
        if self.pubsub:
            await self.pubsub.unsubscribe('orderbook_updates')
            await self.pubsub.close()
        
        # Close Redis connection
        if self.redis_client:
            await self.redis_client.close()
        
        self.logger.info("Redis Orderbook Manager stopped")
    
    async def _listen_for_updates(self):
        """Listen for orderbook updates from Redis"""
        self.logger.info("Starting to listen for orderbook updates...")
        
        try:
            async for message in self.pubsub.listen():
                if not self.running:
                    break
                
                if message['type'] == 'message':
                    try:
                        # Parse the orderbook update
                        update = json.loads(message['data'])
                        exchange = update['exchange']
                        symbol = update.get('symbol')
                        if not symbol:
                            self.logger.warning(f"Orderbook update missing symbol: {update}")
                            continue
                        # Initialize dict for exchange if not present
                        if exchange not in self.orderbooks:
                            self.orderbooks[exchange] = {}
                        # Store the update by exchange and symbol
                        self.orderbooks[exchange][symbol] = update
                        # Log hyperliquid updates specifically
                        if 'hyperliquid' in exchange:
                            self.logger.info(f"Received Hyperliquid update: exchange={exchange}, symbol={symbol}, bid={update.get('best_bid')}, ask={update.get('best_ask')}")
                        # Notify callbacks (support both sync and async callbacks)
                        for callback in self.update_callbacks:
                            try:
                                result = callback(exchange, update)
                                if inspect.isawaitable(result):
                                    await result
                            except Exception as e:
                                self.logger.error(f"Error in update callback: {e}")
                        self.logger.debug(f"Updated orderbook for {exchange} {symbol}")
                    except Exception as e:
                        self.logger.error(f"Error processing orderbook update: {e}")
        
        except asyncio.CancelledError:
            self.logger.info("Orderbook update listener cancelled")
        except Exception as e:
            self.logger.error(f"Error in orderbook update listener: {e}")
    
    def register_update_callback(self, callback: Callable):
        """Register a callback for orderbook updates"""
        self.update_callbacks.append(callback)
    
    def get_best_bid_ask(self, exchange: str, symbol: str) -> tuple[Optional[Decimal], Optional[Decimal]]:
        """Get best bid and ask for an exchange and symbol"""
        if exchange not in self.orderbooks:
            self.logger.debug(f"Exchange {exchange} not found in orderbooks. Available exchanges: {list(self.orderbooks.keys())}")
            return None, None
        if symbol not in self.orderbooks[exchange]:
            self.logger.debug(f"Symbol {symbol} not found for {exchange}. Available symbols: {list(self.orderbooks[exchange].keys())}")
            return None, None
        orderbook = self.orderbooks[exchange][symbol]
        try:
            best_bid = Decimal(str(orderbook['best_bid']))
            best_ask = Decimal(str(orderbook['best_ask']))
            return best_bid, best_ask
        except (KeyError, ValueError) as e:
            self.logger.error(f"Error getting best bid/ask for {exchange} {symbol}: {e}")
            return None, None
    
    def get_midpoint(self, exchange: str, symbol: str) -> Optional[Decimal]:
        """Get midpoint price for an exchange and symbol"""
        if exchange not in self.orderbooks or symbol not in self.orderbooks[exchange]:
            return None
        try:
            return Decimal(str(self.orderbooks[exchange][symbol]['midpoint']))
        except (KeyError, ValueError) as e:
            self.logger.error(f"Error getting midpoint for {exchange} {symbol}: {e}")
            return None
    
    def get_all_midpoints(self) -> Dict[str, Dict[str, Optional[Decimal]]]:
        """Get midpoints for all exchanges and symbols"""
        midpoints = {}
        for exchange in self.orderbooks:
            midpoints[exchange] = {}
            for symbol in self.orderbooks[exchange]:
                midpoints[exchange][symbol] = self.get_midpoint(exchange, symbol)
        return midpoints
    
    def get_available_exchanges(self) -> list[str]:
        """Get list of exchanges with available data"""
        return list(self.orderbooks.keys())
    
    def get_orderbook_age(self, exchange: str, symbol: str) -> Optional[float]:
        """Get age of orderbook data in seconds for a given exchange and symbol"""
        if exchange not in self.orderbooks or symbol not in self.orderbooks[exchange]:
            return None
        try:
            timestamp = self.orderbooks[exchange][symbol]['timestamp']
            if timestamp is None:
                return None
            current_time = datetime.now(timezone.utc).timestamp() * 1000
            return (current_time - timestamp) / 1000
        except (KeyError, ValueError):
            return None
    
    def is_data_fresh(self, exchange: str, symbol: str, max_age_seconds: float = 10.0) -> bool:
        """Check if orderbook data is fresh for a given exchange and symbol"""
        age = self.get_orderbook_age(exchange, symbol)
        if age is None:
            return False
        return age <= max_age_seconds
    
    def get_aggregated_orderbook(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get aggregated orderbook data across all exchanges for a symbol."""
        try:
            midpoints = []
            best_bids = []
            best_asks = []
            
            for exchange in self.orderbooks:
                if symbol in self.orderbooks[exchange]:
                    orderbook = self.orderbooks[exchange][symbol]
                    
                    if 'midpoint' in orderbook:
                        midpoints.append(Decimal(str(orderbook['midpoint'])))
                    
                    if 'best_bid' in orderbook:
                        best_bids.append(Decimal(str(orderbook['best_bid'])))
                    
                    if 'best_ask' in orderbook:
                        best_asks.append(Decimal(str(orderbook['best_ask'])))
            
            if not midpoints:
                return None
            
            # Calculate aggregated values
            aggregated_midpoint = sum(midpoints) / len(midpoints)
            aggregated_best_bid = max(best_bids) if best_bids else None
            aggregated_best_ask = min(best_asks) if best_asks else None
            
            return {
                'symbol': symbol,
                'midpoint': float(aggregated_midpoint),
                'best_bid': float(aggregated_best_bid) if aggregated_best_bid else None,
                'best_ask': float(aggregated_best_ask) if aggregated_best_ask else None,
                'exchanges_count': len(midpoints),
                'timestamp': datetime.now(timezone.utc).timestamp() * 1000
            }
            
        except Exception as e:
            self.logger.error(f"Error getting aggregated orderbook for {symbol}: {e}")
            return None

    def get_orderbook(self, exchange: str, symbol: str) -> Optional[Dict[str, Any]]:
        """Get full orderbook data for an exchange and symbol."""
        if exchange not in self.orderbooks or symbol not in self.orderbooks[exchange]:
            return None
        return self.orderbooks[exchange][symbol]

    def get_status(self) -> Dict[str, Any]:
        """Get status of all orderbooks"""
        status = {
            'running': self.running,
            'exchanges': list(self.orderbooks.keys()),
            'total_exchanges': len(self.orderbooks),
            'exchange_status': {}
        }
        for exchange in self.orderbooks:
            status['exchange_status'][exchange] = {}
            for symbol in self.orderbooks[exchange]:
                orderbook = self.orderbooks[exchange][symbol]
                status['exchange_status'][exchange][symbol] = {
                    'best_bid': orderbook.get('best_bid'),
                    'best_ask': orderbook.get('best_ask'),
                    'midpoint': orderbook.get('midpoint'),
                    'timestamp': orderbook.get('timestamp'),
                    'age_seconds': self.get_orderbook_age(exchange, symbol),
                    'is_fresh': self.is_data_fresh(exchange, symbol)
                }
        return status 
