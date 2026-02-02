import asyncio
import json
import logging
from decimal import Decimal
from typing import Dict, Any
import redis.asyncio as redis
from .orderbook import OrderBook

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='aggregator_service.log'
)
logger = logging.getLogger("aggregator_service")

# Market type mapping helper
def get_market_type(exchange: str) -> str:
    ex = exchange.lower()
    if any(x in ex for x in ["perp", "future", "futures"]):
        return "perp"
    return "spot"

class RedisOrderbookAggregatorService:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis_client = None
        self.pubsub = None
        # Structure: {exchange: {symbol: orderbook_dict}}
        self.orderbooks: Dict[str, Dict[str, Any]] = {}
        # Aggregated books: {market_type: {symbol: OrderBook}}
        self.combined_books: Dict[str, Dict[str, OrderBook]] = {
            "spot": {},
            "perp": {},
            "all": {}
        }
        self.running = False

    async def start(self):
        self.redis_client = redis.from_url(self.redis_url)
        await self.redis_client.ping()
        self.pubsub = self.redis_client.pubsub()
        await self.pubsub.subscribe('orderbook_updates')
        self.running = True
        logger.info("Aggregator service started and listening for orderbook updates...")
        # Start heartbeat task
        asyncio.create_task(self._heartbeat())
        await self._listen_for_updates()

    async def _heartbeat(self):
        """Periodically set a heartbeat key in Redis to signal liveness."""
        while self.running:
            try:
                await self.redis_client.set("aggregator_service:heartbeat", "1", ex=10)
            except Exception as e:
                logger.warning(f"Failed to set heartbeat: {e}")
            await asyncio.sleep(5)

    async def _listen_for_updates(self):
        try:
            async for message in self.pubsub.listen():
                if not self.running:
                    break
                if message['type'] == 'message':
                    try:
                        update = json.loads(message['data'])
                        exchange = update['exchange']
                        symbol = update.get('symbol')
                        if not symbol:
                            logger.warning(f"Orderbook update missing symbol: {update}")
                            continue
                        # Log the number of levels and a sample for each update received
                        logger.info(f"Aggregator received {exchange} {symbol}: {len(update['bids'])} bids, {len(update['asks'])} asks")
                        logger.info(f"Aggregator received {exchange} {symbol} bids sample: {update['bids'][:20]}")
                        logger.info(f"Aggregator received {exchange} {symbol} asks sample: {update['asks'][:20]}")
                        if exchange not in self.orderbooks:
                            self.orderbooks[exchange] = {}
                        self.orderbooks[exchange][symbol] = update
                        # Re-aggregate on every update
                        await self.aggregate_and_publish()
                    except Exception as e:
                        logger.error(f"Error processing orderbook update: {e}")
        except asyncio.CancelledError:
            logger.info("Aggregator service cancelled.")
        except Exception as e:
            logger.error(f"Error in aggregator service: {e}")

    async def aggregate_and_publish(self):
        # Clear previous combined books
        self.combined_books = {"spot": {}, "perp": {}, "all": {}}
        # Aggregate all orderbooks by market type and symbol
        for exchange, symbols in self.orderbooks.items():
            market_type = get_market_type(exchange)
            for symbol, ob_dict in symbols.items():
                # Convert to OrderBook for aggregation
                ob = self._orderbook_from_dict(ob_dict)
                # Spot or perp
                if market_type == "spot":
                    if symbol not in self.combined_books["spot"]:
                        self.combined_books["spot"][symbol] = OrderBook(symbol, "combined_spot")
                    self._add_to_combined(self.combined_books["spot"][symbol], ob)
                else:
                    if symbol not in self.combined_books["perp"]:
                        self.combined_books["perp"][symbol] = OrderBook(symbol, "combined_perp")
                    self._add_to_combined(self.combined_books["perp"][symbol], ob)
                # All
                if symbol not in self.combined_books["all"]:
                    self.combined_books["all"][symbol] = OrderBook(symbol, "combined_all")
                self._add_to_combined(self.combined_books["all"][symbol], ob)
        # Publish to Redis
        await self._publish_combined_books()

    def _orderbook_from_dict(self, ob_dict: Dict[str, Any]) -> OrderBook:
        ob = OrderBook(ob_dict['symbol'], ob_dict.get('exchange', 'unknown'))
        # Bids/asks as list of [price, amount]
        for price_str, amount_str in ob_dict.get('bids', []):
            ob.update_bid(Decimal(price_str), Decimal(amount_str))
        for price_str, amount_str in ob_dict.get('asks', []):
            ob.update_ask(Decimal(price_str), Decimal(amount_str))
        return ob

    def _add_to_combined(self, combined: OrderBook, ob: OrderBook):
        # Add all bids
        for price, amount in ob.bids.items():
            if price in combined.bids:
                combined.bids[price] += amount
            else:
                combined.bids[price] = amount
        # Add all asks
        for price, amount in ob.asks.items():
            if price in combined.asks:
                combined.asks[price] += amount
            else:
                combined.asks[price] = amount
        # No depth/price level limit

    async def _publish_combined_books(self):
        # Publish each combined book as a JSON blob under a Redis key
        for mtype, books in self.combined_books.items():
            for symbol, ob in books.items():
                key = f"combined_orderbook:{mtype}:{symbol}"
                ob_dict = ob.to_dict()
                print(f"Aggregator publishing {key}: {len(ob_dict['bids'])} bids, {len(ob_dict['asks'])} asks")  # <-- ADD THIS LINE
                await self.redis_client.set(key, json.dumps(ob_dict))
        logger.info("Published combined orderbooks to Redis.")

async def main():
    service = RedisOrderbookAggregatorService()
    await service.start()

if __name__ == "__main__":
    asyncio.run(main()) 