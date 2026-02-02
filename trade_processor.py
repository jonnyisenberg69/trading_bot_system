import asyncio
import asyncpg
import logging
import json
import time
from datetime import datetime
import redis.asyncio as redis

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class TradeProcessor:
    def __init__(self, instance_id=None, redis_url="redis://localhost:6379"):
        self.instance_id = instance_id or 'default'
        self.trading_bot_pool = None
        self.redis_url = redis_url
        self.redis_client = None
        # Exchange IDs from the exchanges table (based on the table output)
        self.exchange_id_map = {
            'binance': 34,       # Binance Perpetual
            'binance_spot': 33,  # Binance Spot
            'bybit': 36,         # Bybit Perpetual
            'bybit_spot': 35,    # Bybit Spot
            'mexc': 37,          # MEXC Spot
            'gateio': 38,        # GateIO Spot
            'bitget': 39,        # BitGet Spot
            'hyperliquid': 40    # Hyperliquid Perpetual
        }
        
        # Database connection parameters
        self.trading_bot_params = {
            'database': 'trading_bot',
            'user': 'jonnyisenberg',
            'password': 'hello',
            'host': 'localhost',
        }
        
    async def initialize(self):
        """Initialize database connections"""
        try:
            # Initialize Redis client
            self.redis_client = redis.from_url(self.redis_url)
            
            self.trading_bot_pool = await asyncpg.create_pool(**self.trading_bot_params)
            
            logger.info(f"Trade processor initialized for instance {self.instance_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize trade processor: {e}")
            return False
            
    async def _ensure_tables(self):
        """Ensure all required tables exist"""
        # This method is now empty as table creation should be handled by migrations
        # We keep the method for compatibility but it does nothing
        logger.info("Table verification is now handled by Alembic migrations.")
            
    async def close(self):
        """Close database connections"""
        if self.redis_client:
            await self.redis_client.close()
        if self.trading_bot_pool:
            await self.trading_bot_pool.close()
        logger.info("Trade processor connections closed")
            
    async def process_message(self, message):
        """Process a WebSocket message and store in database"""
        # Validate message
        if not isinstance(message, dict):
            logger.warning(f"Invalid message format: {message}")
            return False
            
        # Check for exchange-specific formats
        if 'topic' in message:
            # Bybit format
            return await self.process_bybit_message(message)
        elif 'e' in message and 'E' in message:
            # Binance format
            return await self.process_binance_message(message)
        elif 'channel' in message and 'instId' in message:
            # OKX format (not currently used)
            return await self.process_okx_message(message)
        elif 'ch' in message and 'ts' in message:
            # Huobi format (not currently used)
            return await self.process_huobi_message(message)
        elif 'method' in message and 'params' in message:
            # MEXC format
            return await self.process_mexc_message(message)
        elif 'time' in message and 'channel' in message and 'event' in message:
            # GateIO format
            return await self.process_gateio_message(message)
        elif 'action' in message and 'arg' in message and 'data' in message:
            # BitGet format
            return await self.process_bitget_message(message)
        elif 'type' in message and 'data' in message and 'fills' in message.get('data', {}):
            # Hyperliquid format
            return await self.process_hyperliquid_message(message)
            
        # Regular format (already normalized)
        exchange = message.get('exchange')
        msg_type = message.get('type')
        
        if not exchange or not msg_type:
            logger.warning(f"Message missing exchange or type: {message}")
            return False
            
        # Process based on message type
        if msg_type == 'trade':
            await self.process_trade(exchange, message)
        elif msg_type == 'order_update' and message.get('status') == 'FILLED':
            await self.process_filled_order(exchange, message)
        elif msg_type == 'position':
            await self.process_position_update(exchange, message)
        else:
            logger.debug(f"Ignoring message type: {msg_type}")
            
        return True
        
    async def process_trade(self, exchange, message):
        """Process a trade message and store in database"""
        try:
            logger.info(f"Processing trade from {exchange}: {message.get('id')}")
            logger.debug(f"Full trade message: {message}")
            
            # Insert into trades table
            await self._insert_trade(exchange, message)
            
        except Exception as e:
            logger.error(f"Error processing {exchange} trade: {e}")
            import traceback
            logger.error(traceback.format_exc())
            
    async def process_filled_order(self, exchange, message):
        """Process a filled order message (may contain trade details)"""
        try:
            # Some exchanges send filled orders separately from trades
            # Extract what we can and insert as a trade if it has all the necessary info
            
            # Check if we have price and amount
            if 'price' in message and 'amount' in message and message.get('filled', 0) > 0:
                # Create trade-like structure
                trade_msg = {
                    'exchange': exchange,
                    'type': 'trade',
                    'symbol': message.get('symbol'),
                    'side': message.get('side'),
                    'price': message.get('price'),
                    'amount': message.get('filled'),
                    'fee': message.get('fee', 0),
                    'timestamp': message.get('timestamp'),
                    'order_id': message.get('id'),
                    'is_maker': message.get('is_maker', False)
                }
                
                # Process as a trade
                await self.process_trade(exchange, trade_msg)
            else:
                logger.debug(f"Filled order missing trade details: {message}")
                
        except Exception as e:
            logger.error(f"Error processing {exchange} filled order: {e}")
            
    async def process_position_update(self, exchange, message):
        """Process a position update message"""
        try:
            # Extract position information
            symbol = message.get('symbol')
            if not symbol:
                logger.warning(f"Position update missing symbol: {message}")
                return
                
            # Extract position amount and handle side
            position_amount = 0
            if 'size' in message:
                position_amount = float(message.get('size', 0))
                if 'side' in message and message.get('side', '').lower() == 'sell':
                    position_amount = -abs(position_amount)
            elif 'position_amnt' in message:
                position_amount = float(message.get('position_amnt', 0))
                
            # Extract entry price
            entry_price = 0
            if 'entry_price' in message:
                entry_price = float(message.get('entry_price', 0))
            elif 'entryPrice' in message:
                entry_price = float(message.get('entryPrice', 0))
                
            # Log the extracted values
            logger.info(f"Extracted position data - Symbol: {symbol}, Amount: {position_amount}, Entry Price: {entry_price}")
            
            # Insert into position table
            await self._insert_position(exchange, position_amount, entry_price)
            
            logger.info(f"Updated {exchange} position: {position_amount} @ {entry_price}")
            
        except Exception as e:
            logger.error(f"Error processing {exchange} position update: {e}")
            logger.error(f"Position message: {message}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _insert_position(self, exchange, position_amount, entry_price):
        """Insert a position record into the database"""
        # This method writes to a legacy table and should be phased out
        # For now, we connect directly if the pool is not available
        try:
            conn = await asyncpg.connect(**self.pos_db_params)
            try:
                table = 'binance_position' if exchange == 'binance' else 'bybit_position'
                timestamp = datetime.utcnow()
                await conn.execute(f'''
                    INSERT INTO {table} (timestamp, position_amnt, entry_price)
                    VALUES ($1, $2, $3)
                ''', timestamp, float(position_amount), float(entry_price))
            finally:
                await conn.close()
        except Exception:
            # Silently fail if position_data db is not available.
            pass
            
    async def _insert_trade(self, exchange, message):
        """Insert a trade record into the trading_bot database using deduplication"""
        from order_management.enhanced_trade_sync import TradeDeduplicationManager
        from database import get_session
        
        # Use the same deduplication system as websocket messages
        # This prevents race conditions with other insertion paths
        try:
            async for session in get_session():
                try:
                    dedup_manager = TradeDeduplicationManager(session, self.redis_client)
                    
                    # Insert with deduplication protection
                    success, reason = await dedup_manager.insert_trade_with_deduplication(message, exchange)
                    
                    if success:
                        logger.info(f"✅ Successfully inserted trade via deduplication: {exchange} {message.get('symbol')} {message.get('side')} {message.get('amount')} @ {message.get('price')}")
                        return True
                    else:
                        logger.info(f"🔄 Trade blocked by deduplication: {reason}")
                        return False
                finally:
                    await session.close()
                    
        except Exception as e:
            logger.error(f"Failed to insert trade via deduplication: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    async def process_binance_message(self, message):
        """Process Binance-specific WebSocket message"""
        event_type = message.get('e', '')
        event_time = message.get('E', '')
        
        logger.info(f"Processing Binance message: {event_type}")
        
        if event_type == 'trade':
            # Spot public trade format
            try:
                symbol = message.get('s', '')
                trade_id = str(message.get('t', ''))
                price = float(message.get('p', 0))
                quantity = float(message.get('q', 0))
                buyer_id = message.get('b', '')
                seller_id = message.get('a', '')
                trade_time = message.get('T', 0)
                is_market_maker = message.get('m', False)
                
                # Determine side based on market maker flag
                # In Binance trade stream, 'm' indicates if buyer is the market maker
                side = 'sell' if is_market_maker else 'buy'
                
                # Create normalized trade message
                trade_msg = {
                    'exchange': 'binance_spot',
                    'type': 'trade',
                    'symbol': symbol,
                    'side': side,
                    'price': price,
                    'amount': quantity,
                    'timestamp': trade_time,
                    'id': trade_id,
                    'exchange_trade_id': trade_id,
                    'order_id': None,  # Public trades don't have order ID
                    'is_maker': is_market_maker,
                    'raw_data': message  # Store original message
                }
                
                await self.process_trade('binance_spot', trade_msg)
            except Exception as e:
                logger.error(f"Error processing Binance spot trade: {e}")
                logger.error(f"Trade data: {message}")
                
        elif event_type == 'executionReport':
            # Spot order execution
            try:
                symbol = message.get('s', '')
                order_id = str(message.get('i', ''))
                client_order_id = message.get('c', '')
                side = message.get('S', '').lower()
                order_type = message.get('o', '')
                price = float(message.get('p', 0))
                quantity = float(message.get('q', 0))
                executed_qty = float(message.get('z', 0))
                cumulative_quote_qty = float(message.get('Z', 0))
                status = message.get('X', '')
                time_in_force = message.get('f', '')
                fill_price = float(message.get('L', 0)) or price
                last_filled_qty = float(message.get('l', 0))
                commission = float(message.get('n', 0))
                commission_asset = message.get('N', '')
                trade_id = str(message.get('t', ''))
                trade_time = message.get('T', 0)
                is_maker = message.get('m', False)
                
                # Only process filled or partially filled orders
                if status in ['FILLED', 'PARTIALLY_FILLED'] and last_filled_qty > 0:
                    # Create normalized trade message for the fill
                    trade_msg = {
                        'exchange': 'binance_spot',
                        'type': 'trade',
                        'symbol': symbol,
                        'side': side,
                        'price': fill_price,
                        'amount': last_filled_qty,
                        'fee': commission,
                        'fee_currency': commission_asset,
                        'timestamp': trade_time,
                        'id': trade_id or f"{order_id}_{trade_time}",
                        'exchange_trade_id': trade_id or f"{order_id}_{trade_time}",
                        'order_id': order_id,
                        'client_order_id': client_order_id,
                        'is_maker': is_maker,
                        'raw_data': message  # Store original message
                    }
                    
                    await self.process_trade('binance_spot', trade_msg)
            except Exception as e:
                logger.error(f"Error processing Binance spot order execution: {e}")
                logger.error(f"Execution data: {message}")
            
        elif event_type == 'ORDER_TRADE_UPDATE':
            # Futures order execution
            try:
                order_data = message.get('o', {})
                symbol = order_data.get('s', '')
                client_order_id = order_data.get('c', '')
                side = order_data.get('S', '').lower()
                order_type = order_data.get('o', '')
                time_in_force = order_data.get('f', '')
                original_qty = float(order_data.get('q', 0))
                original_price = float(order_data.get('p', 0))
                avg_price = float(order_data.get('ap', 0))
                order_status = order_data.get('X', '')
                order_id = str(order_data.get('i', ''))
                executed_qty = float(order_data.get('z', 0))
                last_executed_qty = float(order_data.get('l', 0))
                last_executed_price = float(order_data.get('L', 0))
                commission = float(order_data.get('n', 0))
                commission_asset = order_data.get('N', '')
                trade_time = order_data.get('T', 0)
                trade_id = str(order_data.get('t', ''))
                is_maker = order_data.get('m', False)
                
                # Only process filled or partially filled orders
                if order_status in ['FILLED', 'PARTIALLY_FILLED'] and last_executed_qty > 0:
                    # Create normalized trade message for the fill
                    trade_msg = {
                        'exchange': 'binance',
                        'type': 'trade',
                        'symbol': symbol,
                        'side': side,
                        'price': last_executed_price,
                        'amount': last_executed_qty,
                        'fee': commission,
                        'fee_currency': commission_asset,
                        'timestamp': trade_time,
                        'id': trade_id or f"{order_id}_{trade_time}",
                        'exchange_trade_id': trade_id or f"{order_id}_{trade_time}",
                        'order_id': order_id,
                        'client_order_id': client_order_id,
                        'is_maker': is_maker,
                        'raw_data': message  # Store original message
                    }
                    
                    await self.process_trade('binance', trade_msg)
            except Exception as e:
                logger.error(f"Error processing Binance futures order execution: {e}")
                logger.error(f"Execution data: {message}")
                
        elif event_type == 'ACCOUNT_UPDATE':
            # Account update containing position information
            try:
                account_data = message.get('a', {})
                positions = account_data.get('P', [])
                balances = account_data.get('B', [])
                update_time = message.get('T', 0)
                
                for position in positions:
                    symbol = position.get('s', '')
                    position_amount = float(position.get('pa', 0))
                    entry_price = float(position.get('ep', 0))
                    unrealized_pnl = float(position.get('up', 0))
                    margin_type = position.get('mt', '')
                    isolated_wallet = float(position.get('iw', 0))
                    position_side = position.get('ps', '')
                    
                    # Create normalized position message
                    pos_msg = {
                        'exchange': 'binance',
                        'type': 'position',
                        'symbol': symbol,
                        'size': position_amount,
                        'entry_price': entry_price,
                        'unrealized_pnl': unrealized_pnl,
                        'margin_type': margin_type,
                        'isolated_margin': isolated_wallet,
                        'position_side': position_side,
                        'timestamp': update_time,
                        'raw_data': position  # Store original message
                    }
                    
                    await self.process_position_update('binance', pos_msg)
                    
                # Process balance updates if needed
                for balance in balances:
                    asset = balance.get('a', '')
                    wallet_balance = float(balance.get('wb', 0))
                    cross_wallet_balance = float(balance.get('cw', 0))
                    
                    # You could process balance updates here if needed
                    logger.debug(f"Balance update for {asset}: {wallet_balance}")
            except Exception as e:
                logger.error(f"Error processing Binance account update: {e}")
                logger.error(f"Account data: {message}")
                
        # Add handler for user trades from spot API
        elif event_type == 'outboundAccountPosition':
            # This is just a balance update, no trade information
            pass
            
        # Add handler for user data stream keep-alive
        elif event_type == 'listenKeyExpired':
            logger.warning("Binance listen key expired - reconnection needed")
            
        return True

    async def process_mexc_message(self, message):
        """Process MEXC-specific WebSocket message"""
        method = message.get('method', '')
        params = message.get('params', [])
        
        if not params or not isinstance(params, list):
            logger.warning(f"Invalid MEXC message format: {message}")
            return False
            
        logger.info(f"Processing MEXC message: {method}")
        
        if method == 'push.deal':
            # Trade update from public channel
            for trade in params:
                try:
                    symbol = trade.get('symbol', '')
                    trade_id = str(trade.get('t', ''))
                    price = float(trade.get('p', 0))
                    quantity = float(trade.get('q', 0))
                    trade_time = trade.get('T', 0)
                    is_buyer_maker = trade.get('m', False)
                    
                    # Determine side based on market maker flag
                    side = 'sell' if is_buyer_maker else 'buy'
                    
                    # Create normalized trade message
                    trade_msg = {
                        'exchange': 'mexc',
                        'type': 'trade',
                        'symbol': symbol,
                        'side': side,
                        'price': price,
                        'amount': quantity,
                        'timestamp': trade_time,
                        'id': trade_id,
                        'exchange_trade_id': trade_id,
                        'is_maker': is_buyer_maker,
                        'order_id': None,  # Public trades don't have order ID
                        'raw_data': trade  # Store original message
                    }
                    
                    await self.process_trade('mexc', trade_msg)
                except Exception as e:
                    logger.error(f"Error processing MEXC trade: {e}")
                    logger.error(f"Trade data: {trade}")
                
        elif method == 'push.personal.order':
            # Order update from private channel
            for order in params:
                try:
                    symbol = order.get('symbol', '')
                    order_id = str(order.get('orderId', ''))
                    client_order_id = order.get('clientOrderId', '')
                    side = order.get('side', '').lower()
                    price = float(order.get('price', 0))
                    original_qty = float(order.get('origQty', 0))
                    executed_qty = float(order.get('executedQty', 0))
                    status = order.get('status', '')
                    timestamp = order.get('updateTime', order.get('time', int(time.time() * 1000)))
                    
                    # Only process filled or partially filled orders
                    if status in ['FILLED', 'PARTIALLY_FILLED'] and executed_qty > 0:
                        # For MEXC, we need to create a synthetic trade from order update
                        # since private trade feeds aren't directly available
                        
                        # Use order ID and timestamp to create a unique trade ID
                        trade_id = f"{order_id}_{timestamp}"
                        
                        # Create normalized trade message
                        trade_msg = {
                            'exchange': 'mexc',
                            'type': 'trade',
                            'symbol': symbol,
                            'side': side,
                            'price': price,
                            'amount': executed_qty,  # Use executed quantity
                            'timestamp': timestamp,
                            'id': trade_id,
                            'exchange_trade_id': trade_id,
                            'order_id': order_id,
                            'client_order_id': client_order_id,
                            'is_maker': False,  # Assume taker for orders
                            'fee': order.get('fee', 0),
                            'fee_currency': order.get('feeCurrency', ''),
                            'raw_data': order  # Store original message
                        }
                        
                        await self.process_trade('mexc', trade_msg)
                        
                except Exception as e:
                    logger.error(f"Error processing MEXC order: {e}")
                    logger.error(f"Order data: {order}")
        
        elif method == 'push.personal.trades':
            # Direct trade updates (if available)
            for trade in params:
                try:
                    symbol = trade.get('symbol', '')
                    trade_id = str(trade.get('id', ''))
                    order_id = str(trade.get('orderId', ''))
                    price = float(trade.get('price', 0))
                    quantity = float(trade.get('qty', 0))
                    side = trade.get('side', '').lower()
                    timestamp = trade.get('time', int(time.time() * 1000))
                    fee = float(trade.get('fee', 0))
                    fee_currency = trade.get('feeCoin', '')
                    
                    # Create normalized trade message
                    trade_msg = {
                        'exchange': 'mexc',
                        'type': 'trade',
                        'symbol': symbol,
                        'side': side,
                        'price': price,
                        'amount': quantity,
                        'timestamp': timestamp,
                        'id': trade_id,
                        'exchange_trade_id': trade_id,
                        'order_id': order_id,
                        'is_maker': trade.get('isMaker', False),
                        'fee': fee,
                        'fee_currency': fee_currency,
                        'raw_data': trade  # Store original message
                    }
                    
                    await self.process_trade('mexc', trade_msg)
                except Exception as e:
                    logger.error(f"Error processing MEXC personal trade: {e}")
                    logger.error(f"Trade data: {trade}")
                    
        return True
        
    async def process_gateio_message(self, message):
        """Process GateIO-specific WebSocket message"""
        event = message.get('event', '')
        channel = message.get('channel', '')
        data = message.get('result', {})
        timestamp = message.get('time', int(time.time()))
        
        logger.info(f"Processing GateIO message: {channel} - {event}")
        
        if channel == 'spot.trades':
            # Public trade update
            try:
                symbol = data.get('currency_pair', '')
                trade_id = str(data.get('id', ''))
                price = float(data.get('price', 0))
                amount = float(data.get('amount', 0))
                side = data.get('side', '').lower()
                create_time = data.get('create_time', 0)
                if create_time == 0:
                    create_time = timestamp * 1000  # Convert to milliseconds
                
                # Create normalized trade message
                trade_msg = {
                    'exchange': 'gateio',
                    'type': 'trade',
                    'symbol': symbol,
                    'side': side,
                    'price': price,
                    'amount': amount,
                    'timestamp': create_time,
                    'id': trade_id,
                    'exchange_trade_id': trade_id,
                    'order_id': None,  # Public trades don't have order ID
                    'is_maker': False,  # Can't determine from public feed
                    'raw_data': data  # Store original message
                }
                
                await self.process_trade('gateio', trade_msg)
            except Exception as e:
                logger.error(f"Error processing GateIO public trade: {e}")
                logger.error(f"Trade data: {data}")
            
        elif channel == 'spot.orders':
            # Order update
            try:
                symbol = data.get('currency_pair', '')
                order_id = str(data.get('id', ''))
                client_order_id = data.get('text', '')
                side = data.get('side', '').lower()
                price = float(data.get('price', 0))
                amount = float(data.get('amount', 0))
                filled = float(data.get('filled', 0))
                status = data.get('status', '')
                update_time = data.get('update_time', timestamp)
                if isinstance(update_time, int) and update_time < 9999999999:
                    # Convert seconds to milliseconds if needed
                    update_time = update_time * 1000
                
                # Only process filled or partially filled orders
                if status in ['closed', 'cancelled'] and filled > 0:
                    # Create a synthetic trade from order update
                    # For GateIO, we need to derive the trade info from the order
                    
                    # Use order ID and timestamp to create a unique trade ID
                    trade_id = f"{order_id}_{update_time}"
                    
                    # Get fee info if available
                    fee = data.get('fee', 0)
                    fee_currency = data.get('fee_currency', '')
                    
                    # Get actual fill price (use average if available)
                    fill_price = data.get('avg_deal_price', price)
                    
                    # Create normalized trade message
                    trade_msg = {
                        'exchange': 'gateio',
                        'type': 'trade',
                        'symbol': symbol,
                        'side': side,
                        'price': float(fill_price),
                        'amount': filled,  # Use filled amount
                        'timestamp': update_time,
                        'id': trade_id,
                        'exchange_trade_id': trade_id,
                        'order_id': order_id,
                        'client_order_id': client_order_id,
                        'is_maker': data.get('is_maker', False),
                        'fee': float(fee) if fee else 0,
                        'fee_currency': fee_currency,
                        'raw_data': data  # Store original message
                    }
                    
                    await self.process_trade('gateio', trade_msg)
            except Exception as e:
                logger.error(f"Error processing GateIO order update: {e}")
                logger.error(f"Order data: {data}")
                
        elif channel == 'spot.usertrades':
            # User trades (direct trade feed)
            try:
                # Check if data is a list
                trades_data = data if isinstance(data, list) else [data]
                
                for trade in trades_data:
                    symbol = trade.get('currency_pair', '')
                    trade_id = str(trade.get('id', ''))
                    order_id = str(trade.get('order_id', ''))
                    price = float(trade.get('price', 0))
                    amount = float(trade.get('amount', 0))
                    side = trade.get('side', '').lower()
                    create_time = trade.get('create_time', 0)
                    if create_time == 0:
                        create_time = timestamp * 1000  # Convert to milliseconds
                    
                    # Fee information
                    fee = trade.get('fee', 0)
                    fee_currency = trade.get('fee_currency', '')
                    
                    # Create normalized trade message
                    trade_msg = {
                        'exchange': 'gateio',
                        'type': 'trade',
                        'symbol': symbol,
                        'side': side,
                        'price': price,
                        'amount': amount,
                        'timestamp': create_time,
                        'id': trade_id,
                        'exchange_trade_id': trade_id,
                        'order_id': order_id,
                        'client_order_id': trade.get('text', ''),
                        'is_maker': trade.get('role', '') == 'maker',
                        'fee': float(fee) if fee else 0,
                        'fee_currency': fee_currency,
                        'raw_data': trade  # Store original message
                    }
                    
                    await self.process_trade('gateio', trade_msg)
            except Exception as e:
                logger.error(f"Error processing GateIO user trade: {e}")
                logger.error(f"Trade data: {data}")
                
        return True
        
    async def process_bitget_message(self, message):
        """Process BitGet-specific WebSocket message"""
        action = message.get('action', '')
        arg = message.get('arg', {})
        data = message.get('data', [])
        
        if not isinstance(data, list):
            data = [data]
            
        logger.info(f"Processing BitGet message: {action}")
        
        if action == 'push':
            # Handle different channels
            instType = arg.get('instType', '')
            channel = arg.get('channel', '')
            
            if channel == 'trade':
                # Public trade update
                for trade in data:
                    try:
                        symbol = trade.get('instId', '')
                        trade_id = str(trade.get('tradeId', ''))
                        price = float(trade.get('px', 0))
                        quantity = float(trade.get('sz', 0))
                        side = trade.get('side', '').lower()
                        timestamp = int(trade.get('ts', 0))
                        
                        # Create normalized trade message
                        trade_msg = {
                            'exchange': 'bitget',
                            'type': 'trade',
                            'symbol': symbol,
                            'side': side,
                            'price': price,
                            'amount': quantity,
                            'timestamp': timestamp,
                            'id': trade_id,
                            'exchange_trade_id': trade_id,
                            'order_id': None,  # Public trades don't have order ID
                            'is_maker': False,  # Can't determine from public feed
                            'raw_data': trade  # Store original message
                        }
                        
                        await self.process_trade('bitget', trade_msg)
                    except Exception as e:
                        logger.error(f"Error processing BitGet public trade: {e}")
                        logger.error(f"Trade data: {trade}")
                    
            elif channel == 'orders':
                # Order update
                for order in data:
                    try:
                        symbol = order.get('instId', '')
                        order_id = str(order.get('ordId', ''))
                        client_order_id = order.get('clOrdId', '')
                        side = order.get('side', '').lower()
                        price = float(order.get('px', 0))
                        original_size = float(order.get('sz', 0))
                        filled_size = float(order.get('accFillSz', 0))
                        avg_price = float(order.get('avgPx', price))
                        status = order.get('status', '')
                        timestamp = int(order.get('uTime', order.get('cTime', int(time.time() * 1000))))
                        
                        # Only process filled or partially filled orders
                        if status in ['filled', 'partially_filled'] and filled_size > 0:
                            # For BitGet, create a synthetic trade from order update
                            
                            # Use order ID and timestamp to create a unique trade ID
                            trade_id = f"{order_id}_{timestamp}"
                            
                            # Get fee info if available
                            fee = order.get('fee', 0)
                            fee_currency = order.get('feeCcy', '')
                            
                            # Create normalized trade message
                            trade_msg = {
                                'exchange': 'bitget',
                                'type': 'trade',
                                'symbol': symbol,
                                'side': side,
                                'price': avg_price,  # Use average price for filled amount
                                'amount': filled_size,  # Use filled amount
                                'timestamp': timestamp,
                                'id': trade_id,
                                'exchange_trade_id': trade_id,
                                'order_id': order_id,
                                'client_order_id': client_order_id,
                                'is_maker': order.get('isMaker', False),
                                'fee': float(fee) if fee else 0,
                                'fee_currency': fee_currency,
                                'raw_data': order  # Store original message
                            }
                            
                            await self.process_trade('bitget', trade_msg)
                    except Exception as e:
                        logger.error(f"Error processing BitGet order update: {e}")
                        logger.error(f"Order data: {order}")
            
            elif channel == 'fills':
                # Direct trade fills (user-specific)
                for fill in data:
                    try:
                        symbol = fill.get('instId', '')
                        trade_id = str(fill.get('tradeId', ''))
                        order_id = str(fill.get('ordId', ''))
                        client_order_id = fill.get('clOrdId', '')
                        price = float(fill.get('fillPx', 0))
                        quantity = float(fill.get('fillSz', 0))
                        side = fill.get('side', '').lower()
                        timestamp = int(fill.get('fillTime', int(time.time() * 1000)))
                        
                        # Fee information
                        fee = fill.get('fee', 0)
                        fee_currency = fill.get('feeCcy', '')
                        
                        # Create normalized trade message
                        trade_msg = {
                            'exchange': 'bitget',
                            'type': 'trade',
                            'symbol': symbol,
                            'side': side,
                            'price': price,
                            'amount': quantity,
                            'timestamp': timestamp,
                            'id': trade_id,
                            'exchange_trade_id': trade_id,
                            'order_id': order_id,
                            'client_order_id': client_order_id,
                            'is_maker': fill.get('isMaker', False),
                            'fee': float(fee) if fee else 0,
                            'fee_currency': fee_currency,
                            'raw_data': fill  # Store original message
                        }
                        
                        await self.process_trade('bitget', trade_msg)
                    except Exception as e:
                        logger.error(f"Error processing BitGet fill: {e}")
                        logger.error(f"Fill data: {fill}")
                        
        return True
        
    async def process_hyperliquid_message(self, message):
        """Process Hyperliquid-specific WebSocket message"""
        message_type = message.get('type', '')
        data = message.get('data', {})
        
        logger.info(f"Processing Hyperliquid message: {message_type}")
        
        if message_type == 'fill':
            # Trade fill (private user fills)
            try:
                fills = data.get('fills', [])
                if not isinstance(fills, list):
                    fills = [fills]
                    
                for fill in fills:
                    # Extract basic trade information
                    symbol = fill.get('coin', '')
                    side = 'buy' if fill.get('side', '') == 'B' else 'sell'
                    price = float(fill.get('px', 0))
                    size = float(fill.get('sz', 0))
                    fee = float(fill.get('fee', 0))
                    order_id = str(fill.get('oid', ''))
                    timestamp = int(fill.get('time', 0))
                    
                    # For Hyperliquid, create a unique trade ID
                    trade_id = f"{order_id}_{timestamp}"
                    
                    # Extract additional metadata if available
                    is_maker = fill.get('ismaker', False)
                    fee_currency = fill.get('feeCurrency', 'USD')  # Hyperliquid typically uses USD
                    leverage = fill.get('leverage', None)
                    liquidation = fill.get('liquidation', False)
                    
                    # Create normalized trade message
                    trade_msg = {
                        'exchange': 'hyperliquid',
                        'type': 'trade',
                        'symbol': symbol,
                        'side': side,
                        'price': price,
                        'amount': size,
                        'fee': fee,
                        'fee_currency': fee_currency,
                        'timestamp': timestamp,
                        'id': trade_id,
                        'exchange_trade_id': trade_id,
                        'order_id': order_id,
                        'is_maker': is_maker,
                        'is_liquidation': liquidation,
                        'raw_data': fill  # Store original message
                    }
                    
                    # Add leverage if available
                    if leverage is not None:
                        trade_msg['leverage'] = leverage
                    
                    await self.process_trade('hyperliquid', trade_msg)
            except Exception as e:
                logger.error(f"Error processing Hyperliquid fill: {e}")
                logger.error(f"Fill data: {data}")
                
        elif message_type == 'positions':
            # Position update
            try:
                positions = data.get('positions', [])
                if not isinstance(positions, list):
                    positions = [positions]
                    
                for position in positions:
                    symbol = position.get('coin', '')
                    size = float(position.get('szi', 0))
                    entry_price = float(position.get('entryPx', 0))
                    
                    # Determine side and adjust size
                    if size < 0:
                        side = 'sell'
                    else:
                        side = 'buy'
                    
                    # Extract additional position data
                    margin = position.get('margin', 0)
                    unrealized_pnl = position.get('unrealizedPnl', 0)
                    leverage = position.get('leverage', 0)
                    liquidation_price = position.get('liquidationPx', 0)
                    
                    # Create normalized position message
                    pos_msg = {
                        'exchange': 'hyperliquid',
                        'type': 'position',
                        'symbol': symbol,
                        'size': size,
                        'entry_price': entry_price,
                        'side': side,
                        'margin': float(margin),
                        'unrealized_pnl': float(unrealized_pnl),
                        'leverage': float(leverage),
                        'liquidation_price': float(liquidation_price),
                        'raw_data': position  # Store original message
                    }
                    
                    await self.process_position_update('hyperliquid', pos_msg)
            except Exception as e:
                logger.error(f"Error processing Hyperliquid position update: {e}")
                logger.error(f"Position data: {data}")
                
        elif message_type == 'trades':
            # Public trade updates
            try:
                trades = data.get('trades', [])
                if not isinstance(trades, list):
                    trades = [trades]
                    
                for trade in trades:
                    symbol = trade.get('coin', '')
                    side = 'buy' if trade.get('side', '') == 'B' else 'sell'
                    price = float(trade.get('px', 0))
                    size = float(trade.get('sz', 0))
                    timestamp = int(trade.get('time', 0))
                    
                    # Create unique trade ID
                    trade_id = f"{symbol}_{timestamp}_{price}_{size}"
                    
                    # Create normalized trade message
                    trade_msg = {
                        'exchange': 'hyperliquid',
                        'type': 'trade',
                        'symbol': symbol,
                        'side': side,
                        'price': price,
                        'amount': size,
                        'timestamp': timestamp,
                        'id': trade_id,
                        'exchange_trade_id': trade_id,
                        'order_id': None,  # Public trades don't have order ID
                        'raw_data': trade  # Store original message
                    }
                    
                    await self.process_trade('hyperliquid', trade_msg)
            except Exception as e:
                logger.error(f"Error processing Hyperliquid public trade: {e}")
                logger.error(f"Trade data: {data}")
                
        elif message_type == 'orders':
            # Order updates
            try:
                orders = data.get('orders', [])
                if not isinstance(orders, list):
                    orders = [orders]
                    
                for order in orders:
                    symbol = order.get('coin', '')
                    order_id = str(order.get('oid', ''))
                    side = 'buy' if order.get('side', '') == 'B' else 'sell'
                    price = float(order.get('limitPx', 0))
                    size = float(order.get('sz', 0))
                    status = order.get('status', '')
                    timestamp = int(order.get('timestamp', int(time.time() * 1000)))
                    
                    # Only process filled orders to create synthetic trade events
                    if status == 'filled' and size > 0:
                        # Create unique trade ID
                        trade_id = f"{order_id}_{timestamp}"
                        
                        # Create normalized trade message (synthetic from order)
                        trade_msg = {
                            'exchange': 'hyperliquid',
                            'type': 'trade',
                            'symbol': symbol,
                            'side': side,
                            'price': price,
                            'amount': size,
                            'timestamp': timestamp,
                            'id': trade_id,
                            'exchange_trade_id': trade_id,
                            'order_id': order_id,
                            'raw_data': order  # Store original message
                        }
                        
                        await self.process_trade('hyperliquid', trade_msg)
            except Exception as e:
                logger.error(f"Error processing Hyperliquid order update: {e}")
                logger.error(f"Order data: {data}")
                
        return True

    async def process_okx_message(self, message):
        """Process OKX-specific WebSocket message (stub implementation)"""
        logger.info(f"OKX message processing not fully implemented: {message}")
        return True
        
    async def process_huobi_message(self, message):
        """Process Huobi-specific WebSocket message (stub implementation)"""
        logger.info(f"Huobi message processing not fully implemented: {message}")
        return True


# Standalone function to create and manage a trade processor
async def process_trades_from_websocket(message_queue, instance_id=None):
    """
    Process trades from a message queue and store in database.
    This function runs as a background task.
    
    Args:
        message_queue: asyncio.Queue containing websocket messages
        instance_id: Optional instance identifier
    """
    processor = TradeProcessor(instance_id)
    try:
        await processor.initialize()
        
        while True:
            message = await message_queue.get()
            if message == "STOP":
                break
                
            await processor.process_message(message)
            message_queue.task_done()
            
    except asyncio.CancelledError:
        logger.info("Trade processor task cancelled")
    except Exception as e:
        logger.error(f"Error in trade processor: {e}")
    finally:
        await processor.close()
        logger.info("Trade processor shutdown complete") 
