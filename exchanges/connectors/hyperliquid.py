"""
Hyperliquid exchange connector implementation.

Supports both spot and perpetual futures trading on Hyperliquid.
Features:
- On-chain trading for both spot and futures
- Wallet-based authentication (uses wallet signatures)
- Volume-based rate limiting
"""

import asyncio
import ccxt.pro as ccxt
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime, timezone
import logging
import decimal
import time
import json

from ..base_connector import BaseExchangeConnector, OrderSide, OrderType, OrderStatus

logger = logging.getLogger(__name__)


class HyperliquidConnector(BaseExchangeConnector):
    """
    Hyperliquid exchange connector supporting both spot and futures markets.

    Features:
    - Unified spot and futures API
    - On-chain settlement with fast execution
    - Wallet-based authentication
    - Volume-based rate limiting
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Hyperliquid connector.

        Args:
            config: Configuration dictionary with:
                - api_key: Not used (wallet-based auth)
                - secret: Private key or wallet seed
                - sandbox: Use testnet (default: False)
                - wallet_address: Ethereum wallet address
                - market_type: 'spot' or 'future' (default: 'future')
        """
        super().__init__(config)

        self.wallet_address = config.get('wallet_address')
        self.private_key = config.get('private_key') or config.get('secret')
        self.testnet = config.get('sandbox', False)
        self.market_type = config.get('market_type', 'future')  # Default to futures

        # Also expose as camelCase for WebSocket manager compatibility
        self.walletAddress = self.wallet_address
        self.privateKey = self.private_key

        # Initialize ccxt exchange
        self._init_exchange()

        # Volume-based rate limits (different from other exchanges)
        self.rate_limits = {
            'base_limit': 200,  # Base requests per second
            'volume_multiplier': 0.001  # Additional requests per 1 USDC volume
        }
        self._resolved_symbol_cache: Dict[str, str] = {}

    def _init_exchange(self):
        """Initialize ccxt exchange instance."""
        try:
            # Common options for both spot and futures
            options = {
                'walletAddress': self.wallet_address,
                'privateKey': self.private_key,
                'sandbox': self.testnet,
                'enableRateLimit': True,
                'rateLimit': 100,  # 100ms between requests,
            }

            # Set the appropriate market type
            if self.market_type == 'spot':
                options['options'] = {
                    'defaultType': 'spot',  # Use spot markets
                }
                self.exchange = ccxt.hyperliquid(options)
            else:
                # Default to futures (perpetual swaps)
                options['options'] = {
                    'defaultType': 'swap',  # Perpetual futures
                }
                self.exchange = ccxt.hyperliquid(options)

            self.logger.info(f"Initialized Hyperliquid {self.market_type} exchange")

        except Exception as e:
            self.logger.error(f"Failed to initialize Hyperliquid exchange: {e}")
            raise

    async def connect(self) -> bool:
        """Connect to Hyperliquid and load markets."""
        try:
            # Load markets
            await self.exchange.load_markets()

            # Test connectivity with account info
            try:
                account_info = await self.exchange.fetch_balance()
                self.last_heartbeat = datetime.now(timezone.utc)

                self.connected = True
                self.logger.info(f"Connected to Hyperliquid {self.market_type} "
                               f"(wallet: {self.wallet_address[:8]}...)")

                return True

            except Exception as e:
                # If balance fails, try a simpler connectivity test
                markets = await self.exchange.fetch_markets()
                if markets:
                    self.connected = True
                    self.last_heartbeat = datetime.now(timezone.utc)
                    self.logger.info(f"Connected to Hyperliquid {self.market_type} (limited access)")
                    return True
                else:
                    raise e

        except Exception as e:
            self.logger.error(f"Failed to connect to Hyperliquid {self.market_type}: {e}")
            self.connected = False
            return False

    async def disconnect(self) -> bool:
        """Disconnect from Hyperliquid."""
        try:
            if self.exchange:
                await self.exchange.close()

            self.connected = False
            self.logger.info(f"Disconnected from Hyperliquid {self.market_type}")
            return True

        except Exception as e:
            self.logger.error(f"Error disconnecting from Hyperliquid {self.market_type}: {e}")
            return False

    async def get_balance(self, currency: Optional[str] = None, include_zero: bool = False) -> Dict[str, Decimal]:
        """Get account balance.

        Args:
            currency: Specific currency to get balance for
            include_zero: If True, include zero balances in the result
        """
        try:
            if not await self._check_rate_limit('balance'):
                raise Exception("Rate limit exceeded for balance check")

            balance = await self.exchange.fetch_balance()

            if self.market_type == 'future':
                # For futures, use the total account equity from marginSummary
                if 'info' in balance and isinstance(balance['info'], dict):
                    margin_summary = balance['info'].get('marginSummary', {})
                    if margin_summary and 'accountValue' in margin_summary:
                        # Use accountValue as the total USDT balance
                        account_value = Decimal(str(margin_summary['accountValue']))

                        # If specific currency requested
                        if currency:
                            if currency in ['USDT', 'USD', 'USDC', 'USDT0']:
                                return {currency: account_value}
                            else:
                                # For other currencies, use the standard balance data
                                if currency in balance:
                                    return {
                                        currency: Decimal(str(balance[currency].get('free', 0)))
                                    }
                                else:
                                    return {currency: Decimal('0')}
                        else:
                            # Return accountValue as USDT balance for overall balance queries
                            result = {'USDT': account_value}

                            # Also include any non-USDT asset positions if needed
                            for curr, info in balance.items():
                                if isinstance(info, dict) and 'free' in info:
                                    if curr not in ['USDT', 'USD', 'USDC']:  # Avoid duplicates
                                        free_amount = Decimal(str(info['free']))
                                        if include_zero or free_amount > 0:
                                            result[curr] = free_amount

                            return result

            # For spot or fallback for futures
            if currency:
                # Return specific currency balance
                if currency in balance:
                    return {
                        currency: Decimal(str(balance[currency].get('free', 0)))
                    }
                else:
                    logger.error(f"Currency {currency} not found in balance {balance}")
                    return {currency: Decimal('0')}
            else:
                # Return all non-zero balances (or all if include_zero is True)
                result = {}
                for curr, info in balance.items():
                    if isinstance(info, dict) and 'free' in info:
                        free_amount = Decimal(str(info['free']))
                        if include_zero or free_amount > 0:
                            result[curr] = free_amount

                return result

        except Exception as e:
            self._handle_error(e, "get_balance")
            return {}

    async def get_orderbook(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        """Get orderbook for symbol."""
        try:
            if not await self._check_rate_limit('orderbook'):
                raise Exception("Rate limit exceeded for orderbook")

            # Normalize symbol for Hyperliquid
            hl_symbol = self.normalize_symbol(symbol)

            orderbook = await self.exchange.fetch_order_book(hl_symbol, limit)

            return {
                'symbol': symbol,
                'exchange': 'hyperliquid',
                'timestamp': orderbook.get('timestamp'),
                'datetime': orderbook.get('datetime'),
                'bids': [[Decimal(str(price)), Decimal(str(amount))]
                        for price, amount in orderbook['bids']],
                'asks': [[Decimal(str(price)), Decimal(str(amount))]
                        for price, amount in orderbook['asks']]
            }

        except Exception as e:
            self._handle_error(e, f"get_orderbook({symbol})")
            return {}

    async def place_order(
        self,
        symbol: str,
        side: str,
        amount: Decimal,
        price: Optional[Decimal] = None,
        order_type: str = OrderType.LIMIT,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Place order on Hyperliquid."""
        try:
            if not await self._check_rate_limit('order'):
                raise Exception("Rate limit exceeded for order placement")

            # Normalize inputs
            hl_symbol = self.normalize_symbol(symbol)
            amount_float = float(amount)
            price_float = float(price) if price else None

            # Validate order type
            if order_type not in ["market", "limit"]:
                raise ValueError(f"Unsupported order type: {order_type}")

            # Check required parameters for limit orders
            if order_type == "limit" and price_float is None:
                raise ValueError("Price required for limit orders")

            # Prepare order params for Hyperliquid with proper client order ID encoding
            order_params = params or {}
            
            # Encode client order ID for Hyperliquid if present
            if 'clientOrderId' in order_params:
                encoded_cloid = self._encode_hyperliquid_cloid(order_params['clientOrderId'])
                order_params = {k: v for k, v in order_params.items() if k != 'clientOrderId'}
                if encoded_cloid:
                    order_params['cloid'] = encoded_cloid

            # DEBUG: Log all authentication and order parameters before CCXT call
            self.logger.error(f"=== HYPERLIQUID DEBUG INFO ===")
            self.logger.error(f"Wallet address: '{self.wallet_address}' (type: {type(self.wallet_address)}, len: {len(self.wallet_address or '')})")
            self.logger.error(f"Private key: first 10 chars: '{(self.private_key or '')[:10]}' (type: {type(self.private_key)}, len: {len(self.private_key or '')})")
            self.logger.error(f"Testnet: {self.testnet}")
            self.logger.error(f"Market type: {self.market_type}")
            self.logger.error(f"Order symbol: '{hl_symbol}'")
            self.logger.error(f"Order side: '{side}'")
            self.logger.error(f"Order amount: {amount_float} (type: {type(amount_float)})")
            self.logger.error(f"Order price: {price_float} (type: {type(price_float)})")
            self.logger.error(f"Order params: {order_params}")
            self.logger.error(f"CCXT exchange class: {type(self.exchange)}")
            if hasattr(self.exchange, 'walletAddress'):
                self.logger.error(f"CCXT wallet address: '{self.exchange.walletAddress}'")
            if hasattr(self.exchange, 'privateKey'):
                self.logger.error(f"CCXT private key: first 10 chars: '{(self.exchange.privateKey or '')[:10]}' (len: {len(self.exchange.privateKey or '')})")
            self.logger.error(f"=== END DEBUG INFO ===")

            # Place order via ccxt
            try:
                if order_type == "market":
                    order = await self.exchange.create_market_order(
                        hl_symbol, side, amount_float, price_float, params=order_params
                    )
                elif order_type == "limit":
                    order = await self.exchange.create_limit_order(
                        hl_symbol, side, amount_float, price_float, params=order_params
                    )
            except Exception as ccxt_error:
                self.logger.error(f"=== CCXT ERROR DEBUG ===")
                self.logger.error(f"CCXT error message: '{str(ccxt_error)}'")
                self.logger.error(f"CCXT error type: {type(ccxt_error)}")
                if hasattr(ccxt_error, 'args'):
                    self.logger.error(f"CCXT error args: {ccxt_error.args}")
                self.logger.error(f"=== END CCXT ERROR DEBUG ===")
                raise ccxt_error

            # Normalize response
            return self._normalize_order(order)

        except Exception as e:
            self._handle_error(e, f"place_order({symbol}, {side}, {amount})")
            raise

    async def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Cancel order."""
        try:
            if not await self._check_rate_limit('cancel'):
                raise Exception("Rate limit exceeded for order cancellation")

            hl_symbol = self.normalize_symbol(symbol)

            order = await self.exchange.cancel_order(order_id, hl_symbol)
            return self._normalize_order(order)

        except Exception as e:
            self._handle_error(e, f"cancel_order({order_id}, {symbol})")
            raise

    async def get_order_status(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Get order status."""
        try:
            if not await self._check_rate_limit('query'):
                raise Exception("Rate limit exceeded for order status")

            hl_symbol = self.normalize_symbol(symbol)

            order = await self.exchange.fetch_order(order_id, hl_symbol)
            return self._normalize_order(order)

        except Exception as e:
            self._handle_error(e, f"get_order_status({order_id}, {symbol})")
            return {}

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get open orders."""
        try:
            if not await self._check_rate_limit('query'):
                raise Exception("Rate limit exceeded for open orders")

            hl_symbol = self.normalize_symbol(symbol) if symbol else None

            orders = await self.exchange.fetch_open_orders(hl_symbol)
            return [self._normalize_order(order) for order in orders]

        except Exception as e:
            self._handle_error(e, f"get_open_orders({symbol})")
            return []

    async def get_trade_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
        until: Optional[datetime] = None,
        include_twap: bool = True  # Add parameter to include TWAP fills
    ) -> List[Dict[str, Any]]:
        """
        Get trade history using the fetch_my_trades ccxt method.
        This provides actual trade executions with correct timestamps.
        If include_twap is True, also fetches TWAP slice fills (futures only).
        """
        try:
            if not await self._check_rate_limit('query', weight=1):
                raise Exception("Rate limit exceeded for trade history")

            hyperliquid_symbol = self.normalize_symbol(symbol) if symbol else None
            since_timestamp = int(since.timestamp() * 1000) if since else None
            until_timestamp = int(until.timestamp() * 1000) if until else None

            self.logger.info(
                f"Fetching Hyperliquid {self.market_type} trade history",
                symbol=hyperliquid_symbol,
                start=since.isoformat() if since else "None",
                limit=limit,
                include_twap=include_twap
            )

            params = {
                'paginate': True
            }
            if until_timestamp:
                params['until'] = until_timestamp

            # Fetch regular trades
            trades = await self.exchange.fetch_my_trades(
                symbol=hyperliquid_symbol,
                since=since_timestamp,
                params=params
            )

            self.logger.info(f"Successfully fetched {len(trades)} regular trades from Hyperliquid {self.market_type}")

            # Normalize trades to our standard format
            normalized_trades = [self._normalize_trade(trade) for trade in trades]

            # Fetch TWAP fills if requested (futures only)
            if include_twap and self.market_type == 'future':
                try:
                    twap_fills = await self.get_twap_fills(symbol, since, limit, until)
                    if twap_fills:
                        self.logger.info(f"Successfully fetched {len(twap_fills)} TWAP fills")
                        normalized_trades.extend(twap_fills)

                        # Sort combined list by timestamp
                        # Ensure all timestamps are integers for proper sorting
                        def get_timestamp_int(trade):
                            ts = trade.get('timestamp', 0)
                            if isinstance(ts, str):
                                try:
                                    return int(ts)
                                except (ValueError, TypeError):
                                    return 0
                            elif isinstance(ts, (int, float)):
                                return int(ts)
                            else:
                                return 0

                        normalized_trades.sort(key=get_timestamp_int)

                        # Apply limit to combined result if necessary
                        if limit and len(normalized_trades) > limit:
                            normalized_trades = normalized_trades[:limit]
                except Exception as e:
                    self.logger.warning(f"Failed to fetch TWAP fills: {e}")
                    # Continue with regular trades only

            self.logger.info(f"Total trades returned: {len(normalized_trades)} (regular + TWAP)")
            return normalized_trades

        except Exception as e:
            self.logger.error(f"Error in get_trade_history using fetch_my_trades: {e}", exc_info=True)
            self._handle_error(e, f"get_trade_history({symbol})")
            return []

    async def get_twap_fills(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 2000,
        until: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Get TWAP slice fills from Hyperliquid (futures only).
        This is a custom method to fetch TWAP-specific trades.
        """
        try:
            if not await self._check_rate_limit('query', weight=1):
                raise Exception("Rate limit exceeded for TWAP fills")

            # Direct API call to userTwapSliceFills endpoint
            request = {
                'type': 'userTwapSliceFills',
                'user': self.wallet_address,
            }

            # Use the correct CCXT method for Hyperliquid info endpoint
            response = await self.exchange.public_post_info(request)

            if not response or not isinstance(response, list):
                self.logger.warning("No TWAP fills returned from Hyperliquid")
                return []

            # Convert TWAP fills to standard trade format
            normalized_trades = []
            for twap_fill in response:
                if 'fill' in twap_fill:
                    fill = twap_fill['fill']
                    twap_id = twap_fill.get('twapId')

                    # Check if within time range
                    if 'time' in fill:
                        fill_time = datetime.fromtimestamp(fill['time'] / 1000, tz=timezone.utc)

                        if since and fill_time < since:
                            continue
                        if until and fill_time > until:
                            continue

                    # Create a trade object similar to regular fills
                    trade = {
                        'id': fill.get('tid'),
                        'order': fill.get('oid'),
                        'symbol': f"{fill.get('coin')}/USDC:USDC",
                        'side': 'buy' if fill.get('side') == 'B' else 'sell',
                        'amount': float(fill.get('sz', 0)),
                        'price': float(fill.get('px', 0)),
                        'cost': float(fill.get('sz', 0)) * float(fill.get('px', 0)),
                        'fee': {
                            'cost': float(fill.get('fee', 0)),
                            'currency': fill.get('feeToken', 'USDC')
                        },
                        'timestamp': fill.get('time'),
                        'datetime': datetime.fromtimestamp(fill.get('time', 0) / 1000, tz=timezone.utc).isoformat() if fill.get('time') else None,
                        'info': {
                            **fill,
                            'twapId': twap_id,  # Add TWAP ID to info
                            'isTwapFill': True  # Mark as TWAP fill
                        }
                    }

                    normalized_trades.append(self._normalize_trade(trade))

            # Apply symbol filter if specified
            if symbol:
                normalized_symbol = self.normalize_symbol(symbol)
                normalized_trades = [t for t in normalized_trades if t.get('symbol') == normalized_symbol]

            # Apply limit
            if limit and len(normalized_trades) > limit:
                normalized_trades = normalized_trades[:limit]

            self.logger.info(f"Successfully fetched {len(normalized_trades)} TWAP fills from Hyperliquid")
            return normalized_trades

        except Exception as e:
            self.logger.error(f"Error fetching TWAP fills: {e}", exc_info=True)
            return []

    async def get_positions(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get open positions (futures only)."""
        if self.market_type == 'spot':
            return []  # Spot markets don't have positions

        try:
            if not await self._check_rate_limit('query'):
                raise Exception("Rate limit exceeded for positions")

            positions = await self.exchange.fetch_positions()

            result = []
            for position in positions:
                if symbol and position['symbol'] != self.normalize_symbol(symbol):
                    continue

                # Only include positions with non-zero size
                if abs(float(position.get('contracts', 0))) > 0:
                    result.append(self._normalize_position(position))

            return result

        except Exception as e:
            self._handle_error(e, f"get_positions({symbol})")
            return []

    async def transfer_funds(self, amount: Decimal, to_perp: bool = True) -> bool:
        """
        Transfer USDC between spot and perp accounts.
        Args:
            amount: Amount of USDC to transfer
            to_perp: True for spot -> perp, False for perp -> spot
        Returns:
            True if transfer successful, False otherwise
        """
        try:
            if not await self._check_rate_limit('transfer'):
                raise Exception("Rate limit exceeded for transfer")

            amount_str = str(float(amount))

            self.logger.info(f"Attempting USDC transfer: {amount_str} {'spot->perp' if to_perp else 'perp->spot'}")
            self.logger.info(f"Using wallet: {self.wallet_address}")

            # Create the transfer request using Hyperliquid's usdClassTransfer
            timestamp = int(time.time() * 1000)  # Current timestamp in milliseconds

            # When using an API wallet to transfer on behalf of a main wallet,
            # we need to specify the subaccount in the amount field
            # Check if we're using an API wallet (different from main wallet)
            signing_wallet = self.exchange.walletAddress if hasattr(self.exchange, 'walletAddress') else self.wallet_address

            if signing_wallet != self.wallet_address:
                # We're using an API wallet, need to specify subaccount
                formatted_amount = f"{amount_str} subaccount:{self.wallet_address}"
                self.logger.info(f"Using API wallet {signing_wallet} to transfer for main wallet {self.wallet_address}")
            else:
                # Same wallet, no subaccount needed
                formatted_amount = amount_str

            self.logger.info(f"Using transfer format: {formatted_amount}")

            action = {
                "type": "usdClassTransfer",
                "hyperliquidChain": "Mainnet",  # Always use Mainnet now
                "signatureChainId": "0xa4b1",  # Arbitrum chain ID
                "amount": formatted_amount,
                "toPerp": to_perp,
                "nonce": timestamp
            }

            # Prepare the request body
            request_body = {
                'action': action,
                'nonce': timestamp,
                'signature': {}  # CCXT should handle signing
            }

            self.logger.info(f"Transfer request body: {json.dumps(request_body, indent=2)}")

            # Use CCXT's privatePostExchange which should handle the signing
            try:
                response = await self.exchange.privatePostExchange(request_body)

                if response and response.get('status') == 'ok':
                    self.logger.info(f"Transfer successful: {response}")
                    return True
                else:
                    self.logger.error(f"Transfer failed with response: {response}")
                    return False

            except Exception as e:
                self.logger.error(f"Transfer request failed: {str(e)}")

                # If we get a specific error about deposits, log it clearly
                if "Must deposit before performing actions" in str(e):
                    self.logger.error(
                        f"Transfer failed: The signing wallet {self.exchange.walletAddress if hasattr(self.exchange, 'walletAddress') else 'unknown'} "
                        f"does not have permission to transfer from {self.wallet_address}. "
                        f"You need to either:\n"
                        f"1. Approve the API wallet on Hyperliquid\n"
                        f"2. Use the main wallet's private key instead\n"
                        f"3. Fund the API wallet with USDC"
                    )

                return False

        except Exception as e:
            self.logger.error(f"Error in transfer_funds: {e}", exc_info=True)
            return False



    async def get_max_leverage(self, symbol: str) -> int:
        """
        Fetch the max leverage for a given perp asset using the meta/details endpoint.
        Args:
            symbol: Standard symbol (e.g., BTC/USDT)
        Returns:
            Max leverage as integer
        """
        try:
            # For Hyperliquid, we need to use the info endpoint to get metadata
            request = {
                'type': 'meta'
            }
            meta = await self.exchange.public_post_info(request)

            # Find the asset in the universe
            base = symbol.split('/')[0]
            for asset in meta.get('universe', []):
                if asset.get('name', '').upper() == base.upper():
                    max_leverage = int(asset.get('maxLeverage', 1))
                    self.logger.info(f"Max leverage for {symbol}: {max_leverage}")
                    return max_leverage
            self.logger.warning(f"Asset {symbol} not found in meta/universe, defaulting max leverage to 1")
            return 1
        except Exception as e:
            self.logger.error(f"Error fetching max leverage for {symbol}: {e}")
            return 1

    async def get_market_precision(self, symbol: str) -> Dict[str, int]:
        """
        Fetch the precision requirements for a given asset.
        Args:
            symbol: Standard symbol (e.g., BTC/USDT, UFART/USDC)
        Returns:
            Dict with 'amount' and 'price' precision decimals
        """
        try:
            # For Hyperliquid, we need to use the info endpoint to get metadata
            request = {
                'type': 'meta'
            }
            meta = await self.exchange.public_post_info(request)

            # Find the asset in the universe
            base = symbol.split('/')[0]
            for asset in meta.get('universe', []):
                if asset.get('name', '').upper() == base.upper():
                    # Hyperliquid uses szDecimals for amount precision
                    amount_precision = int(asset.get('szDecimals', 5))  # Default 5 decimals
                    # For price, use 6 decimals as standard for USDC pairs
                    price_precision = 6

                    result = {
                        'amount': amount_precision,
                        'price': price_precision
                    }
                    self.logger.info(f"Market precision for {symbol}: {result}")
                    return result

            self.logger.warning(f"Asset {symbol} not found in meta/universe, using default precision")
            return {'amount': 5, 'price': 6}  # Safe defaults

        except Exception as e:
            self.logger.error(f"Error fetching market precision for {symbol}: {e}")
            return {'amount': 5, 'price': 6}  # Safe defaults

    def _encode_hyperliquid_cloid(self, plain_cloid: str) -> Optional[str]:
        """
        Encode plain text client order ID to Hyperliquid's expected hex format.
        
        Format: 0x + strategy_prefix (14 hex chars) + unique_id (hex)
        Based on existing decoder patterns.
        
        Args:
            plain_cloid: Plain text client order ID like "vol_s_hyp_767960"
            
        Returns:
            Encoded hex client order ID like "0x766f6c756d655f77656967687465640000abc123"
        """
        if not plain_cloid:
            return None
            
        try:
            # Strategy prefix for volume_weighted_top_of_book: "volwgt" -> hex
            strategy_name = "volwgt"  # 6 chars to fit in 7 bytes with padding
            strategy_bytes = strategy_name.encode('ascii')
            
            # Pad to exactly 7 bytes (14 hex characters)
            strategy_padded = strategy_bytes.ljust(7, b'\x00')
            strategy_hex = strategy_padded.hex()  # 14 hex characters
            
            # Convert unique part to hex (take the timestamp/unique part)
            if '_' in plain_cloid:
                unique_part = plain_cloid.split('_')[-1]  # Get last part like "767960"
            else:
                unique_part = plain_cloid
            
            # Convert unique part to hex, but keep it short (max 8 chars)
            unique_part = unique_part[-8:]  # Take last 8 characters
            unique_hex = unique_part.encode('ascii').hex()
            
            # Combine: 0x + strategy_prefix + unique_hex  
            encoded_cloid = f"0x{strategy_hex}{unique_hex}"
            
            self.logger.debug(f"Encoded Hyperliquid cloid: {plain_cloid} -> {encoded_cloid}")
            return encoded_cloid
            
        except Exception as e:
            self.logger.error(f"Error encoding Hyperliquid cloid {plain_cloid}: {e}")
            return None

    def normalize_symbol(self, symbol: str) -> str:
        """Convert standard symbol format to Hyperliquid format."""
        if not symbol:
            return symbol
        cache_key = f"{self.market_type}:{symbol}"
        if cache_key in self._resolved_symbol_cache:
            return self._resolved_symbol_cache[cache_key]

        def resolve_candidate(base_token: str, quote_token: str, perp: bool) -> Optional[str]:
            """Pick the first existing market among candidate base tickers (UXAUT vs UXAUt etc.)."""
            try:
                markets = getattr(self.exchange, 'markets', {}) or {}
                symbols = set(getattr(self.exchange, 'symbols', []) or [])
            except Exception:
                markets = {}
                symbols = set()
            base_candidates = []
            b = base_token
            # Prefer Hyperliquid UXAUT variants for XAUT
            if b.upper() == 'XAUT':
                base_candidates.extend(['XAUT0', 'UXAUT', 'UXAUt', 'XAUT', 'XAUt'])
            else:
                base_candidates.extend([b, b.upper(), b.capitalize()])
            # Build symbols and pick the first that exists
            for cand in base_candidates:
                # Try both spot-style and perp-style suffixes, regardless of requested type
                candidates_to_try = []
                # Normalize quote preference
                if 'XAU' in base_token.upper():
                    qt = 'USDT0'
                else:
                    qt = 'USDC' if quote_token.upper() in ('USDT', 'USD', 'USDC') else quote_token
                candidates_to_try.append(f"{cand}/{qt}")
                candidates_to_try.append(f"{cand}/{qt}:USDC")
                for s in candidates_to_try:
                    if s in symbols or s in markets:
                        return s
            return None

        if self.market_type == 'spot':
            # For spot markets, use <BASE>/USDC format
            if '/' in symbol:
                base, quote = symbol.split('/')
                # Try to resolve actual listed market symbol first
                resolved = resolve_candidate(base, quote, perp=False)
                if resolved:
                    self._resolved_symbol_cache[cache_key] = resolved
                    self.logger.debug(f"Resolved Hyperliquid spot symbol {symbol} -> {resolved}")
                    return resolved
                # Fallback mappings
                if base.upper() == 'XAUT':
                    base = 'UXAUT'
                if 'XAU' in base.upper():
                    quote = 'USDT0'
                elif quote.upper() in ('USDT', 'USD'):
                    quote = 'USDC'
                # Try both suffixes as a last resort
                fallback_candidates = [f"{base}/{quote}", f"{base}/{quote}:USDC"]
                for s in fallback_candidates:
                    try:
                        if s in (getattr(self.exchange, 'symbols', []) or []) or s in (getattr(self.exchange, 'markets', {}) or {}):
                            self._resolved_symbol_cache[cache_key] = s
                            self.logger.debug(f"Resolved Hyperliquid spot symbol {symbol} -> {s}")
                            return s
                    except Exception:
                        pass
                # # As a final attempt, scan listed symbols for any XAUT-like USDC markets
                # try:
                #     sym_list = list(getattr(self.exchange, 'symbols', []) or [])
                #     matches = [s for s in sym_list if ('XAU' in s.upper()) and ('USDC' in s.upper())]
                #     if matches:
                #         chosen = matches[0]
                #         self._resolved_symbol_cache[cache_key] = chosen
                #         self.logger.debug(f"Resolved Hyperliquid spot symbol {symbol} -> {chosen}")
                #         return chosen
                # except Exception:
                #     pass
                # No valid market found
                # self.logger.error(f"No listed Hyperliquid spot market found for {symbol} (tried {fallback_candidates}).")
                # return None

            # Return as-is if already in correct format
            return symbol
        else:
            # For futures markets, use <BASE>/USDC:USDC format
            if '/' in symbol:
                base, quote = symbol.split('/')
                # Try to resolve actual listed market symbol first
                resolved = resolve_candidate(base, quote, perp=True)
                if resolved:
                    self._resolved_symbol_cache[cache_key] = resolved
                    self.logger.debug(f"Resolved Hyperliquid perp symbol {symbol} -> {resolved}")
                    return resolved
                # Fallback mappings
                if base.upper() == 'XAUT':
                    base = 'UXAUT'
                if quote.upper() in ('USDT', 'USD', 'USDC'):
                    # Try both suffixes as a last resort
                    fallback_candidates = [f"{base}/USDC:USDC", f"{base}/USDC"]
                    for s in fallback_candidates:
                        try:
                            if s in (getattr(self.exchange, 'symbols', []) or []) or s in (getattr(self.exchange, 'markets', {}) or {}):
                                self._resolved_symbol_cache[cache_key] = s
                                self.logger.debug(f"Resolved Hyperliquid perp symbol {symbol} -> {s}")
                                return s
                        except Exception:
                            pass
                    try:
                        sym_list = list(getattr(self.exchange, 'symbols', []) or [])
                        matches = [s for s in sym_list if ('XAU' in s.upper()) and ('USDC' in s.upper()) or ('USDT' in s.upper())]
                        if matches:
                            chosen = matches[0]
                            self._resolved_symbol_cache[cache_key] = chosen
                            self.logger.debug(f"Resolved Hyperliquid perp symbol {symbol} -> {chosen}")
                            return chosen
                    except Exception:
                        pass
                    self.logger.error(f"No listed Hyperliquid perp market found for {symbol} (tried {fallback_candidates}).")
                    return None

            # Return as-is if already in correct format or not recognized
            return symbol

    def denormalize_symbol(self, symbol: str) -> str:
        """Convert Hyperliquid symbol to standard format."""
        if not symbol:
            return symbol

        if self.market_type == 'spot':
            # For spot markets, only apply XAUT-specific mapping, leave other symbols unchanged
            if '/USDC' in symbol and ':' not in symbol:
                base = symbol.split('/')[0]
                if base.upper() == 'UXAUT':
                    # Only transform UXAUT to XAUT/USDT0, leave BERA and others unchanged
                    return f"XAUT/USDT0"
                else:
                    # For BERA and other symbols, use as-is
                    return symbol
            elif '/USDT' in symbol and ':' not in symbol:
                base = symbol.split('/')[0]
                if base.upper() == 'UXAUT':
                    base = 'XAUT'
                return f"{base}/USDT0"
        else:
            # For futures markets, preserve original symbols but handle UXAUT -> XAUT mapping
            if '/USDC:USDC' in symbol:
                base = symbol.split('/')[0]
                if base.upper() == 'UXAUT':
                    # Only map UXAUT to XAUT, preserve USDC:USDC format
                    return 'XAUT/USDC:USDC'
                else:
                    # Preserve original USDC:USDC format for other symbols like BERA
                    return symbol
            elif '/USDT' in symbol:
                base = symbol.split('/')[0]
                if base.upper() == 'UXAUT':
                    base = 'XAUT'
                return f"{base}/USDT0"

        # Return as-is if already in standard format
        return symbol

    def _normalize_order(self, order: Dict) -> Dict[str, Any]:
        """Normalize ccxt order to standard format."""

        def safe_decimal(value, default=0):
            """Safely convert value to Decimal."""
            if value is None:
                return None if default is None else Decimal(str(default))
            try:
                return Decimal(str(value))
            except (ValueError, TypeError, decimal.InvalidOperation):
                return None if default is None else Decimal(str(default))

        return {
            'id': order.get('id'),
            'symbol': self.denormalize_symbol(order.get('symbol', '')),
            'side': order.get('side'),
            'amount': safe_decimal(order.get('amount'), 0),
            'price': safe_decimal(order.get('price'), None),
            'filled': safe_decimal(order.get('filled'), 0),
            'remaining': safe_decimal(order.get('remaining'), 0),
            'status': self._normalize_status(order.get('status')),
            'type': order.get('type'),
            'timestamp': order.get('timestamp'),
            'datetime': order.get('datetime'),
            'fee': order.get('fee'),
            'exchange': 'hyperliquid',
            'raw': order
        }

    def _normalize_trade(self, trade: Dict) -> Dict[str, Any]:
        """Normalize ccxt trade to standard format."""
        # Log the raw trade data for debugging
        logger.debug(f"[HYPERLIQUID] Raw trade data: id={trade.get('id')} raw={trade}")

        def safe_decimal(value, default=0):
            """Safely convert value to Decimal."""
            if value is None:
                return None if default is None else Decimal(str(default))
            try:
                return Decimal(str(value))
            except (ValueError, TypeError, decimal.InvalidOperation):
                return None if default is None else Decimal(str(default))

        # Extract client_order_id from info.cloid or info.c for Hyperliquid
        info = trade.get('info', {})
        client_order_id = info.get('cloid') or info.get('c', '')
        if not client_order_id:
            client_order_id = "manual_order"
        else:
            # Decode Hyperliquid client order ID
            from utils.client_order_decoder import ClientOrderDecoder

            decoded_info = ClientOrderDecoder.decode_client_order_id(
                client_order_id,
                'hyperliquid'
            )

            logger.info(
                f"Decoded Hyperliquid client order ID: {client_order_id} -> "
                f"{decoded_info['decoded_cloid']} (strategy: {decoded_info['strategy_type']})"
            )

            # Use decoded version for storage
            client_order_id = decoded_info['decoded_cloid']

        # Ensure symbol is denormalized to standard format
        symbol = self.denormalize_symbol(trade.get('symbol', ''))

        return {
            'id': trade.get('id'),
            'order_id': trade.get('order'),
            'client_order_id': client_order_id,  # Include decoded client order ID
            'symbol': symbol,
            'side': trade.get('side'),
            'amount': safe_decimal(trade.get('amount'), 0),
            'price': safe_decimal(trade.get('price'), 0),
            'cost': safe_decimal(trade.get('cost'), 0),
            'fee': trade.get('fee'),
            'timestamp': trade.get('timestamp'),
            'datetime': trade.get('datetime'),
            'exchange': 'hyperliquid',
            'raw': trade
        }

    def _normalize_position(self, position: Dict) -> Dict[str, Any]:
        """Normalize ccxt position to standard format."""

        def safe_decimal(value, default=0):
            """Safely convert value to Decimal."""
            if value is None:
                return None if default is None else Decimal(str(default))
            try:
                return Decimal(str(value))
            except (ValueError, TypeError, decimal.InvalidOperation):
                return None if default is None else Decimal(str(default))

        contracts = safe_decimal(position.get('contracts'), 0)

        return {
            'symbol': self.denormalize_symbol(position.get('symbol', '')),
            'side': 'long' if contracts > 0 else 'short',
            'size': abs(contracts),
            'entry_price': safe_decimal(position.get('entryPrice'), 0),
            'mark_price': safe_decimal(position.get('markPrice'), 0),
            'unrealized_pnl': safe_decimal(position.get('unrealizedPnl'), 0),
            'percentage': safe_decimal(position.get('percentage'), 0),
            'timestamp': position.get('timestamp'),
            'datetime': position.get('datetime'),
            'exchange': 'hyperliquid',
            'raw': position
        }

    def _normalize_status(self, status: str) -> str:
        """Normalize ccxt order status to standard format."""
        if not status:
            return OrderStatus.PENDING

        status_map = {
            'open': OrderStatus.OPEN,
            'closed': OrderStatus.FILLED,
            'canceled': OrderStatus.CANCELED,
            'cancelled': OrderStatus.CANCELED,
            'rejected': OrderStatus.REJECTED
        }
        return status_map.get(status.lower(), status)