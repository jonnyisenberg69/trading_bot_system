"""
Trade Reconciliation System

Provides comprehensive trade reconciliation across all supported exchanges:
- Compare internal trade records vs exchange trade history
- Detect missing trades and discrepancies  
- Auto-correction capabilities
- Detailed reconciliation reporting
- Real-time monitoring and alerting

Supported Exchanges:
- Binance (spot + futures)
- Bybit (spot + futures) 
- Hyperliquid (perpetual)
- MEXC (spot)
- Gate.io (spot)
- Bitget (spot)
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any, Tuple, Set
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
import structlog
from collections import defaultdict
import uuid

from .tracking import PositionManager
from database.repositories.trade_repository import TradeRepository
from database.repositories.position_repository import PositionRepository
from exchanges.connectors import (
    TRADING_CONNECTORS, 
    create_exchange_connector,
    list_supported_exchanges
)


logger = structlog.get_logger(__name__)


class DiscrepancyType(str, Enum):
    """Types of trade discrepancies."""
    MISSING_INTERNAL = 'missing_internal'  # Trade exists on exchange but not internally
    MISSING_EXCHANGE = 'missing_exchange'  # Trade exists internally but not on exchange
    AMOUNT_MISMATCH = 'amount_mismatch'    # Trade amounts don't match
    PRICE_MISMATCH = 'price_mismatch'      # Trade prices don't match
    FEE_MISMATCH = 'fee_mismatch'          # Fee amounts don't match
    TIMESTAMP_MISMATCH = 'timestamp_mismatch'  # Timestamps significantly different


class ReconciliationStatus(str, Enum):
    """Status of reconciliation process."""
    PENDING = 'pending'
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'
    PARTIAL = 'partial'


@dataclass
class TradeDiscrepancy:
    """Represents a discrepancy found during reconciliation."""
    id: str
    exchange: str
    symbol: str
    discrepancy_type: DiscrepancyType
    internal_trade: Optional[Dict[str, Any]] = None
    exchange_trade: Optional[Dict[str, Any]] = None
    severity: str = 'medium'  # low, medium, high, critical
    description: str = ''
    suggested_action: str = ''
    auto_correctable: bool = False
    created_at: datetime = None
    resolved_at: Optional[datetime] = None
    resolution_action: Optional[str] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if not self.id:
            self.id = str(uuid.uuid4())


@dataclass
class ReconciliationReport:
    """Comprehensive reconciliation report."""
    id: str
    exchange: str
    symbol: Optional[str]
    start_time: datetime
    end_time: datetime
    status: ReconciliationStatus
    
    # Trade counts
    internal_trades_count: int = 0
    exchange_trades_count: int = 0
    matched_trades_count: int = 0
    
    # Discrepancy summary
    discrepancies: List[TradeDiscrepancy] = None
    discrepancies_by_type: Dict[str, int] = None
    
    # Financial impact
    total_amount_discrepancy: Decimal = Decimal('0')
    total_fee_discrepancy: Decimal = Decimal('0')
    
    # Performance metrics
    processing_time_seconds: float = 0
    last_reconciled_trade_time: Optional[datetime] = None
    
    # Auto-correction results
    auto_corrections_applied: int = 0
    manual_review_required: int = 0
    
    def __post_init__(self):
        if self.discrepancies is None:
            self.discrepancies = []
        if self.discrepancies_by_type is None:
            self.discrepancies_by_type = {}
        if not self.id:
            self.id = str(uuid.uuid4())


class TradeReconciliation:
    """
    Trade reconciliation system for comparing internal records with exchange data.
    
    Features:
    - Multi-exchange support with unified interface
    - Real-time and batch reconciliation modes
    - Automatic discrepancy detection and correction
    - Comprehensive reporting and alerting
    - Position impact analysis
    """
    
    def __init__(
        self,
        exchange_connectors: Dict[str, Any],
        trade_repository: TradeRepository,
        position_repository: PositionRepository,
        position_manager: Optional[PositionManager] = None
    ):
        """
        Initialize trade reconciliation system.
        
        Args:
            exchange_connectors: Dictionary of exchange connector instances
            trade_repository: Repository for trade data
            position_repository: Repository for position data
            position_manager: Position manager for real-time updates
        """
        self.exchange_connectors = exchange_connectors
        self.trade_repository = trade_repository
        self.position_repository = position_repository
        self.position_manager = position_manager
        
        # Reconciliation state
        self.active_reconciliations = {}
        self.reconciliation_history = []
        
        # Configuration
        self.config = {
            'reconciliation_window_hours': 24,  # Default reconciliation window
            'batch_size': 1000,  # Trades to process in one batch
            'price_tolerance_percent': 0.01,  # 0.01% price tolerance
            'timestamp_tolerance_seconds': 60,  # 60 second timestamp tolerance
            'auto_correction_enabled': True,
            'max_auto_correction_amount': 100,  # Max amount for auto-correction
            'alert_thresholds': {
                'critical_discrepancy_count': 10,
                'critical_amount_threshold': 1000,
                'missing_trade_threshold': 5
            }
        }
        
        # Supported exchanges and their capabilities
        self.supported_exchanges = list_supported_exchanges()
        
        self.logger = logger.bind(component="TradeReconciliation")
        self.logger.info("Trade reconciliation system initialized")
        
    async def start_reconciliation(
        self,
        exchange: str,
        symbol: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        auto_correct: bool = True
    ) -> ReconciliationReport:
        """
        Start comprehensive trade reconciliation for an exchange.
        
        Args:
            exchange: Exchange name to reconcile
            symbol: Specific symbol to reconcile (None for all)
            start_time: Start time for reconciliation window
            end_time: End time for reconciliation window  
            auto_correct: Whether to apply auto-corrections
            
        Returns:
            ReconciliationReport with detailed results
        """
        # Validate exchange
        if exchange not in self.exchange_connectors:
            raise ValueError(f"Exchange {exchange} not available in connectors")
            
        # Set default time window
        if end_time is None:
            end_time = datetime.utcnow()
        if start_time is None:
            start_time = end_time - timedelta(hours=self.config['reconciliation_window_hours'])
            
        # Create reconciliation report
        report = ReconciliationReport(
            id=str(uuid.uuid4()),
            exchange=exchange,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            status=ReconciliationStatus.RUNNING
        )
        
        self.active_reconciliations[report.id] = report
        start_processing = datetime.utcnow()
        
        try:
            self.logger.info(f"Starting reconciliation for {exchange}", 
                           symbol=symbol, start_time=start_time, end_time=end_time)
            
            # Step 1: Fetch internal trades
            internal_trades = await self._fetch_internal_trades(
                exchange, symbol, start_time, end_time
            )
            report.internal_trades_count = len(internal_trades)
            
            # Step 2: Fetch exchange trades
            exchange_trades = await self._fetch_exchange_trades(
                exchange, symbol, start_time, end_time
            )
            report.exchange_trades_count = len(exchange_trades)
            
            self.logger.info(f"Fetched trades for reconciliation",
                           internal_count=len(internal_trades),
                           exchange_count=len(exchange_trades))
            
            # Step 3: Compare and find discrepancies
            discrepancies = await self._compare_trades(
                internal_trades, exchange_trades, exchange, symbol
            )
            report.discrepancies = discrepancies
            
            # Step 4: Categorize discrepancies
            report.discrepancies_by_type = self._categorize_discrepancies(discrepancies)
            
            # Step 5: Calculate financial impact
            report.total_amount_discrepancy, report.total_fee_discrepancy = \
                self._calculate_financial_impact(discrepancies)
            
            # Step 6: Apply auto-corrections if enabled
            if auto_correct and self.config['auto_correction_enabled']:
                corrections_applied = await self._apply_auto_corrections(
                    discrepancies, exchange, symbol
                )
                report.auto_corrections_applied = corrections_applied
                
            # Step 7: Generate alerts for manual review
            manual_review_items = [d for d in discrepancies if not d.auto_correctable]
            report.manual_review_required = len(manual_review_items)
            
            if manual_review_items:
                await self._generate_alerts(manual_review_items, exchange)
            
            # Complete reconciliation
            report.status = ReconciliationStatus.COMPLETED
            report.matched_trades_count = (report.internal_trades_count + 
                                         report.exchange_trades_count - 
                                         len(discrepancies))
            
        except Exception as e:
            self.logger.error(f"Reconciliation failed for {exchange}: {e}")
            report.status = ReconciliationStatus.FAILED
            raise
            
        finally:
            # Update timing and cleanup
            end_processing = datetime.utcnow()
            report.processing_time_seconds = (end_processing - start_processing).total_seconds()
            
            # Move to history
            self.reconciliation_history.append(report)
            if report.id in self.active_reconciliations:
                del self.active_reconciliations[report.id]
                
        self.logger.info(f"Reconciliation completed for {exchange}",
                        status=report.status,
                        discrepancies=len(report.discrepancies),
                        auto_corrections=report.auto_corrections_applied)
        
        return report
    
    async def reconcile_all_exchanges(
        self,
        symbol: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[ReconciliationReport]:
        """
        Reconcile trades across all connected exchanges.
        
        Args:
            symbol: Specific symbol to reconcile
            start_time: Start time for reconciliation
            end_time: End time for reconciliation
            
        Returns:
            List of reconciliation reports for each exchange
        """
        reports = []
        
        # Process all connected exchanges
        tasks = []
        for exchange in self.exchange_connectors:
            task = asyncio.create_task(
                self.start_reconciliation(exchange, symbol, start_time, end_time)
            )
            tasks.append((exchange, task))
            
        # Wait for all reconciliations to complete
        for exchange, task in tasks:
            try:
                report = await task
                reports.append(report)
            except Exception as e:
                self.logger.error(f"Failed to reconcile {exchange}: {e}")
                
        return reports
    
    async def _fetch_internal_trades(
        self,
        exchange: str,
        symbol: Optional[str],
        start_time: datetime,
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """Fetch internal trade records from database."""
        try:
            # Get exchange ID (this would need to be implemented based on your database schema)
            exchange_id = await self._get_exchange_id(exchange)
            
            if symbol:
                trades = await self.trade_repository.get_trades_by_symbol(
                    symbol=symbol,
                    exchange_id=exchange_id,
                    start_time=start_time,
                    end_time=end_time,
                    limit=10000  # Large limit for reconciliation
                )
            else:
                # Fetch all trades for the exchange in the time range
                trades = await self._fetch_all_exchange_trades_internal(
                    exchange_id, start_time, end_time
                )
                
            # Convert to standard format
            return [self._normalize_internal_trade(trade) for trade in trades]
            
        except Exception as e:
            self.logger.error(f"Failed to fetch internal trades for {exchange}: {e}")
            return []
    
    async def _fetch_exchange_trades(
        self,
        exchange: str,
        symbol: Optional[str],
        start_time: datetime,
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """Fetch trade records directly from exchange."""
        try:
            connector = self.exchange_connectors[exchange]
            
            # Handle single symbol vs all symbols
            if symbol:
                symbols = [symbol]
            else:
                # Get all actively traded symbols for this exchange
                symbols = await self._get_active_symbols(exchange)
            
            all_trades = []
            
            # Fetch trades for each symbol
            for sym in symbols:
                try:
                    trades = await connector.get_trade_history(
                        symbol=sym,
                        since=start_time,
                        limit=1000
                    )
                    
                    # Filter by time range
                    filtered_trades = [
                        trade for trade in trades
                        if start_time <= self._parse_trade_timestamp(trade) <= end_time
                    ]
                    
                    all_trades.extend(filtered_trades)
                    
                    # Rate limiting
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    self.logger.warning(f"Failed to fetch trades for {sym} on {exchange}: {e}")
                    continue
            
            return [self._normalize_exchange_trade(trade, exchange) for trade in all_trades]
            
        except Exception as e:
            self.logger.error(f"Failed to fetch exchange trades for {exchange}: {e}")
            return []
    
    async def _compare_trades(
        self,
        internal_trades: List[Dict[str, Any]],
        exchange_trades: List[Dict[str, Any]],
        exchange: str,
        symbol: Optional[str]
    ) -> List[TradeDiscrepancy]:
        """Compare internal and exchange trades to find discrepancies."""
        discrepancies = []
        
        # Create lookup maps
        internal_map = {trade['exchange_trade_id']: trade for trade in internal_trades}
        exchange_map = {trade['id']: trade for trade in exchange_trades}
        
        # Find trades missing internally
        for trade_id, exchange_trade in exchange_map.items():
            if trade_id not in internal_map:
                discrepancy = TradeDiscrepancy(
                    id=str(uuid.uuid4()),
                    exchange=exchange,
                    symbol=exchange_trade['symbol'],
                    discrepancy_type=DiscrepancyType.MISSING_INTERNAL,
                    exchange_trade=exchange_trade,
                    severity='high',
                    description=f"Trade {trade_id} exists on exchange but not in internal records",
                    suggested_action="Add missing trade to internal records",
                    auto_correctable=True
                )
                discrepancies.append(discrepancy)
        
        # Find trades missing on exchange
        for trade_id, internal_trade in internal_map.items():
            if trade_id not in exchange_map:
                discrepancy = TradeDiscrepancy(
                    id=str(uuid.uuid4()),
                    exchange=exchange,
                    symbol=internal_trade['symbol'],
                    discrepancy_type=DiscrepancyType.MISSING_EXCHANGE,
                    internal_trade=internal_trade,
                    severity='critical',
                    description=f"Trade {trade_id} exists in internal records but not on exchange",
                    suggested_action="Investigate phantom trade - possible data corruption",
                    auto_correctable=False
                )
                discrepancies.append(discrepancy)
        
        # Compare matching trades for data discrepancies
        for trade_id in set(internal_map.keys()) & set(exchange_map.keys()):
            internal_trade = internal_map[trade_id]
            exchange_trade = exchange_map[trade_id]
            
            # Check amounts
            if not self._amounts_match(internal_trade['amount'], exchange_trade['amount']):
                discrepancy = TradeDiscrepancy(
                    id=str(uuid.uuid4()),
                    exchange=exchange,
                    symbol=internal_trade['symbol'],
                    discrepancy_type=DiscrepancyType.AMOUNT_MISMATCH,
                    internal_trade=internal_trade,
                    exchange_trade=exchange_trade,
                    severity='high',
                    description=f"Amount mismatch for trade {trade_id}: "
                              f"internal={internal_trade['amount']}, exchange={exchange_trade['amount']}",
                    suggested_action="Update internal trade amount to match exchange",
                    auto_correctable=True
                )
                discrepancies.append(discrepancy)
            
            # Check prices
            if not self._prices_match(internal_trade['price'], exchange_trade['price']):
                discrepancy = TradeDiscrepancy(
                    id=str(uuid.uuid4()),
                    exchange=exchange,
                    symbol=internal_trade['symbol'],
                    discrepancy_type=DiscrepancyType.PRICE_MISMATCH,
                    internal_trade=internal_trade,
                    exchange_trade=exchange_trade,
                    severity='medium',
                    description=f"Price mismatch for trade {trade_id}: "
                              f"internal={internal_trade['price']}, exchange={exchange_trade['price']}",
                    suggested_action="Update internal trade price to match exchange",
                    auto_correctable=True
                )
                discrepancies.append(discrepancy)
            
            # Check fees
            if not self._fees_match(internal_trade.get('fee'), exchange_trade.get('fee')):
                discrepancy = TradeDiscrepancy(
                    id=str(uuid.uuid4()),
                    exchange=exchange,
                    symbol=internal_trade['symbol'],
                    discrepancy_type=DiscrepancyType.FEE_MISMATCH,
                    internal_trade=internal_trade,
                    exchange_trade=exchange_trade,
                    severity='low',
                    description=f"Fee mismatch for trade {trade_id}",
                    suggested_action="Update internal trade fee to match exchange",
                    auto_correctable=True
                )
                discrepancies.append(discrepancy)
        
        return discrepancies
    
    async def _apply_auto_corrections(
        self,
        discrepancies: List[TradeDiscrepancy],
        exchange: str,
        symbol: Optional[str]
    ) -> int:
        """Apply automatic corrections for reconcilable discrepancies."""
        corrections_applied = 0
        
        for discrepancy in discrepancies:
            if not discrepancy.auto_correctable:
                continue
                
            try:
                # Apply correction based on discrepancy type
                if discrepancy.discrepancy_type == DiscrepancyType.MISSING_INTERNAL:
                    # Add missing trade to internal records
                    await self._add_missing_internal_trade(discrepancy.exchange_trade, exchange)
                    corrections_applied += 1
                    
                elif discrepancy.discrepancy_type == DiscrepancyType.AMOUNT_MISMATCH:
                    # Update internal trade amount
                    await self._update_internal_trade_amount(
                        discrepancy.internal_trade, discrepancy.exchange_trade['amount']
                    )
                    corrections_applied += 1
                    
                elif discrepancy.discrepancy_type == DiscrepancyType.PRICE_MISMATCH:
                    # Update internal trade price
                    await self._update_internal_trade_price(
                        discrepancy.internal_trade, discrepancy.exchange_trade['price']
                    )
                    corrections_applied += 1
                    
                elif discrepancy.discrepancy_type == DiscrepancyType.FEE_MISMATCH:
                    # Update internal trade fee
                    await self._update_internal_trade_fee(
                        discrepancy.internal_trade, discrepancy.exchange_trade.get('fee')
                    )
                    corrections_applied += 1
                
                # Mark as resolved
                discrepancy.resolved_at = datetime.utcnow()
                discrepancy.resolution_action = "auto_correction_applied"
                
            except Exception as e:
                self.logger.error(f"Failed to apply auto-correction for discrepancy {discrepancy.id}: {e}")
                continue
        
        if corrections_applied > 0:
            self.logger.info(f"Applied {corrections_applied} auto-corrections for {exchange}")
            
            # Update positions if position manager is available
            if self.position_manager:
                await self._refresh_positions_after_corrections(exchange, symbol)
        
        return corrections_applied
    
    def _categorize_discrepancies(self, discrepancies: List[TradeDiscrepancy]) -> Dict[str, int]:
        """Categorize discrepancies by type."""
        categories = defaultdict(int)
        for discrepancy in discrepancies:
            categories[discrepancy.discrepancy_type.value] += 1
        return dict(categories)
    
    def _calculate_financial_impact(
        self, 
        discrepancies: List[TradeDiscrepancy]
    ) -> Tuple[Decimal, Decimal]:
        """Calculate total financial impact of discrepancies."""
        total_amount_discrepancy = Decimal('0')
        total_fee_discrepancy = Decimal('0')
        
        for discrepancy in discrepancies:
            if discrepancy.discrepancy_type == DiscrepancyType.AMOUNT_MISMATCH:
                internal_amount = Decimal(str(discrepancy.internal_trade['amount']))
                exchange_amount = Decimal(str(discrepancy.exchange_trade['amount']))
                total_amount_discrepancy += abs(internal_amount - exchange_amount)
                
            elif discrepancy.discrepancy_type == DiscrepancyType.FEE_MISMATCH:
                internal_fee = Decimal(str(discrepancy.internal_trade.get('fee', {}).get('cost', 0)))
                exchange_fee = Decimal(str(discrepancy.exchange_trade.get('fee', {}).get('cost', 0)))
                total_fee_discrepancy += abs(internal_fee - exchange_fee)
        
        return total_amount_discrepancy, total_fee_discrepancy
    
    async def _generate_alerts(
        self, 
        discrepancies: List[TradeDiscrepancy], 
        exchange: str
    ) -> None:
        """Generate alerts for manual review items."""
        # Group by severity
        critical_discrepancies = [d for d in discrepancies if d.severity == 'critical']
        high_discrepancies = [d for d in discrepancies if d.severity == 'high']
        
        # Generate critical alerts
        if len(critical_discrepancies) >= self.config['alert_thresholds']['critical_discrepancy_count']:
            await self._send_critical_alert(
                f"Critical reconciliation discrepancies detected for {exchange}",
                f"Found {len(critical_discrepancies)} critical discrepancies requiring immediate attention"
            )
        
        # Generate summary alert
        if high_discrepancies or critical_discrepancies:
            await self._send_summary_alert(exchange, discrepancies)
    
    # Helper methods for trade comparison
    def _amounts_match(self, amount1: float, amount2: float) -> bool:
        """Check if trade amounts match within tolerance."""
        if amount1 == 0 and amount2 == 0:
            return True
        tolerance = abs(amount1) * 0.0001  # 0.01% tolerance
        return abs(amount1 - amount2) <= tolerance
    
    def _prices_match(self, price1: float, price2: float) -> bool:
        """Check if trade prices match within tolerance."""
        if price1 == 0 and price2 == 0:
            return True
        tolerance_percent = self.config['price_tolerance_percent'] / 100
        tolerance = abs(price1) * tolerance_percent
        return abs(price1 - price2) <= tolerance
    
    def _fees_match(self, fee1: Optional[Dict], fee2: Optional[Dict]) -> bool:
        """Check if trade fees match."""
        if fee1 is None and fee2 is None:
            return True
        if fee1 is None or fee2 is None:
            return False
            
        cost1 = fee1.get('cost', 0)
        cost2 = fee2.get('cost', 0)
        
        if cost1 == 0 and cost2 == 0:
            return True
            
        tolerance = max(abs(cost1), abs(cost2)) * 0.01  # 1% tolerance for fees
        return abs(cost1 - cost2) <= tolerance
    
    # Utility methods (stubs - would need full implementation based on your schema)
    async def _get_exchange_id(self, exchange: str) -> int:
        """Get exchange ID from database."""
        # This would need to be implemented based on your database schema
        exchange_map = {
            'binance': 1, 'bybit': 2, 'hyperliquid': 3,
            'mexc': 4, 'gateio': 5, 'bitget': 6
        }
        return exchange_map.get(exchange, 0)
    
    async def _get_active_symbols(self, exchange: str) -> List[str]:
        """Get list of actively traded symbols for an exchange."""
        # This would query your database for symbols that have recent activity
        return ['BTC/USDT', 'ETH/USDT', 'BTC/USDC:USDC']  # Placeholder
    
    def _parse_trade_timestamp(self, trade: Dict[str, Any]) -> datetime:
        """Parse trade timestamp to datetime."""
        timestamp = trade.get('timestamp', trade.get('datetime'))
        if isinstance(timestamp, int):
            return datetime.fromtimestamp(timestamp / 1000)
        elif isinstance(timestamp, str):
            return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        return datetime.utcnow()
    
    def _normalize_internal_trade(self, trade) -> Dict[str, Any]:
        """Normalize internal trade record to standard format."""
        return {
            'id': trade.exchange_trade_id,
            'exchange_trade_id': trade.exchange_trade_id,
            'symbol': trade.symbol,
            'side': trade.side,
            'amount': float(trade.amount),
            'price': float(trade.price),
            'cost': float(trade.cost),
            'fee': {'cost': float(trade.fee_cost or 0), 'currency': trade.fee_currency},
            'timestamp': trade.timestamp,
            'order_id': trade.order_id
        }
    
    def _normalize_exchange_trade(self, trade: Dict[str, Any], exchange: str) -> Dict[str, Any]:
        """Normalize exchange trade to standard format."""
        return {
            'id': trade.get('id'),
            'symbol': trade.get('symbol'),
            'side': trade.get('side'),
            'amount': float(trade.get('amount', 0)),
            'price': float(trade.get('price', 0)),
            'cost': float(trade.get('cost', 0)),
            'fee': trade.get('fee', {}),
            'timestamp': self._parse_trade_timestamp(trade),
            'order_id': trade.get('order'),
            'exchange': exchange
        }
    
    # Auto-correction implementation stubs
    async def _add_missing_internal_trade(self, exchange_trade: Dict[str, Any], exchange: str):
        """Add missing trade to internal records."""
        exchange_id = await self._get_exchange_id(exchange)
        await self.trade_repository.save_trade(exchange_trade, exchange_id)
        
    async def _update_internal_trade_amount(self, internal_trade: Dict[str, Any], correct_amount: float):
        """Update internal trade amount."""
        # Implementation would update the trade record in database
        pass
        
    async def _update_internal_trade_price(self, internal_trade: Dict[str, Any], correct_price: float):
        """Update internal trade price."""
        # Implementation would update the trade record in database
        pass
        
    async def _update_internal_trade_fee(self, internal_trade: Dict[str, Any], correct_fee: Dict):
        """Update internal trade fee."""
        # Implementation would update the trade record in database
        pass
        
    async def _refresh_positions_after_corrections(self, exchange: str, symbol: Optional[str]):
        """Refresh positions after applying corrections."""
        if self.position_manager:
            # This would trigger position recalculation
            pass
    
    async def _send_critical_alert(self, title: str, message: str):
        """Send critical alert (email, Slack, etc.)."""
        self.logger.critical(title, message=message)
        
    async def _send_summary_alert(self, exchange: str, discrepancies: List[TradeDiscrepancy]):
        """Send summary alert."""
        self.logger.warning(f"Reconciliation discrepancies for {exchange}", 
                          count=len(discrepancies))
    
    async def _fetch_all_exchange_trades_internal(self, exchange_id: int, start_time: datetime, end_time: datetime):
        """Fetch all internal trades for an exchange in time range."""
        # This would be implemented to fetch trades across all symbols
        return []
    
    # Public API methods
    async def get_reconciliation_status(self, reconciliation_id: str) -> Optional[ReconciliationReport]:
        """Get status of ongoing or completed reconciliation."""
        if reconciliation_id in self.active_reconciliations:
            return self.active_reconciliations[reconciliation_id]
        
        for report in self.reconciliation_history:
            if report.id == reconciliation_id:
                return report
        
        return None
    
    async def get_recent_discrepancies(
        self, 
        exchange: Optional[str] = None,
        hours: int = 24
    ) -> List[TradeDiscrepancy]:
        """Get recent discrepancies across exchanges."""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        discrepancies = []
        
        for report in self.reconciliation_history:
            if report.start_time >= cutoff_time:
                if exchange is None or report.exchange == exchange:
                    discrepancies.extend(report.discrepancies)
        
        return discrepancies
    
    def get_reconciliation_summary(self) -> Dict[str, Any]:
        """Get overall reconciliation system summary."""
        return {
            'active_reconciliations': len(self.active_reconciliations),
            'total_reconciliations': len(self.reconciliation_history),
            'supported_exchanges': list(self.supported_exchanges.keys()),
            'recent_discrepancies': len(self.get_recent_discrepancies()),
            'config': self.config
        }
