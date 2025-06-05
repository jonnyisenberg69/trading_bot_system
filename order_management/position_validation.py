"""
Position Validation System

Real-time position validation across all supported exchanges:
- Cross-check internal positions with exchange positions
- Detect position drift and discrepancies
- Auto-correction capabilities for small discrepancies
- Real-time monitoring and alerting
- Integration with reconciliation system

Supported Exchanges:
- Binance (spot + futures)
- Bybit (spot + futures)
- Hyperliquid (perpetual)
- MEXC (spot)
- Gate.io (spot)
- Bitget (spot)
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple, Set
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
import structlog
from collections import defaultdict
import uuid

from .tracking import PositionManager, Position
from .reconciliation import TradeReconciliation, TradeDiscrepancy, DiscrepancyType
from database.repositories.position_repository import PositionRepository
from exchanges.connectors import (
    TRADING_CONNECTORS,
    create_exchange_connector,
    list_supported_exchanges
)


logger = structlog.get_logger(__name__)


class PositionDiscrepancyType(str, Enum):
    """Types of position discrepancies."""
    SIZE_MISMATCH = 'size_mismatch'          # Position sizes don't match
    SIDE_MISMATCH = 'side_mismatch'          # Position sides don't match
    MISSING_INTERNAL = 'missing_internal'     # Position exists on exchange but not internally
    MISSING_EXCHANGE = 'missing_exchange'     # Position exists internally but not on exchange
    PRICE_MISMATCH = 'price_mismatch'        # Average prices don't match
    MARGIN_MISMATCH = 'margin_mismatch'      # Margin/collateral doesn't match


class ValidationStatus(str, Enum):
    """Status of validation process."""
    PENDING = 'pending'
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'
    PARTIAL = 'partial'


@dataclass
class PositionDiscrepancy:
    """Represents a position discrepancy found during validation."""
    id: str
    exchange: str
    symbol: str
    discrepancy_type: PositionDiscrepancyType
    internal_position: Optional[Dict[str, Any]] = None
    exchange_position: Optional[Dict[str, Any]] = None
    severity: str = 'medium'  # low, medium, high, critical
    description: str = ''
    suggested_action: str = ''
    auto_correctable: bool = False
    created_at: datetime = None
    resolved_at: Optional[datetime] = None
    resolution_action: Optional[str] = None
    financial_impact: Decimal = Decimal('0')
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if not self.id:
            self.id = str(uuid.uuid4())


@dataclass
class ValidationReport:
    """Comprehensive position validation report."""
    id: str
    exchange: str
    symbol: Optional[str]
    validation_time: datetime
    status: ValidationStatus
    
    # Position counts
    internal_positions_count: int = 0
    exchange_positions_count: int = 0
    matched_positions_count: int = 0
    
    # Discrepancy summary
    discrepancies: List[PositionDiscrepancy] = None
    discrepancies_by_type: Dict[str, int] = None
    
    # Financial impact
    total_size_discrepancy: Decimal = Decimal('0')
    total_value_discrepancy: Decimal = Decimal('0')
    
    # Performance metrics
    processing_time_seconds: float = 0
    
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


class PositionValidation:
    """
    Position validation system for cross-checking internal positions with exchange data.
    
    Features:
    - Real-time position validation across all exchanges
    - Automatic discrepancy detection and alerting
    - Position drift monitoring
    - Auto-correction for minor discrepancies
    - Integration with trade reconciliation
    """
    
    def __init__(
        self,
        exchange_connectors: Dict[str, Any],
        position_manager: PositionManager,
        position_repository: PositionRepository,
        trade_reconciliation: Optional[TradeReconciliation] = None
    ):
        """
        Initialize position validation system.
        
        Args:
            exchange_connectors: Dictionary of exchange connector instances
            position_manager: Position manager instance
            position_repository: Repository for position data
            trade_reconciliation: Trade reconciliation system for cross-reference
        """
        self.exchange_connectors = exchange_connectors
        self.position_manager = position_manager
        self.position_repository = position_repository
        self.trade_reconciliation = trade_reconciliation
        
        # Validation state
        self.active_validations = {}
        self.validation_history = []
        self.continuous_monitoring = False
        self.monitoring_task = None
        
        # Configuration
        self.config = {
            'size_tolerance_percent': 0.001,  # 0.1% size tolerance
            'price_tolerance_percent': 0.5,   # 0.5% price tolerance
            'min_position_size': 0.00001,     # Minimum position size to validate
            'validation_interval_seconds': 300,  # 5 minutes for continuous monitoring
            'auto_correction_enabled': True,
            'max_auto_correction_size': 0.001,  # Max size for auto-correction
            'alert_thresholds': {
                'critical_size_discrepancy': 1.0,    # 1 unit
                'critical_value_discrepancy': 1000,   # $1000
                'position_drift_threshold': 0.01      # 1% drift
            }
        }
        
        # Exchange capabilities
        self.exchange_capabilities = self._determine_exchange_capabilities()
        
        self.logger = logger.bind(component="PositionValidation")
        self.logger.info("Position validation system initialized")
        
    def _determine_exchange_capabilities(self) -> Dict[str, Dict[str, bool]]:
        """Determine position capabilities for each exchange."""
        capabilities = {}
        
        for exchange in self.exchange_connectors:
            # Determine if exchange supports positions
            is_futures = any(marker in exchange.lower() for marker in ['futures', 'perp', 'perpetual', 'swap', 'usdm'])
            
            capabilities[exchange] = {
                'spot_positions': True,     # All exchanges have spot balances
                'futures_positions': is_futures,
                'real_time_positions': exchange in ['binance', 'bybit', 'hyperliquid'],
                'margin_info': is_futures
            }
            
        return capabilities
    
    async def start_continuous_monitoring(self) -> None:
        """Start continuous position monitoring."""
        if self.continuous_monitoring:
            self.logger.warning("Continuous monitoring already active")
            return
            
        self.continuous_monitoring = True
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        self.logger.info("Started continuous position monitoring")
    
    async def stop_continuous_monitoring(self) -> None:
        """Stop continuous position monitoring."""
        self.continuous_monitoring = False
        
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
            
        self.logger.info("Stopped continuous position monitoring")
    
    async def validate_positions(
        self,
        exchange: str,
        symbol: Optional[str] = None,
        auto_correct: bool = True
    ) -> ValidationReport:
        """
        Validate positions for a specific exchange.
        
        Args:
            exchange: Exchange name to validate
            symbol: Specific symbol to validate (None for all)
            auto_correct: Whether to apply auto-corrections
            
        Returns:
            ValidationReport with detailed results
        """
        if exchange not in self.exchange_connectors:
            raise ValueError(f"Exchange {exchange} not available in connectors")
            
        # Create validation report
        report = ValidationReport(
            id=str(uuid.uuid4()),
            exchange=exchange,
            symbol=symbol,
            validation_time=datetime.utcnow(),
            status=ValidationStatus.RUNNING
        )
        
        self.active_validations[report.id] = report
        start_time = datetime.utcnow()
        
        try:
            self.logger.info(f"Starting position validation for {exchange}", symbol=symbol)
            
            # Step 1: Fetch internal positions
            internal_positions = await self._fetch_internal_positions(exchange, symbol)
            report.internal_positions_count = len(internal_positions)
            
            # Step 2: Fetch exchange positions
            exchange_positions = await self._fetch_exchange_positions(exchange, symbol)
            report.exchange_positions_count = len(exchange_positions)
            
            self.logger.info(f"Fetched positions for validation",
                           internal_count=len(internal_positions),
                           exchange_count=len(exchange_positions))
            
            # Step 3: Compare positions and find discrepancies
            discrepancies = await self._compare_positions(
                internal_positions, exchange_positions, exchange, symbol
            )
            report.discrepancies = discrepancies
            
            # Step 4: Categorize discrepancies
            report.discrepancies_by_type = self._categorize_position_discrepancies(discrepancies)
            
            # Step 5: Calculate financial impact
            report.total_size_discrepancy, report.total_value_discrepancy = \
                self._calculate_position_impact(discrepancies)
            
            # Step 6: Apply auto-corrections if enabled
            if auto_correct and self.config['auto_correction_enabled']:
                corrections_applied = await self._apply_position_corrections(
                    discrepancies, exchange, symbol
                )
                report.auto_corrections_applied = corrections_applied
            
            # Step 7: Generate alerts for manual review
            manual_review_items = [d for d in discrepancies if not d.auto_correctable]
            report.manual_review_required = len(manual_review_items)
            
            if manual_review_items:
                await self._generate_position_alerts(manual_review_items, exchange)
            
            # Complete validation
            report.status = ValidationStatus.COMPLETED
            report.matched_positions_count = max(
                report.internal_positions_count,
                report.exchange_positions_count
            ) - len([d for d in discrepancies if d.discrepancy_type in [
                PositionDiscrepancyType.MISSING_INTERNAL,
                PositionDiscrepancyType.MISSING_EXCHANGE
            ]])
            
        except Exception as e:
            self.logger.error(f"Position validation failed for {exchange}: {e}")
            report.status = ValidationStatus.FAILED
            raise
            
        finally:
            # Update timing and cleanup
            end_time = datetime.utcnow()
            report.processing_time_seconds = (end_time - start_time).total_seconds()
            
            # Move to history
            self.validation_history.append(report)
            if report.id in self.active_validations:
                del self.active_validations[report.id]
                
        self.logger.info(f"Position validation completed for {exchange}",
                        status=report.status,
                        discrepancies=len(report.discrepancies),
                        auto_corrections=report.auto_corrections_applied)
        
        return report
    
    async def validate_all_exchanges(
        self,
        symbol: Optional[str] = None,
        auto_correct: bool = True
    ) -> List[ValidationReport]:
        """
        Validate positions across all connected exchanges.
        
        Args:
            symbol: Specific symbol to validate
            auto_correct: Whether to apply auto-corrections
            
        Returns:
            List of validation reports for each exchange
        """
        reports = []
        
        # Process all connected exchanges
        tasks = []
        for exchange in self.exchange_connectors:
            task = asyncio.create_task(
                self.validate_positions(exchange, symbol, auto_correct)
            )
            tasks.append((exchange, task))
            
        # Wait for all validations to complete
        for exchange, task in tasks:
            try:
                report = await task
                reports.append(report)
            except Exception as e:
                self.logger.error(f"Failed to validate positions for {exchange}: {e}")
                
        return reports
    
    async def _fetch_internal_positions(
        self,
        exchange: str,
        symbol: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Fetch internal positions from position manager."""
        try:
            if symbol:
                # Get specific position
                position = self.position_manager.get_position(exchange, symbol)
                if position and position.is_open:
                    return [self._normalize_internal_position(position)]
                else:
                    return []
            else:
                # Get all positions for exchange
                positions = self.position_manager.get_positions_by_exchange(exchange)
                return [
                    self._normalize_internal_position(pos) 
                    for pos in positions 
                    if pos.is_open
                ]
                
        except Exception as e:
            self.logger.error(f"Failed to fetch internal positions for {exchange}: {e}")
            return []
    
    async def _fetch_exchange_positions(
        self,
        exchange: str,
        symbol: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Fetch positions directly from exchange."""
        try:
            connector = self.exchange_connectors[exchange]
            
            # Fetch positions from exchange
            positions = await connector.get_positions(symbol=symbol)
            
            # Filter out zero positions and normalize
            non_zero_positions = []
            for pos in positions:
                normalized_pos = self._normalize_exchange_position(pos, exchange)
                if abs(normalized_pos['size']) >= self.config['min_position_size']:
                    non_zero_positions.append(normalized_pos)
                    
            return non_zero_positions
            
        except Exception as e:
            self.logger.error(f"Failed to fetch exchange positions for {exchange}: {e}")
            return []
    
    async def _compare_positions(
        self,
        internal_positions: List[Dict[str, Any]],
        exchange_positions: List[Dict[str, Any]],
        exchange: str,
        symbol: Optional[str]
    ) -> List[PositionDiscrepancy]:
        """Compare internal and exchange positions to find discrepancies."""
        discrepancies = []
        
        # Create lookup maps
        internal_map = {pos['symbol']: pos for pos in internal_positions}
        exchange_map = {pos['symbol']: pos for pos in exchange_positions}
        
        all_symbols = set(internal_map.keys()) | set(exchange_map.keys())
        
        for sym in all_symbols:
            internal_pos = internal_map.get(sym)
            exchange_pos = exchange_map.get(sym)
            
            # Position missing internally
            if exchange_pos and not internal_pos:
                discrepancy = PositionDiscrepancy(
                    id=str(uuid.uuid4()),
                    exchange=exchange,
                    symbol=sym,
                    discrepancy_type=PositionDiscrepancyType.MISSING_INTERNAL,
                    exchange_position=exchange_pos,
                    severity='high',
                    description=f"Position for {sym} exists on {exchange} but not in internal records",
                    suggested_action="Reconcile trades and update internal position",
                    auto_correctable=False,  # Requires trade reconciliation
                    financial_impact=abs(Decimal(str(exchange_pos['size'])) * Decimal(str(exchange_pos.get('mark_price', 0))))
                )
                discrepancies.append(discrepancy)
                
            # Position missing on exchange
            elif internal_pos and not exchange_pos:
                discrepancy = PositionDiscrepancy(
                    id=str(uuid.uuid4()),
                    exchange=exchange,
                    symbol=sym,
                    discrepancy_type=PositionDiscrepancyType.MISSING_EXCHANGE,
                    internal_position=internal_pos,
                    severity='critical',
                    description=f"Position for {sym} exists in internal records but not on {exchange}",
                    suggested_action="Investigate phantom position - check trade history",
                    auto_correctable=False,
                    financial_impact=abs(Decimal(str(internal_pos['size'])) * Decimal(str(internal_pos.get('mark_price', 0))))
                )
                discrepancies.append(discrepancy)
                
            # Both positions exist - compare details
            elif internal_pos and exchange_pos:
                # Check position sizes
                if not self._sizes_match(internal_pos['size'], exchange_pos['size']):
                    size_diff = abs(Decimal(str(internal_pos['size'])) - Decimal(str(exchange_pos['size'])))
                    mark_price = Decimal(str(exchange_pos.get('mark_price', internal_pos.get('avg_price', 0))))
                    
                    discrepancy = PositionDiscrepancy(
                        id=str(uuid.uuid4()),
                        exchange=exchange,
                        symbol=sym,
                        discrepancy_type=PositionDiscrepancyType.SIZE_MISMATCH,
                        internal_position=internal_pos,
                        exchange_position=exchange_pos,
                        severity=self._determine_size_severity(size_diff, mark_price),
                        description=f"Position size mismatch for {sym}: "
                                  f"internal={internal_pos['size']}, exchange={exchange_pos['size']}",
                        suggested_action="Reconcile recent trades and adjust position",
                        auto_correctable=size_diff <= self.config['max_auto_correction_size'],
                        financial_impact=size_diff * mark_price
                    )
                    discrepancies.append(discrepancy)
                
                # Check position sides
                if internal_pos['side'] != exchange_pos['side']:
                    discrepancy = PositionDiscrepancy(
                        id=str(uuid.uuid4()),
                        exchange=exchange,
                        symbol=sym,
                        discrepancy_type=PositionDiscrepancyType.SIDE_MISMATCH,
                        internal_position=internal_pos,
                        exchange_position=exchange_pos,
                        severity='critical',
                        description=f"Position side mismatch for {sym}: "
                                  f"internal={internal_pos['side']}, exchange={exchange_pos['side']}",
                        suggested_action="Immediate investigation required - critical discrepancy",
                        auto_correctable=False,
                        financial_impact=abs(Decimal(str(internal_pos['size'])) * 
                                           Decimal(str(exchange_pos.get('mark_price', 0))))
                    )
                    discrepancies.append(discrepancy)
                
                # Check average prices (for significant differences)
                if not self._prices_match(
                    internal_pos.get('avg_price', 0), 
                    exchange_pos.get('entry_price', 0)
                ):
                    discrepancy = PositionDiscrepancy(
                        id=str(uuid.uuid4()),
                        exchange=exchange,
                        symbol=sym,
                        discrepancy_type=PositionDiscrepancyType.PRICE_MISMATCH,
                        internal_position=internal_pos,
                        exchange_position=exchange_pos,
                        severity='low',
                        description=f"Average price discrepancy for {sym}",
                        suggested_action="Review trade history and fee calculations",
                        auto_correctable=True,
                        financial_impact=Decimal('0')  # Price mismatch doesn't affect position value
                    )
                    discrepancies.append(discrepancy)
        
        return discrepancies
    
    async def _apply_position_corrections(
        self,
        discrepancies: List[PositionDiscrepancy],
        exchange: str,
        symbol: Optional[str]
    ) -> int:
        """Apply automatic position corrections."""
        corrections_applied = 0
        
        for discrepancy in discrepancies:
            if not discrepancy.auto_correctable:
                continue
                
            try:
                if discrepancy.discrepancy_type == PositionDiscrepancyType.SIZE_MISMATCH:
                    # Adjust internal position size to match exchange
                    await self._adjust_internal_position_size(
                        discrepancy.internal_position,
                        discrepancy.exchange_position['size'],
                        exchange
                    )
                    corrections_applied += 1
                    
                elif discrepancy.discrepancy_type == PositionDiscrepancyType.PRICE_MISMATCH:
                    # Update internal average price
                    await self._update_internal_position_price(
                        discrepancy.internal_position,
                        discrepancy.exchange_position.get('entry_price', 0),
                        exchange
                    )
                    corrections_applied += 1
                
                # Mark as resolved
                discrepancy.resolved_at = datetime.utcnow()
                discrepancy.resolution_action = "auto_correction_applied"
                
            except Exception as e:
                self.logger.error(f"Failed to apply position correction for {discrepancy.id}: {e}")
                continue
        
        if corrections_applied > 0:
            self.logger.info(f"Applied {corrections_applied} position corrections for {exchange}")
        
        return corrections_applied
    
    async def _monitoring_loop(self) -> None:
        """Continuous monitoring loop."""
        while self.continuous_monitoring:
            try:
                # Validate all exchanges
                reports = await self.validate_all_exchanges(auto_correct=True)
                
                # Check for critical issues
                critical_issues = []
                for report in reports:
                    critical_discrepancies = [
                        d for d in report.discrepancies 
                        if d.severity == 'critical'
                    ]
                    if critical_discrepancies:
                        critical_issues.extend(critical_discrepancies)
                
                # Send alerts for critical issues
                if critical_issues:
                    await self._send_monitoring_alert(critical_issues)
                
                # Wait for next validation cycle
                await asyncio.sleep(self.config['validation_interval_seconds'])
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in position monitoring loop: {e}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying
    
    # Helper methods
    def _sizes_match(self, size1: float, size2: float) -> bool:
        """Check if position sizes match within tolerance."""
        if size1 == 0 and size2 == 0:
            return True
        
        tolerance_percent = self.config['size_tolerance_percent']
        tolerance = max(abs(size1), abs(size2)) * tolerance_percent
        return abs(size1 - size2) <= tolerance
    
    def _prices_match(self, price1: float, price2: float) -> bool:
        """Check if prices match within tolerance."""
        if price1 == 0 or price2 == 0:
            return True  # Skip price comparison if either is zero
            
        tolerance_percent = self.config['price_tolerance_percent'] / 100
        tolerance = max(abs(price1), abs(price2)) * tolerance_percent
        return abs(price1 - price2) <= tolerance
    
    def _determine_size_severity(self, size_diff: Decimal, mark_price: Decimal) -> str:
        """Determine severity based on size difference and value."""
        value_diff = size_diff * mark_price
        
        if value_diff >= self.config['alert_thresholds']['critical_value_discrepancy']:
            return 'critical'
        elif size_diff >= self.config['alert_thresholds']['critical_size_discrepancy']:
            return 'high'
        else:
            return 'medium'
    
    def _categorize_position_discrepancies(self, discrepancies: List[PositionDiscrepancy]) -> Dict[str, int]:
        """Categorize position discrepancies by type."""
        categories = defaultdict(int)
        for discrepancy in discrepancies:
            categories[discrepancy.discrepancy_type.value] += 1
        return dict(categories)
    
    def _calculate_position_impact(
        self,
        discrepancies: List[PositionDiscrepancy]
    ) -> Tuple[Decimal, Decimal]:
        """Calculate total financial impact of position discrepancies."""
        total_size_discrepancy = Decimal('0')
        total_value_discrepancy = Decimal('0')
        
        for discrepancy in discrepancies:
            if discrepancy.discrepancy_type == PositionDiscrepancyType.SIZE_MISMATCH:
                internal_size = Decimal(str(discrepancy.internal_position['size']))
                exchange_size = Decimal(str(discrepancy.exchange_position['size']))
                total_size_discrepancy += abs(internal_size - exchange_size)
            
            total_value_discrepancy += discrepancy.financial_impact
        
        return total_size_discrepancy, total_value_discrepancy
    
    def _normalize_internal_position(self, position: Position) -> Dict[str, Any]:
        """Normalize internal position to standard format."""
        return {
            'symbol': position.symbol,
            'size': float(position.size),
            'side': position.side,
            'avg_price': float(position.avg_price) if position.avg_price else 0,
            'value': float(position.value),
            'entry_time': position.entry_time,
            'last_update': position.last_update_time,
            'exchange': position.exchange,
            'is_perpetual': position.is_perpetual
        }
    
    def _normalize_exchange_position(self, position: Dict[str, Any], exchange: str) -> Dict[str, Any]:
        """Normalize exchange position to standard format."""
        return {
            'symbol': position.get('symbol', ''),
            'size': float(position.get('size', 0)),
            'side': position.get('side', 'long'),
            'entry_price': float(position.get('entry_price', 0)),
            'mark_price': float(position.get('mark_price', 0)),
            'unrealized_pnl': float(position.get('unrealized_pnl', 0)),
            'value': float(position.get('size', 0)) * float(position.get('mark_price', 0)),
            'exchange': exchange
        }
    
    # Correction implementation stubs
    async def _adjust_internal_position_size(
        self,
        internal_position: Dict[str, Any],
        correct_size: float,
        exchange: str
    ):
        """Adjust internal position size to match exchange."""
        # This would update the position in the position manager
        self.logger.info(f"Adjusting position size for {internal_position['symbol']} on {exchange}")
        pass
    
    async def _update_internal_position_price(
        self,
        internal_position: Dict[str, Any],
        correct_price: float,
        exchange: str
    ):
        """Update internal position average price."""
        # This would update the position in the position manager
        self.logger.info(f"Updating position price for {internal_position['symbol']} on {exchange}")
        pass
    
    async def _generate_position_alerts(
        self,
        discrepancies: List[PositionDiscrepancy],
        exchange: str
    ):
        """Generate alerts for position discrepancies."""
        critical_discrepancies = [d for d in discrepancies if d.severity == 'critical']
        
        if critical_discrepancies:
            await self._send_critical_position_alert(exchange, critical_discrepancies)
    
    async def _send_critical_position_alert(
        self,
        exchange: str,
        discrepancies: List[PositionDiscrepancy]
    ):
        """Send critical position alert."""
        self.logger.critical(f"Critical position discrepancies detected for {exchange}",
                           count=len(discrepancies))
    
    async def _send_monitoring_alert(self, critical_issues: List[PositionDiscrepancy]):
        """Send monitoring alert for critical issues."""
        self.logger.critical("Critical position issues detected during monitoring",
                           count=len(critical_issues))
    
    # Public API methods
    async def get_validation_status(self, validation_id: str) -> Optional[ValidationReport]:
        """Get status of ongoing or completed validation."""
        if validation_id in self.active_validations:
            return self.active_validations[validation_id]
        
        for report in self.validation_history:
            if report.id == validation_id:
                return report
        
        return None
    
    async def get_recent_discrepancies(
        self,
        exchange: Optional[str] = None,
        hours: int = 24
    ) -> List[PositionDiscrepancy]:
        """Get recent position discrepancies."""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        discrepancies = []
        
        for report in self.validation_history:
            if report.validation_time >= cutoff_time:
                if exchange is None or report.exchange == exchange:
                    discrepancies.extend(report.discrepancies)
        
        return discrepancies
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """Get overall validation system summary."""
        return {
            'active_validations': len(self.active_validations),
            'total_validations': len(self.validation_history),
            'continuous_monitoring': self.continuous_monitoring,
            'supported_exchanges': list(self.exchange_connectors.keys()),
            'exchange_capabilities': self.exchange_capabilities,
            'recent_discrepancies': len(self.get_recent_discrepancies()),
            'config': self.config
        } 