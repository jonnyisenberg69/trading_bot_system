import React, { useState, useEffect, useCallback } from 'react';
import { 
  X, 
  RefreshCw, 
  TrendingUp, 
  TrendingDown, 
  Calculator,
  Clock,
  Target,
  DollarSign,
  BarChart3,
  AlertCircle,
  Activity,
  Layers,
  Crosshair,
  Users
} from 'lucide-react';
import { api } from '../services/api';

const StackedMarketMakingDetails = ({ bot, onClose }) => {
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [botData, setBotData] = useState(bot);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Fetch individual bot data with performance information
  const fetchBotData = useCallback(async () => {
    try {
      setError(null);
      const response = await api.bots.getById(bot.instance_id);
      setBotData(response.data);
      console.log('StackedMarketMakingDetails: Fetched bot data:', response.data);
    } catch (error) {
      console.error('StackedMarketMakingDetails: Error fetching bot data:', error);
      setError(error.message || 'Failed to fetch bot data');
      setBotData(bot);
    }
  }, [bot]);

  // Fetch bot data when component mounts
  useEffect(() => {
    const loadData = async () => {
      setLoading(true);
      await fetchBotData();
      setLoading(false);
    };
    
    loadData();
  }, [bot.instance_id, fetchBotData]);

  // Extract performance and strategy data from the correct paths
  const performance = botData?.performance || {};
  const strategyState = botData || {};
  
  // Inventory data from strategy state (like volume weighted strategy)
  const inventoryState = strategyState.inventory_state || {};
  const currentInventory = inventoryState.current_inventory || 0;
  const targetInventory = inventoryState.target_inventory || 0;
  const excessInventory = inventoryState.excess_inventory || 0;
  const excessInventoryPercentage = inventoryState.excess_inventory_percentage || 100.0;
  const targetExcessAmount = inventoryState.target_excess_amount || 0;
  const inventoryCoefficient = inventoryState.inventory_coefficient || 0;
  const inventoryPrice = inventoryState.inventory_price || null;
  
  // TOB and Passive line data
  const tobLines = strategyState.tob_lines || [];
  const passiveLines = strategyState.passive_lines || [];
  
  // Performance metrics 
  const performanceData = strategyState.performance || {};
  const ordersPlacedTob = performanceData.orders_placed_tob || 0;
  const ordersPlacedPassive = performanceData.orders_placed_passive || 0;
  const ordersCancelledTimeout = performanceData.orders_cancelled_timeout || 0;
  const ordersCancelledDrift = performanceData.orders_cancelled_drift || 0;
  const ordersCancelledReplace = performanceData.orders_cancelled_replace || 0;
  const coefficientAdjustments = performanceData.coefficient_adjustments || 0;
  const smartPricingDecisions = performanceData.smart_pricing_decisions || 0;
  
  // Multi-symbol and coefficient config
  const multiSymbolConfig = strategyState.multi_symbol_config || {};
  const coefficientConfig = strategyState.coefficient_config || {};
  const deltaState = strategyState.delta_state || {};

  useEffect(() => {
    if (performance.last_updated) {
      setLastUpdate(new Date(performance.last_updated));
    }
  }, [performance.last_updated]);

  const handleRefresh = async () => {
    setRefreshing(true);
    try {
      await fetchBotData();
      setLastUpdate(new Date());
    } catch (error) {
      console.error('StackedMarketMakingDetails: Error during refresh:', error);
    } finally {
      setRefreshing(false);
    }
  };

  const formatNumber = (num, decimals = 4) => {
    if (num === null || num === undefined) return 'N/A';
    return parseFloat(num).toFixed(decimals);
  };

  const formatCurrency = (num, decimals = 2) => {
    if (num === null || num === undefined) return '$0.00';
    const formatted = parseFloat(num).toFixed(decimals);
    return `$${formatted}`;
  };

  const getCoefficientColor = (coefficient) => {
    if (coefficient > 1.5) return 'text-green-400';
    if (coefficient < 0.5) return 'text-red-400';
    return 'text-yellow-400';
  };

  const getCoefficientIcon = (coefficient) => {
    if (coefficient > 1.5) return <TrendingUp className="h-4 w-4" />;
    if (coefficient < 0.5) return <TrendingDown className="h-4 w-4" />;
    return <Activity className="h-4 w-4" />;
  };

  const getInventoryCoefficientColor = (coefficient) => {
    if (coefficient > 0.5) return 'text-green-400';
    if (coefficient < -0.5) return 'text-red-400';
    return 'text-yellow-400';
  };

  if (loading) {
    return (
      <div className="fixed inset-0 bg-black bg-opacity-75 flex items-center justify-center z-50">
        <div className="bg-gray-800 rounded-lg p-6 w-full max-w-4xl">
          <div className="flex items-center justify-center space-x-2">
            <RefreshCw className="h-6 w-6 animate-spin text-blue-400" />
            <span className="text-white">Loading strategy details...</span>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="fixed inset-0 bg-black bg-opacity-75 flex items-center justify-center z-50">
      <div className="bg-gray-800 rounded-lg p-6 w-full max-w-5xl max-h-[90vh] flex flex-col">
        {/* Header */}
        <div className="flex justify-between items-center mb-6 flex-shrink-0">
          <div>
            <h2 className="text-xl font-semibold text-white">Stacked Market Making Strategy Details</h2>
            <p className="text-gray-400">{botData?.instance_id} - {botData?.symbol}</p>
            {error && (
              <div className="flex items-center gap-2 mt-2 text-yellow-400">
                <AlertCircle className="h-4 w-4" />
                <span className="text-sm">Using cached data: {error}</span>
              </div>
            )}
          </div>
          <div className="flex items-center gap-3">
            <button
              onClick={handleRefresh}
              disabled={refreshing}
              className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white px-3 py-1 rounded text-sm"
            >
              <RefreshCw className={`h-4 w-4 ${refreshing ? 'animate-spin' : ''}`} />
              Refresh
            </button>
            <button onClick={onClose} className="text-gray-400 hover:text-white">
              <X className="h-6 w-6" />
            </button>
          </div>
        </div>

        {/* Content */}
        <div className="overflow-y-auto pr-2 flex-grow space-y-6">
          {/* Strategy Overview */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="bg-gray-700 rounded-lg p-4">
              <div className="flex items-center gap-2 mb-3">
                <Layers className="h-5 w-5 text-blue-400" />
                <h3 className="text-lg font-semibold text-white">TOB Lines</h3>
              </div>
              <div className="text-2xl font-bold text-blue-400">{tobLines.length}</div>
              <div className="text-sm text-gray-400">
                Active: {tobLines.filter(line => line.active_orders > 0).length}
              </div>
            </div>

            <div className="bg-gray-700 rounded-lg p-4">
              <div className="flex items-center gap-2 mb-3">
                <Crosshair className="h-5 w-5 text-purple-400" />
                <h3 className="text-lg font-semibold text-white">Passive Lines</h3>
              </div>
              <div className="text-2xl font-bold text-purple-400">{passiveLines.length}</div>
              <div className="text-sm text-gray-400">
                Active: {passiveLines.filter(line => line.active_orders > 0).length}
              </div>
            </div>

            <div className="bg-gray-700 rounded-lg p-4">
              <div className="flex items-center gap-2 mb-3">
                <Users className="h-5 w-5 text-green-400" />
                <h3 className="text-lg font-semibold text-white">Total Orders</h3>
              </div>
              <div className="text-2xl font-bold text-green-400">
                {ordersPlacedTob + ordersPlacedPassive}
              </div>
              <div className="text-sm text-gray-400">
                TOB: {ordersPlacedTob} | Passive: {ordersPlacedPassive}
              </div>
            </div>
          </div>

          {/* Inventory Management */}
          <div className="bg-gray-700 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-3">
              <Target className="h-5 w-5 text-yellow-400" />
              <h3 className="text-lg font-semibold text-white">Inventory Management</h3>
            </div>
            
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="space-y-3">
                <div className="flex justify-between">
                  <span className="text-gray-400">Current:</span>
                  <span className="text-white font-mono">{formatNumber(currentInventory)}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Target:</span>
                  <span className="text-white font-mono">{formatNumber(targetInventory)}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Excess:</span>
                  <span className={`font-mono ${excessInventory >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                    {formatNumber(excessInventory)}
                  </span>
                </div>
                {inventoryPrice && (
                  <div className="flex justify-between pt-2 border-t border-gray-600">
                    <span className="text-gray-400">Avg Price:</span>
                    <span className="text-yellow-400 font-mono">
                      {formatCurrency(inventoryPrice, 6)}
                    </span>
                  </div>
                )}
              </div>
              
              <div className="space-y-3">
                <div className="flex justify-between">
                  <span className="text-gray-400">Excess %:</span>
                  <span className="text-white font-mono">{excessInventoryPercentage}%</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Target Amount:</span>
                  <span className="text-white font-mono">{formatNumber(targetExcessAmount)}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Inventory Coefficient:</span>
                  <span className={`font-mono ${getInventoryCoefficientColor(inventoryCoefficient)}`}>
                    {formatNumber(inventoryCoefficient, 3)}
                  </span>
                </div>
              </div>
            </div>
          </div>

          {/* Delta Targeting */}
          <div className="bg-gray-700 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-3">
              <Calculator className="h-5 w-5 text-blue-400" />
              <h3 className="text-lg font-semibold text-white">Delta Targeting</h3>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
              <div className="flex justify-between">
                <span className="text-gray-400">Source:</span>
                <span className="text-white">{deltaState.target_inventory_source || 'manual'}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-400">Threshold (%):</span>
                <span className="text-white">{deltaState.delta_change_threshold_pct ?? 'N/A'}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-400">Trade Fraction:</span>
                <span className="text-white">{deltaState.delta_trade_fraction ?? 'N/A'}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-400">Target Sign:</span>
                <span className="text-white">{deltaState.delta_target_sign ?? 'N/A'}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-400">Prevent Unprofitable:</span>
                <span className="text-white">{deltaState.prevent_unprofitable_trades ? 'Enabled' : 'Disabled'}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-400">Last Delta Target:</span>
                <span className="text-white">{formatNumber(deltaState.last_delta_target)}</span>
              </div>
            </div>
          </div>

          {/* TOB Lines Detail */}
          <div className="bg-gray-700 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-3">
              <Layers className="h-5 w-5 text-blue-400" />
              <h3 className="text-lg font-semibold text-white">TOB Lines Detail</h3>
            </div>
            
            {tobLines.length > 0 ? (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {tobLines.map((line, index) => (
                  <div key={line.line_id || index} className="bg-gray-600 rounded p-3">
                    <div className="flex justify-between items-center mb-2">
                      <span className="text-white font-medium">Line {line.line_id}</span>
                      <span className="text-blue-400 text-sm">{line.sides}</span>
                    </div>
                    <div className="text-sm text-gray-300 space-y-1">
                      <div className="flex justify-between">
                        <span>Active Orders:</span>
                        <span className="text-white">{line.active_orders || 0}</span>
                      </div>
                      <div className="flex justify-between">
                        <span>Coefficient:</span>
                        <span className={getCoefficientColor(line.current_coefficient)}>
                          {formatNumber(line.current_coefficient, 3)}
                        </span>
                      </div>
                      <div className="flex justify-between">
                        <span>Spread:</span>
                        <span className="text-white">{line.spread_bps} bps</span>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-center text-gray-400">No TOB lines configured</div>
            )}
          </div>

          {/* Passive Lines Detail */}
          <div className="bg-gray-700 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-3">
              <Crosshair className="h-5 w-5 text-purple-400" />
              <h3 className="text-lg font-semibold text-white">Passive Lines Detail</h3>
            </div>
            
            {passiveLines.length > 0 ? (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {passiveLines.map((line, index) => (
                  <div key={line.line_id || index} className="bg-gray-600 rounded p-3">
                    <div className="flex justify-between items-center mb-2">
                      <span className="text-white font-medium">Line {line.line_id}</span>
                      <span className="text-purple-400 text-sm">{line.sides}</span>
                    </div>
                    <div className="text-sm text-gray-300 space-y-1">
                      <div className="flex justify-between">
                        <span>Active Orders:</span>
                        <span className="text-white">{line.active_orders || 0}</span>
                      </div>
                      <div className="flex justify-between">
                        <span>Spread Coeff:</span>
                        <span className={getCoefficientColor(line.current_spread_coefficient)}>
                          {formatNumber(line.current_spread_coefficient, 3)}
                        </span>
                      </div>
                      <div className="flex justify-between">
                        <span>Quantity Coeff:</span>
                        <span className={getCoefficientColor(line.current_quantity_coefficient)}>
                          {formatNumber(line.current_quantity_coefficient, 3)}
                        </span>
                      </div>
                      <div className="flex justify-between">
                        <span>Mid Spread:</span>
                        <span className="text-white">{line.mid_spread_bps} bps</span>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-center text-gray-400">No passive lines configured</div>
            )}
          </div>

          {/* Multi-Symbol Trading Status */}
          <div className="bg-gray-700 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-3">
              <BarChart3 className="h-5 w-5 text-green-400" />
              <h3 className="text-lg font-semibold text-white">Multi-Symbol Trading</h3>
            </div>
            
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
              <div>
                <span className="text-gray-400 block">Primary Pairs:</span>
                <span className="text-green-400 font-semibold">
                  {multiSymbolConfig.quote_currencies?.length || 0}
                </span>
              </div>
              <div>
                <span className="text-gray-400 block">Smart Pricing:</span>
                <span className="text-white font-semibold">
                  {smartPricingDecisions}
                </span>
              </div>
              <div>
                <span className="text-gray-400 block">Pricing Source:</span>
                <span className="text-purple-400 font-semibold capitalize">
                  {multiSymbolConfig.smart_pricing_source || 'aggregated'}
                </span>
              </div>
              <div>
                <span className="text-gray-400 block">Exchanges:</span>
                <span className="text-white font-semibold">
                  {botData?.exchanges?.length || 0}
                </span>
              </div>
            </div>

            {/* Quote Currencies */}
            <div className="mt-4 pt-3 border-t border-gray-600">
              <div className="flex items-center gap-2 mb-2">
                <DollarSign className="h-4 w-4 text-yellow-400" />
                <span className="text-gray-400 text-sm">Quote Currencies:</span>
              </div>
              <div className="flex flex-wrap gap-2">
                {(multiSymbolConfig.quote_currencies || []).map(currency => (
                  <span key={currency} className="bg-gray-600 text-yellow-400 px-3 py-1 rounded text-sm font-mono">
                    {currency}
                  </span>
                ))}
              </div>
            </div>

            {/* Hedging Targets */}
            <div className="mt-4 pt-3 border-t border-gray-600">
              <div className="flex items-center gap-2 mb-2">
                <DollarSign className="h-4 w-4 text-yellow-400" />
                <span className="text-gray-400 text-sm">Hedging Targets:</span>
              </div>
              <div className="flex flex-wrap gap-2">
                {multiSymbolConfig.hedging_targets && Object.keys(multiSymbolConfig.hedging_targets).length > 0 ? (
                  Object.entries(multiSymbolConfig.hedging_targets).map(([currency, target]) => (
                    <span key={currency} className="bg-gray-600 text-white px-3 py-1 rounded text-sm font-mono">
                      {currency}: {parseFloat(target).toFixed(8)}
                    </span>
                  ))
                ) : (
                  <span className="text-gray-400 text-sm">None</span>
                )}
              </div>
            </div>
          </div>

          {/* Strategy Performance */}
          <div className="bg-gray-700 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-3">
              <Activity className="h-5 w-5 text-orange-400" />
              <h3 className="text-lg font-semibold text-white">Strategy Performance</h3>
            </div>
            
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
              <div>
                <span className="text-gray-400 block">Status:</span>
                <span className={`font-semibold ${botData?.status === 'running' ? 'text-green-400' : 'text-red-400'}`}>
                  {botData?.status || 'unknown'}
                </span>
              </div>
              <div>
                <span className="text-gray-400 block">TOB Orders:</span>
                <span className="text-blue-400 font-semibold">
                  {ordersPlacedTob}
                </span>
              </div>
              <div>
                <span className="text-gray-400 block">Passive Orders:</span>
                <span className="text-purple-400 font-semibold">
                  {ordersPlacedPassive}
                </span>
              </div>
              <div>
                <span className="text-gray-400 block">Coefficient Updates:</span>
                <span className="text-white font-semibold">
                  {coefficientAdjustments}
                </span>
              </div>
            </div>

            <div className="grid grid-cols-3 gap-4 text-sm mt-3 pt-3 border-t border-gray-600">
              <div>
                <span className="text-gray-400 block">Cancelled (Timeout):</span>
                <span className="text-red-400 font-semibold">
                  {ordersCancelledTimeout}
                </span>
              </div>
              <div>
                <span className="text-gray-400 block">Cancelled (Drift):</span>
                <span className="text-yellow-400 font-semibold">
                  {ordersCancelledDrift}
                </span>
              </div>
              <div>
                <span className="text-gray-400 block">Cancelled (Replace):</span>
                <span className="text-orange-400 font-semibold">
                  {ordersCancelledReplace}
                </span>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4 text-sm mt-3 pt-3 border-t border-gray-600">
              <div>
                <span className="text-gray-400 block">Taker Check:</span>
                <span className={`font-semibold ${multiSymbolConfig.taker_check ? 'text-green-400' : 'text-red-400'}`}>
                  {multiSymbolConfig.taker_check ? 'Enabled' : 'Disabled'}
                </span>
              </div>
              <div>
                <span className="text-gray-400 block">Hedging:</span>
                <span className={`font-semibold ${multiSymbolConfig.hedging_enabled ? 'text-green-400' : 'text-red-400'}`}>
                  {multiSymbolConfig.hedging_enabled ? 'Enabled' : 'Disabled'}
                </span>
              </div>
            </div>
          </div>

          {/* Inventory Coefficient Display */}
          <div className="bg-gray-700 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-3">
              <Calculator className="h-5 w-5 text-yellow-400" />
              <h3 className="text-lg font-semibold text-white">Inventory Coefficient</h3>
            </div>
            
            <div className="bg-gray-600 rounded p-4">
              <div className="flex justify-between items-center">
                <span className="text-white font-medium">Current Coefficient</span>
                <div className="flex items-center gap-2">
                  {getCoefficientIcon(Math.abs(inventoryCoefficient))}
                  <span className={`font-mono text-2xl ${getInventoryCoefficientColor(inventoryCoefficient)}`}>
                    {formatNumber(inventoryCoefficient, 3)}
                  </span>
                </div>
              </div>
              
              <div className="mt-3 pt-3 border-t border-gray-500">
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div>
                    <span className="text-gray-400">Range:</span>
                    <span className="text-white ml-2">[-1.000, +1.000]</span>
                  </div>
                  <div>
                    <span className="text-gray-400">Impact:</span>
                    <span className="text-white ml-2">
                      {inventoryCoefficient > 0 ? 'Long Bias' : inventoryCoefficient < 0 ? 'Short Bias' : 'Neutral'}
                    </span>
                  </div>
                </div>
              </div>

              {inventoryPrice && (
                <div className="mt-3 pt-3 border-t border-gray-500">
                  <div className="flex justify-between">
                    <span className="text-gray-400">Inventory Price:</span>
                    <span className="text-yellow-400 font-mono">
                      {formatCurrency(inventoryPrice, 6)}
                    </span>
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* Multi-Reference Pricing */}
          <div className="bg-gray-700 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-3">
              <Calculator className="h-5 w-5 text-cyan-400" />
              <h3 className="text-lg font-semibold text-white">Multi-Reference Pricing</h3>
            </div>
            
            <div className="grid grid-cols-2 md:grid-cols-3 gap-4 text-sm">
              <div>
                <span className="text-gray-400 block">Pricing Decisions:</span>
                <span className="text-cyan-400 font-semibold">
                  {smartPricingDecisions}
                </span>
              </div>
              <div>
                <span className="text-gray-400 block">Pricing Method:</span>
                <span className="text-white font-semibold capitalize">
                  {botData?.config?.smart_pricing_source || 'aggregated'}
                </span>
              </div>
              <div>
                <span className="text-gray-400 block">Cross-Conversion:</span>
                <span className="text-green-400 font-semibold">
                  Active
                </span>
              </div>
            </div>

            <div className="mt-3 pt-3 border-t border-gray-600">
              <div className="text-xs text-gray-400">
                Supports: BTC, ETH, BNB, USDC, FDUSD, TRY conversion via reference pairs
              </div>
            </div>
          </div>

          {/* Exchange Coefficients */}
          <div className="bg-gray-700 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-3">
              <Calculator className="h-5 w-5 text-blue-400" />
              <h3 className="text-lg font-semibold text-white">Exchange Coefficients</h3>
            </div>
            
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {Object.entries(coefficientConfig.exchange_coefficients || {}).map(([exchange, coefficient]) => (
                <div key={exchange} className="bg-gray-600 rounded p-3">
                  <div className="flex justify-between items-center">
                    <span className="text-white font-medium">{exchange}</span>
                    <div className="flex items-center gap-2">
                      {getCoefficientIcon(coefficient)}
                      <span className={`font-mono text-lg ${getCoefficientColor(coefficient)}`}>
                        {formatNumber(coefficient, 6)}
                      </span>
                    </div>
                  </div>
                  <div className="mt-2 text-xs text-gray-400">
                    Rate multiplier: {(coefficient * 100).toFixed(2)}%
                  </div>
                </div>
              ))}
              
              {Object.keys(coefficientConfig.exchange_coefficients || {}).length === 0 && (
                <div className="col-span-2 text-center text-gray-400">
                  No coefficients calculated yet
                </div>
              )}
            </div>
            
            <div className="mt-3 pt-3 border-t border-gray-600">
              <div className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <span className="text-gray-400">Calculation Method:</span>
                  <span className="text-white ml-2 capitalize">{coefficientConfig.coefficient_method || 'min'}</span>
                </div>
                <div>
                  <span className="text-gray-400">Last Update:</span>
                  <span className="text-white ml-2">
                    {coefficientConfig.last_coefficient_update ? 
                      new Date(coefficientConfig.last_coefficient_update).toLocaleTimeString() : 
                      'Never'
                    }
                  </span>
                </div>
              </div>
            </div>
          </div>

          {/* Moving Average Configuration */}
          <div className="bg-gray-700 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-3">
              <Clock className="h-5 w-5 text-purple-400" />
              <h3 className="text-lg font-semibold text-white">Moving Average Configuration</h3>
            </div>
            
            <div className="grid grid-cols-2 gap-4">
              <div>
                <span className="text-gray-400 block mb-2">Time Periods:</span>
                <div className="flex flex-wrap gap-2">
                  {(coefficientConfig.time_periods || []).map(period => (
                    <span key={period} className="bg-gray-600 text-white px-3 py-1 rounded text-sm">
                      {period}
                    </span>
                  ))}
                  {(!coefficientConfig.time_periods || coefficientConfig.time_periods.length === 0) && (
                    <span className="text-gray-400">Default periods</span>
                  )}
                </div>
              </div>
              
              <div>
                <span className="text-gray-400 block mb-2">Coefficient Method:</span>
                <span className="bg-gray-600 text-white px-3 py-1 rounded text-sm capitalize">
                  {coefficientConfig.coefficient_method || 'min'}
                </span>
                
                <div className="mt-2 text-xs text-gray-400">
                  Bounds: [{coefficientConfig.min_coefficient || 0.2}, {coefficientConfig.max_coefficient || 3.0}]
                </div>
              </div>
            </div>
          </div>

          {/* Last Update Info */}
          {lastUpdate && (
            <div className="text-center text-gray-400 text-sm pt-4 border-t border-gray-600">
              Last updated: {lastUpdate.toLocaleString()}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default StackedMarketMakingDetails;
