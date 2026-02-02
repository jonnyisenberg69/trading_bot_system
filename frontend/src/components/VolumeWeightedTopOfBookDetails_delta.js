import React, { useState, useEffect } from 'react';
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
  Activity
} from 'lucide-react';
import { api } from '../services/api';

const VolumeWeightedTopOfBookDetails = ({ bot, onClose }) => {
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [botData, setBotData] = useState(bot);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Fetch individual bot data with performance information
  const fetchBotData = async () => {
    try {
      setError(null);
      const response = await api.bots.getById(bot.instance_id);
      setBotData(response.data);
      console.log('VolumeWeightedTopOfBookDetails: Fetched bot data:', response.data);
    } catch (error) {
      console.error('VolumeWeightedTopOfBookDetails: Error fetching bot data:', error);
      setError(error.message || 'Failed to fetch bot data');
      setBotData(bot);
    }
  };

  // Fetch bot data when component mounts
  useEffect(() => {
    const loadData = async () => {
      setLoading(true);
      await fetchBotData();
      setLoading(false);
    };
    
    loadData();
  }, [bot.instance_id]);

  // Extract performance data
  const performance = botData?.performance || {};
  const currentInventory = performance.current_inventory || 0;
  const targetInventory = performance.target_inventory || 0;
  const excessInventory = performance.excess_inventory || 0;
  const excessPercentage = performance.excess_inventory_percentage || 0;
  const targetExcessAmount = performance.target_excess_amount || 0;
  const inventoryPrice = performance.inventory_price || null;
  const exchangeCoefficients = performance.exchange_coefficients || {};
  const coefficientMethod = performance.coefficient_method || 'min';
  const coefficientBounds = performance.coefficient_bounds || { min: 0.2, max: 3.0 };
  const lastCoefficientUpdate = performance.last_coefficient_update;
  const timePeriods = performance.time_periods || [];
  const targetInventorySource = performance.target_inventory_source || 'manual';
  const deltaThreshold = performance.delta_change_threshold_pct;
  const deltaTradeFraction = performance.delta_trade_fraction;
  const deltaTargetSign = performance.delta_target_sign;
  const preventUnprofitableTrades = performance.prevent_unprofitable_trades;
  const lastDeltaTarget = performance.last_delta_target;
  const lastDeltaUpdatedAt = performance.last_delta_updated_at;

  useEffect(() => {
    if (lastCoefficientUpdate) {
      setLastUpdate(new Date(lastCoefficientUpdate));
    }
  }, [lastCoefficientUpdate]);

  const handleRefresh = async () => {
    setRefreshing(true);
    try {
      await fetchBotData();
      setLastUpdate(new Date());
    } catch (error) {
      console.error('VolumeWeightedTopOfBookDetails: Error during refresh:', error);
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

  if (loading) {
    return (
      <div className="fixed inset-0 bg-black bg-opacity-75 flex items-center justify-center z-50">
        <div className="bg-gray-800 rounded-lg p-6 w-full max-w-3xl">
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
      <div className="bg-gray-800 rounded-lg p-6 w-full max-w-3xl max-h-[90vh] flex flex-col">
        {/* Header */}
        <div className="flex justify-between items-center mb-6 flex-shrink-0">
          <div>
            <h2 className="text-xl font-semibold text-white">Volume-Weighted Strategy Details</h2>
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
          {/* Exchange Coefficients */}
          <div className="bg-gray-700 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-3">
              <Calculator className="h-5 w-5 text-blue-400" />
              <h3 className="text-lg font-semibold text-white">Exchange Coefficients</h3>
            </div>
            
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {Object.entries(exchangeCoefficients).map(([exchange, coefficient]) => (
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
              
              {Object.keys(exchangeCoefficients).length === 0 && (
                <div className="col-span-2 text-center text-gray-400">
                  No coefficients calculated yet
                </div>
              )}
            </div>
            
            <div className="mt-3 pt-3 border-t border-gray-600">
              <div className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <span className="text-gray-400">Calculation Method:</span>
                  <span className="text-white ml-2 capitalize">{coefficientMethod}</span>
                </div>
                <div>
                  <span className="text-gray-400">Bounds:</span>
                  <span className="text-white ml-2">[{coefficientBounds.min}, {coefficientBounds.max}]</span>
                </div>
              </div>
            </div>
          </div>

          {/* Time Periods */}
          <div className="bg-gray-700 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-3">
              <Clock className="h-5 w-5 text-purple-400" />
              <h3 className="text-lg font-semibold text-white">Moving Average Time Periods</h3>
            </div>
            <div className="flex flex-wrap gap-2">
              {timePeriods.map(period => (
                <span key={period} className="bg-gray-600 text-white px-3 py-1 rounded text-sm">
                  {period}
                </span>
              ))}
              {timePeriods.length === 0 && (
                <span className="text-gray-400">No time periods configured</span>
              )}
            </div>
          </div>

          {/* Inventory Overview */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="bg-gray-700 rounded-lg p-4">
              <div className="flex items-center gap-2 mb-3">
                <Target className="h-5 w-5 text-yellow-400" />
                <h3 className="text-lg font-semibold text-white">Inventory</h3>
              </div>
              <div className="space-y-2">
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
            </div>

            <div className="bg-gray-700 rounded-lg p-4">
              <div className="flex items-center gap-2 mb-3">
                <BarChart3 className="h-5 w-5 text-green-400" />
                <h3 className="text-lg font-semibold text-white">Execution Target</h3>
              </div>
              <div className="space-y-2">
                <div className="flex justify-between">
                  <span className="text-gray-400">Excess %:</span>
                  <span className="text-white font-mono">{excessPercentage}%</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Target Amount:</span>
                  <span className="text-white font-mono">{formatNumber(targetExcessAmount)}</span>
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
            <div className="grid grid-cols-2 md:grid-cols-3 gap-4 text-sm">
              <div>
                <span className="text-gray-400 block">Source:</span>
                <span className="text-white font-semibold">{targetInventorySource}</span>
              </div>
              <div>
                <span className="text-gray-400 block">Threshold (%):</span>
                <span className="text-white font-semibold">{deltaThreshold ?? 'N/A'}</span>
              </div>
              <div>
                <span className="text-gray-400 block">Trade Fraction:</span>
                <span className="text-white font-semibold">{deltaTradeFraction ?? 'N/A'}</span>
              </div>
              <div>
                <span className="text-gray-400 block">Target Sign:</span>
                <span className="text-white font-semibold">{deltaTargetSign ?? 'N/A'}</span>
              </div>
              <div>
                <span className="text-gray-400 block">Prevent Unprofitable:</span>
                <span className="text-white font-semibold">{preventUnprofitableTrades ? 'Enabled' : 'Disabled'}</span>
              </div>
              <div>
                <span className="text-gray-400 block">Last Delta Target:</span>
                <span className="text-white font-semibold">{formatNumber(lastDeltaTarget)}</span>
              </div>
              {lastDeltaUpdatedAt && (
                <div className="md:col-span-2">
                  <span className="text-gray-400 block">Last Delta Update:</span>
                  <span className="text-white font-semibold">{new Date(lastDeltaUpdatedAt).toLocaleString()}</span>
                </div>
              )}
            </div>
          </div>

          {/* Strategy Status */}
          <div className="bg-gray-700 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-3">
              <Activity className="h-5 w-5 text-purple-400" />
              <h3 className="text-lg font-semibold text-white">Strategy Status</h3>
            </div>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
              <div>
                <span className="text-gray-400 block">Status:</span>
                <span className={`font-semibold ${botData?.status === 'running' ? 'text-green-400' : 'text-red-400'}`}>
                  {botData?.status || 'unknown'}
                </span>
              </div>
              <div>
                <span className="text-gray-400 block">Orders Placed:</span>
                <span className="text-white font-semibold">
                  {performance.orders_placed || 0}
                </span>
              </div>
              <div>
                <span className="text-gray-400 block">Orders Filled:</span>
                <span className="text-white font-semibold">
                  {performance.orders_filled || 0}
                </span>
              </div>
              <div>
                <span className="text-gray-400 block">Coefficient Updates:</span>
                <span className="text-white font-semibold">
                  {performance.coefficient_updates || 0}
                </span>
              </div>
            </div>
            
            <div className="grid grid-cols-2 gap-4 text-sm mt-3 pt-3 border-t border-gray-600">
              <div>
                <span className="text-gray-400 block">Bid Levels:</span>
                <span className="text-white font-semibold">
                  {performance.bid_levels || 0}
                </span>
              </div>
              <div>
                <span className="text-gray-400 block">Offer Levels:</span>
                <span className="text-white font-semibold">
                  {performance.offer_levels || 0}
                </span>
              </div>
            </div>
            {performance.orders_skipped_inventory_protection > 0 && (
              <div className="mt-3 pt-3 border-t border-gray-600">
                <div className="flex items-center gap-2">
                  <AlertCircle className="h-4 w-4 text-yellow-400" />
                  <span className="text-gray-400 text-sm">
                    Orders skipped due to inventory protection: 
                    <span className="text-yellow-400 font-semibold ml-1">
                      {performance.orders_skipped_inventory_protection || 0}
                    </span>
                  </span>
                </div>
              </div>
            )}
          </div>

          {/* Last Update */}
          {lastUpdate && (
            <div className="bg-gray-700 rounded-lg p-4">
              <div className="flex items-center gap-2 mb-2">
                <Clock className="h-5 w-5 text-gray-400" />
                <h3 className="text-lg font-semibold text-white">Last Coefficient Update</h3>
              </div>
              <div className="text-center">
                <span className="text-gray-300 font-mono">
                  {lastUpdate.toLocaleString()}
                </span>
                <p className="text-gray-400 text-sm mt-1">
                  Coefficients are recalculated every 10 seconds
                </p>
              </div>
            </div>
          )}

          {/* Info Box */}
          <div className="bg-blue-900/20 border border-blue-500/50 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-2">
              <AlertCircle className="h-5 w-5 text-blue-400" />
              <h3 className="text-lg font-semibold text-blue-400">Volume-Weighted Strategy</h3>
            </div>
            <p className="text-blue-300 text-sm">
              This strategy dynamically adjusts execution rates based on real-time volume analysis across exchanges.
              Higher coefficients indicate favorable volume patterns and increase the execution rate for that exchange.
              The strategy calculates its own moving averages based on the configured time periods.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default VolumeWeightedTopOfBookDetails; 