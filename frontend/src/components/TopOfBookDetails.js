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
  AlertCircle
} from 'lucide-react';
import { api } from '../services/api';

const TopOfBookDetails = ({ bot, onClose }) => {
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [botData, setBotData] = useState(bot); // Local state for bot data with performance
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Fetch individual bot data with performance information
  const fetchBotData = async () => {
    try {
      setError(null);
      const response = await api.bots.getById(bot.instance_id);
      setBotData(response.data);
      console.log('TopOfBookDetails: Fetched bot data with performance:', response.data);
      console.log('TopOfBookDetails: Performance data:', response.data.performance);
      console.log('TopOfBookDetails: Live position data:', response.data.performance?.live_position_data);
    } catch (error) {
      console.error('TopOfBookDetails: Error fetching bot data:', error);
      setError(error.message || 'Failed to fetch bot data');
      // Fallback to the original bot data
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

  // Extract live position data from bot performance stats
  const liveData = botData?.performance?.live_position_data || {};
  const inventoryPrice = liveData.inventory_price;
  const realizedPnl = liveData.realized_pnl || 0;
  const unrealizedPnl = liveData.unrealized_pnl || 0;
  const totalPnl = realizedPnl + unrealizedPnl;
  const lastUpdated = liveData.last_updated;

  // Extract other relevant data
  const currentInventory = botData?.performance?.current_inventory || 0;
  const targetInventory = botData?.performance?.target_inventory || 0;
  const excessInventory = currentInventory - targetInventory;
  const accountingMethod = botData?.performance?.accounting_method || 'FIFO';

  // Add more debugging
  console.log('TopOfBookDetails: Extracted values:', {
    inventoryPrice,
    realizedPnl,
    unrealizedPnl,
    totalPnl,
    lastUpdated,
    currentInventory,
    targetInventory,
    excessInventory,
    accountingMethod
  });

  useEffect(() => {
    if (lastUpdated) {
      setLastUpdate(new Date(lastUpdated));
    }
  }, [lastUpdated]);

  const handleRefresh = async () => {
    setRefreshing(true);
    try {
      await fetchBotData();
      setLastUpdate(new Date());
      console.log('TopOfBookDetails: Refresh completed');
    } catch (error) {
      console.error('TopOfBookDetails: Error during refresh:', error);
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

  const getPnlColor = (pnl) => {
    if (pnl > 0) return 'text-green-400';
    if (pnl < 0) return 'text-red-400';
    return 'text-gray-400';
  };

  const getPnlIcon = (pnl) => {
    if (pnl > 0) return <TrendingUp className="h-4 w-4" />;
    if (pnl < 0) return <TrendingDown className="h-4 w-4" />;
    return <BarChart3 className="h-4 w-4" />;
  };

  if (loading) {
    return (
      <div className="fixed inset-0 bg-black bg-opacity-75 flex items-center justify-center z-50">
        <div className="bg-gray-800 rounded-lg p-6 w-full max-w-2xl">
          <div className="flex items-center justify-center space-x-2">
            <RefreshCw className="h-6 w-6 animate-spin text-blue-400" />
            <span className="text-white">Loading position details...</span>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="fixed inset-0 bg-black bg-opacity-75 flex items-center justify-center z-50">
      <div className="bg-gray-800 rounded-lg p-6 w-full max-w-2xl max-h-[90vh] flex flex-col">
        {/* Header */}
        <div className="flex justify-between items-center mb-6 flex-shrink-0">
          <div>
            <h2 className="text-xl font-semibold text-white">Position Details</h2>
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

        {/* Live Data Grid */}
        <div className="overflow-y-auto pr-2 flex-grow space-y-6">
          {/* Accounting Method */}
          <div className="bg-gray-700 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-3">
              <Calculator className="h-5 w-5 text-blue-400" />
              <h3 className="text-lg font-semibold text-white">Accounting Method</h3>
            </div>
            <div className="text-center">
              <span className="text-2xl font-bold text-blue-400">{accountingMethod}</span>
              <p className="text-gray-400 text-sm mt-1">
                {accountingMethod === 'FIFO' && 'First In, First Out - Oldest positions closed first'}
                {accountingMethod === 'LIFO' && 'Last In, First Out - Newest positions closed first'}
                {accountingMethod === 'AVERAGE_COST' && 'Average Cost Basis - Weighted average pricing'}
              </p>
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
              </div>
            </div>

            <div className="bg-gray-700 rounded-lg p-4">
              <div className="flex items-center gap-2 mb-3">
                <DollarSign className="h-5 w-5 text-green-400" />
                <h3 className="text-lg font-semibold text-white">Inventory Price</h3>
              </div>
              <div className="text-center">
                <span className="text-2xl font-bold text-green-400 font-mono">
                  {inventoryPrice ? formatCurrency(inventoryPrice, 6) : 'N/A'}
                </span>
                <p className="text-gray-400 text-sm mt-1">
                  {accountingMethod} calculated entry price
                </p>
                {!inventoryPrice && (
                  <p className="text-yellow-400 text-xs mt-1">
                    No position or tracking not initialized
                  </p>
                )}
              </div>
            </div>
          </div>

          {/* P&L Overview */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="bg-gray-700 rounded-lg p-4">
              <div className="flex items-center gap-2 mb-3">
                {getPnlIcon(realizedPnl)}
                <h3 className="text-lg font-semibold text-white">Realized P&L</h3>
              </div>
              <div className="text-center">
                <span className={`text-2xl font-bold font-mono ${getPnlColor(realizedPnl)}`}>
                  {formatCurrency(realizedPnl)}
                </span>
                <p className="text-gray-400 text-sm mt-1">From closed positions</p>
              </div>
            </div>

            <div className="bg-gray-700 rounded-lg p-4">
              <div className="flex items-center gap-2 mb-3">
                {getPnlIcon(unrealizedPnl)}
                <h3 className="text-lg font-semibold text-white">Unrealized P&L</h3>
              </div>
              <div className="text-center">
                <span className={`text-2xl font-bold font-mono ${getPnlColor(unrealizedPnl)}`}>
                  {formatCurrency(unrealizedPnl)}
                </span>
                <p className="text-gray-400 text-sm mt-1">From open positions</p>
              </div>
            </div>

            <div className="bg-gray-700 rounded-lg p-4">
              <div className="flex items-center gap-2 mb-3">
                {getPnlIcon(totalPnl)}
                <h3 className="text-lg font-semibold text-white">Total P&L</h3>
              </div>
              <div className="text-center">
                <span className={`text-2xl font-bold font-mono ${getPnlColor(totalPnl)}`}>
                  {formatCurrency(totalPnl)}
                </span>
                <p className="text-gray-400 text-sm mt-1">Combined P&L</p>
              </div>
            </div>
          </div>

          {/* Strategy Status */}
          <div className="bg-gray-700 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-3">
              <BarChart3 className="h-5 w-5 text-purple-400" />
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
                <span className="text-gray-400 block">Active Orders:</span>
                <span className="text-white font-semibold">
                  {botData?.performance?.active_orders || 0}
                </span>
              </div>
              <div>
                <span className="text-gray-400 block">Orders Placed:</span>
                <span className="text-white font-semibold">
                  {botData?.performance?.orders_placed || 0}
                </span>
              </div>
              <div>
                <span className="text-gray-400 block">Orders Filled:</span>
                <span className="text-white font-semibold">
                  {botData?.performance?.orders_filled || 0}
                </span>
              </div>
            </div>
          </div>

          {/* Last Update */}
          {lastUpdate && (
            <div className="bg-gray-700 rounded-lg p-4">
              <div className="flex items-center gap-2 mb-2">
                <Clock className="h-5 w-5 text-gray-400" />
                <h3 className="text-lg font-semibold text-white">Last Update</h3>
              </div>
              <div className="text-center">
                <span className="text-gray-300 font-mono">
                  {lastUpdate.toLocaleString()}
                </span>
                <p className="text-gray-400 text-sm mt-1">
                  Position data last calculated
                </p>
              </div>
            </div>
          )}

          {/* Debug Information */}
          {botData?.performance?.position_tracking_initialized === false && (
            <div className="bg-yellow-900/20 border border-yellow-500/50 rounded-lg p-4">
              <div className="flex items-center gap-2 mb-2">
                <AlertCircle className="h-5 w-5 text-yellow-400" />
                <h3 className="text-lg font-semibold text-yellow-400">Position Tracking Status</h3>
              </div>
              <p className="text-yellow-300 text-sm">
                Position tracking is not yet initialized. This may be normal for new strategies or during startup.
                Live position data will be available once the strategy processes trades.
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default TopOfBookDetails; 