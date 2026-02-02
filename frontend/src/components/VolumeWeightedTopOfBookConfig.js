import React, { useState, useEffect } from 'react';
import { 
  TrendingUp, 
  RefreshCw, 
  AlertCircle, 
  Info,
  Plus,
  Minus,
  Clock,
  Settings
} from 'lucide-react';
import { api } from '../services/api';

const VolumeWeightedTopOfBookConfig = ({ 
  config, 
  onConfigChange,
  symbol,
  exchanges = []
}) => {
  const [currentMidpoint, setCurrentMidpoint] = useState(null);
  const [showAdvanced, setShowAdvanced] = useState(false);

  useEffect(() => {
    // Load current price when symbol or exchanges change
    if (symbol && exchanges && exchanges.length > 0) {
      loadCurrentPrice();
    }
  }, [symbol, exchanges]);

  const loadCurrentPrice = async () => {
    if (!symbol || !exchanges || exchanges.length === 0) return;
    
    try {
      const response = await api.strategies.getCurrentPrice(symbol, exchanges);
      if (response.data.aggregated_midpoint) {
        setCurrentMidpoint(response.data.aggregated_midpoint);
      } else {
        setCurrentMidpoint(null);
      }
    } catch (error) {
      console.error('Failed to load current price:', error);
      setCurrentMidpoint(null);
    }
  };

  const handleConfigChange = (field, value) => {
    onConfigChange({ ...config, [field]: value });
  };

  const handleTimePeriodToggle = (period) => {
    const currentPeriods = config.time_periods || [];
    if (currentPeriods.includes(period)) {
      handleConfigChange('time_periods', currentPeriods.filter(p => p !== period));
    } else {
      handleConfigChange('time_periods', [...currentPeriods, period]);
    }
  };

  const handleBidLevelChange = (index, field, value) => {
    const newBidLevels = [...(config.bid_levels || [])];
    newBidLevels[index] = { ...newBidLevels[index], [field]: value };
    onConfigChange({ ...config, bid_levels: newBidLevels });
  };

  const handleOfferLevelChange = (index, field, value) => {
    const newOfferLevels = [...(config.offer_levels || [])];
    newOfferLevels[index] = { ...newOfferLevels[index], [field]: value };
    onConfigChange({ ...config, offer_levels: newOfferLevels });
  };

  const addBidLevel = () => {
    const newBidLevels = [...(config.bid_levels || []), {
      id: Date.now() + Math.random(),
      max_price: currentMidpoint || 0,
      base_hourly_rate: 100,
      rate_currency: 'base',
      drift_bps: 50,
      timeout_seconds: 300
    }];
    onConfigChange({ ...config, bid_levels: newBidLevels });
  };

  const addOfferLevel = () => {
    const newOfferLevels = [...(config.offer_levels || []), {
      id: Date.now() + Math.random(),
      min_price: currentMidpoint || 0,
      base_hourly_rate: 100,
      rate_currency: 'base',
      drift_bps: 50,
      timeout_seconds: 300
    }];
    onConfigChange({ ...config, offer_levels: newOfferLevels });
  };

  const removeBidLevel = (index) => {
    const newBidLevels = config.bid_levels.filter((_, i) => i !== index);
    onConfigChange({ ...config, bid_levels: newBidLevels });
  };

  const removeOfferLevel = (index) => {
    const newOfferLevels = config.offer_levels.filter((_, i) => i !== index);
    onConfigChange({ ...config, offer_levels: newOfferLevels });
  };

  // Available time periods for moving average analysis
  const availableTimePeriods = [
    // Short-term (≤ 1 Hour)
    { value: '30s', label: '30 Seconds', group: 'short' },
    { value: '1min', label: '1 Minute', group: 'short' },
    { value: '5min', label: '5 Minutes', group: 'short' },
    { value: '15min', label: '15 Minutes', group: 'short' },
    { value: '30min', label: '30 Minutes', group: 'short' },
    { value: '60min', label: '1 Hour', group: 'short' },
    
    // Medium-term (4-24 Hours)
    { value: '240min', label: '4 Hours', group: 'medium' },
    { value: '480min', label: '8 Hours', group: 'medium' },
    { value: '720min', label: '12 Hours', group: 'medium' },
    { value: '1080min', label: '18 Hours', group: 'medium' },
    { value: '1440min', label: '24 Hours', group: 'medium' },
    
    // Long-term (Days)
    { value: '2880min', label: '2 Days', group: 'long' },
    { value: '4320min', label: '3 Days', group: 'long' },
    { value: '10080min', label: '7 Days', group: 'long' },
    { value: '20160min', label: '14 Days', group: 'long' },
    { value: '43200min', label: '30 Days', group: 'long' }
  ];

  const selectedPeriods = config.time_periods || [];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-white">Volume-Weighted Top of Book Configuration</h2>
          <p className="text-gray-400">Configure dynamic rate adjustment based on exchange volume analysis</p>
          {currentMidpoint && (
            <p className="text-sm text-blue-400">Current Price: {parseFloat(currentMidpoint).toFixed(6)}</p>
          )}
        </div>
        <button
          onClick={() => setShowAdvanced(!showAdvanced)}
          className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-3 py-1 rounded text-sm"
        >
          <Settings className="h-4 w-4" />
          {showAdvanced ? 'Simple' : 'Advanced'}
        </button>
      </div>

      {/* Basic Configuration */}
      <div className="space-y-4">
        <h3 className="text-lg font-semibold text-white">Basic Configuration</h3>
        
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {/* Start Time */}
          <div>
            <label className="block text-gray-400 text-sm mb-2">Start Time</label>
            <input
              type="datetime-local"
              value={config.start_time || new Date().toISOString().slice(0, 16)}
              onChange={(e) => handleConfigChange('start_time', e.target.value)}
              className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            />
          </div>

          {/* Target Inventory */}
          <div>
            <label className="block text-gray-400 text-sm mb-2">Target Inventory (Base)</label>
            <input
              type="number"
              value={config.target_inventory || ''}
              onChange={(e) => handleConfigChange('target_inventory', parseFloat(e.target.value) || 0)}
              className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
              placeholder="e.g. 300000"
              step="0.01"
            />
          </div>

          {/* Excess Inventory Percentage */}
          <div>
            <label className="block text-gray-400 text-sm mb-2">Excess Inventory Percentage (%)</label>
            <input
              type="number"
              value={config.excess_inventory_percentage || ''}
              onChange={(e) => handleConfigChange('excess_inventory_percentage', parseFloat(e.target.value) || 0)}
              className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
              placeholder="e.g. 10"
              min="0"
              max="100"
              step="0.1"
            />
          </div>

          {/* Spread */}
          <div>
            <label className="block text-gray-400 text-sm mb-2">Spread (bps)</label>
            <input
              type="number"
              value={config.spread_bps || ''}
              onChange={(e) => handleConfigChange('spread_bps', parseInt(e.target.value) || 0)}
              className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
              placeholder="e.g. 25"
              min="0"
              step="1"
            />
          </div>

          {/* Sides */}
          <div>
            <label className="block text-gray-400 text-sm mb-2">Sides</label>
            <select
              value={config.sides || 'both'}
              onChange={(e) => handleConfigChange('sides', e.target.value)}
              className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            >
              <option value="both">Both</option>
              <option value="bid">Bid Only</option>
              <option value="offer">Offer Only</option>
            </select>
          </div>

          {/* Taker Check */}
          <div>
            <label className="block text-gray-400 text-sm mb-2">Taker Check</label>
            <select
              value={config.taker_check !== undefined ? config.taker_check.toString() : 'true'}
              onChange={(e) => handleConfigChange('taker_check', e.target.value === 'true')}
              className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            >
              <option value="true">Enabled</option>
              <option value="false">Disabled</option>
            </select>
          </div>

          {/* Accounting Method */}
          <div>
            <label className="block text-gray-400 text-sm mb-2">Accounting Method</label>
            <select
              value={config.accounting_method || 'FIFO'}
              onChange={(e) => handleConfigChange('accounting_method', e.target.value)}
              className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            >
              <option value="FIFO">FIFO</option>
              <option value="LIFO">LIFO</option>
              <option value="AVERAGE_COST">Average Cost</option>
            </select>
          </div>

          {/* Coefficient Method */}
          <div>
            <label className="block text-gray-400 text-sm mb-2">Coefficient Method</label>
            <select
              value={config.coefficient_method || 'min'}
              onChange={(e) => handleConfigChange('coefficient_method', e.target.value)}
              className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            >
              <option value="min">Minimum (Conservative)</option>
              <option value="mid">Midpoint (Balanced)</option>
              <option value="max">Maximum (Aggressive)</option>
            </select>
          </div>

          {/* Min Coefficient */}
          <div>
            <label className="block text-gray-400 text-sm mb-2">Min Coefficient</label>
            <input
              type="number"
              value={config.min_coefficient || 0.2}
              onChange={(e) => handleConfigChange('min_coefficient', parseFloat(e.target.value) || 0.2)}
              className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
              min="0.1"
              max="1"
              step="0.1"
            />
          </div>

          {/* Max Coefficient */}
          <div>
            <label className="block text-gray-400 text-sm mb-2">Max Coefficient</label>
            <input
              type="number"
              value={config.max_coefficient || 3.0}
              onChange={(e) => handleConfigChange('max_coefficient', parseFloat(e.target.value) || 3.0)}
              className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
              min="1"
              max="10"
              step="0.1"
            />
          </div>
        </div>
      </div>

      {/* Time Period Selection */}
      <div className="space-y-4">
        <div className="flex justify-between items-center">
          <h3 className="text-lg font-semibold text-white">Moving Average Time Periods</h3>
          <span className="text-sm text-gray-400">Select periods for volume analysis</span>
        </div>

        <div className="space-y-4">
          {/* Short-term periods */}
          <div>
            <h4 className="text-gray-300 text-sm font-semibold mb-2">Short-term (≤ 1 Hour)</h4>
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
              {availableTimePeriods.filter(p => p.group === 'short').map((period) => (
                <label key={period.value} className="flex items-center">
                  <input
                    type="checkbox"
                    checked={selectedPeriods.includes(period.value)}
                    onChange={() => handleTimePeriodToggle(period.value)}
                    className="mr-2 h-4 w-4 text-blue-600 bg-gray-600 border-gray-500 rounded focus:ring-blue-500"
                  />
                  <span className="text-white text-sm">{period.label}</span>
                </label>
              ))}
            </div>
          </div>

          {/* Medium-term periods */}
          <div>
            <h4 className="text-gray-300 text-sm font-semibold mb-2">Medium-term (4-24 Hours)</h4>
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
              {availableTimePeriods.filter(p => p.group === 'medium').map((period) => (
                <label key={period.value} className="flex items-center">
                  <input
                    type="checkbox"
                    checked={selectedPeriods.includes(period.value)}
                    onChange={() => handleTimePeriodToggle(period.value)}
                    className="mr-2 h-4 w-4 text-blue-600 bg-gray-600 border-gray-500 rounded focus:ring-blue-500"
                  />
                  <span className="text-white text-sm">{period.label}</span>
                </label>
              ))}
            </div>
          </div>

          {/* Long-term periods */}
          <div>
            <h4 className="text-gray-300 text-sm font-semibold mb-2">Long-term (Days)</h4>
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
              {availableTimePeriods.filter(p => p.group === 'long').map((period) => (
                <label key={period.value} className="flex items-center">
                  <input
                    type="checkbox"
                    checked={selectedPeriods.includes(period.value)}
                    onChange={() => handleTimePeriodToggle(period.value)}
                    className="mr-2 h-4 w-4 text-blue-600 bg-gray-600 border-gray-500 rounded focus:ring-blue-500"
                  />
                  <span className="text-white text-sm">{period.label}</span>
                </label>
              ))}
            </div>
          </div>

          <div className="mt-3 space-y-2">
            <div className="text-sm text-gray-400">
              Selected: {selectedPeriods.length} periods
            </div>
            {selectedPeriods.some(p => parseInt(p) > 1440) && (
              <div className="bg-yellow-900/20 border border-yellow-600 rounded p-2 text-yellow-400 text-xs">
                <p className="font-semibold mb-1">⚠️ Note on Long-term Periods:</p>
                <p>Daily periods (2-30 days) require significant historical data to calculate accurate moving averages.</p>
                <p className="mt-1">Initial calculations may be incomplete until sufficient data is collected.</p>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Bid Price Levels */}
      {(config.sides === 'both' || config.sides === 'bid') && (
      <div className="space-y-4">
        <div className="flex justify-between items-center">
          <h3 className="text-lg font-semibold text-white">
            Bid Price Levels ({(config.bid_levels || []).length})
          </h3>
          <button
            type="button"
            onClick={(e) => {
              e.preventDefault();
              e.stopPropagation();
              addBidLevel();
            }}
            className="flex items-center gap-2 bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded text-sm"
          >
            <Plus className="h-4 w-4" />
            Add Bid Level
          </button>
        </div>

        {(config.bid_levels || []).map((level, index) => (
          <div key={level.id || `bid-${index}`} className="bg-gray-700 rounded p-4 space-y-3">
            <div className="flex justify-between items-center">
              <h4 className="text-white font-medium">Bid Level {index + 1}</h4>
              <button
                onClick={() => removeBidLevel(index)}
                className="text-red-400 hover:text-red-300"
              >
                <Minus className="h-4 w-4" />
              </button>
            </div>

            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
              <div>
                <label className="block text-gray-400 text-xs mb-1">Max Price</label>
                <input
                  type="number"
                  value={level.max_price}
                  onChange={(e) => handleBidLevelChange(index, 'max_price', parseFloat(e.target.value) || 0)}
                  className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                  step="0.000001"
                />
              </div>

              <div>
                <label className="block text-gray-400 text-xs mb-1">Base Hourly Rate</label>
                <input
                  type="number"
                  value={level.base_hourly_rate}
                  onChange={(e) => handleBidLevelChange(index, 'base_hourly_rate', parseFloat(e.target.value) || 0)}
                  className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                  step="0.001"
                />
              </div>

              <div>
                <label className="block text-gray-400 text-xs mb-1">Currency</label>
                <select
                  value={level.rate_currency}
                  onChange={(e) => handleBidLevelChange(index, 'rate_currency', e.target.value)}
                  className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                >
                  <option value="base">Base</option>
                  <option value="quote">Quote</option>
                </select>
              </div>

              <div>
                <label className="block text-gray-400 text-xs mb-1">Drift (bps)</label>
                <input
                  type="number"
                  value={level.drift_bps}
                  onChange={(e) => handleBidLevelChange(index, 'drift_bps', parseFloat(e.target.value) || 0)}
                  className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                  step="0.1"
                />
              </div>

              <div>
                <label className="block text-gray-400 text-xs mb-1">Timeout (sec)</label>
                <input
                  type="number"
                  value={level.timeout_seconds}
                  onChange={(e) => handleBidLevelChange(index, 'timeout_seconds', parseInt(e.target.value) || 0)}
                  className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                  min="1"
                />
              </div>
            </div>
          </div>
        ))}

        {(config.bid_levels || []).length === 0 && (
          <div className="bg-gray-700 rounded p-4 text-center text-gray-400">
            No bid levels configured. Click "Add Bid Level" to get started.
          </div>
        )}
      </div>
      )}

      {/* Offer Price Levels */}
      {(config.sides === 'both' || config.sides === 'offer') && (
      <div className="space-y-4">
        <div className="flex justify-between items-center">
          <h3 className="text-lg font-semibold text-white">
            Offer Price Levels ({(config.offer_levels || []).length})
          </h3>
          <button
            type="button"
            onClick={(e) => {
              e.preventDefault();
              e.stopPropagation();
              addOfferLevel();
            }}
            className="flex items-center gap-2 bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded text-sm"
          >
            <Plus className="h-4 w-4" />
            Add Offer Level
          </button>
        </div>

        {(config.offer_levels || []).map((level, index) => (
          <div key={level.id || `offer-${index}`} className="bg-gray-700 rounded p-4 space-y-3">
            <div className="flex justify-between items-center">
              <h4 className="text-white font-medium">Offer Level {index + 1}</h4>
              <button
                onClick={() => removeOfferLevel(index)}
                className="text-red-400 hover:text-red-300"
              >
                <Minus className="h-4 w-4" />
              </button>
            </div>

            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
              <div>
                <label className="block text-gray-400 text-xs mb-1">Min Price</label>
                <input
                  type="number"
                  value={level.min_price}
                  onChange={(e) => handleOfferLevelChange(index, 'min_price', parseFloat(e.target.value) || 0)}
                  className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                  step="0.000001"
                />
              </div>

              <div>
                <label className="block text-gray-400 text-xs mb-1">Base Hourly Rate</label>
                <input
                  type="number"
                  value={level.base_hourly_rate}
                  onChange={(e) => handleOfferLevelChange(index, 'base_hourly_rate', parseFloat(e.target.value) || 0)}
                  className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                  step="0.001"
                />
              </div>

              <div>
                <label className="block text-gray-400 text-xs mb-1">Currency</label>
                <select
                  value={level.rate_currency}
                  onChange={(e) => handleOfferLevelChange(index, 'rate_currency', e.target.value)}
                  className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                >
                  <option value="base">Base</option>
                  <option value="quote">Quote</option>
                </select>
              </div>

              <div>
                <label className="block text-gray-400 text-xs mb-1">Drift (bps)</label>
                <input
                  type="number"
                  value={level.drift_bps}
                  onChange={(e) => handleOfferLevelChange(index, 'drift_bps', parseFloat(e.target.value) || 0)}
                  className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                  step="0.1"
                />
              </div>

              <div>
                <label className="block text-gray-400 text-xs mb-1">Timeout (sec)</label>
                <input
                  type="number"
                  value={level.timeout_seconds}
                  onChange={(e) => handleOfferLevelChange(index, 'timeout_seconds', parseInt(e.target.value) || 0)}
                  className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                  min="1"
                />
              </div>
            </div>
          </div>
        ))}

        {(config.offer_levels || []).length === 0 && (
          <div className="bg-gray-700 rounded p-4 text-center text-gray-400">
            No offer levels configured. Click "Add Offer Level" to get started.
          </div>
        )}
      </div>
      )}

      {/* Advanced Configuration */}
      {showAdvanced && (
        <div className="space-y-4">
          <h3 className="text-lg font-semibold text-white">Advanced Information</h3>
          
          <div className="bg-gray-700 rounded p-4 space-y-4">
            <div>
              <h4 className="text-white font-medium mb-2">How Volume-Weighted Coefficients Work</h4>
              <p className="text-gray-400 text-sm">
                The strategy calculates moving average ratios across the selected time periods to determine
                volume-weighted coefficients for each exchange. The calculation method (min/max/mid)
                determines how multiple ratios are combined into a single coefficient.
              </p>
              <p className="text-gray-400 text-sm mt-2">
                Coefficients are automatically updated every 10 seconds based on real-time trade data.
                Each exchange gets its own coefficient that adjusts the base hourly rates up or down.
              </p>
            </div>

            <div>
              <h4 className="text-white font-medium mb-2">Time Period Selection Guidelines</h4>
              <ul className="text-gray-400 text-sm space-y-1 list-disc list-inside">
                <li>Short-term periods (≤1h): React quickly to market changes</li>
                <li>Medium-term periods (4-24h): Capture daily patterns</li>
                <li>Long-term periods (days): Identify trends (requires historical data)</li>
                <li>Mix different periods for balanced analysis</li>
              </ul>
            </div>

            <div>
              <h4 className="text-white font-medium mb-2">Coefficient Bounds</h4>
              <p className="text-gray-400 text-sm">
                Min coefficient: Lowest multiplier applied to base rates (default: 0.2 = 20% of base rate)<br/>
                Max coefficient: Highest multiplier applied to base rates (default: 3.0 = 300% of base rate)
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Configuration Summary */}
      <div className="bg-gray-700 rounded p-4">
        <h3 className="text-lg font-semibold text-white mb-3">Configuration Summary</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 text-sm">
          <div>
            <span className="text-gray-400">Strategy:</span>
            <span className="text-white ml-2">Volume-Weighted Top of Book</span>
          </div>
          <div>
            <span className="text-gray-400">Target Inventory:</span>
            <span className="text-white ml-2">{config.target_inventory || 0}</span>
          </div>
          <div>
            <span className="text-gray-400">Excess %:</span>
            <span className="text-white ml-2">{config.excess_inventory_percentage || 0}%</span>
          </div>
          <div>
            <span className="text-gray-400">Spread:</span>
            <span className="text-white ml-2">{config.spread_bps || 0} bps</span>
          </div>
          <div>
            <span className="text-gray-400">Sides:</span>
            <span className="text-white ml-2">{config.sides || 'both'}</span>
          </div>
          <div>
            <span className="text-gray-400">Taker Check:</span>
            <span className="text-white ml-2">{config.taker_check ? 'Enabled' : 'Disabled'}</span>
          </div>
          <div>
            <span className="text-gray-400">Accounting:</span>
            <span className="text-white ml-2">{config.accounting_method || 'FIFO'}</span>
          </div>
          <div>
            <span className="text-gray-400">Time Periods:</span>
            <span className="text-white ml-2">
              {selectedPeriods.length} selected
              {(() => {
                const shortTerm = selectedPeriods.filter(p => parseInt(p) <= 60).length;
                const mediumTerm = selectedPeriods.filter(p => parseInt(p) > 60 && parseInt(p) <= 1440).length;
                const longTerm = selectedPeriods.filter(p => parseInt(p) > 1440).length;
                const parts = [];
                if (shortTerm > 0) parts.push(`${shortTerm} short`);
                if (mediumTerm > 0) parts.push(`${mediumTerm} medium`);
                if (longTerm > 0) parts.push(`${longTerm} long`);
                return parts.length > 0 ? ` (${parts.join(', ')})` : '';
              })()}
            </span>
          </div>
          <div>
            <span className="text-gray-400">Coefficient Method:</span>
            <span className="text-white ml-2">{config.coefficient_method || 'min'}</span>
          </div>
          <div>
            <span className="text-gray-400">Coefficient Range:</span>
            <span className="text-white ml-2">[{config.min_coefficient || 0.2}, {config.max_coefficient || 3.0}]</span>
          </div>
          <div>
            <span className="text-gray-400">Bid Levels:</span>
            <span className="text-white ml-2">{(config.bid_levels || []).length}</span>
          </div>
          <div>
            <span className="text-gray-400">Offer Levels:</span>
            <span className="text-white ml-2">{(config.offer_levels || []).length}</span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default VolumeWeightedTopOfBookConfig; 