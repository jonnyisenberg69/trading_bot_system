import React, { useState, useEffect, useRef } from 'react';
import { 
  Plus, 
  Minus, 
  Save, 
  Play, 
  Square, 
  RefreshCw, 
  Trash2,
  Copy,
  Settings,
  AlertCircle,
  CheckCircle,
  Clock,
  Zap,
  Target,
  TrendingUp,
  Calculator
} from 'lucide-react';
import { api } from '../services/api';

const AggressiveTwapV2Config = ({ 
  config,
  onConfigChange,
  symbol,
  exchanges = []
}) => {
  const [presets, setPresets] = useState({});
  const [selectedPreset, setSelectedPreset] = useState('');
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [currentMidpoint, setCurrentMidpoint] = useState(null);
  const [baseCoin, setBaseCoin] = useState('BTC');
  const [isNewConfig, setIsNewConfig] = useState(true);
  const initializedRef = useRef(false);

  const availableCoins = ['BTC', 'ETH', 'BNB', 'ADA', 'SOL', 'DOT', 'MATIC', 'AVAX', 'BERA'];
  const popularSymbols = [
    'BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'ADA/USDT', 
    'SOL/USDT', 'DOT/USDT', 'MATIC/USDT', 'AVAX/USDT', 'BERA/USDT'
  ];

  const granularityUnits = [
    { value: 'seconds', label: 'Seconds' },
    { value: 'minutes', label: 'Minutes' },
    { value: 'hours', label: 'Hours' }
  ];

  const positionCurrencies = [
    { value: 'base', label: 'Base Currency' },
    { value: 'usd', label: 'USD' }
  ];

  // Ensure config has intervals array and detect if new/existing config
  useEffect(() => {
    if (!initializedRef.current) {
      if (!config.intervals || !Array.isArray(config.intervals)) {
        onConfigChange({ ...config, intervals: [] });
        setIsNewConfig(true);
      } else if (config.intervals.length > 0) {
        // If we already have intervals, this is an existing config
        setIsNewConfig(false);
      }
      initializedRef.current = true;
    }
  }, [config]); // Only run when config changes and not yet initialized

  // Ensure all intervals always have the required fields
  useEffect(() => {
    if (config.intervals && config.intervals.length > 0) {
      const needsUpdate = config.intervals.some(interval => 
        interval.bps_move === undefined || interval.move_position === undefined
      );
      
      if (needsUpdate) {
        const updatedIntervals = config.intervals.map(interval => ({
          ...interval,
          bps_move: interval.bps_move ?? 0,
          move_position: interval.move_position ?? 0
        }));
        onConfigChange({ ...config, intervals: updatedIntervals });
      }
    }
  }, [config.intervals]); // This is safe since it only triggers when intervals change

  useEffect(() => {
    loadInitialData();
  }, []);

  useEffect(() => {
    // Extract base coin from symbol
    if (symbol && symbol.includes('/')) {
      const base = symbol.split('/')[0];
      if (availableCoins.includes(base)) {
        setBaseCoin(base);
      }
    }
  }, [symbol, config.base_coin]); // Removed onConfigChange from dependencies

  useEffect(() => {
    // Auto-derive base coin from symbol and update config
    if (symbol && symbol.includes('/')) {
      const [base] = symbol.split('/');
      setBaseCoin(base);
      
      // Update the config with the base coin if it's not already set
      if (!config.base_coin) {
        onConfigChange({ ...config, base_coin: base });
      }
    }
  }, [symbol, config.base_coin]); // Removed onConfigChange from dependencies

  useEffect(() => {
    // Load current price only for new configs
    if (isNewConfig && symbol && exchanges && exchanges.length > 0) {
      loadCurrentPrice();
    }
  }, [symbol, exchanges, isNewConfig]);

  // REMOVED: Auto-generation should only happen when user clicks "Auto Generate"
  // This ensures users see the table with all columns before any intervals are created

  const loadInitialData = async () => {
    await Promise.all([
      loadPresets(),
      loadCurrentPrice()
    ]);
  };

  const loadPresets = async () => {
    try {
      const response = await api.strategies.aggressiveTwap.getPresets();
      setPresets(response.data.presets);
    } catch (error) {
      console.error('Failed to load presets:', error);
    }
  };

  const loadCurrentPrice = async () => {
    if (!symbol || !exchanges || exchanges.length === 0) return;
    
    try {
      const response = await api.strategies.getCurrentPrice(symbol, exchanges);
      if (response.data.aggregated_midpoint) {
        setCurrentMidpoint(response.data.aggregated_midpoint);
      } else {
        console.warn('No price data available from exchanges');
        setCurrentMidpoint(null);
      }
    } catch (error) {
      console.error('Failed to load current price:', error);
      setCurrentMidpoint(null);
    }
  };

  const generateIntervals = () => {
    if (!currentMidpoint || !config.target_price || !config.target_position_total) return;

    const totalIntervals = calculateTotalIntervals();
    if (totalIntervals <= 0) return;

    const startPrice = parseFloat(currentMidpoint);
    const endPrice = parseFloat(config.target_price);
    const totalPosition = parseFloat(config.target_position_total);
    
    if (isNaN(startPrice) || isNaN(endPrice) || isNaN(totalPosition)) return;

    const priceStep = (endPrice - startPrice) / totalIntervals;
    const positionPerInterval = totalPosition / totalIntervals;

    const intervals = [];
    for (let i = 0; i < totalIntervals; i++) {
      const targetPrice = startPrice + (priceStep * (i + 1));
      intervals.push({
        target_price: parseFloat(targetPrice.toFixed(6)),
        target_position: parseFloat(positionPerInterval.toFixed(6)),
        bps_move: 0,  // Always include with 0 default
        move_position: 0  // Always include with 0 default
      });
    }

    // If no start_time is set, set it to current time in the correct format
    const updatedConfig = { 
      ...config, 
      intervals: intervals  // Explicitly set the new intervals array
    };
    
    if (!config.start_time || config.start_time === null) {
      updatedConfig.start_time = new Date().toISOString().slice(0, 16);
    }

    // Force a complete config update to ensure React re-renders properly
    onConfigChange(updatedConfig);
  };

  const calculateTotalIntervals = () => {
    const { total_time_hours, granularity_value, granularity_unit } = config;
    
    let intervalDurationHours;
    switch (granularity_unit) {
      case 'seconds':
        intervalDurationHours = granularity_value / 3600;
        break;
      case 'minutes':
        intervalDurationHours = granularity_value / 60;
        break;
      case 'hours':
        intervalDurationHours = granularity_value;
        break;
      default:
        return 0;
    }

    return Math.floor(total_time_hours / intervalDurationHours);
  };

  const updateInterval = (index, field, value) => {
    const updatedIntervals = config.intervals.map((interval, i) => {
      if (i === index) {
        // Convert to number for numeric fields
        if (field === 'bps_move' || field === 'move_position') {
          return { ...interval, [field]: parseFloat(value) || 0 };
        }
        return { ...interval, [field]: value };
      }
      return interval;
    });
    onConfigChange({ ...config, intervals: updatedIntervals });
  };

  const addInterval = () => {
    const newInterval = {
      id: Date.now() + Math.random(), // Unique ID for React key
      target_price: config.target_price || '',
      target_position: '0.01',
      bps_move: 0,  // Always include with 0 default
      move_position: 0  // Always include with 0 default
    };
    onConfigChange({
      ...config,
      intervals: [...config.intervals, newInterval]
    });
  };

  const removeInterval = (index) => {
    if (config.intervals.length > 1) {
      onConfigChange({
        ...config,
        intervals: config.intervals.filter((_, i) => i !== index)
      });
    }
  };

  const loadPreset = (presetName) => {
    const preset = presets[presetName];
    if (preset) {
      onConfigChange(preset);
      setSelectedPreset(presetName);
    }
  };

  const savePreset = async () => {
    const presetName = prompt('Enter preset name:');
    if (presetName) {
      try {
        await api.strategies.aggressiveTwap.savePreset(presetName, config);
        await loadPresets();
        alert('Preset saved successfully!');
      } catch (error) {
        console.error('Failed to save preset:', error);
        alert('Failed to save preset');
      }
    }
  };

  const copyConfig = () => {
    navigator.clipboard.writeText(JSON.stringify(config, null, 2));
    alert('Configuration copied to clipboard!');
  };

  const totalIntervals = calculateTotalIntervals();
  const totalIntervalPosition = config.intervals.reduce((sum, interval) => sum + parseFloat(interval.target_position || 0), 0);

  return (
    <div className="bg-gray-800 rounded-lg p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-white flex items-center gap-2">
            <TrendingUp className="h-6 w-6 text-red-500" />
            Aggressive TWAP V2 Configuration
          </h2>
          <p className="text-gray-400">Configure time-weighted strategy with sub-interval targeting</p>
        </div>
        <div className="flex gap-2">
          <button
            type="button"
            onClick={() => setShowAdvanced(!showAdvanced)}
            className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-3 py-1 rounded text-sm"
          >
            <Settings className="h-4 w-4" />
            {showAdvanced ? 'Simple' : 'Advanced'}
          </button>
          <button
            type="button"
            onClick={copyConfig}
            className="flex items-center gap-2 bg-gray-600 hover:bg-gray-700 text-white px-3 py-1 rounded text-sm"
          >
            <Copy className="h-4 w-4" />
            Copy Config
          </button>
        </div>
      </div>

      {/* Basic Configuration */}
      <div className="bg-gray-700 rounded p-4 space-y-4">
        <h3 className="text-lg font-semibold text-white">Basic Configuration</h3>
        
        {/* Current Price Display */}
        <div className="bg-blue-600/20 border border-blue-500/30 rounded p-3 flex justify-between items-center">
          {currentMidpoint ? (
            <p className="text-blue-300 text-sm">
              Current {symbol} price: <span className="font-bold text-white">${currentMidpoint}</span>
            </p>
          ) : (
            <p className="text-blue-300 text-sm">
              {isNewConfig ? 'Loading price...' : 'Price not loaded (using saved configuration)'}
            </p>
          )}
          {!isNewConfig && (
            <button
              type="button"
              onClick={loadCurrentPrice}
              className="text-blue-400 hover:text-blue-300 text-sm flex items-center gap-1"
            >
              <RefreshCw className="h-4 w-4" />
              Refresh Price
            </button>
          )}
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Total Time */}
          <div className="space-y-2">
            <label className="block text-gray-400 text-sm">Total Time (hours)</label>
            <input
              type="number"
              value={config.total_time_hours}
              onChange={(e) => onConfigChange({ ...config, total_time_hours: parseFloat(e.target.value) || 0 })}
              min="0.1"
              step="0.1"
              className="w-full bg-gray-600 text-white rounded px-3 py-2"
            />
          </div>

          {/* Start Time */}
          <div className="space-y-2">
            <label className="block text-gray-400 text-sm">Start Time (UTC)</label>
            <input
              type="datetime-local"
              value={config.start_time || ''}
              onChange={(e) => onConfigChange({ ...config, start_time: e.target.value || null })}
              className="w-full bg-gray-600 text-white rounded px-3 py-2 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
            <p className="text-xs text-gray-400">Enter UTC time when to start the strategy (leave empty for immediate start)</p>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Target Price */}
          <div className="space-y-2">
            <label className="block text-gray-400 text-sm">Target Price</label>
            <input
              type="number"
              value={config.target_price}
              onChange={(e) => onConfigChange({ ...config, target_price: e.target.value })}
              step="0.000001"
              className="w-full bg-gray-600 text-white rounded px-3 py-2"
              placeholder="Target price to reach"
            />
          </div>

          {/* Target Position Total */}
          <div className="space-y-2">
            <label className="block text-gray-400 text-sm">
              Target Position Total
            </label>
            <input
              type="number"
              value={config.target_position_total}
              onChange={(e) => onConfigChange({ ...config, target_position_total: e.target.value })}
              step="0.000001"
              className="w-full bg-gray-600 text-white rounded px-3 py-2"
              placeholder={`Maximum position size in ${config.position_currency === 'base' ? baseCoin : 'USD'}`}
            />
            <p className="text-xs text-gray-400">
              Currency: {config.position_currency === 'base' ? baseCoin : 'USD'}
            </p>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          {/* Granularity */}
          <div className="space-y-2">
            <label className="block text-gray-400 text-sm">Granularity Value</label>
            <input
              type="number"
              value={config.granularity_value}
              onChange={(e) => onConfigChange({ ...config, granularity_value: parseInt(e.target.value) || 0 })}
              min="1"
              className="w-full bg-gray-600 text-white rounded px-3 py-2"
            />
          </div>

          <div className="space-y-2">
            <label className="block text-gray-400 text-sm">Granularity Unit</label>
            <select
              value={config.granularity_unit}
              onChange={(e) => onConfigChange({ ...config, granularity_unit: e.target.value })}
              className="w-full bg-gray-600 text-white rounded px-3 py-2"
            >
              {granularityUnits.map(unit => (
                <option key={unit.value} value={unit.value}>{unit.label}</option>
              ))}
            </select>
          </div>

          {/* Frequency */}
          <div className="space-y-2">
            <label className="block text-gray-400 text-sm">Frequency (seconds)</label>
            <input
              type="number"
              value={config.frequency_seconds}
              onChange={(e) => onConfigChange({ ...config, frequency_seconds: parseInt(e.target.value) || 0 })}
              min="1"
              className="w-full bg-gray-600 text-white rounded px-3 py-2"
            />
          </div>

          {/* Position Currency */}
          <div className="space-y-2">
            <label className="block text-gray-400 text-sm">Target Position Currency</label>
            <select
              value={config.position_currency}
              onChange={(e) => onConfigChange({ ...config, position_currency: e.target.value })}
              className="w-full bg-gray-600 text-white rounded px-3 py-2"
            >
              {positionCurrencies.map(currency => (
                <option key={currency.value} value={currency.value}>{currency.label}</option>
              ))}
            </select>
            <p className="text-xs text-gray-400">
              Currency for all position amounts in this strategy
            </p>
          </div>
        </div>

        {/* Cooldown Period */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="space-y-2">
            <label className="block text-gray-400 text-sm">Cooldown Period (seconds)</label>
            <input
              type="number"
              value={config.cooldown_period_seconds}
              onChange={(e) => onConfigChange({ ...config, cooldown_period_seconds: parseInt(e.target.value) || 0 })}
              min="0"
              className="w-full bg-gray-600 text-white rounded px-3 py-2"
            />
            <p className="text-xs text-gray-400">Time other strategies pause after aggressive TWAP places orders</p>
          </div>

          {showAdvanced && (
            <div className="space-y-2">
              <label className="block text-gray-400 text-sm">Position Log Interval (seconds)</label>
              <input
                type="number"
                value={config.position_log_interval || 5}
                onChange={(e) => onConfigChange({ ...config, position_log_interval: parseInt(e.target.value) || 5 })}
                min="1"
                className="w-full bg-gray-600 text-white rounded px-3 py-2"
              />
              <p className="text-xs text-gray-400">How often to log current position</p>
            </div>
          )}
        </div>
      </div>

      {/* Interval Configuration */}
      <div className="bg-gray-700 rounded p-4 space-y-4">
        <div className="flex justify-between items-center">
          <h3 className="text-lg font-semibold text-white flex items-center gap-2">
            <Calculator className="h-5 w-5" />
            Price Intervals ({totalIntervals} intervals)
          </h3>
          <div className="flex gap-2">
            <button
              type="button"
              onClick={generateIntervals}
              className="bg-blue-600 hover:bg-blue-700 text-white px-3 py-1 rounded text-sm flex items-center gap-2"
            >
              <RefreshCw className="h-4 w-4" />
              Auto Generate
            </button>
            <button
              type="button"
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                addInterval();
              }}
              className="bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded text-sm flex items-center gap-2"
            >
              <Plus className="h-4 w-4" />
              Add
            </button>
          </div>
        </div>

        {/* Validation Warnings */}
        {totalIntervalPosition > parseFloat(config.target_position_total || 0) && (
          <div className="bg-red-900 border border-red-600 rounded p-3 flex items-center gap-2">
            <AlertCircle className="h-5 w-5 text-red-400" />
            <span className="text-red-200">
              Warning: Total interval positions ({totalIntervalPosition.toFixed(6)}) exceed target position total ({config.target_position_total})
            </span>
          </div>
        )}

        {/* Intervals Table */}
        <div className="overflow-x-auto">
          <table className="w-full text-sm" style={{ minWidth: '800px' }}>
            <thead>
              <tr className="border-b border-gray-600">
                <th className="text-left text-gray-400 py-2 px-2">Interval</th>
                <th className="text-left text-gray-400 py-2 px-2">Target Price</th>
                <th className="text-left text-gray-400 py-2 px-2">
                  Target Position ({config.position_currency === 'base' ? baseCoin : 'USD'})
                </th>
                <th className="text-left text-gray-400 py-2 px-2">BPS Move</th>
                <th className="text-left text-gray-400 py-2 px-2">
                  Move Budget ({config.position_currency === 'base' ? baseCoin : 'USD'})
                </th>
                <th className="text-left text-gray-400 py-2 px-2">Actions</th>
              </tr>
            </thead>
            <tbody>
              {config.intervals && config.intervals.length > 0 ? (
                config.intervals.map((interval, index) => (
                  <tr key={interval.id || `interval-${index}`} className="border-b border-gray-600">
                    <td className="py-2 px-2 text-white">{index + 1}</td>
                    <td className="py-2 px-2">
                      <input
                        type="number"
                        value={interval.target_price}
                        onChange={(e) => updateInterval(index, 'target_price', e.target.value)}
                        step="0.000001"
                        className="w-full bg-gray-600 text-white rounded px-2 py-1 text-sm"
                      />
                    </td>
                    <td className="py-2 px-2">
                      <input
                        type="number"
                        value={interval.target_position}
                        onChange={(e) => updateInterval(index, 'target_position', e.target.value)}
                        step="0.000001"
                        className="w-full bg-gray-600 text-white rounded px-2 py-1 text-sm"
                      />
                    </td>
                    <td className="py-2 px-2">
                      <input
                        type="number"
                        value={interval.bps_move ?? 0}
                        onChange={(e) => updateInterval(index, 'bps_move', e.target.value)}
                        placeholder="BPS"
                        className="w-full bg-gray-600 text-white rounded px-2 py-1 text-sm"
                      />
                    </td>
                    <td className="py-2 px-2">
                      <input
                        type="number"
                        value={interval.move_position ?? 0}
                        onChange={(e) => updateInterval(index, 'move_position', e.target.value)}
                        step="0.000001"
                        placeholder="0"
                        className="w-full bg-gray-600 text-white rounded px-2 py-1 text-sm"
                      />
                    </td>
                    <td className="py-2 px-2">
                      <button
                        type="button"
                        onClick={() => removeInterval(index)}
                        disabled={config.intervals.length <= 1}
                        className="text-red-400 hover:text-red-300 disabled:text-gray-500"
                      >
                        <Trash2 className="h-4 w-4" />
                      </button>
                    </td>
                  </tr>
                ))
              ) : (
                <>
                  {/* Show a disabled example row so users can see all columns */}
                  <tr className="border-b border-gray-600 opacity-50">
                    <td className="py-2 px-2 text-gray-500">1</td>
                    <td className="py-2 px-2">
                      <input
                        type="number"
                        disabled
                        placeholder="Target Price"
                        className="w-full bg-gray-700 text-gray-500 rounded px-2 py-1 text-sm cursor-not-allowed"
                      />
                    </td>
                    <td className="py-2 px-2">
                      <input
                        type="number"
                        disabled
                        placeholder="Target Position"
                        className="w-full bg-gray-700 text-gray-500 rounded px-2 py-1 text-sm cursor-not-allowed"
                      />
                    </td>
                    <td className="py-2 px-2">
                      <input
                        type="number"
                        disabled
                        placeholder="BPS Move"
                        className="w-full bg-gray-700 text-gray-500 rounded px-2 py-1 text-sm cursor-not-allowed"
                      />
                    </td>
                    <td className="py-2 px-2">
                      <input
                        type="number"
                        disabled
                        placeholder="Move Budget"
                        className="w-full bg-gray-700 text-gray-500 rounded px-2 py-1 text-sm cursor-not-allowed"
                      />
                    </td>
                    <td className="py-2 px-2">
                      <button type="button" disabled className="text-gray-600 cursor-not-allowed">
                        <Trash2 className="h-4 w-4" />
                      </button>
                    </td>
                  </tr>
                  <tr className="border-b border-gray-600">
                    <td colSpan="6" className="py-4 text-center text-gray-400">
                      <p className="text-sm">Click "Auto Generate" or "Add" to create intervals</p>
                    </td>
                  </tr>
                </>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Strategy Summary */}
      <div className="bg-blue-600/20 border border-blue-500/30 rounded p-4">
        <div className="text-sm text-blue-300">
          <p>Strategy will create {totalIntervals} intervals over {config.total_time_hours} hours</p>
          <p>Each interval will have multiple sub-intervals based on frequency ({config.frequency_seconds}s)</p>
          <p>Total position allocation: {totalIntervalPosition.toFixed(6)} {config.position_currency === 'base' ? baseCoin : 'USD'}</p>
          {config.target_price && currentMidpoint && (
            <p>Price movement: {currentMidpoint.toFixed(6)} → {config.target_price} ({((parseFloat(config.target_price) - currentMidpoint) / currentMidpoint * 100).toFixed(2)}%)</p>
          )}
          <p className="mt-2 text-yellow-300">
            ⚡ New: Each sub-interval uses 100% of interval budget. BPS moves use separate move budget.
          </p>
        </div>
      </div>

      {/* Advanced Settings */}
      {showAdvanced && (
        <div className="bg-gray-700 rounded p-4 space-y-4">
          <h3 className="text-lg font-semibold text-white">Advanced Settings</h3>
          <div className="bg-yellow-600/20 border border-yellow-500/30 rounded p-3">
            <p className="text-yellow-300 text-sm">
              V2 Features: Sub-intervals with budget allocation, price reversion handling, and liquidity-based order routing
            </p>
          </div>
        </div>
      )}
    </div>
  );
};

export default AggressiveTwapV2Config; 
