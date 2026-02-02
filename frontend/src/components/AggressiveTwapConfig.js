import React, { useState, useEffect } from 'react';
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

const AggressiveTwapConfig = ({ 
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

  useEffect(() => {
    loadInitialData();
  }, []);

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
    // Load current price when symbol or exchanges change
    if (symbol && exchanges && exchanges.length > 0) {
      loadCurrentPrice();
    }
  }, [symbol, exchanges]);

  useEffect(() => {
    // Auto-generate intervals when key parameters change
    if (config.total_time_hours && config.granularity_value && config.granularity_unit && config.target_price && config.target_position_total && currentMidpoint) {
      generateIntervals();
    }
  }, [config.total_time_hours, config.granularity_value, config.granularity_unit, config.target_price, config.target_position_total, config.start_time, currentMidpoint]); // generateIntervals is defined in scope

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
        target_position: parseFloat(positionPerInterval.toFixed(6))
      });
    }

    // If no start_time is set, set it to current time in the correct format
    const updatedConfig = { ...config, intervals };
    if (!config.start_time || config.start_time === null) {
      updatedConfig.start_time = new Date().toISOString().slice(0, 16);
    }

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
    const updatedIntervals = config.intervals.map((interval, i) => 
      i === index ? { ...interval, [field]: value } : interval
    );
    onConfigChange({ ...config, intervals: updatedIntervals });
  };

  const addInterval = () => {
    console.log('AggressiveTwapConfig: addInterval called');
    const newInterval = {
      id: Date.now() + Math.random(), // Unique ID for React key
      target_price: config.target_price || '',
      target_position: '0.01'
    };
    console.log('AggressiveTwapConfig: New interval:', newInterval);
    onConfigChange({
      ...config,
      intervals: [...config.intervals, newInterval]
    });
    console.log('AggressiveTwapConfig: onConfigChange called for interval addition');
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
            Aggressive TWAP Configuration
          </h2>
          <p className="text-gray-400">Configure time-weighted strategy to push price to target</p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={() => setShowAdvanced(!showAdvanced)}
            className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-3 py-1 rounded text-sm"
          >
            <Settings className="h-4 w-4" />
            {showAdvanced ? 'Simple' : 'Advanced'}
          </button>
          <button
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
        {currentMidpoint && (
          <div className="bg-blue-600/20 border border-blue-500/30 rounded p-3">
            <p className="text-blue-300 text-sm">
              Current {symbol} price: <span className="font-bold text-white">${currentMidpoint}</span>
            </p>
          </div>
        )}

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
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-600">
                <th className="text-left text-gray-400 py-2">Interval</th>
                <th className="text-left text-gray-400 py-2">Target Price</th>
                <th className="text-left text-gray-400 py-2">
                  Target Position ({config.position_currency === 'base' ? baseCoin : 'USD'})
                </th>
                <th className="text-left text-gray-400 py-2">Actions</th>
              </tr>
            </thead>
            <tbody>
              {config.intervals.map((interval, index) => (
                <tr key={interval.id || `interval-${index}`} className="border-b border-gray-600">
                  <td className="py-2 text-white">{index + 1}</td>
                  <td className="py-2">
                    <input
                      type="number"
                      value={interval.target_price}
                      onChange={(e) => updateInterval(index, 'target_price', e.target.value)}
                      step="0.000001"
                      className="w-full bg-gray-600 text-white rounded px-2 py-1 text-sm"
                    />
                  </td>
                  <td className="py-2">
                    <input
                      type="number"
                      value={interval.target_position}
                      onChange={(e) => updateInterval(index, 'target_position', e.target.value)}
                      step="0.000001"
                      className="w-full bg-gray-600 text-white rounded px-2 py-1 text-sm"
                    />
                  </td>
                  <td className="py-2">
                    <button
                      onClick={() => removeInterval(index)}
                      disabled={config.intervals.length <= 1}
                      className="text-red-400 hover:text-red-300 disabled:text-gray-500"
                    >
                      <Trash2 className="h-4 w-4" />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {config.intervals.length === 0 && (
          <div className="text-center py-8 text-gray-400">
            <Target className="h-12 w-12 mx-auto mb-2 opacity-50" />
            <p>No intervals configured. Click "Auto Generate" or "Add" to create intervals.</p>
          </div>
        )}
      </div>

      {/* Strategy Summary */}
      <div className="bg-blue-600/20 border border-blue-500/30 rounded p-4">
        <div className="text-sm text-blue-300">
          <p>Strategy will create {totalIntervals} intervals over {config.total_time_hours} hours</p>
          <p>Total position allocation: {totalIntervalPosition.toFixed(6)} {config.position_currency === 'base' ? baseCoin : 'USD'}</p>
          {config.target_price && currentMidpoint && (
            <p>Price movement: {currentMidpoint.toFixed(6)} → {config.target_price} ({((parseFloat(config.target_price) - currentMidpoint) / currentMidpoint * 100).toFixed(2)}%)</p>
          )}
        </div>
      </div>
    </div>
  );
};

export default AggressiveTwapConfig;
