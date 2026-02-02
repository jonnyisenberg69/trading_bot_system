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

const TopOfBookConfig = ({ 
  config,
  onConfigChange,
  symbol,
  exchanges = []
}) => {
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [currentMidpoint, setCurrentMidpoint] = useState(null);

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
        console.warn('No price data available from exchanges');
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

  const handleBidRateChange = (index, field, value) => {
    const newBidRates = [...(config.bid_hourly_rates || [])];
    newBidRates[index] = { ...newBidRates[index], [field]: value };
    onConfigChange({ ...config, bid_hourly_rates: newBidRates });
  };

  const handleOfferRateChange = (index, field, value) => {
    const newOfferRates = [...(config.offer_hourly_rates || [])];
    newOfferRates[index] = { ...newOfferRates[index], [field]: value };
    onConfigChange({ ...config, offer_hourly_rates: newOfferRates });
  };

  const addBidRate = () => {
    console.log('TopOfBookConfig: addBidRate called, currentMidpoint:', currentMidpoint);
    try {
      const newBidRates = [...(config.bid_hourly_rates || []), {
        id: Date.now() + Math.random(), // Unique ID for React key
        max_price: currentMidpoint || 0,
        hourly_rate: 0.01,
        rate_currency: 'base',
        drift_bps: 50,
        timeout_seconds: 300
      }];
      console.log('TopOfBookConfig: New bid rates:', newBidRates);
      onConfigChange({ ...config, bid_hourly_rates: newBidRates });
      console.log('TopOfBookConfig: onConfigChange called for bid rate addition');
    } catch (error) {
      console.error('TopOfBookConfig: Error adding bid rate:', error);
    }
  };

  const addOfferRate = () => {
    console.log('TopOfBookConfig: addOfferRate called, currentMidpoint:', currentMidpoint);
    try {
      const newOfferRates = [...(config.offer_hourly_rates || []), {
        id: Date.now() + Math.random(), // Unique ID for React key
        min_price: currentMidpoint || 0,
        hourly_rate: 0.01,
        rate_currency: 'base',
        drift_bps: 50,
        timeout_seconds: 300
      }];
      console.log('TopOfBookConfig: New offer rates:', newOfferRates);
      onConfigChange({ ...config, offer_hourly_rates: newOfferRates });
      console.log('TopOfBookConfig: onConfigChange called for offer rate addition');
    } catch (error) {
      console.error('TopOfBookConfig: Error adding offer rate:', error);
    }
  };

  const removeBidRate = (index) => {
    const newBidRates = config.bid_hourly_rates.filter((_, i) => i !== index);
    onConfigChange({ ...config, bid_hourly_rates: newBidRates });
  };

  const removeOfferRate = (index) => {
    const newOfferRates = config.offer_hourly_rates.filter((_, i) => i !== index);
    onConfigChange({ ...config, offer_hourly_rates: newOfferRates });
  };

  const shouldShowBidRates = config.sides === 'both' || config.sides === 'bid';
  const shouldShowOfferRates = config.sides === 'both' || config.sides === 'offer';

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-white">Top of Book Configuration</h2>
          <p className="text-gray-400">Configure inventory-based market making at best bid/offer</p>
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

          {/* Sides */}
          <div>
            <label className="block text-gray-400 text-sm mb-2">Sides</label>
            <select
              value={config.sides || 'both'}
              onChange={(e) => handleConfigChange('sides', e.target.value)}
              className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            >
              <option value="both">Both (Bid & Offer)</option>
              <option value="bid">Bid Only</option>
              <option value="offer">Offer Only</option>
            </select>
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

          {/* Taker Check */}
          <div>
            <label className="block text-gray-400 text-sm mb-2">Taker Check</label>
            <select
              value={config.taker_check ? 'true' : 'false'}
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
              <option value="FIFO">FIFO (First In, First Out)</option>
              <option value="LIFO">LIFO (Last In, First Out)</option>
              <option value="AVERAGE_COST">Average Cost Basis</option>
            </select>
          </div>
        </div>
      </div>

      {/* Bid Hourly Rates */}
      {shouldShowBidRates && (
        <div className="space-y-4">
          <div className="flex justify-between items-center">
            <h3 className="text-lg font-semibold text-white">
              Bid Hourly Rates ({(config.bid_hourly_rates || []).length})
            </h3>
            <button
              type="button"
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                addBidRate();
              }}
              className="flex items-center gap-2 bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded text-sm"
            >
              <Plus className="h-4 w-4" />
              Add Bid Rate
            </button>
          </div>

          {(config.bid_hourly_rates || []).map((rate, index) => (
            <div key={rate.id || `bid-${index}`} className="bg-gray-700 rounded p-4 space-y-3">
              <div className="flex justify-between items-center">
                <h4 className="text-white font-medium">Bid Rate {index + 1}</h4>
                <button
                  onClick={() => removeBidRate(index)}
                  className="text-red-400 hover:text-red-300"
                >
                  <Minus className="h-4 w-4" />
                </button>
              </div>

              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
                {/* Max Price */}
                <div>
                  <label className="block text-gray-400 text-xs mb-1">Max Price</label>
                  <input
                    type="number"
                    value={rate.max_price}
                    onChange={(e) => handleBidRateChange(index, 'max_price', parseFloat(e.target.value) || 0)}
                    className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                    step="0.000001"
                  />
                </div>

                {/* Hourly Rate */}
                <div>
                  <label className="block text-gray-400 text-xs mb-1">Hourly Rate</label>
                  <input
                    type="number"
                    value={rate.hourly_rate}
                    onChange={(e) => handleBidRateChange(index, 'hourly_rate', parseFloat(e.target.value) || 0)}
                    className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                    step="0.001"
                  />
                </div>

                {/* Rate Currency */}
                <div>
                  <label className="block text-gray-400 text-xs mb-1">Currency</label>
                  <select
                    value={rate.rate_currency}
                    onChange={(e) => handleBidRateChange(index, 'rate_currency', e.target.value)}
                    className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                  >
                    <option value="base">Base</option>
                    <option value="quote">Quote</option>
                  </select>
                </div>

                {/* Drift */}
                <div>
                  <label className="block text-gray-400 text-xs mb-1">Drift (bps)</label>
                  <input
                    type="number"
                    value={rate.drift_bps}
                    onChange={(e) => handleBidRateChange(index, 'drift_bps', parseFloat(e.target.value) || 0)}
                    className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                    step="0.1"
                  />
                </div>

                {/* Timeout */}
                <div>
                  <label className="block text-gray-400 text-xs mb-1">Timeout (sec)</label>
                  <input
                    type="number"
                    value={rate.timeout_seconds}
                    onChange={(e) => handleBidRateChange(index, 'timeout_seconds', parseInt(e.target.value) || 0)}
                    className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                    min="1"
                  />
                </div>
              </div>
            </div>
          ))}

          {(config.bid_hourly_rates || []).length === 0 && (
            <div className="bg-gray-700 rounded p-4 text-center text-gray-400">
              No bid hourly rates configured. Click "Add Bid Rate" to get started.
            </div>
          )}
        </div>
      )}

      {/* Offer Hourly Rates */}
      {shouldShowOfferRates && (
        <div className="space-y-4">
          <div className="flex justify-between items-center">
            <h3 className="text-lg font-semibold text-white">
              Offer Hourly Rates ({(config.offer_hourly_rates || []).length})
            </h3>
            <button
              type="button"
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                addOfferRate();
              }}
              className="flex items-center gap-2 bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded text-sm"
            >
              <Plus className="h-4 w-4" />
              Add Offer Rate
            </button>
          </div>

          {(config.offer_hourly_rates || []).map((rate, index) => (
            <div key={rate.id || `offer-${index}`} className="bg-gray-700 rounded p-4 space-y-3">
              <div className="flex justify-between items-center">
                <h4 className="text-white font-medium">Offer Rate {index + 1}</h4>
                <button
                  onClick={() => removeOfferRate(index)}
                  className="text-red-400 hover:text-red-300"
                >
                  <Minus className="h-4 w-4" />
                </button>
              </div>

              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
                {/* Min Price */}
                <div>
                  <label className="block text-gray-400 text-xs mb-1">Min Price</label>
                  <input
                    type="number"
                    value={rate.min_price}
                    onChange={(e) => handleOfferRateChange(index, 'min_price', parseFloat(e.target.value) || 0)}
                    className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                    step="0.000001"
                  />
                </div>

                {/* Hourly Rate */}
                <div>
                  <label className="block text-gray-400 text-xs mb-1">Hourly Rate</label>
                  <input
                    type="number"
                    value={rate.hourly_rate}
                    onChange={(e) => handleOfferRateChange(index, 'hourly_rate', parseFloat(e.target.value) || 0)}
                    className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                    step="0.001"
                  />
                </div>

                {/* Rate Currency */}
                <div>
                  <label className="block text-gray-400 text-xs mb-1">Currency</label>
                  <select
                    value={rate.rate_currency}
                    onChange={(e) => handleOfferRateChange(index, 'rate_currency', e.target.value)}
                    className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                  >
                    <option value="base">Base</option>
                    <option value="quote">Quote</option>
                  </select>
                </div>

                {/* Drift */}
                <div>
                  <label className="block text-gray-400 text-xs mb-1">Drift (bps)</label>
                  <input
                    type="number"
                    value={rate.drift_bps}
                    onChange={(e) => handleOfferRateChange(index, 'drift_bps', parseFloat(e.target.value) || 0)}
                    className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                    step="0.1"
                  />
                </div>

                {/* Timeout */}
                <div>
                  <label className="block text-gray-400 text-xs mb-1">Timeout (sec)</label>
                  <input
                    type="number"
                    value={rate.timeout_seconds}
                    onChange={(e) => handleOfferRateChange(index, 'timeout_seconds', parseInt(e.target.value) || 0)}
                    className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                    min="1"
                  />
                </div>
              </div>
            </div>
          ))}

          {(config.offer_hourly_rates || []).length === 0 && (
            <div className="bg-gray-700 rounded p-4 text-center text-gray-400">
              No offer hourly rates configured. Click "Add Offer Rate" to get started.
            </div>
          )}
        </div>
      )}

      {/* Advanced Configuration */}
      {showAdvanced && (
        <div className="space-y-4">
          <h3 className="text-lg font-semibold text-white">Advanced Configuration</h3>
          
          <div className="bg-gray-700 rounded p-4">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {/* Additional advanced settings can be added here */}
              <div className="text-gray-400 text-sm">
                <p>Advanced settings are managed through the basic configuration above.</p>
                <p>The strategy automatically handles:</p>
                <ul className="list-disc list-inside mt-2 space-y-1">
                  <li>Inventory calculation from trade history</li>
                  <li>Best bid/offer tracking</li>
                  <li>Spread crossing prevention</li>
                  <li>Time-in-zone tracking</li>
                  <li>Hourly rate execution</li>
                </ul>
              </div>
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
            <span className="text-white ml-2">Top of Book</span>
          </div>
          <div>
            <span className="text-gray-400">Sides:</span>
            <span className="text-white ml-2">{config.sides || 'both'}</span>
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
            <span className="text-gray-400">Taker Check:</span>
            <span className="text-white ml-2">{config.taker_check ? 'Enabled' : 'Disabled'}</span>
          </div>
          <div>
            <span className="text-gray-400">Accounting Method:</span>
            <span className="text-white ml-2">{config.accounting_method || 'FIFO'}</span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default TopOfBookConfig;