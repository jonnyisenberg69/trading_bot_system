import React from 'react';

const TargetedBuybotConfig = ({ config, onConfigChange }) => {
  const handleChange = (field, value, type = 'string') => {
    let finalValue = value;
    if (type === 'number') {
      finalValue = parseFloat(value) || 0;
    } else if (type === 'integer') {
      finalValue = parseInt(value, 10) || 0;
    } else if (type === 'boolean') {
      finalValue = value;
    }
    
    const updates = { ...config, [field]: finalValue };
    
    onConfigChange(updates);
  };

  return (
    <div className="space-y-4">
      <h3 className="text-lg font-semibold text-white">Targeted Buybot Configuration</h3>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Start Time */}
        <div>
          <label className="block text-gray-400 text-sm mb-2">Start Time</label>
          <input
            type="datetime-local"
            value={config.start_time || ''}
            onChange={(e) => handleChange('start_time', e.target.value)}
            className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
          />
        </div>

        {/* Total Time */}
        <div>
          <label className="block text-gray-400 text-sm mb-2">Total Time (hours)</label>
          <input
            type="number"
            value={config.total_time_hours || 0}
            onChange={(e) => handleChange('total_time_hours', e.target.value, 'number')}
            className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            min="0.1"
            step="0.1"
          />
        </div>

        {/* Maximum Buy Price */}
        <div>
          <label className="block text-gray-400 text-sm mb-2">Maximum Buy Price</label>
          <input
            type="number"
            value={config.maximum_buy_price || ''}
            onChange={(e) => handleChange('maximum_buy_price', e.target.value === '' ? null : e.target.value, 'number')}
            className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            min="0"
            step="0.0001"
            placeholder="Optional"
          />
          <p className="text-xs text-gray-500 mt-1">Will not quote above this price</p>
        </div>

        {/* Price Comparison Mode */}
        <div>
          <label className="block text-gray-400 text-sm mb-2">Price Comparison Mode</label>
          <select
            value={config.price_comparison_mode || 'lesser'}
            onChange={(e) => handleChange('price_comparison_mode', e.target.value)}
            className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
          >
            <option value="lesser">Lesser (MIN)</option>
            <option value="greater">Greater (MAX)</option>
            <option value="inventory_only">Inventory Only</option>
            <option value="interval_only">Interval Only</option>
            <option value="target_only">Target Only</option>
          </select>
          <p className="text-xs text-gray-500 mt-1">How to compare inventory, interval, and target prices</p>
        </div>

        {/* Spread */}
        <div>
          <label className="block text-gray-400 text-sm mb-2">Spread (bps)</label>
          <input
            type="number"
            value={config.spread_bps || 0}
            onChange={(e) => handleChange('spread_bps', e.target.value, 'integer')}
            className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            min="0"
          />
        </div>

        {/* Timeout */}
        <div>
          <label className="block text-gray-400 text-sm mb-2">Timeout (seconds)</label>
          <input
            type="number"
            value={config.timeout_seconds || 0}
            onChange={(e) => handleChange('timeout_seconds', e.target.value, 'integer')}
            className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            min="1"
          />
        </div>

        {/* Drift */}
        <div>
          <label className="block text-gray-400 text-sm mb-2">Drift (bps)</label>
          <input
            type="number"
            value={config.drift_bps || 0}
            onChange={(e) => handleChange('drift_bps', e.target.value, 'integer')}
            className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            min="0"
          />
        </div>

        {/* Buy Mode */}
        <div>
          <label className="block text-gray-400 text-sm mb-2">Buy Mode</label>
          <select
            value={config.buy_mode || 'percent_of_target'}
            onChange={(e) => handleChange('buy_mode', e.target.value)}
            className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
          >
            <option value="percent_of_target">Percent of Target</option>
            <option value="hourly_rate">Hourly Rate</option>
          </select>
        </div>

        {/* Total Buy Target Amount (for percent_of_target mode) */}
        {config.buy_mode === 'percent_of_target' && (
          <div>
            <label className="block text-gray-400 text-sm mb-2">Total Buy Target Amount</label>
            <input
              type="number"
              value={config.total_buy_target_amount || ''}
              onChange={(e) => handleChange('total_buy_target_amount', e.target.value, 'number')}
              className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
              min="0"
              step="0.001"
              placeholder="Total amount to acquire"
            />
          </div>
        )}

        {/* Buy Target Value */}
        <div>
          <label className="block text-gray-400 text-sm mb-2">
            {config.buy_mode === 'hourly_rate' ? 'Hourly Rate' : 'Percent to Buy (%)'}
          </label>
          <input
            type="number"
            value={config.buy_target_value || ''}
            onChange={(e) => handleChange('buy_target_value', e.target.value, 'number')}
            className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            min="0"
            step={config.buy_mode === 'hourly_rate' ? '0.001' : '0.1'}
            placeholder={config.buy_mode === 'hourly_rate' ? 'Amount per hour' : 'Percent of total target'}
          />
          <p className="text-xs text-gray-500 mt-1">
            {config.buy_mode === 'hourly_rate' 
              ? 'Amount to buy per hour (in target currency)' 
              : 'TOTAL percentage of target to buy over the entire time period'}
          </p>
        </div>

        {/* Target Currency (for hourly rate mode) */}
        {config.buy_mode === 'hourly_rate' && (
          <div>
            <label className="block text-gray-400 text-sm mb-2">Target Currency</label>
            <select
              value={config.target_currency || 'base'}
              onChange={(e) => handleChange('target_currency', e.target.value)}
              className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            >
              <option value="base">Base</option>
              <option value="quote">Quote</option>
            </select>
          </div>
        )}

        {/* Taker Check */}
        <div className="md:col-span-2">
          <label className="flex items-center gap-2 text-gray-400 text-sm">
            <input
              type="checkbox"
              checked={config.taker_check || false}
              onChange={(e) => handleChange('taker_check', e.target.checked, 'boolean')}
              className="rounded border-gray-600"
            />
            Enable Taker Check
          </label>
          <p className="text-xs text-gray-500 mt-1">Prevents quotes from crossing the spread</p>
        </div>
      </div>
    </div>
  );
};

export default TargetedBuybotConfig; 
