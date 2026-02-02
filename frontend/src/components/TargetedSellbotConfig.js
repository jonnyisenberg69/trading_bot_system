import React from 'react';

const TargetedSellbotConfig = ({ config, onConfigChange }) => {
  const handleChange = (field, value, type = 'string') => {
    let finalValue = value;
    if (type === 'number') {
      finalValue = parseFloat(value) || 0;
    } else if (type === 'integer') {
      finalValue = parseInt(value, 10) || 0;
    } else if (type === 'boolean') {
      finalValue = value;
    }
    
    // When sell_mode is set, ensure tracking_mode is also set appropriately
    const updates = { ...config, [field]: finalValue };
    if (field === 'sell_mode') {
      // Set tracking_mode based on sell_mode for backend compatibility
      if (value === 'percent_inventory') {
        updates.tracking_mode = 'percentage';
      } else if (value === 'hourly_rate') {
        updates.tracking_mode = 'target';
        // Ensure sell_target_value is initialized if not present
        if (updates.sell_target_value === undefined || updates.sell_target_value === null || updates.sell_target_value === '') {
          updates.sell_target_value = 0;
        }
      }
    }
    
    onConfigChange(updates);
  };

  return (
    <div className="space-y-4">
      <h3 className="text-lg font-semibold text-white">Targeted Sellbot Configuration</h3>
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

        {/* Minimum Sell Price */}
        <div>
          <label className="block text-gray-400 text-sm mb-2">Minimum Sell Price</label>
          <input
            type="number"
            value={config.minimum_sell_price || ''}
            onChange={(e) => handleChange('minimum_sell_price', e.target.value === '' ? null : e.target.value, 'number')}
            className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            min="0"
            step="0.0001"
            placeholder="Optional"
          />
          <p className="text-xs text-gray-500 mt-1">Will not quote below this price</p>
        </div>

        {/* Price Comparison Mode */}
        <div>
          <label className="block text-gray-400 text-sm mb-2">Price Comparison Mode</label>
          <select
            value={config.price_comparison_mode || 'greater'}
            onChange={(e) => handleChange('price_comparison_mode', e.target.value)}
            className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
          >
            <option value="greater">Greater (MAX)</option>
            <option value="lesser">Lesser (MIN)</option>
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

        {/* Sell Mode */}
        <div>
          <label className="block text-gray-400 text-sm mb-2">Sell Mode</label>
          <select
            value={config.sell_mode || 'percent_inventory'}
            onChange={(e) => handleChange('sell_mode', e.target.value)}
            className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
          >
            <option value="percent_inventory">Percent of Inventory</option>
            <option value="hourly_rate">Hourly Rate</option>
          </select>
        </div>

        {/* Sell Target Value */}
        <div>
          <label className="block text-gray-400 text-sm mb-2">
            {config.sell_mode === 'hourly_rate' ? 'Hourly Rate' : 'Percent to Sell (%)'}
          </label>
          <input
            type="number"
            value={config.sell_target_value || ''}
            onChange={(e) => handleChange('sell_target_value', e.target.value, 'number')}
            className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            min="0"
            step={config.sell_mode === 'hourly_rate' ? '0.001' : '0.1'}
            placeholder={config.sell_mode === 'hourly_rate' ? 'Amount per hour' : 'Percent of inventory'}
          />
          <p className="text-xs text-gray-500 mt-1">
            {config.sell_mode === 'hourly_rate' 
              ? 'Amount to sell per hour (in target currency)' 
              : 'TOTAL percentage of inventory to sell over the entire time period'}
          </p>
        </div>

        {/* Target Currency (for hourly rate mode) */}
        {config.sell_mode === 'hourly_rate' && (
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
        
        {/* Legacy fields for backward compatibility - only show if using old config WITHOUT sell_mode */}
        {config.tracking_mode && !config.sell_mode && (
          <>
            {config.tracking_mode === 'percentage' ? (
              <div>
                <label className="block text-gray-400 text-sm mb-2">Total Percentage (%) - Legacy Field</label>
                <input
                  type="number"
                  value={config.hourly_percentage || 0}
                  onChange={(e) => handleChange('hourly_percentage', e.target.value, 'number')}
                  className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
                  min="0"
                  max="100"
                  step="0.1"
                />
                <p className="text-xs text-gray-500 mt-1">Note: Despite the field name, this is the TOTAL % to sell (not per hour)</p>
              </div>
            ) : config.tracking_mode === 'target' ? (
              <>
                <div>
                  <label className="block text-gray-400 text-sm mb-2">Target Amount</label>
                  <input
                    type="number"
                    value={config.target_amount || ''}
                    onChange={(e) => handleChange('target_amount', e.target.value, 'number')}
                    className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
                    min="0"
                    step="0.001"
                  />
                </div>
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
              </>
            ) : null}
          </>
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

export default TargetedSellbotConfig; 
