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
  Zap
} from 'lucide-react';
import { api } from '../services/api';

const PassiveQuotingConfig = ({ 
  config,
  onConfigChange 
}) => {
  const [showAdvanced, setShowAdvanced] = useState(false);
  
  const handleLineChange = (index, field, value) => {
    const newLines = [...config.lines];
    newLines[index] = { ...newLines[index], [field]: value };
    onConfigChange({ ...config, lines: newLines });
  };

  const addLine = () => {
    console.log('PassiveQuotingConfig: addLine called');
    const newLines = [...(config.lines || []), {
      id: Date.now() + Math.random(), // Unique ID for React key
      timeout: 300,
      drift: 50,
      quantity: 0.01,
      quantity_randomization_factor: 10,
      spread: 25,
      sides: 'both'
    }];
    console.log('PassiveQuotingConfig: New lines:', newLines);
    onConfigChange({ ...config, lines: newLines });
    console.log('PassiveQuotingConfig: onConfigChange called for line addition');
  };

  const removeLine = (index) => {
    if (config.lines.length > 1) {
      const newLines = config.lines.filter((_, i) => i !== index);
      onConfigChange({ ...config, lines: newLines });
    }
  };
  
  const handleQuantityCurrencyChange = (value) => {
    onConfigChange({ ...config, quantity_currency: value });
  };

  const handleBaseCoinChange = (value) => {
    onConfigChange({ ...config, base_coin: value.toUpperCase() });
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-white">Passive Quoting Configuration</h2>
          <p className="text-gray-400">Configure multi-line passive market making strategy</p>
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
        
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Base Coin */}
          <div>
            <label className="block text-gray-400 text-sm mb-2">Base Coin</label>
            <input
              type="text"
              value={config.base_coin || ''}
              onChange={(e) => handleBaseCoinChange(e.target.value)}
              className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
              placeholder="e.g. BERA"
            />
          </div>

          {/* Quantity Currency */}
          <div>
            <label className="block text-gray-400 text-sm mb-2">Quantity Currency</label>
            <select
              value={config.quantity_currency || 'base'}
              onChange={(e) => handleQuantityCurrencyChange(e.target.value)}
              className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            >
              <option value="base">Base (e.g., {config.base_coin || 'BERA'})</option>
              <option value="quote">Quote (e.g., USDT)</option>
            </select>
          </div>
        </div>
      </div>

      {/* Quote Lines */}
      <div className="space-y-4">
        <div className="flex justify-between items-center">
          <h3 className="text-lg font-semibold text-white">Quote Lines ({config.lines?.length || 0})</h3>
          <button
            type="button"
            onClick={(e) => {
              e.preventDefault();
              e.stopPropagation();
              addLine();
            }}
            className="flex items-center gap-2 bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded text-sm"
          >
            <Plus className="h-4 w-4" />
            Add Line
          </button>
        </div>

        {(config.lines || []).map((line, index) => (
          <div key={line.id || `line-${index}`} className="bg-gray-700 rounded p-4 space-y-3">
            <div className="flex justify-between items-center">
              <h4 className="text-white font-medium">Line {index + 1}</h4>
              {config.lines.length > 1 && (
                <button
                  onClick={() => removeLine(index)}
                  className="text-red-400 hover:text-red-300"
                >
                  <Minus className="h-4 w-4" />
                </button>
              )}
            </div>

            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
              {/* Timeout */}
              <div>
                <label className="block text-gray-400 text-xs mb-1">Timeout (sec)</label>
                <input
                  type="number"
                  value={line.timeout}
                  onChange={(e) => handleLineChange(index, 'timeout', parseInt(e.target.value) || 0)}
                  className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                  min="1"
                />
              </div>

              {/* Drift */}
              <div>
                <label className="block text-gray-400 text-xs mb-1">Drift (bps)</label>
                <input
                  type="number"
                  value={line.drift}
                  onChange={(e) => handleLineChange(index, 'drift', parseFloat(e.target.value) || 0)}
                  className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                  min="0"
                  step="0.1"
                />
              </div>

              {/* Quantity */}
              <div>
                <label className="block text-gray-400 text-xs mb-1">Quantity</label>
                <input
                  type="number"
                  value={line.quantity}
                  onChange={(e) => handleLineChange(index, 'quantity', parseFloat(e.target.value) || 0)}
                  className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                  min="0"
                  step="0.001"
                />
              </div>

              {/* Randomization */}
              <div>
                <label className="block text-gray-400 text-xs mb-1">Random (%)</label>
                <input
                  type="number"
                  value={line.quantity_randomization_factor}
                  onChange={(e) => handleLineChange(index, 'quantity_randomization_factor', parseFloat(e.target.value) || 0)}
                  className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                  min="0"
                  max="100"
                  step="0.1"
                />
              </div>

              {/* Spread */}
              <div>
                <label className="block text-gray-400 text-xs mb-1">Spread (bps)</label>
                <input
                  type="number"
                  value={line.spread}
                  onChange={(e) => handleLineChange(index, 'spread', parseFloat(e.target.value) || 0)}
                  className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                  min="0.1"
                  step="0.1"
                />
              </div>

              {/* Sides */}
              <div>
                <label className="block text-gray-400 text-xs mb-1">Sides</label>
                <select
                  value={line.sides}
                  onChange={(e) => handleLineChange(index, 'sides', e.target.value)}
                  className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                >
                  <option value="both">Both</option>
                  <option value="bid">Bid Only</option>
                  <option value="offer">Offer Only</option>
                </select>
              </div>
            </div>

            {showAdvanced && (
              <div className="text-xs text-gray-400 bg-gray-800 p-2 rounded">
                <strong>Line {index + 1} Summary:</strong> 
                {line.sides === 'both' ? ' Bid & Ask' : ` ${line.sides.charAt(0).toUpperCase() + line.sides.slice(1)} only`} 
                {' '}@ {line.spread}bps spread, {line.quantity} {config.quantity_currency} 
                (±{line.quantity_randomization_factor}%), timeout {line.timeout}s, drift {line.drift}bps
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
};

export default PassiveQuotingConfig; 
