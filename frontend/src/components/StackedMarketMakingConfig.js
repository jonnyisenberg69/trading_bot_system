import React, { useState, useEffect } from 'react';
import { 
  TrendingUp, 
  RefreshCw, 
  AlertCircle, 
  Plus,
  Minus,
  Settings,
  Layers,
  Target,
  Zap,
  DollarSign,
  Clock,
  Calculator,
  Copy,
  BookOpen,
  BarChart3,
  Eye,
  ChevronDown,
  ChevronRight
} from 'lucide-react';
import { api } from '../services/api';

const StackedMarketMakingConfig = ({ 
  config, 
  onConfigChange,
  symbol,
  exchanges = []
}) => {
  const [currentMidpoint, setCurrentMidpoint] = useState(null);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [activeTab, setActiveTab] = useState('global'); // global, inventory, tob, passive, volume, advanced
  const [expandedSections, setExpandedSections] = useState(new Set(['global']));
  const [newHedgeCurrency, setNewHedgeCurrency] = useState('');
  const [newHedgeTarget, setNewHedgeTarget] = useState('');

  useEffect(() => {
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

  // Initialize config with defaults if needed
  const ensureDefaults = () => {
    const defaults = {
      base_coin: symbol ? symbol.split('/')[0].toUpperCase() : '',
      quote_currencies: ['USDT'],
      
      // Inventory config defaults
      inventory: {
        target_inventory: '300000',
        max_inventory_deviation: '50000', 
        inventory_price_method: 'accounting',
        manual_inventory_price: null,
        start_time: new Date().toISOString().slice(0, 16)
      },
      
      // TOB lines defaults
      tob_lines: [],
      
      // Passive lines defaults  
      passive_lines: [],
      
      // Volume coefficient defaults (proven system)
      time_periods: ['5min', '15min', '30min'],
      coefficient_method: 'min',
      min_coefficient: 0.2,
      max_coefficient: 3.0,
      
      // Advanced defaults
      smart_pricing_source: 'aggregated',
      taker_check: true,
      hedging_enabled: true,
      hedging_targets: {},
      leverage: '1.0',
      moving_average_periods: [5, 15, 60]
    };
    
    return { ...defaults, ...config };
  };

  const normalizedConfig = ensureDefaults();

  const handleConfigChange = (field, value) => {
    onConfigChange({ ...normalizedConfig, [field]: value });
  };

  const handleHedgingTargetChange = (currency, value) => {
    const targets = { ...(normalizedConfig.hedging_targets || {}) };
    targets[currency] = value;
    onConfigChange({ ...normalizedConfig, hedging_targets: targets });
  };

  const handleHedgingTargetRemove = (currency) => {
    const targets = { ...(normalizedConfig.hedging_targets || {}) };
    delete targets[currency];
    onConfigChange({ ...normalizedConfig, hedging_targets: targets });
  };

  const handleHedgingTargetAdd = () => {
    const cur = (newHedgeCurrency || '').toUpperCase().trim();
    const val = (newHedgeTarget || '').trim();
    if (!cur || cur === 'USDT') {
      alert('Please enter a non-USDT currency code.');
      return;
    }
    if (val === '') {
      alert('Please enter a numeric target amount.');
      return;
    }
    const targets = { ...(normalizedConfig.hedging_targets || {}) };
    targets[cur] = val;
    onConfigChange({ ...normalizedConfig, hedging_targets: targets });
    setNewHedgeCurrency('');
    setNewHedgeTarget('');
  };

  const handleInventoryChange = (field, value) => {
    const newInventory = { ...normalizedConfig.inventory, [field]: value };
    onConfigChange({ ...normalizedConfig, inventory: newInventory });
  };

  const handleTOBLineChange = (index, field, value) => {
    const newTOBLines = [...(normalizedConfig.tob_lines || [])];
    newTOBLines[index] = { ...newTOBLines[index], [field]: value };
    onConfigChange({ ...normalizedConfig, tob_lines: newTOBLines });
  };

  const handlePassiveLineChange = (index, field, value) => {
    const newPassiveLines = [...(normalizedConfig.passive_lines || [])];
    newPassiveLines[index] = { ...newPassiveLines[index], [field]: value };
    onConfigChange({ ...normalizedConfig, passive_lines: newPassiveLines });
  };

  const addTOBLine = () => {
    const newTOBLine = {
      line_id: (normalizedConfig.tob_lines || []).length,
      min_price: currentMidpoint ? (currentMidpoint * 0.95).toFixed(6) : '',
      max_price: currentMidpoint ? (currentMidpoint * 1.05).toFixed(6) : '',
      hourly_quantity: '100',
      quantity_currency: 'base',
      sides: 'both',
      spread_from_inventory: true,
      spread_bps: '50',
      drift_bps: '30',
      timeout_seconds: 30,
      coefficient_method: 'inventory',
      min_coefficient: '0.1',
      max_coefficient: '2.0'
    };
    
    onConfigChange({ 
      ...normalizedConfig, 
      tob_lines: [...(normalizedConfig.tob_lines || []), newTOBLine] 
    });
  };

  const addPassiveLine = () => {
    const newPassiveLine = {
      line_id: (normalizedConfig.passive_lines || []).length,
      mid_spread_bps: '20',
      quantity: '100',
      sides: 'both',
      spread_coefficient_method: 'inventory',
      quantity_coefficient_method: 'volume',
      min_spread_bps: '10',
      max_spread_bps: '200',
      drift_bps: '20',
      timeout_seconds: 60,
      randomization_factor: '0.05'
    };
    
    onConfigChange({ 
      ...normalizedConfig, 
      passive_lines: [...(normalizedConfig.passive_lines || []), newPassiveLine] 
    });
  };

  const removeTOBLine = (index) => {
    const newTOBLines = normalizedConfig.tob_lines.filter((_, i) => i !== index);
    onConfigChange({ ...normalizedConfig, tob_lines: newTOBLines });
  };

  const removePassiveLine = (index) => {
    const newPassiveLines = normalizedConfig.passive_lines.filter((_, i) => i !== index);
    onConfigChange({ ...normalizedConfig, passive_lines: newPassiveLines });
  };

  const handleTimePeriodToggle = (period) => {
    const currentPeriods = normalizedConfig.time_periods || [];
    if (currentPeriods.includes(period)) {
      handleConfigChange('time_periods', currentPeriods.filter(p => p !== period));
    } else {
      handleConfigChange('time_periods', [...currentPeriods, period]);
    }
  };

  const toggleSection = (section) => {
    const newExpanded = new Set(expandedSections);
    if (newExpanded.has(section)) {
      newExpanded.delete(section);
    } else {
      newExpanded.add(section);
    }
    setExpandedSections(newExpanded);
  };

  const copyConfig = () => {
    navigator.clipboard.writeText(JSON.stringify(normalizedConfig, null, 2));
    alert('Configuration copied to clipboard!');
  };

  // Available time periods
  const timePeriods = [
    { value: '30s', label: '30 Seconds', group: 'short' },
    { value: '1min', label: '1 Minute', group: 'short' },
    { value: '5min', label: '5 Minutes', group: 'short' },
    { value: '15min', label: '15 Minutes', group: 'short' },
    { value: '30min', label: '30 Minutes', group: 'short' },
    { value: '60min', label: '1 Hour', group: 'short' },
    { value: '240min', label: '4 Hours', group: 'medium' },
    { value: '480min', label: '8 Hours', group: 'medium' },
    { value: '720min', label: '12 Hours', group: 'medium' },
    { value: '1440min', label: '24 Hours', group: 'medium' },
    { value: '2880min', label: '2 Days', group: 'long' },
    { value: '10080min', label: '7 Days', group: 'long' }
  ];

  const selectedTimePeriods = normalizedConfig.time_periods || [];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-white flex items-center gap-2">
            <Layers className="h-6 w-6 text-purple-500" />
            Stacked Market Making Configuration
          </h2>
          <p className="text-gray-400">Dual-line strategy: TOB (local pricing) + Passive (aggregated pricing)</p>
          {currentMidpoint && (
            <p className="text-sm text-blue-400">
              Current {symbol} price: <span className="font-bold text-white">${currentMidpoint}</span>
            </p>
          )}
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

      {/* Global Configuration */}
      <div className="bg-gray-800 rounded-lg p-6">
        <div 
          className="flex items-center justify-between cursor-pointer"
          onClick={() => toggleSection('global')}
        >
          <h3 className="text-lg font-semibold text-white flex items-center gap-2">
            <Target className="h-5 w-5 text-blue-400" />
            Global Configuration
          </h3>
          {expandedSections.has('global') ? 
            <ChevronDown className="h-5 w-5 text-gray-400" /> : 
            <ChevronRight className="h-5 w-5 text-gray-400" />
          }
        </div>
        
        {expandedSections.has('global') && (
          <div className="mt-4 space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <label className="block text-gray-400 text-sm mb-2">Base Coin</label>
                <input
                  type="text"
                  value={normalizedConfig.base_coin || ''}
                  onChange={(e) => handleConfigChange('base_coin', e.target.value.toUpperCase())}
                  className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
                  placeholder="e.g. BERA"
                />
              </div>
              
              <div>
                <label className="block text-gray-400 text-sm mb-2">Quote Currencies</label>
                <input
                  type="text"
                  value={normalizedConfig.quote_currencies?.join(', ') || 'USDT'}
                  onChange={(e) => handleConfigChange('quote_currencies', e.target.value.split(',').map(s => s.trim().toUpperCase()))}
                  className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
                  placeholder="USDT, BTC, ETH"
                />
                <p className="text-xs text-gray-500 mt-1">Comma-separated list</p>
              </div>
              
              <div>
                <label className="block text-gray-400 text-sm mb-2">Leverage</label>
                <input
                  type="number"
                  value={normalizedConfig.leverage || '1.0'}
                  onChange={(e) => handleConfigChange('leverage', e.target.value)}
                  className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
                  min="0.1"
                  step="0.1"
                />
              </div>
            </div>
            
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="flex items-center gap-2 text-gray-400 text-sm">
                  <input
                    type="checkbox"
                    checked={normalizedConfig.taker_check || false}
                    onChange={(e) => handleConfigChange('taker_check', e.target.checked)}
                    className="rounded border-gray-600"
                  />
                  Enable Taker Check
                </label>
                <p className="text-xs text-gray-500 mt-1">Prevents quotes from crossing the spread</p>
              </div>
              
              <div>
                <label className="flex items-center gap-2 text-gray-400 text-sm">
                  <input
                    type="checkbox"
                    checked={normalizedConfig.hedging_enabled || false}
                    onChange={(e) => handleConfigChange('hedging_enabled', e.target.checked)}
                    className="rounded border-gray-600"
                  />
                  Enable Cross-Currency Hedging
                </label>
                <p className="text-xs text-gray-500 mt-1">Auto-hedge non-USDT pairs to USDT</p>
              </div>
            </div>

            {/* Hedging Targets Editor */}
            <div className="mt-6">
              <h4 className="text-white font-medium mb-2">Hedging Targets</h4>
              <p className="text-xs text-gray-500 mb-3">
                Configure target inventory per quote currency for hedging on the same exchange. USDT is never hedged.
              </p>

              {Object.keys(normalizedConfig.hedging_targets || {}).length === 0 ? (
                <div className="text-gray-400 text-sm mb-2">No hedging targets configured.</div>
              ) : (
                <div className="space-y-2">
                  {Object.entries(normalizedConfig.hedging_targets || {}).map(([currency, target]) => (
                    <div key={currency} className="grid grid-cols-1 md:grid-cols-3 gap-3 items-center bg-gray-700 rounded p-3">
                      <div>
                        <label className="block text-gray-400 text-xs mb-1">Currency</label>
                        <input
                          type="text"
                          value={currency}
                          readOnly
                          className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                        />
                      </div>
                      <div>
                        <label className="block text-gray-400 text-xs mb-1">Target Amount</label>
                        <input
                          type="number"
                          value={target}
                          onChange={(e) => handleHedgingTargetChange(currency, e.target.value)}
                          className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                          step="0.00000001"
                        />
                      </div>
                      <div className="flex items-end">
                        <button
                          type="button"
                          onClick={() => handleHedgingTargetRemove(currency)}
                          className="px-3 py-1 bg-red-600 hover:bg-red-700 text-white rounded text-sm"
                        >
                          Remove
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              )}

              <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mt-3">
                <div>
                  <label className="block text-gray-400 text-xs mb-1">New Currency</label>
                  <input
                    type="text"
                    value={newHedgeCurrency}
                    onChange={(e) => setNewHedgeCurrency(e.target.value.toUpperCase())}
                    placeholder="e.g. BTC"
                    className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                  />
                </div>
                <div>
                  <label className="block text-gray-400 text-xs mb-1">Target Amount</label>
                  <input
                    type="number"
                    value={newHedgeTarget}
                    onChange={(e) => setNewHedgeTarget(e.target.value)}
                    placeholder="e.g. 0.0"
                    step="0.00000001"
                    className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                  />
                </div>
                <div className="flex items-end">
                  <button
                    type="button"
                    onClick={handleHedgingTargetAdd}
                    className="px-3 py-1 bg-blue-600 hover:bg-blue-700 text-white rounded text-sm"
                  >
                    Add Hedging Target
                  </button>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Inventory Management */}
      <div className="bg-gray-800 rounded-lg p-6">
        <div 
          className="flex items-center justify-between cursor-pointer"
          onClick={() => toggleSection('inventory')}
        >
          <h3 className="text-lg font-semibold text-white flex items-center gap-2">
            <BarChart3 className="h-5 w-5 text-yellow-400" />
            Inventory Management
          </h3>
          {expandedSections.has('inventory') ? 
            <ChevronDown className="h-5 w-5 text-gray-400" /> : 
            <ChevronRight className="h-5 w-5 text-gray-400" />
          }
        </div>
        
        {expandedSections.has('inventory') && (
          <div className="mt-4 space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="block text-gray-400 text-sm mb-2">Target Inventory</label>
                <input
                  type="number"
                  value={normalizedConfig.inventory?.target_inventory || ''}
                  onChange={(e) => handleInventoryChange('target_inventory', e.target.value)}
                  className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
                  placeholder="e.g. 300000"
                  step="0.01"
                />
                <p className="text-xs text-gray-500 mt-1">Target inventory amount in base currency</p>
              </div>
              
              <div>
                <label className="block text-gray-400 text-sm mb-2">Max Inventory Deviation</label>
                <input
                  type="number"
                  value={normalizedConfig.inventory?.max_inventory_deviation || ''}
                  onChange={(e) => handleInventoryChange('max_inventory_deviation', e.target.value)}
                  className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
                  placeholder="e.g. 50000"
                  step="0.01"
                />
                <p className="text-xs text-gray-500 mt-1">Maximum allowed deviation from target</p>
              </div>
              
              <div>
                <label className="block text-gray-400 text-sm mb-2">Inventory Price Method</label>
                <select
                  value={normalizedConfig.inventory?.inventory_price_method || 'accounting'}
                  onChange={(e) => handleInventoryChange('inventory_price_method', e.target.value)}
                  className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
                >
                  <option value="accounting">Accounting (Weighted Average)</option>
                  <option value="manual">Manual (Fixed Price)</option>
                  <option value="mark_to_market">Mark-to-Market (Current Price)</option>
                </select>
              </div>
              
              {normalizedConfig.inventory?.inventory_price_method === 'manual' && (
                <div>
                  <label className="block text-gray-400 text-sm mb-2">Manual Inventory Price</label>
                  <input
                    type="number"
                    value={normalizedConfig.inventory?.manual_inventory_price || ''}
                    onChange={(e) => handleInventoryChange('manual_inventory_price', e.target.value)}
                    className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
                    placeholder="e.g. 2.50"
                    step="0.000001"
                  />
                </div>
              )}
              
              <div>
                <label className="block text-gray-400 text-sm mb-2">Start Time (UTC)</label>
                <input
                  type="datetime-local"
                  value={normalizedConfig.inventory?.start_time || ''}
                  onChange={(e) => handleInventoryChange('start_time', e.target.value)}
                  className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
                />
              </div>
            </div>
            
            <div className="bg-blue-600/20 border border-blue-500/30 rounded p-3">
              <p className="text-blue-300 text-sm">
                <strong>Inventory Coefficient:</strong> Calculated as (excess_inventory - target_inventory) / target_inventory<br/>
                Range: -1 (max short) → 0 (at target) → +1 (max long)
              </p>
            </div>
          </div>
        )}
      </div>

      {/* TOB Lines */}
      <div className="bg-gray-800 rounded-lg p-6">
        <div 
          className="flex items-center justify-between cursor-pointer"
          onClick={() => toggleSection('tob')}
        >
          <h3 className="text-lg font-semibold text-white flex items-center gap-2">
            <Target className="h-5 w-5 text-green-400" />
            TOB Lines ({(normalizedConfig.tob_lines || []).length}) - Local Exchange Pricing
          </h3>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={(e) => { e.stopPropagation(); addTOBLine(); }}
              className="flex items-center gap-2 bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded text-sm"
            >
              <Plus className="h-4 w-4" />
              Add TOB Line
            </button>
            {expandedSections.has('tob') ? 
              <ChevronDown className="h-5 w-5 text-gray-400" /> : 
              <ChevronRight className="h-5 w-5 text-gray-400" />
            }
          </div>
        </div>
        
        {expandedSections.has('tob') && (
          <div className="mt-4 space-y-4">
            {(normalizedConfig.tob_lines || []).map((line, index) => (
              <div key={line.line_id || `tob-${index}`} className="bg-gray-700 rounded p-4 space-y-3">
                <div className="flex justify-between items-center">
                  <h4 className="text-white font-medium">TOB Line {index + 1}</h4>
                  <button
                    onClick={() => removeTOBLine(index)}
                    className="text-red-400 hover:text-red-300"
                  >
                    <Minus className="h-4 w-4" />
                  </button>
                </div>

                <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
                  <div>
                    <label className="block text-gray-400 text-xs mb-1">Min Price</label>
                    <input
                      type="number"
                      value={line.min_price || ''}
                      onChange={(e) => handleTOBLineChange(index, 'min_price', e.target.value)}
                      className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                      step="0.000001"
                      placeholder="Zone min"
                    />
                  </div>
                  
                  <div>
                    <label className="block text-gray-400 text-xs mb-1">Max Price</label>
                    <input
                      type="number"
                      value={line.max_price || ''}
                      onChange={(e) => handleTOBLineChange(index, 'max_price', e.target.value)}
                      className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                      step="0.000001"
                      placeholder="Zone max"
                    />
                  </div>
                  
                  <div>
                    <label className="block text-gray-400 text-xs mb-1">Hourly Qty</label>
                    <input
                      type="number"
                      value={line.hourly_quantity || ''}
                      onChange={(e) => handleTOBLineChange(index, 'hourly_quantity', e.target.value)}
                      className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                      step="0.001"
                    />
                  </div>
                  
                  <div>
                    <label className="block text-gray-400 text-xs mb-1">Qty Currency</label>
                    <select
                      value={line.quantity_currency || 'base'}
                      onChange={(e) => handleTOBLineChange(index, 'quantity_currency', e.target.value)}
                      className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                    >
                      <option value="base">Base</option>
                      <option value="quote">Quote</option>
                    </select>
                  </div>
                  
                  <div>
                    <label className="block text-gray-400 text-xs mb-1">Sides</label>
                    <select
                      value={line.sides || 'both'}
                      onChange={(e) => handleTOBLineChange(index, 'sides', e.target.value)}
                      className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                    >
                      <option value="both">Both</option>
                      <option value="bid">Bid Only</option>
                      <option value="offer">Offer Only</option>
                    </select>
                  </div>
                  
                  <div>
                    <label className="block text-gray-400 text-xs mb-1">Spread (bps)</label>
                    <input
                      type="number"
                      value={line.spread_bps || ''}
                      onChange={(e) => handleTOBLineChange(index, 'spread_bps', e.target.value)}
                      className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                      step="0.1"
                    />
                  </div>
                </div>

                {showAdvanced && (
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mt-3 pt-3 border-t border-gray-600">
                    <div>
                      <label className="flex items-center gap-1 text-gray-400 text-xs mb-1">
                        <input
                          type="checkbox"
                          checked={line.spread_from_inventory || false}
                          onChange={(e) => handleTOBLineChange(index, 'spread_from_inventory', e.target.checked)}
                          className="rounded border-gray-600"
                        />
                        Spread from Inventory
                      </label>
                    </div>
                    
                    <div>
                      <label className="block text-gray-400 text-xs mb-1">Drift (bps)</label>
                      <input
                        type="number"
                        value={line.drift_bps || ''}
                        onChange={(e) => handleTOBLineChange(index, 'drift_bps', e.target.value)}
                        className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 text-sm"
                        step="0.1"
                      />
                    </div>
                    
                    <div>
                      <label className="block text-gray-400 text-xs mb-1">Timeout (sec)</label>
                      <input
                        type="number"
                        value={line.timeout_seconds || ''}
                        onChange={(e) => handleTOBLineChange(index, 'timeout_seconds', parseInt(e.target.value) || 0)}
                        className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 text-sm"
                        min="1"
                      />
                    </div>
                    
                    <div>
                      <label className="block text-gray-400 text-xs mb-1">Coefficient Method</label>
                      <select
                        value={line.coefficient_method || 'inventory'}
                        onChange={(e) => handleTOBLineChange(index, 'coefficient_method', e.target.value)}
                        className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 text-sm"
                      >
                        <option value="inventory">Inventory</option>
                        <option value="volume">Volume</option>
                        <option value="both">Both</option>
                      </select>
                    </div>
                  </div>
                )}
              </div>
            ))}
            
            {(normalizedConfig.tob_lines || []).length === 0 && (
              <div className="text-center py-8 text-gray-400">
                <Target className="h-12 w-12 mx-auto mb-2 opacity-50" />
                <p>No TOB lines configured. Click "Add TOB Line" to create local exchange quotes.</p>
                <p className="text-xs mt-1">TOB lines quote at local exchange top-of-book (bid + min tick, ask - min tick)</p>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Passive Lines */}
      <div className="bg-gray-800 rounded-lg p-6">
        <div 
          className="flex items-center justify-between cursor-pointer"
          onClick={() => toggleSection('passive')}
        >
          <h3 className="text-lg font-semibold text-white flex items-center gap-2">
            <BookOpen className="h-5 w-5 text-purple-400" />
            Passive Lines ({(normalizedConfig.passive_lines || []).length}) - Aggregated Book Pricing
          </h3>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={(e) => { e.stopPropagation(); addPassiveLine(); }}
              className="flex items-center gap-2 bg-purple-600 hover:bg-purple-700 text-white px-3 py-1 rounded text-sm"
            >
              <Plus className="h-4 w-4" />
              Add Passive Line
            </button>
            {expandedSections.has('passive') ? 
              <ChevronDown className="h-5 w-5 text-gray-400" /> : 
              <ChevronRight className="h-5 w-5 text-gray-400" />
            }
          </div>
        </div>
        
        {expandedSections.has('passive') && (
          <div className="mt-4 space-y-4">
            {(normalizedConfig.passive_lines || []).map((line, index) => (
              <div key={line.line_id || `passive-${index}`} className="bg-gray-700 rounded p-4 space-y-3">
                <div className="flex justify-between items-center">
                  <h4 className="text-white font-medium">Passive Line {index + 1}</h4>
                  <button
                    onClick={() => removePassiveLine(index)}
                    className="text-red-400 hover:text-red-300"
                  >
                    <Minus className="h-4 w-4" />
                  </button>
                </div>

                <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
                  <div>
                    <label className="block text-gray-400 text-xs mb-1">Mid Spread (bps)</label>
                    <input
                      type="number"
                      value={line.mid_spread_bps || ''}
                      onChange={(e) => handlePassiveLineChange(index, 'mid_spread_bps', e.target.value)}
                      className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                      step="0.1"
                    />
                  </div>
                  
                  <div>
                    <label className="block text-gray-400 text-xs mb-1">Quantity</label>
                    <input
                      type="number"
                      value={line.quantity || ''}
                      onChange={(e) => handlePassiveLineChange(index, 'quantity', e.target.value)}
                      className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                      step="0.001"
                    />
                  </div>
                  
                  <div>
                    <label className="block text-gray-400 text-xs mb-1">Sides</label>
                    <select
                      value={line.sides || 'both'}
                      onChange={(e) => handlePassiveLineChange(index, 'sides', e.target.value)}
                      className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                    >
                      <option value="both">Both</option>
                      <option value="bid">Bid Only</option>
                      <option value="offer">Offer Only</option>
                    </select>
                  </div>
                  
                  <div>
                    <label className="block text-gray-400 text-xs mb-1">Min Spread (bps)</label>
                    <input
                      type="number"
                      value={line.min_spread_bps || ''}
                      onChange={(e) => handlePassiveLineChange(index, 'min_spread_bps', e.target.value)}
                      className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                      step="0.1"
                    />
                  </div>
                  
                  <div>
                    <label className="block text-gray-400 text-xs mb-1">Max Spread (bps)</label>
                    <input
                      type="number"
                      value={line.max_spread_bps || ''}
                      onChange={(e) => handlePassiveLineChange(index, 'max_spread_bps', e.target.value)}
                      className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                      step="0.1"
                    />
                  </div>
                  
                  <div>
                    <label className="block text-gray-400 text-xs mb-1">Randomization</label>
                    <input
                      type="number"
                      value={line.randomization_factor || ''}
                      onChange={(e) => handlePassiveLineChange(index, 'randomization_factor', e.target.value)}
                      className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                      step="0.01"
                      min="0"
                      max="1"
                    />
                  </div>
                </div>

                {showAdvanced && (
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mt-3 pt-3 border-t border-gray-600">
                    <div>
                      <label className="block text-gray-400 text-xs mb-1">Spread Coeff Method</label>
                      <select
                        value={line.spread_coefficient_method || 'inventory'}
                        onChange={(e) => handlePassiveLineChange(index, 'spread_coefficient_method', e.target.value)}
                        className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 text-sm"
                      >
                        <option value="inventory">Inventory</option>
                        <option value="volume">Volume</option>
                        <option value="both">Both</option>
                        <option value="none">None</option>
                      </select>
                    </div>
                    
                    <div>
                      <label className="block text-gray-400 text-xs mb-1">Qty Coeff Method</label>
                      <select
                        value={line.quantity_coefficient_method || 'volume'}
                        onChange={(e) => handlePassiveLineChange(index, 'quantity_coefficient_method', e.target.value)}
                        className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 text-sm"
                      >
                        <option value="inventory">Inventory</option>
                        <option value="volume">Volume</option>
                        <option value="both">Both</option>
                        <option value="none">None</option>
                      </select>
                    </div>
                    
                    <div>
                      <label className="block text-gray-400 text-xs mb-1">Drift (bps)</label>
                      <input
                        type="number"
                        value={line.drift_bps || ''}
                        onChange={(e) => handlePassiveLineChange(index, 'drift_bps', e.target.value)}
                        className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 text-sm"
                        step="0.1"
                      />
                    </div>
                    
                    <div>
                      <label className="block text-gray-400 text-xs mb-1">Timeout (sec)</label>
                      <input
                        type="number"
                        value={line.timeout_seconds || ''}
                        onChange={(e) => handlePassiveLineChange(index, 'timeout_seconds', parseInt(e.target.value) || 0)}
                        className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 text-sm"
                        min="1"
                      />
                    </div>
                  </div>
                )}
              </div>
            ))}
            
            {(normalizedConfig.passive_lines || []).length === 0 && (
              <div className="text-center py-8 text-gray-400">
                <BookOpen className="h-12 w-12 mx-auto mb-2 opacity-50" />
                <p>No passive lines configured. Click "Add Passive Line" to create aggregated book quotes.</p>
                <p className="text-xs mt-1">Passive lines quote based on combined orderbook from all exchanges</p>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Volume Coefficient System */}
      <div className="bg-gray-800 rounded-lg p-6">
        <div 
          className="flex items-center justify-between cursor-pointer"
          onClick={() => toggleSection('volume')}
        >
          <h3 className="text-lg font-semibold text-white flex items-center gap-2">
            <Zap className="h-5 w-5 text-orange-400" />
            Volume Coefficient System (Proven Technology)
          </h3>
          {expandedSections.has('volume') ? 
            <ChevronDown className="h-5 w-5 text-gray-400" /> : 
            <ChevronRight className="h-5 w-5 text-gray-400" />
          }
        </div>
        
        {expandedSections.has('volume') && (
          <div className="mt-4 space-y-4">
            <div className="bg-green-600/20 border border-green-500/30 rounded p-3">
              <p className="text-green-300 text-sm">
                <strong>Volume Coefficient System:</strong> Uses the same proven technology as volume_weighted_top_of_book.py<br/>
                Analyzes real-time trade volume across moving average windows to calculate dynamic execution coefficients.
              </p>
            </div>
            
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <label className="block text-gray-400 text-sm mb-2">Coefficient Method</label>
                <select
                  value={normalizedConfig.coefficient_method || 'min'}
                  onChange={(e) => handleConfigChange('coefficient_method', e.target.value)}
                  className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
                >
                  <option value="min">Minimum (Conservative)</option>
                  <option value="mid">Midpoint (Balanced)</option>
                  <option value="max">Maximum (Aggressive)</option>
                </select>
              </div>
              
              <div>
                <label className="block text-gray-400 text-sm mb-2">Min Coefficient</label>
                <input
                  type="number"
                  value={normalizedConfig.min_coefficient || 0.2}
                  onChange={(e) => handleConfigChange('min_coefficient', parseFloat(e.target.value) || 0.2)}
                  className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
                  min="0.1"
                  max="1"
                  step="0.1"
                />
              </div>
              
              <div>
                <label className="block text-gray-400 text-sm mb-2">Max Coefficient</label>
                <input
                  type="number"
                  value={normalizedConfig.max_coefficient || 3.0}
                  onChange={(e) => handleConfigChange('max_coefficient', parseFloat(e.target.value) || 3.0)}
                  className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
                  min="1"
                  max="10"
                  step="0.1"
                />
              </div>
            </div>
            
            {/* Time Periods */}
            <div>
              <label className="block text-gray-400 text-sm mb-2">Moving Average Time Periods</label>
              <div className="space-y-3">
                {['short', 'medium', 'long'].map(group => (
                  <div key={group}>
                    <h4 className="text-gray-300 text-sm font-semibold mb-2 capitalize">
                      {group}-term {group === 'short' && '(≤ 1 Hour)'}
                      {group === 'medium' && '(4-24 Hours)'}
                      {group === 'long' && '(Days)'}
                    </h4>
                    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
                      {timePeriods.filter(p => p.group === group).map(period => (
                        <label key={period.value} className="flex items-center">
                          <input
                            type="checkbox"
                            checked={selectedTimePeriods.includes(period.value)}
                            onChange={() => handleTimePeriodToggle(period.value)}
                            className="mr-2 h-4 w-4 text-blue-600 bg-gray-600 border-gray-500 rounded focus:ring-blue-500"
                          />
                          <span className="text-white text-sm">{period.label}</span>
                        </label>
                      ))}
                    </div>
                  </div>
                ))}
                
                <div className="mt-3 space-y-2">
                  <div className="text-sm text-gray-400">
                    Selected: {selectedTimePeriods.length} periods
                  </div>
                  {selectedTimePeriods.some(p => parseInt(p) > 1440) && (
                    <div className="bg-yellow-900/20 border border-yellow-600 rounded p-2 text-yellow-400 text-xs">
                      <p className="font-semibold mb-1">⚠️ Note on Long-term Periods:</p>
                      <p>Daily periods (2-30 days) require significant historical data to calculate accurate moving averages.</p>
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Smart Pricing Configuration */}
      {showAdvanced && (
        <div className="bg-gray-800 rounded-lg p-6">
          <div 
            className="flex items-center justify-between cursor-pointer"
            onClick={() => toggleSection('pricing')}
          >
            <h3 className="text-lg font-semibold text-white flex items-center gap-2">
              <Calculator className="h-5 w-5 text-cyan-400" />
              Smart Pricing Engine
            </h3>
            {expandedSections.has('pricing') ? 
              <ChevronDown className="h-5 w-5 text-gray-400" /> : 
              <ChevronRight className="h-5 w-5 text-gray-400" />
            }
          </div>
          
          {expandedSections.has('pricing') && (
            <div className="mt-4 space-y-4">
              <div>
                <label className="block text-gray-400 text-sm mb-2">Smart Pricing Source</label>
                <select
                  value={normalizedConfig.smart_pricing_source || 'aggregated'}
                  onChange={(e) => handleConfigChange('smart_pricing_source', e.target.value)}
                  className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
                >
                  <option value="aggregated">Aggregated (Combined book from all exchanges)</option>
                  <option value="component_venues">Component Venues (Selected venues only)</option>
                  <option value="liquidity_weighted">Liquidity Weighted (Honor per-venue depth share)</option>
                  <option value="single_venue">Single Venue (Specific exchange only)</option>
                </select>
                <p className="text-xs text-gray-500 mt-1">
                  How passive lines determine reference pricing from orderbook data
                </p>
              </div>
              
              <div className="bg-cyan-600/20 border border-cyan-500/30 rounded p-3">
                <p className="text-cyan-300 text-sm">
                  <strong>Smart Pricing:</strong> Passive lines use this setting to choose between aggregated pricing, 
                  component venue pricing, liquidity-weighted pricing, or single venue pricing for their reference mid price.
                </p>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Configuration Summary */}
      <div className="bg-gray-700 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-white mb-3 flex items-center gap-2">
          <Eye className="h-5 w-5" />
          Configuration Summary
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 text-sm">
          <div>
            <span className="text-gray-400">Strategy:</span>
            <span className="text-white ml-2">Stacked Market Making</span>
          </div>
          <div>
            <span className="text-gray-400">Base Coin:</span>
            <span className="text-white ml-2">{normalizedConfig.base_coin || 'Not set'}</span>
          </div>
          <div>
            <span className="text-gray-400">Quote Currencies:</span>
            <span className="text-white ml-2">{normalizedConfig.quote_currencies?.join(', ') || 'USDT'}</span>
          </div>
          <div>
            <span className="text-gray-400">TOB Lines:</span>
            <span className="text-white ml-2">{(normalizedConfig.tob_lines || []).length}</span>
          </div>
          <div>
            <span className="text-gray-400">Passive Lines:</span>
            <span className="text-white ml-2">{(normalizedConfig.passive_lines || []).length}</span>
          </div>
          <div>
            <span className="text-gray-400">Target Inventory:</span>
            <span className="text-white ml-2">{normalizedConfig.inventory?.target_inventory || 'Not set'}</span>
          </div>
          <div>
            <span className="text-gray-400">Volume Periods:</span>
            <span className="text-white ml-2">{selectedTimePeriods.length} selected</span>
          </div>
          <div>
            <span className="text-gray-400">Smart Pricing:</span>
            <span className="text-white ml-2">{normalizedConfig.smart_pricing_source || 'aggregated'}</span>
          </div>
          <div>
            <span className="text-gray-400">Hedging:</span>
            <span className="text-white ml-2">{normalizedConfig.hedging_enabled ? 'Enabled' : 'Disabled'}</span>
          </div>
        </div>
        
        <div className="mt-4 pt-4 border-t border-gray-600">
          <div className="bg-purple-600/20 border border-purple-500/30 rounded p-3">
            <p className="text-purple-300 text-sm">
              <strong>Stacked Strategy:</strong> Runs {(normalizedConfig.tob_lines || []).length} TOB lines (local exchange pricing) 
              + {(normalizedConfig.passive_lines || []).length} passive lines (aggregated pricing) simultaneously with 
              inventory-based and volume-based coefficient adjustments.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default StackedMarketMakingConfig;
