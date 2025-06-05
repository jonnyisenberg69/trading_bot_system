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
  onCreateBot, 
  onQuickUpdate, 
  onRefresh
}) => {
  const [config, setConfig] = useState({
    base_coin: 'BTC',
    quantity_currency: 'base',
    exchanges: [],
    lines: [
      {
        timeout: 300,
        drift: 50,
        quantity: 0.01,
        quantity_randomization_factor: 10,
        spread: 25,
        sides: 'both'
      }
    ]
  });

  const [presets, setPresets] = useState({});
  const [selectedPreset, setSelectedPreset] = useState('');
  const [loading, setLoading] = useState(false);
  const [symbol, setSymbol] = useState('BTC/USDT');
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [quickUpdateBot, setQuickUpdateBot] = useState('');
  const [exchanges, setExchanges] = useState([]);
  const [existingBots, setExistingBots] = useState([]);

  const availableCoins = ['BTC', 'ETH', 'BNB', 'ADA', 'SOL', 'DOT', 'MATIC', 'AVAX'];
  const popularSymbols = [
    'BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'ADA/USDT', 
    'SOL/USDT', 'DOT/USDT', 'MATIC/USDT', 'AVAX/USDT'
  ];

  useEffect(() => {
    loadInitialData();
  }, []);

  const loadInitialData = async () => {
    await Promise.all([
      loadPresets(),
      loadDefaultConfig(),
      loadExchanges(),
      loadBots()
    ]);
  };

  const loadPresets = async () => {
    try {
      const response = await api.strategies.passiveQuoting.getPresets();
      setPresets(response.data.presets);
    } catch (error) {
      console.error('Failed to load presets:', error);
    }
  };

  const loadDefaultConfig = async () => {
    try {
      const response = await api.strategies.passiveQuoting.getConfig();
      if (response.data.default_config) {
        setConfig(response.data.default_config);
      }
    } catch (error) {
      console.error('Failed to load default config:', error);
    }
  };

  const loadExchanges = async () => {
    try {
      const response = await api.exchanges.getConnected();
      setExchanges(response.data.connections);
    } catch (error) {
      console.error('Failed to load exchanges:', error);
    }
  };

  const loadBots = async () => {
    try {
      const response = await api.bots.getAll();
      setExistingBots(response.data.instances);
    } catch (error) {
      console.error('Failed to load bots:', error);
    }
  };

  const addLine = () => {
    setConfig(prev => ({
      ...prev,
      lines: [
        ...prev.lines,
        {
          timeout: 300,
          drift: 50,
          quantity: 0.01,
          quantity_randomization_factor: 10,
          spread: 25,
          sides: 'both'
        }
      ]
    }));
  };

  const removeLine = (index) => {
    if (config.lines.length > 1) {
      setConfig(prev => ({
        ...prev,
        lines: prev.lines.filter((_, i) => i !== index)
      }));
    }
  };

  const updateLine = (index, field, value) => {
    setConfig(prev => ({
      ...prev,
      lines: prev.lines.map((line, i) => 
        i === index ? { ...line, [field]: value } : line
      )
    }));
  };

  const loadPreset = (presetName) => {
    const preset = presets[presetName];
    if (preset) {
      setConfig(preset);
      setSelectedPreset(presetName);
    }
  };

  const savePreset = async () => {
    const presetName = prompt('Enter preset name:');
    if (presetName) {
      try {
        await api.strategies.passiveQuoting.savePreset(presetName, config);
        await loadPresets();
        alert('Preset saved successfully!');
      } catch (error) {
        console.error('Failed to save preset:', error);
        alert('Failed to save preset');
      }
    }
  };

  const createBot = async () => {
    try {
      setLoading(true);
      const response = await api.strategies.passiveQuoting.createBot({
        symbol,
        config
      });
      
      alert(`Bot created successfully! Instance ID: ${response.data.instance_id}`);
      if (onCreateBot) onCreateBot(response.data);
      if (onRefresh) onRefresh();
      await loadBots(); // Refresh bot list
    } catch (error) {
      console.error('Failed to create bot:', error);
      alert('Failed to create bot: ' + (error.response?.data?.detail || error.message));
    } finally {
      setLoading(false);
    }
  };

  const quickUpdate = async () => {
    if (!quickUpdateBot) {
      alert('Please select a bot to update');
      return;
    }

    try {
      setLoading(true);
      const response = await api.strategies.passiveQuoting.quickUpdate({
        instance_id: quickUpdateBot,
        config
      });
      
      alert(`Bot updated successfully in ${response.data.update_duration_seconds}s!`);
      if (onQuickUpdate) onQuickUpdate(response.data);
      if (onRefresh) onRefresh();
      await loadBots(); // Refresh bot list
    } catch (error) {
      console.error('Failed to update bot:', error);
      alert('Failed to update bot: ' + (error.response?.data?.detail || error.message));
    } finally {
      setLoading(false);
    }
  };

  const copyConfig = () => {
    navigator.clipboard.writeText(JSON.stringify(config, null, 2));
    alert('Configuration copied to clipboard!');
  };

  const getPassiveQuotingBots = () => {
    return existingBots.filter(bot => bot.strategy === 'passive_quoting');
  };

  return (
    <div className="bg-gray-800 rounded-lg p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-white">Passive Quoting Configuration</h2>
          <p className="text-gray-400">Configure multi-line passive market making strategy</p>
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

      {/* Quick Actions */}
      <div className="bg-gray-700 rounded p-4 space-y-3">
        <h3 className="text-lg font-semibold text-white">Quick Actions</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Quick Update */}
          <div className="space-y-2">
            <label className="block text-gray-400 text-sm">Quick Update Existing Bot</label>
            <div className="flex gap-2">
              <select
                value={quickUpdateBot}
                onChange={(e) => setQuickUpdateBot(e.target.value)}
                className="flex-1 bg-gray-600 text-white px-3 py-2 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
              >
                <option value="">Select bot to update...</option>
                {getPassiveQuotingBots().map(bot => (
                  <option key={bot.instance_id} value={bot.instance_id}>
                    {bot.instance_id} ({bot.status})
                  </option>
                ))}
              </select>
              <button
                onClick={quickUpdate}
                disabled={loading || !quickUpdateBot}
                className="flex items-center gap-1 bg-orange-600 hover:bg-orange-700 disabled:opacity-50 text-white px-3 py-2 rounded text-sm"
              >
                <Zap className="h-4 w-4" />
                Update
              </button>
            </div>
          </div>

          {/* Presets */}
          <div className="space-y-2">
            <label className="block text-gray-400 text-sm">Load Preset</label>
            <div className="flex gap-2">
              <select
                value={selectedPreset}
                onChange={(e) => {
                  setSelectedPreset(e.target.value);
                  if (e.target.value) loadPreset(e.target.value);
                }}
                className="flex-1 bg-gray-600 text-white px-3 py-2 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
              >
                <option value="">Select preset...</option>
                {Object.keys(presets).map(preset => (
                  <option key={preset} value={preset}>
                    {preset}
                  </option>
                ))}
              </select>
              <button
                onClick={savePreset}
                className="flex items-center gap-1 bg-blue-600 hover:bg-blue-700 text-white px-3 py-2 rounded text-sm"
              >
                <Save className="h-4 w-4" />
                Save
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Basic Configuration */}
      <div className="space-y-4">
        <h3 className="text-lg font-semibold text-white">Basic Configuration</h3>
        
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {/* Symbol */}
          <div>
            <label className="block text-gray-400 text-sm mb-2">Trading Symbol</label>
            <select
              value={symbol}
              onChange={(e) => setSymbol(e.target.value)}
              className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            >
              {popularSymbols.map(sym => (
                <option key={sym} value={sym}>{sym}</option>
              ))}
            </select>
          </div>

          {/* Base Coin */}
          <div>
            <label className="block text-gray-400 text-sm mb-2">Base Coin</label>
            <select
              value={config.base_coin}
              onChange={(e) => setConfig(prev => ({ ...prev, base_coin: e.target.value }))}
              className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            >
              {availableCoins.map(coin => (
                <option key={coin} value={coin}>{coin}</option>
              ))}
            </select>
          </div>

          {/* Quantity Currency */}
          <div>
            <label className="block text-gray-400 text-sm mb-2">Quantity Currency</label>
            <select
              value={config.quantity_currency}
              onChange={(e) => setConfig(prev => ({ ...prev, quantity_currency: e.target.value }))}
              className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
            >
              <option value="base">Base (e.g., BTC)</option>
              <option value="quote">Quote (e.g., USDT)</option>
            </select>
          </div>
        </div>

        {/* Exchanges */}
        <div>
          <label className="block text-gray-400 text-sm mb-2">
            Exchanges ({config.exchanges.length} selected)
          </label>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
            {exchanges.map(exchange => (
              <label key={exchange.connection_id} className="flex items-center space-x-2">
                <input
                  type="checkbox"
                  checked={config.exchanges.includes(exchange.name)}
                  onChange={(e) => {
                    if (e.target.checked) {
                      setConfig(prev => ({
                        ...prev,
                        exchanges: [...prev.exchanges, exchange.name]
                      }));
                    } else {
                      setConfig(prev => ({
                        ...prev,
                        exchanges: prev.exchanges.filter(ex => ex !== exchange.name)
                      }));
                    }
                  }}
                  className="rounded"
                />
                <span className="text-white text-sm capitalize">
                  {exchange.name}
                </span>
                {exchange.status === 'connected' ? (
                  <CheckCircle className="h-3 w-3 text-green-500" />
                ) : (
                  <AlertCircle className="h-3 w-3 text-red-500" />
                )}
              </label>
            ))}
          </div>
        </div>
      </div>

      {/* Quote Lines */}
      <div className="space-y-4">
        <div className="flex justify-between items-center">
          <h3 className="text-lg font-semibold text-white">Quote Lines ({config.lines.length})</h3>
          <button
            onClick={addLine}
            className="flex items-center gap-2 bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded text-sm"
          >
            <Plus className="h-4 w-4" />
            Add Line
          </button>
        </div>

        {config.lines.map((line, index) => (
          <div key={index} className="bg-gray-700 rounded p-4 space-y-3">
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
                  onChange={(e) => updateLine(index, 'timeout', parseInt(e.target.value) || 0)}
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
                  onChange={(e) => updateLine(index, 'drift', parseFloat(e.target.value) || 0)}
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
                  onChange={(e) => updateLine(index, 'quantity', parseFloat(e.target.value) || 0)}
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
                  onChange={(e) => updateLine(index, 'quantity_randomization_factor', parseFloat(e.target.value) || 0)}
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
                  onChange={(e) => updateLine(index, 'spread', parseFloat(e.target.value) || 0)}
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
                  onChange={(e) => updateLine(index, 'sides', e.target.value)}
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

      {/* Actions */}
      <div className="flex gap-4 pt-4">
        <button
          onClick={createBot}
          disabled={loading || config.exchanges.length === 0}
          className="flex items-center gap-2 bg-green-600 hover:bg-green-700 disabled:opacity-50 text-white px-6 py-3 rounded-lg font-medium"
        >
          {loading ? (
            <RefreshCw className="h-5 w-5 animate-spin" />
          ) : (
            <Play className="h-5 w-5" />
          )}
          Create Bot
        </button>

        <button
          onClick={() => loadInitialData()}
          className="flex items-center gap-2 bg-gray-600 hover:bg-gray-700 text-white px-6 py-3 rounded-lg"
        >
          <RefreshCw className="h-5 w-5" />
          Reset to Default
        </button>
      </div>

      {/* Configuration Summary */}
      {showAdvanced && (
        <div className="bg-gray-700 rounded p-4">
          <h4 className="text-white font-medium mb-2">Configuration Summary</h4>
          <div className="text-sm text-gray-300 space-y-1">
            <div>Symbol: {symbol}</div>
            <div>Base Coin: {config.base_coin}</div>
            <div>Quantity Currency: {config.quantity_currency}</div>
            <div>Exchanges: {config.exchanges.join(', ') || 'None selected'}</div>
            <div>Quote Lines: {config.lines.length}</div>
            <div>Total Estimated Orders: {config.lines.length * 2} (assuming both sides per line)</div>
          </div>
        </div>
      )}
    </div>
  );
};

export default PassiveQuotingConfig; 