import React, { useState, useEffect } from 'react';
import { 
  Play, 
  Square, 
  Plus, 
  Trash2, 
  RefreshCw, 
  Activity,
  Pause,
  AlertCircle,
  CheckCircle,
  Clock,
  Minus,
  Save,
  Check
} from 'lucide-react';
import { api } from '../services/api';

const BotManager = ({ onRefresh }) => {
  const [bots, setBots] = useState([]);
  const [loading, setLoading] = useState(true);
  const [actionLoading, setActionLoading] = useState({});
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [exchanges, setExchanges] = useState([]);
  const [savedConfigs, setSavedConfigs] = useState({});
  const [configSaved, setConfigSaved] = useState(false);
  const [symbolValidation, setSymbolValidation] = useState({ valid: false, loading: false });
  const [newBot, setNewBot] = useState({
    strategy: 'passive_quoting',
    symbol: '',
    exchanges: [],
    config: {
      base_coin: '',
      quantity_currency: 'base',
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
    }
  });

  const availableStrategies = [
    { value: 'passive_quoting', label: 'Passive Quoting' },
    { value: 'market_making', label: 'Market Making' },
    { value: 'arbitrage', label: 'Arbitrage' },
    { value: 'grid_trading', label: 'Grid Trading' },
    { value: 'dca', label: 'Dollar Cost Averaging' }
  ];

  const popularPairs = [
    'BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'ADA/USDT', 
    'SOL/USDT', 'DOT/USDT', 'MATIC/USDT', 'AVAX/USDT',
    'BERA/USDT', 'APE/USDT', 'LINK/USDT', 'UNI/USDT'
  ];

  useEffect(() => {
    fetchBots();
    fetchExchanges();
    
    // Load saved configs
    const saved = localStorage.getItem('trading_bot_saved_configs');
    if (saved) {
      try {
        setSavedConfigs(JSON.parse(saved));
      } catch (error) {
        console.error('Failed to load saved configs:', error);
      }
    }
  }, []);

  // Utility functions for symbol handling
  const extractBaseCoin = (tradingPair) => {
    if (!tradingPair || !tradingPair.includes('/')) return '';
    return tradingPair.split('/')[0].toUpperCase();
  };

  const validateTradingPair = (pair) => {
    if (!pair) return false;
    const parts = pair.split('/');
    return parts.length === 2 && parts[0].length > 0 && parts[1].length > 0;
  };

  // Debounced symbol validation
  const validateSymbolWithAPI = async (symbol) => {
    if (!symbol || !validateTradingPair(symbol)) {
      setSymbolValidation({ valid: false, loading: false, error: 'Invalid format' });
      return;
    }

    setSymbolValidation({ valid: false, loading: true });

    try {
      // Include selected exchanges for more accurate validation
      const selectedExchanges = newBot.exchanges.length > 0 ? newBot.exchanges : null;
      const response = await api.strategies.validateSymbol(symbol, selectedExchanges);
      
      setSymbolValidation({
        valid: response.data.valid,
        loading: false,
        data: response.data,
        error: response.data.error || null
      });

      // Auto-populate base coin if validation successful
      if (response.data.valid && response.data.base_coin) {
        updateConfig('base_coin', response.data.base_coin);
      }
    } catch (error) {
      console.error('Symbol validation error:', error);
      setSymbolValidation({
        valid: false,
        loading: false,
        error: 'Validation failed'
      });
    }
  };

  // Debounce symbol validation
  const debouncedValidation = (() => {
    let timeoutId;
    return (symbol) => {
      clearTimeout(timeoutId);
      timeoutId = setTimeout(() => validateSymbolWithAPI(symbol), 500);
    };
  })();

  const saveConfig = () => {
    const configKey = `${newBot.strategy}_${newBot.symbol}`;
    const newConfigs = {
      ...savedConfigs,
      [configKey]: {
        symbol: newBot.symbol,
        config: newBot.config,
        saved_at: new Date().toISOString()
      }
    };
    setSavedConfigs(newConfigs);
    localStorage.setItem('trading_bot_saved_configs', JSON.stringify(newConfigs));
    setConfigSaved(true);
    
    // Clear the checkmark after 3 seconds
    setTimeout(() => setConfigSaved(false), 3000);
  };

  const fetchBots = async () => {
    try {
      setLoading(true);
      const response = await api.bots.getAll();
      setBots(response.data.instances);
    } catch (error) {
      console.error('Failed to fetch bots:', error);
    } finally {
      setLoading(false);
    }
  };

  const fetchExchanges = async () => {
    try {
      const response = await api.exchanges.getConnected();
      setExchanges(response.data.connections);
    } catch (error) {
      console.error('Failed to fetch exchanges:', error);
    }
  };

  const handleBotAction = async (botId, action) => {
    try {
      setActionLoading(prev => ({ ...prev, [botId]: action }));
      
      switch (action) {
        case 'start':
          await api.bots.start(botId);
          break;
        case 'stop':
          await api.bots.stop(botId);
          break;
        case 'delete':
          await api.bots.delete(botId);
          break;
        default:
          return;
      }
      
      await fetchBots();
      onRefresh && onRefresh();
    } catch (error) {
      console.error(`Failed to ${action} bot:`, error);
      alert(`Failed to ${action} bot: ${error.response?.data?.detail || error.message}`);
    } finally {
      setActionLoading(prev => ({ ...prev, [botId]: null }));
    }
  };

  const handleCreateBot = async (e) => {
    e.preventDefault();
    
    if (!symbolValidation.valid) {
      alert('Please enter a valid trading pair in format: BASE/QUOTE (e.g., BERA/USDT)');
      return;
    }
    
    if (newBot.exchanges.length === 0) {
      alert('Please select at least one exchange');
      return;
    }

    try {
      if (newBot.strategy === 'passive_quoting') {
        // Use the passive quoting API endpoint
        await api.strategies.passiveQuoting.createBot({
          symbol: newBot.symbol,
          config: newBot.config
        });
      } else {
        // Use the generic bot creation endpoint
      await api.bots.create(newBot);
      }
      
      setShowCreateForm(false);
      setNewBot({
        strategy: 'passive_quoting',
        symbol: '',
        exchanges: [],
        config: {
          base_coin: '',
          quantity_currency: 'base',
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
        }
      });
      setConfigSaved(false);
      setSymbolValidation({ valid: false, loading: false });
      await fetchBots();
      onRefresh && onRefresh();
    } catch (error) {
      console.error('Failed to create bot:', error);
      alert(`Failed to create bot: ${error.response?.data?.detail || error.message}`);
    }
  };

  const handleTradingPairChange = (value) => {
    setNewBot(prev => ({ ...prev, symbol: value }));
    
    // Auto-populate base coin if valid trading pair
    if (validateTradingPair(value)) {
      const baseCoin = extractBaseCoin(value);
      updateConfig('base_coin', baseCoin);
    }
    
    // Trigger API validation
    debouncedValidation(value);
  };

  const addQuoteLine = () => {
    setNewBot(prev => ({
      ...prev,
      config: {
        ...prev.config,
        lines: [
          ...prev.config.lines,
          {
            timeout: 300,
            drift: 50,
            quantity: 0.01,
            quantity_randomization_factor: 10,
            spread: 25,
            sides: 'both'
          }
        ]
      }
    }));
  };

  const removeQuoteLine = (index) => {
    if (newBot.config.lines.length > 1) {
      setNewBot(prev => ({
        ...prev,
        config: {
          ...prev.config,
          lines: prev.config.lines.filter((_, i) => i !== index)
        }
      }));
    }
  };

  const updateQuoteLine = (index, field, value) => {
    setNewBot(prev => ({
      ...prev,
      config: {
        ...prev.config,
        lines: prev.config.lines.map((line, i) =>
          i === index ? { ...line, [field]: value } : line
        )
      }
    }));
  };

  const updateConfig = (field, value) => {
    setNewBot(prev => ({
      ...prev,
      config: {
        ...prev.config,
        [field]: value
      }
    }));
  };

  const getStatusIcon = (status) => {
    switch (status) {
      case 'running':
        return <Activity className="h-4 w-4 text-green-500" />;
      case 'stopped':
        return <Pause className="h-4 w-4 text-gray-500" />;
      case 'starting':
        return <RefreshCw className="h-4 w-4 text-blue-500 animate-spin" />;
      case 'stopping':
        return <RefreshCw className="h-4 w-4 text-orange-500 animate-spin" />;
      case 'error':
        return <AlertCircle className="h-4 w-4 text-red-500" />;
      default:
        return <Clock className="h-4 w-4 text-gray-500" />;
    }
  };

  const getStatusBadge = (status) => {
    const baseClasses = "px-2 py-1 rounded-full text-xs font-medium";
    switch (status) {
      case 'running':
        return `${baseClasses} bg-green-500/20 text-green-400`;
      case 'stopped':
        return `${baseClasses} bg-gray-500/20 text-gray-400`;
      case 'starting':
        return `${baseClasses} bg-blue-500/20 text-blue-400`;
      case 'stopping':
        return `${baseClasses} bg-orange-500/20 text-orange-400`;
      case 'error':
        return `${baseClasses} bg-red-500/20 text-red-400`;
      default:
        return `${baseClasses} bg-gray-500/20 text-gray-400`;
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500"></div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-white">Bot Management</h1>
          <p className="text-gray-400 mt-1">
            Create and manage trading bot instances with different strategies
          </p>
        </div>
        <div className="flex gap-3">
          <button
            onClick={() => setShowCreateForm(true)}
            className="flex items-center gap-2 bg-green-600 hover:bg-green-700 text-white px-4 py-2 rounded-lg transition-colors"
          >
            <Plus className="h-4 w-4" />
            New Bot
          </button>
          <button
            onClick={fetchBots}
            className="flex items-center gap-2 bg-gray-600 hover:bg-gray-700 text-white px-4 py-2 rounded-lg transition-colors"
          >
            <RefreshCw className="h-4 w-4" />
            Refresh
          </button>
        </div>
      </div>

      {/* Bot List */}
      {bots.length > 0 ? (
        <div className="space-y-4">
          {bots.map((bot) => (
            <div key={bot.instance_id} className="bg-gray-800 rounded-lg p-6">
              <div className="flex items-center justify-between">
                {/* Bot Info */}
                <div className="flex items-center gap-4">
                  <div className="bg-gray-700 p-3 rounded-lg">
                    {getStatusIcon(bot.status)}
                  </div>
                  <div>
                    <h3 className="text-lg font-semibold text-white">
                      {bot.strategy.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())}
                    </h3>
                    <p className="text-gray-400">
                      {bot.symbol} on {bot.exchanges.join(', ')}
                    </p>
                    <p className="text-gray-500 text-sm">
                      ID: {bot.instance_id}
                    </p>
                    {bot.strategy === 'passive_quoting' && (
                      <p className="text-gray-500 text-xs">
                        {bot.config?.lines?.length || 0} quote lines
                      </p>
                    )}
                  </div>
                </div>

                {/* Status and Actions */}
                <div className="flex items-center gap-4">
                  <div className="text-right">
                    <span className={getStatusBadge(bot.status)}>
                      {bot.status}
                    </span>
                    {bot.start_time && (
                      <p className="text-gray-400 text-xs mt-1">
                        Started: {new Date(bot.start_time).toLocaleString()}
                      </p>
                    )}
                  </div>

                  {/* Action Buttons */}
                  <div className="flex gap-2">
                    {bot.status === 'stopped' || bot.status === 'error' ? (
                      <button
                        onClick={() => handleBotAction(bot.instance_id, 'start')}
                        disabled={actionLoading[bot.instance_id] === 'start'}
                        className="bg-green-600 hover:bg-green-700 disabled:opacity-50 text-white p-2 rounded transition-colors"
                        title="Start Bot"
                      >
                        {actionLoading[bot.instance_id] === 'start' ? (
                          <RefreshCw className="h-4 w-4 animate-spin" />
                        ) : (
                          <Play className="h-4 w-4" />
                        )}
                      </button>
                    ) : (
                      <button
                        onClick={() => handleBotAction(bot.instance_id, 'stop')}
                        disabled={actionLoading[bot.instance_id] === 'stop'}
                        className="bg-orange-600 hover:bg-orange-700 disabled:opacity-50 text-white p-2 rounded transition-colors"
                        title="Stop Bot"
                      >
                        {actionLoading[bot.instance_id] === 'stop' ? (
                          <RefreshCw className="h-4 w-4 animate-spin" />
                        ) : (
                          <Square className="h-4 w-4" />
                        )}
                      </button>
                    )}

                    <button
                      onClick={() => handleBotAction(bot.instance_id, 'delete')}
                      disabled={actionLoading[bot.instance_id] === 'delete'}
                      className="bg-red-600 hover:bg-red-700 disabled:opacity-50 text-white p-2 rounded transition-colors"
                      title="Delete Bot"
                    >
                      {actionLoading[bot.instance_id] === 'delete' ? (
                        <RefreshCw className="h-4 w-4 animate-spin" />
                      ) : (
                        <Trash2 className="h-4 w-4" />
                      )}
                    </button>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div className="text-center py-12">
          <Activity className="h-16 w-16 text-gray-500 mx-auto mb-4" />
          <h3 className="text-xl font-semibold text-white mb-2">No Bots Created</h3>
          <p className="text-gray-400 mb-4">Create your first trading bot to get started</p>
          <button
            onClick={() => setShowCreateForm(true)}
            className="bg-green-600 hover:bg-green-700 text-white px-6 py-3 rounded-lg transition-colors"
          >
            Create Bot
          </button>
        </div>
      )}

      {/* Create Bot Modal */}
      {showCreateForm && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 overflow-y-auto">
          <div className="bg-gray-800 rounded-lg p-6 w-full max-w-4xl max-h-[90vh] overflow-y-auto m-4">
            <h2 className="text-xl font-semibold text-white mb-4">
              Create New Trading Bot
            </h2>
            
            <form onSubmit={handleCreateBot} className="space-y-6">
              {/* Basic Configuration */}
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              {/* Strategy */}
              <div>
                <label className="block text-gray-400 text-sm mb-2">
                  Trading Strategy
                </label>
                <select
                  value={newBot.strategy}
                  onChange={(e) => setNewBot(prev => ({ ...prev, strategy: e.target.value }))}
                  className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
                  required
                >
                  {availableStrategies.map(strategy => (
                    <option key={strategy.value} value={strategy.value}>
                      {strategy.label}
                    </option>
                  ))}
                </select>
              </div>

                {/* Trading Pair */}
                <div className="relative">
                  <label className="block text-gray-400 text-sm mb-2">
                    Trading Pair
                  </label>
                  <div className="relative">
                    <input
                      type="text"
                      value={newBot.symbol}
                      onChange={(e) => handleTradingPairChange(e.target.value)}
                      className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none pr-10"
                      placeholder="e.g., BERA/USDT"
                      required
                      list="popular-pairs"
                    />
                    <datalist id="popular-pairs">
                      {popularPairs.map(pair => (
                        <option key={pair} value={pair} />
                      ))}
                    </datalist>
                    
                    {/* Validation Status Icon */}
                    <div className="absolute right-2 top-2">
                      {symbolValidation.loading ? (
                        <RefreshCw className="h-5 w-5 text-blue-500 animate-spin" />
                      ) : symbolValidation.valid ? (
                        <Check className="h-5 w-5 text-green-500" />
                      ) : newBot.symbol && symbolValidation.error ? (
                        <AlertCircle className="h-5 w-5 text-red-500" />
                      ) : null}
                    </div>
                  </div>
                  
                  {/* Validation Messages */}
                  {newBot.symbol && !symbolValidation.loading && (
                    <div className="mt-1">
                      {symbolValidation.valid ? (
                        <div className="flex items-center gap-1">
                          <Check className="h-3 w-3 text-green-500" />
                          <p className="text-xs text-green-400">
                            Valid trading pair - {symbolValidation.data?.base_coin}/{symbolValidation.data?.quote_coin}
                          </p>
                        </div>
                      ) : symbolValidation.error ? (
                        <div className="flex items-center gap-1">
                          <AlertCircle className="h-3 w-3 text-red-500" />
                          <p className="text-xs text-red-400">
                            {symbolValidation.error}
                          </p>
                        </div>
                      ) : (
                        <p className="text-xs text-gray-500">
                          Enter any trading pair (BASE/QUOTE format)
                        </p>
                      )}
                    </div>
                  )}

                  {/* Exchange-specific symbols preview */}
                  {symbolValidation.valid && symbolValidation.data?.exchange_symbols && 
                   Object.keys(symbolValidation.data.exchange_symbols).length > 0 && (
                    <div className="mt-2 p-2 bg-gray-700 rounded text-xs">
                      <p className="text-gray-300 mb-1">Exchange formats:</p>
                      {Object.entries(symbolValidation.data.exchange_symbols).map(([exchange, symbol]) => (
                        <div key={exchange} className="flex justify-between">
                          <span className="text-gray-400">{exchange}:</span>
                          <span className="text-white">{symbol}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                {/* Base Coin (for passive quoting) */}
                {newBot.strategy === 'passive_quoting' && (
              <div>
                <label className="block text-gray-400 text-sm mb-2">
                      Base Coin
                      {newBot.config.base_coin && (
                        <span className="text-green-400 ml-1">✓</span>
                      )}
                </label>
                    <input
                      type="text"
                      value={newBot.config.base_coin}
                      onChange={(e) => updateConfig('base_coin', e.target.value.toUpperCase())}
                  className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
                      placeholder="Auto-filled from trading pair"
                      readOnly={validateTradingPair(newBot.symbol)}
                    />
                    <p className="text-xs text-gray-500 mt-1">
                      {validateTradingPair(newBot.symbol) ? 'Auto-populated from trading pair' : 'Will auto-fill when trading pair is entered'}
                    </p>
                  </div>
                )}
              </div>

              {/* Save Configuration */}
              <div className="flex justify-between items-center bg-gray-700 rounded p-3">
                <div>
                  <h4 className="text-white font-medium">Configuration</h4>
                  <p className="text-gray-400 text-sm">Save this configuration for future use</p>
                </div>
                <button
                  type="button"
                  onClick={saveConfig}
                  disabled={!newBot.symbol || !symbolValidation.valid || symbolValidation.loading}
                  className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white px-3 py-2 rounded transition-colors"
                >
                  {configSaved ? (
                    <>
                      <Check className="h-4 w-4" />
                      Saved
                    </>
                  ) : (
                    <>
                      <Save className="h-4 w-4" />
                      Save Config
                    </>
                  )}
                </button>
              </div>

              {/* Passive Quoting Specific Configuration */}
              {newBot.strategy === 'passive_quoting' && (
                <div className="space-y-4">
                  <h3 className="text-lg font-semibold text-white">Passive Quoting Configuration</h3>
                  
                  {/* Quantity Currency */}
                  <div>
                    <label className="block text-gray-400 text-sm mb-2">
                      Quantity Currency
                    </label>
                    <select
                      value={newBot.config.quantity_currency}
                      onChange={(e) => updateConfig('quantity_currency', e.target.value)}
                      className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
                    >
                      <option value="base">Base (e.g., {newBot.config.base_coin || 'BTC'})</option>
                      <option value="quote">Quote (e.g., {newBot.symbol ? newBot.symbol.split('/')[1] : 'USDT'})</option>
                    </select>
                  </div>

                  {/* Quote Lines */}
                  <div>
                    <div className="flex justify-between items-center mb-3">
                      <label className="block text-gray-400 text-sm">
                        Quote Lines ({newBot.config.lines.length})
                      </label>
                      <button
                        type="button"
                        onClick={addQuoteLine}
                        className="flex items-center gap-1 bg-blue-600 hover:bg-blue-700 text-white px-2 py-1 rounded text-sm"
                      >
                        <Plus className="h-3 w-3" />
                        Add Line
                      </button>
                    </div>

                    {newBot.config.lines.map((line, index) => (
                      <div key={index} className="bg-gray-700 rounded p-3 mb-3">
                        <div className="flex justify-between items-center mb-2">
                          <h4 className="text-white font-medium text-sm">Line {index + 1}</h4>
                          {newBot.config.lines.length > 1 && (
                            <button
                              type="button"
                              onClick={() => removeQuoteLine(index)}
                              className="text-red-400 hover:text-red-300"
                            >
                              <Minus className="h-4 w-4" />
                            </button>
                          )}
                        </div>

                        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-2">
                          {/* Timeout */}
                          <div>
                            <label className="block text-gray-400 text-xs mb-1">Timeout (sec)</label>
                            <input
                              type="number"
                              value={line.timeout}
                              onChange={(e) => updateQuoteLine(index, 'timeout', parseInt(e.target.value) || 0)}
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
                              onChange={(e) => updateQuoteLine(index, 'drift', parseFloat(e.target.value) || 0)}
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
                              onChange={(e) => updateQuoteLine(index, 'quantity', parseFloat(e.target.value) || 0)}
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
                              onChange={(e) => updateQuoteLine(index, 'quantity_randomization_factor', parseFloat(e.target.value) || 0)}
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
                              onChange={(e) => updateQuoteLine(index, 'spread', parseFloat(e.target.value) || 0)}
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
                              onChange={(e) => updateQuoteLine(index, 'sides', e.target.value)}
                              className="w-full bg-gray-600 text-white px-2 py-1 rounded border border-gray-500 focus:border-blue-500 focus:outline-none text-sm"
                            >
                              <option value="both">Both</option>
                              <option value="bid">Bid Only</option>
                              <option value="offer">Offer Only</option>
                            </select>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Exchanges */}
              <div>
                <label className="block text-gray-400 text-sm mb-2">
                  Select Exchanges ({newBot.exchanges.length} selected)
                </label>
                <div className="space-y-2 max-h-64 overflow-y-auto">
                  {exchanges.map(exchange => {
                    // Create display name with proper formatting
                    const displayName = `${exchange.name.charAt(0).toUpperCase() + exchange.name.slice(1)} ${exchange.exchange_type.toUpperCase()}`;
                    const isSelected = newBot.exchanges.includes(exchange.connection_id);
                    
                    return (
                    <label key={exchange.connection_id} className="flex items-center">
                      <input
                        type="checkbox"
                          checked={isSelected}
                        onChange={(e) => {
                            let newExchanges;
                          if (e.target.checked) {
                              newExchanges = [...newBot.exchanges, exchange.connection_id];
                          } else {
                              newExchanges = newBot.exchanges.filter(ex => ex !== exchange.connection_id);
                            }
                            
                            setNewBot(prev => ({
                              ...prev,
                              exchanges: newExchanges
                            }));
                            
                            if (newBot.strategy === 'passive_quoting') {
                              updateConfig('exchanges', newExchanges);
                            }
                            
                            // Re-validate symbol with updated exchanges
                            if (newBot.symbol && symbolValidation.valid) {
                              debouncedValidation(newBot.symbol);
                          }
                        }}
                        className="mr-2"
                      />
                        <span className="text-white font-medium">
                          {displayName}
                        </span>
                        <span className="text-gray-400 text-xs ml-2">
                          ({exchange.connection_id})
                      </span>
                      {exchange.status === 'connected' ? (
                        <CheckCircle className="h-4 w-4 text-green-500 ml-2" />
                      ) : (
                        <AlertCircle className="h-4 w-4 text-red-500 ml-2" />
                      )}
                    </label>
                    );
                  })}
                </div>
                {exchanges.length === 0 && (
                  <p className="text-red-400 text-sm">
                    No connected exchanges available. Please configure exchanges first.
                  </p>
                )}
                
                {/* Quick Selection Buttons */}
                {exchanges.length > 0 && (
                  <div className="mt-3 space-y-2">
                    <div className="flex gap-2 flex-wrap">
                      <button
                        type="button"
                        onClick={() => {
                          const allConnected = exchanges.filter(ex => ex.status === 'connected').map(ex => ex.connection_id);
                          setNewBot(prev => ({ ...prev, exchanges: allConnected }));
                          if (newBot.strategy === 'passive_quoting') {
                            updateConfig('exchanges', allConnected);
                          }
                        }}
                        className="text-xs bg-blue-600 hover:bg-blue-700 text-white px-2 py-1 rounded"
                      >
                        Select All Connected
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          const spotExchanges = exchanges.filter(ex => ex.exchange_type === 'spot' && ex.status === 'connected').map(ex => ex.connection_id);
                          setNewBot(prev => ({ ...prev, exchanges: spotExchanges }));
                          if (newBot.strategy === 'passive_quoting') {
                            updateConfig('exchanges', spotExchanges);
                          }
                        }}
                        className="text-xs bg-green-600 hover:bg-green-700 text-white px-2 py-1 rounded"
                      >
                        Spot Only
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          const perpExchanges = exchanges.filter(ex => ex.exchange_type === 'perp' && ex.status === 'connected').map(ex => ex.connection_id);
                          setNewBot(prev => ({ ...prev, exchanges: perpExchanges }));
                          if (newBot.strategy === 'passive_quoting') {
                            updateConfig('exchanges', perpExchanges);
                          }
                        }}
                        className="text-xs bg-purple-600 hover:bg-purple-700 text-white px-2 py-1 rounded"
                      >
                        Perp Only
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          setNewBot(prev => ({ ...prev, exchanges: [] }));
                          if (newBot.strategy === 'passive_quoting') {
                            updateConfig('exchanges', []);
                          }
                        }}
                        className="text-xs bg-gray-600 hover:bg-gray-700 text-white px-2 py-1 rounded"
                      >
                        Clear All
                      </button>
                    </div>
                  </div>
                )}
              </div>

              <div className="flex gap-3 pt-4">
                <button
                  type="submit"
                  disabled={newBot.exchanges.length === 0 || !symbolValidation.valid || symbolValidation.loading}
                  className="flex-1 bg-green-600 hover:bg-green-700 disabled:opacity-50 text-white py-2 rounded transition-colors"
                >
                  {symbolValidation.loading ? 'Validating...' : 'Create Bot'}
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setShowCreateForm(false);
                    setConfigSaved(false);
                    setSymbolValidation({ valid: false, loading: false });
                  }}
                  className="flex-1 bg-gray-600 hover:bg-gray-700 text-white py-2 rounded transition-colors"
                >
                  Cancel
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
};

export default BotManager; 