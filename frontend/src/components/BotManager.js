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
  Check,
  Save,
  Eye,
  Settings,
  Clock
} from 'lucide-react';
import { api } from '../services/api';
import AggressiveTwapConfig from './AggressiveTwapConfig';
import AggressiveTwapV2Config from './AggressiveTwapV2Config';
import BotConfigModal from './BotConfigModal';
import PassiveQuotingConfig from './PassiveQuotingConfig';
import TargetedSellbotConfig from './TargetedSellbotConfig';
import TargetedBuybotConfig from './TargetedBuybotConfig';
import TopOfBookConfig from './TopOfBookConfig';
import VolumeWeightedTopOfBookConfig from './VolumeWeightedTopOfBookConfig';
import VolumeWeightedTopOfBookConfigDelta from './VolumeWeightedTopOfBookConfig_delta';
import StackedMarketMakingConfig from './StackedMarketMakingConfig';
import StackedMarketMakingConfigDelta from './StackedMarketMakingConfig_delta';
import TopOfBookDetails from './TopOfBookDetails';
import VolumeWeightedTopOfBookDetails from './VolumeWeightedTopOfBookDetails';
import VolumeWeightedTopOfBookDetailsDelta from './VolumeWeightedTopOfBookDetails_delta';
import StackedMarketMakingDetails from './StackedMarketMakingDetails';
import StackedMarketMakingDetailsDelta from './StackedMarketMakingDetails_delta';
import ExchangeSelector from './ExchangeSelector';
import MarketMakingConfig from './MarketMakingConfig';

const BotManager = ({ onRefresh }) => {
  const [bots, setBots] = useState([]);
  const [loading, setLoading] = useState(true);
  const [actionLoading, setActionLoading] = useState({});
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [savedConfigs, setSavedConfigs] = useState({});
  const [configSaved, setConfigSaved] = useState(false);
  const [symbolValidation, setSymbolValidation] = useState({ valid: false, loading: false });
  const [selectedBot, setSelectedBot] = useState(null);
  const [detailsBot, setDetailsBot] = useState(null);
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

  // Aggressive TWAP specific state
  const [aggressiveTwapConfig, setAggressiveTwapConfig] = useState({
    base_coin: '',
    total_time_hours: 1,
    granularity_value: 15,
    granularity_unit: 'minutes',
    frequency_seconds: 30,
    target_price: '',
    target_position_total: '',
    position_currency: 'base',
    cooldown_period_seconds: 60,
    start_time: new Date().toISOString().slice(0, 16), // Initialize with current time in correct format
    intervals: []
  });

  // Targeted Sellbot specific state
  const [targetedSellbotConfig, setTargetedSellbotConfig] = useState({
    start_time: new Date().toISOString().slice(0, 16),
    total_time_hours: 24,
    spread_bps: 50,
    timeout_seconds: 300,
    drift_bps: 100,
    sell_mode: 'percent_inventory',
    sell_target_value: 10,
    tracking_mode: 'percentage',
    hourly_percentage: 10,
    target_amount: '',
    target_currency: 'base',
    taker_check: true,
    pricing_algorithm: 'greater_of',
    price_comparison_mode: 'greater',
    minimum_sell_price: null
  });

  // Targeted Buybot specific state
  const [targetedBuybotConfig, setTargetedBuybotConfig] = useState({
    start_time: new Date().toISOString().slice(0, 16),
    total_time_hours: 24,
    spread_bps: 50,
    timeout_seconds: 300,
    drift_bps: 100,
    buy_mode: 'percent_of_target',
    buy_target_value: 10,
    total_buy_target_amount: 1000,
    target_currency: 'base',
    taker_check: true,
    price_comparison_mode: 'lesser',
    maximum_buy_price: null
  });

  // Top of Book specific state
  const [topOfBookConfig, setTopOfBookConfig] = useState({
    start_time: new Date().toISOString().slice(0, 16),
    sides: 'both',
    target_inventory: 300000,
    excess_inventory_percentage: 10,
    spread_bps: 25,
    taker_check: true,
    accounting_method: 'FIFO',
    bid_hourly_rates: [],
    offer_hourly_rates: []
  });

  // Volume-Weighted Top of Book specific state
  const [volumeWeightedTopOfBookConfig, setVolumeWeightedTopOfBookConfig] = useState({
    start_time: new Date().toISOString().slice(0, 16),
    sides: 'both',
    target_inventory: 100000,
    excess_inventory_percentage: 10,
    spread_bps: 10,
    taker_check: true,
    accounting_method: 'FIFO',
    time_periods: ['5min', '15min', '30min'],
    coefficient_method: 'min',
    min_coefficient: 0.2,
    max_coefficient: 3.0,
    bid_levels: [],
    offer_levels: []
  });

  // Volume-Weighted Top of Book (Delta) specific state
  const [volumeWeightedTopOfBookDeltaConfig, setVolumeWeightedTopOfBookDeltaConfig] = useState({
    start_time: new Date().toISOString().slice(0, 16),
    sides: 'both',
    target_inventory: 100000,
    excess_inventory_percentage: 10,
    spread_bps: 10,
    taker_check: true,
    accounting_method: 'FIFO',
    time_periods: ['5min', '15min', '30min'],
    coefficient_method: 'min',
    min_coefficient: 0.2,
    max_coefficient: 3.0,
    bid_levels: [],
    offer_levels: [],
    target_inventory_source: 'delta_calc',
    delta_service_url: 'http://127.0.0.1:8085',
    delta_refresh_seconds: 5,
    delta_change_threshold_pct: 0.025,
    delta_trade_fraction: 0.1,
    delta_target_sign: -1,
    prevent_unprofitable_trades: false
  });

  // Stacked Market Making specific state
  const [stackedMarketMakingConfig, setStackedMarketMakingConfig] = useState({
    base_coin: '',
    quote_currencies: ['USDT'],
    inventory: {
      target_inventory: '300000',
      max_inventory_deviation: '50000',
      inventory_price_method: 'accounting',
      manual_inventory_price: null,
      start_time: new Date().toISOString().slice(0, 16)
    },
    tob_lines: [],
    passive_lines: [],
    time_periods: ['5min', '15min', '30min'],
    coefficient_method: 'min',
    min_coefficient: 0.2,
    max_coefficient: 3.0,
    smart_pricing_source: 'aggregated',
    taker_check: true,
    hedging_enabled: true,
    hedging_targets: {},
    leverage: '1.0',
    moving_average_periods: [5, 15, 60]
  });

  // Stacked Market Making (Delta) specific state
  const [stackedMarketMakingDeltaConfig, setStackedMarketMakingDeltaConfig] = useState({
    base_coin: '',
    quote_currencies: ['USDT'],
    inventory: {
      target_inventory: '300000',
      max_inventory_deviation: '50000',
      inventory_price_method: 'accounting',
      manual_inventory_price: null,
      start_time: new Date().toISOString().slice(0, 16)
    },
    tob_lines: [],
    passive_lines: [],
    time_periods: ['5min', '15min', '30min'],
    coefficient_method: 'min',
    min_coefficient: 0.2,
    max_coefficient: 3.0,
    smart_pricing_source: 'aggregated',
    taker_check: true,
    hedging_enabled: true,
    hedging_targets: {},
    leverage: '1.0',
    moving_average_periods: [5, 15, 60],
    target_inventory_source: 'delta_calc',
    delta_service_url: 'http://127.0.0.1:8085',
    delta_refresh_seconds: 5,
    delta_change_threshold_pct: 0.025,
    delta_trade_fraction: 0.1,
    delta_target_sign: -1,
    prevent_unprofitable_trades: false
  });

  const availableStrategies = [
    { value: 'passive_quoting', label: 'Passive Quoting' },
    { value: 'aggressive_twap', label: 'Aggressive TWAP' },
    { value: 'aggressive_twap_v2', label: 'Aggressive TWAP V2' },
    { value: 'targeted_sellbot', label: 'Targeted Sellbot' },
    { value: 'targeted_buybot', label: 'Targeted Buybot' },
    { value: 'top_of_book', label: 'Top of Book' },
    { value: 'volume_weighted_top_of_book', label: 'Volume-Weighted Top of Book' },
    { value: 'volume_weighted_top_of_book_delta', label: 'Volume-Weighted Top of Book (Delta)' },
    { value: 'stacked_market_making', label: 'Stacked Market Making' },
    { value: 'stacked_market_making_delta', label: 'Stacked Market Making (Delta)' },
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
      
      // Handle different error response formats
      let errorMessage = 'Unknown error occurred';
      
      if (error.response?.data?.detail) {
        // Handle FastAPI validation errors
        if (Array.isArray(error.response.data.detail)) {
          errorMessage = error.response.data.detail.map(err => 
            `${err.loc?.join('.')} - ${err.msg}`
          ).join('; ');
        } else {
          errorMessage = error.response.data.detail;
        }
      } else if (error.message) {
        errorMessage = error.message;
      }
      
      alert(`Failed to ${action} bot: ${errorMessage}`);
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

    // Additional validation for aggressive TWAP
    if (newBot.strategy === 'aggressive_twap' || newBot.strategy === 'aggressive_twap_v2') {
      if (!aggressiveTwapConfig.base_coin) {
        alert('Base coin is required. Please enter a valid trading pair.');
        return;
      }
      
      if (!aggressiveTwapConfig.target_price) {
        alert('Target price is required.');
        return;
      }
      
      if (!aggressiveTwapConfig.target_position_total) {
        alert('Target position total is required.');
        return;
      }
      
      // Check intervals - if empty but auto-generation conditions are met, wait a moment
      if (!aggressiveTwapConfig.intervals || aggressiveTwapConfig.intervals.length === 0) {
        // Check if auto-generation should work (all required fields are filled)
        const canAutoGenerate = aggressiveTwapConfig.total_time_hours && 
                               aggressiveTwapConfig.granularity_value && 
                               aggressiveTwapConfig.granularity_unit && 
                               aggressiveTwapConfig.target_price && 
                               aggressiveTwapConfig.target_position_total;
        
        if (canAutoGenerate) {
          // Wait a moment for auto-generation to complete
          await new Promise(resolve => setTimeout(resolve, 200));
          
          // Check again after waiting
          if (!aggressiveTwapConfig.intervals || aggressiveTwapConfig.intervals.length === 0) {
            alert('Auto-generation failed. Please manually configure at least one interval or check your inputs.');
            return;
          }
        } else {
          alert('Price intervals are required. Please configure at least one interval or use Auto Generate.');
          return;
        }
      }
    }

    // Additional validation for stacked market making
    if (newBot.strategy === 'stacked_market_making' || newBot.strategy === 'stacked_market_making_delta') {
      const activeConfig = newBot.strategy === 'stacked_market_making_delta'
        ? stackedMarketMakingDeltaConfig
        : stackedMarketMakingConfig;
      if (!activeConfig.base_coin) {
        alert('Base coin is required. Please enter a valid trading pair.');
        return;
      }
      
      if (!activeConfig.inventory?.target_inventory) {
        alert('Target inventory is required for inventory management.');
        return;
      }
      
      if (!activeConfig.inventory?.max_inventory_deviation) {
        alert('Max inventory deviation is required for inventory management.');
        return;
      }
      
      const hasTOBLines = (activeConfig.tob_lines || []).length > 0;
      const hasPassiveLines = (activeConfig.passive_lines || []).length > 0;
      
      if (!hasTOBLines && !hasPassiveLines) {
        alert('At least one TOB line or Passive line must be configured. This strategy requires either local exchange quoting (TOB) or aggregated book quoting (Passive).');
        return;
      }
    }

    try {
      if (newBot.strategy === 'passive_quoting') {
        // Use the passive quoting API endpoint
        await api.strategies.passiveQuoting.createBot({
          symbol: newBot.symbol,
          config: {
            ...newBot.config,
            exchanges: newBot.exchanges
          }
        });
      } else if (newBot.strategy === 'market_making') {
        // Use helper that posts to generic bot creation with proper payload
        await api.marketMaking.createBot({
          symbol: newBot.symbol,
          config: {
            ...newBot.config,
            exchanges: newBot.exchanges,
          },
        });
      } else if (newBot.strategy === 'aggressive_twap' || newBot.strategy === 'aggressive_twap_v2') {
        // Convert start_time to ISO format if provided
        const configToSend = { ...aggressiveTwapConfig };
        // Keep the local time as intended by the user
        // Don't convert to UTC - the backend will handle timezone conversion
        
        // Use the aggressive TWAP API endpoint
        await api.strategies.aggressiveTwap.createBot({
          strategy: newBot.strategy,
          symbol: newBot.symbol,
          config: {
            ...configToSend,
            exchanges: newBot.exchanges
          }
        });
      } else if (newBot.strategy === 'targeted_sellbot') {
        // Use the generic bot creation endpoint for now
        // TODO: Create specific API endpoint for targeted_sellbot
        await api.bots.create({
          strategy: newBot.strategy,
          symbol: newBot.symbol,
          exchanges: newBot.exchanges,
          config: targetedSellbotConfig
        });
      } else if (newBot.strategy === 'targeted_buybot') {
        // Use the generic bot creation endpoint for now
        // TODO: Create specific API endpoint for targeted_buybot
        await api.bots.create({
          strategy: newBot.strategy,
          symbol: newBot.symbol,
          exchanges: newBot.exchanges,
          config: targetedBuybotConfig
        });
      } else if (newBot.strategy === 'top_of_book') {
        // Use the generic bot creation endpoint for top_of_book
        await api.bots.create({
          strategy: newBot.strategy,
          symbol: newBot.symbol,
          exchanges: newBot.exchanges,
          config: topOfBookConfig
        });
      } else if (newBot.strategy === 'volume_weighted_top_of_book') {
        // Use the generic bot creation endpoint for volume_weighted_top_of_book
        await api.bots.create({
          strategy: newBot.strategy,
          symbol: newBot.symbol,
          exchanges: newBot.exchanges,
          config: volumeWeightedTopOfBookConfig
        });
      } else if (newBot.strategy === 'volume_weighted_top_of_book_delta') {
        await api.bots.create({
          strategy: newBot.strategy,
          symbol: newBot.symbol,
          exchanges: newBot.exchanges,
          config: volumeWeightedTopOfBookDeltaConfig
        });
      } else if (newBot.strategy === 'stacked_market_making') {
        // Use the generic bot creation endpoint for stacked_market_making
        await api.bots.create({
          strategy: newBot.strategy,
          symbol: newBot.symbol,
          exchanges: newBot.exchanges,
          config: stackedMarketMakingConfig
        });
      } else if (newBot.strategy === 'stacked_market_making_delta') {
        await api.bots.create({
          strategy: newBot.strategy,
          symbol: newBot.symbol,
          exchanges: newBot.exchanges,
          config: stackedMarketMakingDeltaConfig
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
      setAggressiveTwapConfig({
        base_coin: '',
        total_time_hours: 1,
        granularity_value: 15,
        granularity_unit: 'minutes',
        frequency_seconds: 30,
        target_price: '',
        target_position_total: '',
        position_currency: 'base',
        cooldown_period_seconds: 60,
        start_time: new Date().toISOString().slice(0, 16),
        intervals: []
      });
      setTargetedSellbotConfig({
        start_time: new Date().toISOString().slice(0, 16),
        total_time_hours: 24,
        spread_bps: 50,
        timeout_seconds: 300,
        drift_bps: 100,
        sell_mode: 'percent_inventory',
        sell_target_value: 10,
        tracking_mode: 'percentage',
        hourly_percentage: 10,
        target_amount: '',
        target_currency: 'base',
        taker_check: true,
        pricing_algorithm: 'greater_of',
        price_comparison_mode: 'greater',
        minimum_sell_price: null
      });
      setTargetedBuybotConfig({
        start_time: new Date().toISOString().slice(0, 16),
        total_time_hours: 24,
        spread_bps: 50,
        timeout_seconds: 300,
        drift_bps: 100,
        buy_mode: 'percent_of_target',
        buy_target_value: 10,
        total_buy_target_amount: 1000,
        target_currency: 'base',
        taker_check: true,
        price_comparison_mode: 'lesser',
        maximum_buy_price: null
      });
      setTopOfBookConfig({
        start_time: new Date().toISOString().slice(0, 16),
        sides: 'both',
        target_inventory: 300000,
        excess_inventory_percentage: 10,
        spread_bps: 25,
        taker_check: true,
        bid_hourly_rates: [],
        offer_hourly_rates: []
      });
      setVolumeWeightedTopOfBookConfig({
        start_time: new Date().toISOString().slice(0, 16),
        sides: 'both',
        target_inventory: 100000,
        excess_inventory_percentage: 10,
        spread_bps: 10,
        taker_check: true,
        accounting_method: 'FIFO',
        time_periods: ['5min', '15min', '30min'],
        coefficient_method: 'min',
        min_coefficient: 0.2,
        max_coefficient: 3.0,
        bid_levels: [],
        offer_levels: []
      });
      setVolumeWeightedTopOfBookDeltaConfig({
        start_time: new Date().toISOString().slice(0, 16),
        sides: 'both',
        target_inventory: 100000,
        excess_inventory_percentage: 10,
        spread_bps: 10,
        taker_check: true,
        accounting_method: 'FIFO',
        time_periods: ['5min', '15min', '30min'],
        coefficient_method: 'min',
        min_coefficient: 0.2,
        max_coefficient: 3.0,
        bid_levels: [],
        offer_levels: [],
        target_inventory_source: 'delta_calc',
        delta_service_url: 'http://127.0.0.1:8085',
        delta_refresh_seconds: 5,
        delta_change_threshold_pct: 0.025,
        delta_trade_fraction: 0.1,
        delta_target_sign: -1,
        prevent_unprofitable_trades: false
      });
      setStackedMarketMakingConfig({
        base_coin: '',
        quote_currencies: ['USDT'],
        inventory: {
          target_inventory: '300000',
          max_inventory_deviation: '50000',
          inventory_price_method: 'accounting',
          manual_inventory_price: null,
          start_time: new Date().toISOString().slice(0, 16)
        },
        tob_lines: [],
        passive_lines: [],
        time_periods: ['5min', '15min', '30min'],
        coefficient_method: 'min',
        min_coefficient: 0.2,
        max_coefficient: 3.0,
        smart_pricing_source: 'aggregated',
        taker_check: true,
        hedging_enabled: true,
        hedging_targets: {},
        leverage: '1.0',
        moving_average_periods: [5, 15, 60]
      });
      setStackedMarketMakingDeltaConfig({
        base_coin: '',
        quote_currencies: ['USDT'],
        inventory: {
          target_inventory: '300000',
          max_inventory_deviation: '50000',
          inventory_price_method: 'accounting',
          manual_inventory_price: null,
          start_time: new Date().toISOString().slice(0, 16)
        },
        tob_lines: [],
        passive_lines: [],
        time_periods: ['5min', '15min', '30min'],
        coefficient_method: 'min',
        min_coefficient: 0.2,
        max_coefficient: 3.0,
        smart_pricing_source: 'aggregated',
        taker_check: true,
        hedging_enabled: true,
        hedging_targets: {},
        leverage: '1.0',
        moving_average_periods: [5, 15, 60],
        target_inventory_source: 'delta_calc',
        delta_service_url: 'http://127.0.0.1:8085',
        delta_refresh_seconds: 5,
        delta_change_threshold_pct: 0.025,
        delta_trade_fraction: 0.1,
        delta_target_sign: -1,
        prevent_unprofitable_trades: false
      });
      setConfigSaved(false);
      setSymbolValidation({ valid: false, loading: false });
      await fetchBots();
      onRefresh && onRefresh();
    } catch (error) {
      console.error('Failed to create bot:', error);
      
      // Handle different error response formats
      let errorMessage = 'Unknown error occurred';
      
      if (error.response?.data?.detail) {
        // Handle FastAPI validation errors
        if (Array.isArray(error.response.data.detail)) {
          errorMessage = error.response.data.detail.map(err => 
            `${err.loc?.join('.')} - ${err.msg}`
          ).join('; ');
        } else {
          errorMessage = error.response.data.detail;
        }
      } else if (error.message) {
        errorMessage = error.message;
      }
      
      alert(`Failed to create bot: ${errorMessage}`);
    }
  };

  const handleTradingPairChange = (value) => {
    setNewBot(prev => ({ ...prev, symbol: value }));
    
    // Auto-populate base coin if valid trading pair
    if (validateTradingPair(value)) {
      const baseCoin = extractBaseCoin(value);
      updateConfig('base_coin', baseCoin);
      
      // Also update aggressive TWAP config base_coin
      setAggressiveTwapConfig(prev => ({
        ...prev,
        base_coin: baseCoin
      }));
      
      // Also update stacked market making config base_coin
      setStackedMarketMakingConfig(prev => ({
        ...prev,
        base_coin: baseCoin
      }));

      // Also update stacked market making delta config base_coin
      setStackedMarketMakingDeltaConfig(prev => ({
        ...prev,
        base_coin: baseCoin
      }));
    }
    
    // Trigger API validation
    debouncedValidation(value);
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

  const handleViewConfig = async (botId) => {
    try {
      console.log('Opening config for bot:', botId);
      const response = await api.bots.getById(botId);
      console.log('Setting selectedBot:', response.data);
      setSelectedBot(response.data);
    } catch (error) {
      console.error('Failed to fetch bot config:', error);
      alert('Failed to fetch bot configuration.');
    }
  };

  const handleSaveConfig = async (botId, newConfig, newExchanges) => {
    try {
      console.log('Saving config for bot:', botId);
      await api.bots.updateConfig(botId, newConfig, newExchanges);
      console.log('Config saved successfully');
      
      // Update the selectedBot with the new config to prevent stale state
      if (selectedBot && selectedBot.instance_id === botId) {
        setSelectedBot(prev => ({
          ...prev,
          config: newConfig,
          exchanges: newExchanges
        }));
      }
      
      // Refresh the bot list to show updated status (but don't await it)
      fetchBots().catch(console.error);
      onRefresh && onRefresh();
    } catch (error) {
      console.error('Failed to save bot config:', error);
      // Re-throw to be caught by the modal
      throw error;
    }
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
                    {/* Details button for top_of_book strategy */}
                    {(bot.strategy === 'top_of_book' || bot.strategy === 'TopOfBookStrategy') && (
                      <button
                        onClick={() => setDetailsBot(bot)}
                        className="bg-purple-600 hover:bg-purple-700 text-white p-2 rounded transition-colors"
                        title="View Position Details"
                      >
                        <Eye className="h-4 w-4" />
                      </button>
                    )}
                    {/* Details button for volume_weighted_top_of_book strategy */}
                    {(bot.strategy === 'volume_weighted_top_of_book' || bot.strategy === 'volume_weighted_top_of_book_delta') && (
                      <button
                        onClick={() => setDetailsBot(bot)}
                        className="bg-purple-600 hover:bg-purple-700 text-white p-2 rounded transition-colors"
                        title="View Strategy Details"
                      >
                        <Eye className="h-4 w-4" />
                      </button>
                    )}
                    {/* Details button for stacked_market_making strategy */}
                    {(bot.strategy === 'stacked_market_making' || bot.strategy === 'stacked_market_making_delta') && (
                      <button
                        onClick={() => setDetailsBot(bot)}
                        className="bg-purple-600 hover:bg-purple-700 text-white p-2 rounded transition-colors"
                        title="View Stacked Strategy Details"
                      >
                        <Eye className="h-4 w-4" />
                      </button>
                    )}
                    <button
                      onClick={() => handleViewConfig(bot.instance_id)}
                      className="bg-blue-600 hover:bg-blue-700 text-white p-2 rounded transition-colors"
                      title="View / Edit Config"
                    >
                      <Settings className="h-4 w-4" />
                    </button>
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

                {/* Base Coin (for passive quoting, aggressive TWAP, targeted sellbot, and stacked market making) */}
                {(newBot.strategy === 'passive_quoting' || newBot.strategy === 'aggressive_twap' || newBot.strategy === 'targeted_sellbot' || newBot.strategy === 'stacked_market_making') && (
              <div>
                <label className="block text-gray-400 text-sm mb-2">
                      Base Coin
                      {(newBot.config.base_coin || (newBot.strategy === 'aggressive_twap' && newBot.symbol) || (newBot.strategy === 'targeted_sellbot' && newBot.symbol) || (newBot.strategy === 'stacked_market_making' && stackedMarketMakingConfig.base_coin)) && (
                        <span className="text-green-400 ml-1">✓</span>
                      )}
                </label>
                    <input
                      type="text"
                      value={newBot.strategy === 'aggressive_twap' || newBot.strategy === 'targeted_sellbot' ? extractBaseCoin(newBot.symbol) : newBot.strategy === 'stacked_market_making' ? stackedMarketMakingConfig.base_coin : newBot.config.base_coin}
                      onChange={(e) => {
                        if (newBot.strategy === 'passive_quoting') {
                          updateConfig('base_coin', e.target.value.toUpperCase());
                        } else if (newBot.strategy === 'stacked_market_making') {
                          setStackedMarketMakingConfig(prev => ({ 
                            ...prev, 
                            base_coin: e.target.value.toUpperCase() 
                          }));
                        }
                      }}
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

              {/* Dynamic Strategy Configuration Forms */}
              <div className="pt-4">
                {newBot.strategy === 'aggressive_twap' && (
                  <AggressiveTwapConfig
                    config={aggressiveTwapConfig}
                    onConfigChange={setAggressiveTwapConfig}
                    symbol={newBot.symbol}
                    exchanges={newBot.exchanges}
                  />
                )}
                {newBot.strategy === 'aggressive_twap_v2' && (
                  <AggressiveTwapV2Config
                    config={aggressiveTwapConfig}
                    onConfigChange={setAggressiveTwapConfig}
                    symbol={newBot.symbol}
                    exchanges={newBot.exchanges}
                  />
                )}
                {newBot.strategy === 'targeted_sellbot' && (
                  <TargetedSellbotConfig
                    config={targetedSellbotConfig}
                    onConfigChange={setTargetedSellbotConfig}
                  />
                )}
                {newBot.strategy === 'targeted_buybot' && (
                  <TargetedBuybotConfig
                    config={targetedBuybotConfig}
                    onConfigChange={setTargetedBuybotConfig}
                  />
                )}
                {newBot.strategy === 'top_of_book' && (
                  <TopOfBookConfig
                    config={topOfBookConfig}
                    onConfigChange={setTopOfBookConfig}
                    symbol={newBot.symbol}
                    exchanges={newBot.exchanges}
                  />
                )}
                {newBot.strategy === 'passive_quoting' && (
                  <PassiveQuotingConfig
                    config={newBot.config}
                    onConfigChange={(config) => setNewBot(prev => ({ ...prev, config }))}
                  />
                )}
                {newBot.strategy === 'market_making' && (
                  <MarketMakingConfig
                    config={newBot.config}
                    onConfigChange={(config) => setNewBot(prev => ({ ...prev, config }))}
                    symbol={newBot.symbol}
                    exchanges={newBot.exchanges}
                  />
                )}
                {newBot.strategy === 'volume_weighted_top_of_book' && (
                  <VolumeWeightedTopOfBookConfig
                    config={volumeWeightedTopOfBookConfig}
                    onConfigChange={setVolumeWeightedTopOfBookConfig}
                    symbol={newBot.symbol}
                    exchanges={newBot.exchanges}
                  />
                )}
                {newBot.strategy === 'volume_weighted_top_of_book_delta' && (
                  <VolumeWeightedTopOfBookConfigDelta
                    config={volumeWeightedTopOfBookDeltaConfig}
                    onConfigChange={setVolumeWeightedTopOfBookDeltaConfig}
                    symbol={newBot.symbol}
                    exchanges={newBot.exchanges}
                  />
                )}

                {newBot.strategy === 'stacked_market_making' && (
                  <StackedMarketMakingConfig
                    config={stackedMarketMakingConfig}
                    onConfigChange={setStackedMarketMakingConfig}
                    symbol={newBot.symbol}
                    exchanges={newBot.exchanges}
                  />
                )}
                {newBot.strategy === 'stacked_market_making_delta' && (
                  <StackedMarketMakingConfigDelta
                    config={stackedMarketMakingDeltaConfig}
                    onConfigChange={setStackedMarketMakingDeltaConfig}
                    symbol={newBot.symbol}
                    exchanges={newBot.exchanges}
                  />
                )}
                  </div>

              {/* Exchanges */}
              <ExchangeSelector
                selectedExchanges={newBot.exchanges}
                onSelectionChange={(exchanges) => setNewBot(prev => ({ ...prev, exchanges }))}
              />

              <div className="flex gap-3 pt-4">
                <button
                  type="submit"
                  disabled={
                    newBot.exchanges.length === 0 || 
                    !symbolValidation.valid || 
                    symbolValidation.loading ||
                    (newBot.strategy === 'aggressive_twap' && (!aggressiveTwapConfig.target_price || !aggressiveTwapConfig.target_position_total || aggressiveTwapConfig.intervals.length === 0)) ||
                    (newBot.strategy === 'stacked_market_making' && (!stackedMarketMakingConfig.base_coin || ((stackedMarketMakingConfig.tob_lines || []).length === 0 && (stackedMarketMakingConfig.passive_lines || []).length === 0)))
                  }
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

      {selectedBot && (
        <BotConfigModal
          bot={selectedBot}
          onClose={() => {
            console.log('Closing BotConfigModal');
            setSelectedBot(null);
          }}
          onSave={handleSaveConfig}
        />
      )}

      {detailsBot && (
        <>
          {detailsBot.strategy === 'top_of_book' || detailsBot.strategy === 'TopOfBookStrategy' ? (
            <TopOfBookDetails
              bot={detailsBot}
              onClose={() => setDetailsBot(null)}
            />
          ) : detailsBot.strategy === 'volume_weighted_top_of_book' ? (
            <VolumeWeightedTopOfBookDetails
              bot={detailsBot}
              onClose={() => setDetailsBot(null)}
            />
          ) : detailsBot.strategy === 'volume_weighted_top_of_book_delta' ? (
            <VolumeWeightedTopOfBookDetailsDelta
              bot={detailsBot}
              onClose={() => setDetailsBot(null)}
            />
          ) : detailsBot.strategy === 'stacked_market_making' ? (
            <StackedMarketMakingDetails
              bot={detailsBot}
              onClose={() => setDetailsBot(null)}
            />
          ) : detailsBot.strategy === 'stacked_market_making_delta' ? (
            <StackedMarketMakingDetailsDelta
              bot={detailsBot}
              onClose={() => setDetailsBot(null)}
            />
          ) : null}
        </>
      )}
    </div>
  );
};

export default BotManager; 
