import React, { useState, useEffect } from 'react';
import { 
  Shield, 
  TrendingUp, 
  TrendingDown, 
  AlertTriangle, 
  Activity, 
  DollarSign, 
  BarChart3,
  RefreshCw,
  Settings,
  Download,
  Clock,
  ChevronDown,
  ChevronRight
} from 'lucide-react';

const RiskMonitoring = () => {
  const [accounts, setAccounts] = useState([]);
  const [selectedAccount, setSelectedAccount] = useState('');
  const [positionData, setPositionData] = useState({});
  const [riskMetrics, setRiskMetrics] = useState({});
  const [loading, setLoading] = useState(false);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [timeRanges, setTimeRanges] = useState({});
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [wsConnected, setWsConnected] = useState(false);
  const [wsConnection, setWsConnection] = useState(null);
  const [activeSymbols, setActiveSymbols] = useState([]);
  const [error, setError] = useState(null);
  const [syncStatus, setSyncStatus] = useState({});
  const [wsStatus, setWsStatus] = useState('disconnected');
  const [expandedPositions, setExpandedPositions] = useState(new Set());

  // Helper function to safely convert string/number to number
  const safeNumber = (value, defaultValue = 0) => {
    if (value === null || value === undefined || value === '') return defaultValue;
    const num = typeof value === 'string' ? parseFloat(value) : Number(value);
    return isNaN(num) ? defaultValue : num;
  };

  // Helper function to format currency
  const formatCurrency = (value) => {
    return safeNumber(value).toFixed(2);
  };

  // Helper function to format number with decimals
  const formatNumber = (value, decimals = 4) => {
    return safeNumber(value).toFixed(decimals);
  };

  // Initialize time ranges for each account
  const initializeTimeRanges = (accountList) => {
    const ranges = {};
    accountList.forEach(account => {
      ranges[account.name] = {
        startTime: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().slice(0, 16),
        endTime: new Date().toISOString().slice(0, 16)
      };
    });
    setTimeRanges(ranges);
  };

  // Load accounts data
  const loadAccounts = async () => {
    try {
      setError(null);
      const response = await fetch('/api/system/accounts');
      
      if (!response.ok) {
        throw new Error(`Failed to load accounts: ${response.status}`);
      }
      
      const data = await response.json();
      console.log('Accounts loaded:', data);
      
      setAccounts(data.accounts || []);
      
      // Initialize time ranges for all accounts
      const ranges = {};
      data.accounts.forEach(account => {
        ranges[account.name] = {
          startTime: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().slice(0, 16),
          endTime: new Date().toISOString().slice(0, 16)
        };
      });
      setTimeRanges(ranges);
      
      // Auto-select first account and get its symbols
      if (data.accounts && data.accounts.length > 0) {
        const firstAccount = data.accounts[0];
        setSelectedAccount(firstAccount.name);
        
        // Set active symbols from the account
        const symbols = firstAccount.active_symbols || ['BERA/USDT'];
        setActiveSymbols(symbols);
        console.log('Active symbols from account:', symbols);
      }
    } catch (err) {
      console.error('Error loading accounts:', err);
      setError(err.message);
      // Fallback symbols if accounts fail to load
      setActiveSymbols(['BERA/USDT']);
    }
  };

  // Fetch position data
  const fetchPositionData = async () => {
    setLoading(true);
    try {
      const endpoints = [
        '/api/positions/summary',
        '/api/positions/net',
        '/api/positions/export/json'
      ];

      const responses = await Promise.all(
        endpoints.map(endpoint => fetch(endpoint).catch(err => {
          console.warn(`Failed to fetch ${endpoint}:`, err);
          return { ok: false, json: () => Promise.resolve({}) };
        }))
      );

      const responseData = await Promise.all(
        responses.map(r => r.ok ? r.json() : {})
      );

      const [summaryData, netData, exportData] = responseData;

      setPositionData({
        summary: summaryData.summary || {},
        netPositions: netData.net_positions || {},
        detailed: exportData.detailed_positions || []
      });

      calculateRiskMetrics(summaryData.summary || {}, netData.net_positions || {});
      setLastUpdate(new Date());
    } catch (error) {
      console.error('Error fetching position data:', error);
    } finally {
      setLoading(false);
    }
  };

  // Calculate risk metrics
  const calculateRiskMetrics = (summary, netPositions) => {
    // Provide safe defaults for summary
    const safeSummary = {
      long_value: 0,
      short_value: 0,
      net_exposure: 0,
      positions_by_exchange: {},
      ...summary
    };
    
    // Provide safe defaults for netPositions
    const safeNetPositions = netPositions || {};
    
    const metrics = {
      totalExposure: safeSummary.long_value + safeSummary.short_value,
      netExposure: safeSummary.net_exposure,
      longShortRatio: safeSummary.short_value > 0 ? safeSummary.long_value / safeSummary.short_value : 0,
      portfolioDiversification: Object.keys(safeNetPositions).length,
      riskScore: calculateRiskScore(safeSummary, safeNetPositions),
      exchangeDistribution: safeSummary.positions_by_exchange,
      symbolConcentration: calculateSymbolConcentration(safeNetPositions)
    };
    setRiskMetrics(metrics);
  };

  const calculateRiskScore = (summary, netPositions) => {
    let score = 0;
    
    // Exposure risk (safe access)
    const netExposure = Math.abs(summary.net_exposure || 0);
    if (netExposure > 10000) score += 30;
    else if (netExposure > 5000) score += 20;
    else score += 10;

    // Concentration risk (safe access)
    const positionValues = Object.values(netPositions || {}).map(p => Math.abs(p.value || 0));
    const maxPosition = positionValues.length > 0 ? Math.max(...positionValues) : 0;
    const totalExposure = (summary.long_value || 0) + (summary.short_value || 0);
    const concentrationRatio = totalExposure > 0 ? maxPosition / totalExposure : 0;
    
    if (concentrationRatio > 0.5) score += 40;
    else if (concentrationRatio > 0.3) score += 25;
    else score += 10;

    // Exchange diversification (safe access)
    const exchangeCount = Object.keys(summary.positions_by_exchange || {}).length;
    if (exchangeCount < 3) score += 30;
    else if (exchangeCount < 5) score += 20;
    else score += 10;

    return Math.min(score, 100);
  };

  const calculateSymbolConcentration = (netPositions) => {
    const safePositions = netPositions || {};
    const totalValue = Object.values(safePositions).reduce((sum, pos) => sum + Math.abs(pos.value || 0), 0);
    const concentrations = {};
    
    Object.entries(safePositions).forEach(([symbol, pos]) => {
      concentrations[symbol] = totalValue > 0 ? (Math.abs(pos.value || 0) / totalValue) * 100 : 0;
    });
    
    return concentrations;
  };

  // Update time range for account
  const updateTimeRange = (accountName, field, value) => {
    setTimeRanges(prev => ({
      ...prev,
      [accountName]: {
        ...prev[accountName],
        [field]: value
      }
    }));
  };

  // Sync trades for time range
  const handleSyncTrades = async () => {
    if (!selectedAccount || !timeRanges[selectedAccount].startTime || !timeRanges[selectedAccount].endTime) {
      setError('Please select an account and set both start and end times');
      return;
    }

    if (!activeSymbols || activeSymbols.length === 0) {
      setError('No trading symbols configured for this account');
      return;
    }

    setLoading(true);
    setError(null);
    setSyncStatus({ status: 'running', message: 'Starting trade synchronization...' });

    try {
      const response = await fetch('/api/system/sync-trades', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          account: selectedAccount,
          start_time: timeRanges[selectedAccount].startTime,
          end_time: timeRanges[selectedAccount].endTime,
          symbols: activeSymbols  // Use symbols from the selected account
        }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || `Sync failed: ${response.status}`);
      }

      const result = await response.json();
      console.log('Sync result:', result);
      
      setSyncStatus({
        status: 'completed',
        message: result.message || 'Trade synchronization completed',
        summary: result.summary,
        results: result.results
      });

      // Reload positions after successful sync
      await fetchPositionData();
      
    } catch (err) {
      console.error('Error syncing trades:', err);
      setError(err.message);
      setSyncStatus({
        status: 'failed',
        message: `Sync failed: ${err.message}`
      });
    } finally {
      setLoading(false);
    }
  };

  // WebSocket connection for real-time updates
  useEffect(() => {
    if (autoRefresh) {
      const ws = new WebSocket('ws://localhost:8000/ws/risk-monitoring');
      
      ws.onopen = () => {
        console.log('WebSocket connected for risk monitoring');
        setWsConnected(true);
        setWsConnection(ws);
      };
      
      ws.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);
          console.log('WebSocket message received:', message);
          
          if (message.type === 'initial_data' || message.type === 'position_update') {
            const data = message.data;
            
            // Update position data with safe defaults
            setPositionData({
              summary: data.summary || {},
              netPositions: data.net_positions || {},
              detailed: data.detailed_positions || []
            });
            
            // Update risk metrics
            if (data.risk_metrics) {
              setRiskMetrics(data.risk_metrics);
            } else if (data.summary && data.net_positions) {
              calculateRiskMetrics(data.summary, data.net_positions);
            }
            
            setLastUpdate(new Date());
          }
          
          if (message.type === 'trade_update') {
            console.log('Trade update received:', message);
            // Show a brief notification for trade updates
            if (message.data && message.data.symbol) {
              console.log(`🔄 Trade executed: ${message.data.symbol} ${message.data.side} ${message.data.amount}`);
            }
          }
          
          if (message.type === 'position_change') {
            console.log('Position change received:', message);
            // Refresh position data when positions change
            fetchPositionData();
          }
          
        } catch (error) {
          console.error('Error parsing WebSocket message:', error);
        }
      };
      
      ws.onclose = () => {
        console.log('WebSocket disconnected');
        setWsConnected(false);
      };
      
      ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        setWsConnected(false);
      };
      
      // Cleanup function
      return () => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.close();
        }
      };
    }
  }, [autoRefresh]);

  // Initial load
  useEffect(() => {
    loadAccounts();
    fetchPositionData();
  }, []);

  const getRiskColor = (score) => {
    if (score <= 30) return 'text-green-500';
    if (score <= 60) return 'text-yellow-500';
    return 'text-red-500';
  };

  const getRiskBgColor = (score) => {
    if (score <= 30) return 'bg-green-500/10 border-green-500/20';
    if (score <= 60) return 'bg-yellow-500/10 border-yellow-500/20';
    return 'bg-red-500/10 border-red-500/20';
  };

  const getRiskLabel = (score) => {
    if (score <= 30) return 'Low Risk';
    if (score <= 60) return 'Medium Risk';
    return 'High Risk';
  };

  // Toggle position expansion
  const togglePositionExpansion = (symbol) => {
    const newExpanded = new Set(expandedPositions);
    if (newExpanded.has(symbol)) {
      newExpanded.delete(symbol);
    } else {
      newExpanded.add(symbol);
    }
    setExpandedPositions(newExpanded);
  };

  // Get detailed positions for a symbol
  const getDetailedPositionsForSymbol = (symbol) => {
    return (positionData.detailed || []).filter(pos => pos.symbol === symbol && pos.is_open);
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Shield className="h-8 w-8 text-blue-500" />
          <div>
            <h1 className="text-2xl font-bold text-white">Risk Monitoring</h1>
            <p className="text-gray-400">Real-time portfolio risk analysis</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setAutoRefresh(!autoRefresh)}
            className={`p-2 rounded-lg transition-colors ${
              autoRefresh ? 'bg-blue-600 text-white' : 'bg-gray-700 text-gray-300'
            }`}
            title="Auto-refresh every 15 seconds"
          >
            <Settings className="h-5 w-5" />
          </button>
          <button
            onClick={fetchPositionData}
            disabled={loading}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg transition-colors disabled:opacity-50"
          >
            <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>
      </div>

      {/* Loading bar */}
      {loading && (
        <div className="w-full bg-gray-700 rounded-full h-1">
          <div className="bg-blue-600 h-1 rounded-full animate-pulse" style={{ width: '70%' }}></div>
        </div>
      )}

      {/* Last update info */}
      {lastUpdate && (
        <div className="bg-blue-600/10 border border-blue-600/20 rounded-lg p-3 flex items-center gap-2">
          <Clock className="h-4 w-4 text-blue-400" />
          <span className="text-blue-400 text-sm">
            Last updated: {lastUpdate.toLocaleString()}
            {autoRefresh && wsConnected && ' (Real-time updates active)'}
            {autoRefresh && !wsConnected && ' (WebSocket disconnected)'}
          </span>
          {wsConnected && (
            <div className="ml-auto flex items-center gap-1">
              <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
              <span className="text-green-400 text-xs">Live</span>
            </div>
          )}
          {!wsConnected && autoRefresh && (
            <div className="ml-auto flex items-center gap-1">
              <div className="w-2 h-2 bg-red-500 rounded-full"></div>
              <span className="text-red-400 text-xs">Offline</span>
            </div>
          )}
        </div>
      )}

      {/* Account Configuration */}
      <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
        <h2 className="text-lg font-semibold text-white mb-4">Account Configuration</h2>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-2">Account</label>
            <select
              value={selectedAccount}
              onChange={(e) => setSelectedAccount(e.target.value)}
              className="w-full bg-gray-700 border border-gray-600 rounded-lg px-3 py-2 text-white focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            >
              <option value="all">All Accounts</option>
              {accounts.map(account => (
                <option key={account.name} value={account.name}>
                  {account.name}
                </option>
              ))}
            </select>
          </div>
          
          {selectedAccount !== 'all' && timeRanges[selectedAccount] && (
            <>
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-2">Start Time (UTC)</label>
                <input
                  type="datetime-local"
                  value={timeRanges[selectedAccount].startTime}
                  onChange={(e) => updateTimeRange(selectedAccount, 'startTime', e.target.value)}
                  className="w-full bg-gray-700 border border-gray-600 rounded-lg px-3 py-2 text-white focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-2">End Time (UTC)</label>
                <input
                  type="datetime-local"
                  value={timeRanges[selectedAccount].endTime}
                  onChange={(e) => updateTimeRange(selectedAccount, 'endTime', e.target.value)}
                  className="w-full bg-gray-700 border border-gray-600 rounded-lg px-3 py-2 text-white focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
              <div className="flex items-end">
                <button
                  onClick={handleSyncTrades}
                  disabled={loading}
                  className="w-full bg-green-600 hover:bg-green-700 disabled:bg-gray-600 text-white rounded-lg px-4 py-2 font-medium transition-colors"
                >
                  {loading ? 'Syncing...' : 'Sync Trades'}
                </button>
              </div>
            </>
          )}
        </div>
        
        {/* Show active symbols */}
        {selectedAccount !== 'all' && activeSymbols.length > 0 && (
          <div className="mt-4 p-3 bg-blue-600/10 border border-blue-600/20 rounded-lg">
            <p className="text-blue-400 text-sm font-medium mb-2">Trading Symbols for {selectedAccount}:</p>
            <div className="flex flex-wrap gap-2">
              {activeSymbols.map((symbol, index) => (
                <span
                  key={index}
                  className="inline-block px-2 py-1 bg-blue-600/20 text-blue-400 text-xs rounded-md font-mono"
                >
                  {symbol}
                </span>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Error Display */}
      {error && (
        <div className="bg-red-600/10 border border-red-600/20 rounded-lg p-4">
          <div className="flex items-center gap-2">
            <AlertTriangle className="h-5 w-5 text-red-400" />
            <p className="text-red-400 font-medium">Error</p>
          </div>
          <p className="text-red-300 mt-1">{error}</p>
          <button
            onClick={() => setError(null)}
            className="mt-2 text-red-400 hover:text-red-300 text-sm underline"
          >
            Dismiss
          </button>
        </div>
      )}

      {/* Sync Status Display */}
      {syncStatus.status && (
        <div className={`border rounded-lg p-4 ${
          syncStatus.status === 'running' ? 'bg-blue-600/10 border-blue-600/20' :
          syncStatus.status === 'completed' ? 'bg-green-600/10 border-green-600/20' :
          'bg-red-600/10 border-red-600/20'
        }`}>
          <div className="flex items-center gap-2">
            {syncStatus.status === 'running' && (
              <>
                <Activity className="h-5 w-5 text-blue-400 animate-pulse" />
                <p className="text-blue-400 font-medium">Synchronizing Trades</p>
              </>
            )}
            {syncStatus.status === 'completed' && (
              <>
                <TrendingUp className="h-5 w-5 text-green-400" />
                <p className="text-green-400 font-medium">Sync Completed</p>
              </>
            )}
            {syncStatus.status === 'failed' && (
              <>
                <AlertTriangle className="h-5 w-5 text-red-400" />
                <p className="text-red-400 font-medium">Sync Failed</p>
              </>
            )}
          </div>
          <p className={`mt-1 ${
            syncStatus.status === 'running' ? 'text-blue-300' :
            syncStatus.status === 'completed' ? 'text-green-300' :
            'text-red-300'
          }`}>
            {syncStatus.message}
          </p>
          
          {syncStatus.summary && (
            <div className="mt-3 grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
              <div className="bg-gray-700/50 rounded-lg p-2">
                <p className="text-gray-400">Exchanges</p>
                <p className="text-white font-medium">{syncStatus.summary.total_exchanges}</p>
              </div>
              <div className="bg-gray-700/50 rounded-lg p-2">
                <p className="text-gray-400">Success Rate</p>
                <p className="text-white font-medium">{syncStatus.summary.success_rate}</p>
              </div>
              <div className="bg-gray-700/50 rounded-lg p-2">
                <p className="text-gray-400">Trades Synced</p>
                <p className="text-white font-medium">{syncStatus.summary.total_trades_synced}</p>
              </div>
              <div className="bg-gray-700/50 rounded-lg p-2">
                <p className="text-gray-400">Requests Made</p>
                <p className="text-white font-medium">{syncStatus.summary.total_requests_made}</p>
              </div>
            </div>
          )}
          
          {syncStatus.status !== 'running' && (
            <button
              onClick={() => setSyncStatus({})}
              className="mt-2 text-gray-400 hover:text-gray-300 text-sm underline"
            >
              Dismiss
            </button>
          )}
        </div>
      )}

      {/* Risk Overview Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <div className={`bg-gray-800 rounded-xl p-6 border ${getRiskBgColor(safeNumber(riskMetrics.riskScore))}`}>
          <div className="flex items-center justify-between">
            <div>
              <p className="text-gray-400 text-sm">Risk Score</p>
              <p className={`text-3xl font-bold ${getRiskColor(safeNumber(riskMetrics.riskScore))}`}>
                {formatNumber(riskMetrics.riskScore || 0, 0)}
              </p>
              <span className={`inline-block px-2 py-1 rounded text-xs font-medium ${getRiskBgColor(safeNumber(riskMetrics.riskScore))}`}>
                {getRiskLabel(safeNumber(riskMetrics.riskScore))}
              </span>
            </div>
            <AlertTriangle className={`h-8 w-8 ${getRiskColor(safeNumber(riskMetrics.riskScore))}`} />
          </div>
        </div>

        <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-gray-400 text-sm">Total Exposure</p>
              <p className="text-3xl font-bold text-white">
                ${formatCurrency(riskMetrics.totalExposure || 0)}
              </p>
              <p className="text-gray-400 text-sm">
                Net: ${formatCurrency(riskMetrics.netExposure || 0)}
              </p>
            </div>
            <DollarSign className="h-8 w-8 text-blue-500" />
          </div>
        </div>

        <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-gray-400 text-sm">Long/Short Ratio</p>
              <p className="text-3xl font-bold text-white">
                {formatNumber(riskMetrics.longShortRatio || 0, 2)}
              </p>
              <p className="text-gray-400 text-sm">
                {safeNumber(positionData.summary?.open_positions)} positions
              </p>
            </div>
            <BarChart3 className="h-8 w-8 text-purple-500" />
          </div>
        </div>

        <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-gray-400 text-sm">Diversification</p>
              <p className="text-3xl font-bold text-white">
                {formatNumber(riskMetrics.portfolioDiversification || 0, 0)}
              </p>
              <p className="text-gray-400 text-sm">symbols</p>
            </div>
            <TrendingUp className="h-8 w-8 text-green-500" />
          </div>
        </div>
      </div>

      {/* Position Details Table with Exchange Breakdown */}
      <div className="bg-gray-800 rounded-xl border border-gray-700 overflow-hidden">
        <div className="p-6 border-b border-gray-700">
          <h2 className="text-lg font-semibold text-white">Position Details</h2>
          <p className="text-gray-400 text-sm mt-1">Click on a position to view exchange breakdown</p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-700/50">
              <tr>
                <th className="px-6 py-3 text-left text-sm font-medium text-gray-300 w-8"></th>
                <th className="px-6 py-3 text-left text-sm font-medium text-gray-300">Symbol</th>
                <th className="px-6 py-3 text-left text-sm font-medium text-gray-300">Type</th>
                <th className="px-6 py-3 text-right text-sm font-medium text-gray-300">Size</th>
                <th className="px-6 py-3 text-right text-sm font-medium text-gray-300">Value</th>
                <th className="px-6 py-3 text-right text-sm font-medium text-gray-300">Side</th>
                <th className="px-6 py-3 text-right text-sm font-medium text-gray-300">Avg Price</th>
                <th className="px-6 py-3 text-right text-sm font-medium text-gray-300">Concentration</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-700">
              {Object.entries(positionData.netPositions || {}).map(([symbol, position]) => {
                const isExpanded = expandedPositions.has(symbol);
                const detailedPositions = getDetailedPositionsForSymbol(symbol);
                
                return position.is_open && (
                  <React.Fragment key={symbol}>
                    {/* Net Position Row */}
                    <tr 
                      className="hover:bg-gray-700/50 cursor-pointer transition-colors"
                      onClick={() => togglePositionExpansion(symbol)}
                    >
                      <td className="px-6 py-4">
                        {isExpanded ? (
                          <ChevronDown className="h-4 w-4 text-gray-400" />
                        ) : (
                          <ChevronRight className="h-4 w-4 text-gray-400" />
                        )}
                      </td>
                      <td className="px-6 py-4 text-white font-medium">{symbol}</td>
                      <td className="px-6 py-4">
                        <span className="inline-block px-2 py-1 bg-blue-600/20 text-blue-400 text-xs rounded-md font-medium">
                          Net Position
                        </span>
                      </td>
                      <td className="px-6 py-4 text-right text-white font-semibold">{formatNumber(position.size)}</td>
                      <td className="px-6 py-4 text-right text-white font-semibold">${formatCurrency(position.value)}</td>
                      <td className="px-6 py-4 text-right">
                        <span className={`inline-block px-2 py-1 rounded text-xs font-medium ${
                          position.side === 'long' ? 'bg-green-500/20 text-green-400' :
                          position.side === 'short' ? 'bg-red-500/20 text-red-400' :
                          'bg-gray-500/20 text-gray-400'
                        }`}>
                          {position.side || 'flat'}
                        </span>
                      </td>
                      <td className="px-6 py-4 text-right text-white font-semibold">
                        ${formatNumber(position.avg_price)}
                      </td>
                      <td className="px-6 py-4 text-right text-white">
                        {formatNumber(riskMetrics.symbolConcentration?.[symbol] || 0, 1)}%
                      </td>
                    </tr>

                    {/* Exchange Breakdown Rows */}
                    {isExpanded && detailedPositions.map((pos, index) => (
                      <tr key={`${symbol}-${pos.exchange}-${index}`} className="bg-gray-700/30">
                        <td className="px-6 py-3"></td>
                        <td className="px-6 py-3 text-gray-300 pl-8">└ {pos.symbol}</td>
                        <td className="px-6 py-3">
                          <div className="flex items-center gap-2">
                            <span className={`inline-block px-2 py-1 text-xs rounded-md font-medium ${
                              pos.is_perpetual 
                                ? 'bg-purple-600/20 text-purple-400' 
                                : 'bg-gray-600/20 text-gray-400'
                            }`}>
                              {pos.exchange}
                            </span>
                            {pos.is_perpetual && (
                              <span className="inline-block px-1 py-0.5 bg-purple-500/20 text-purple-300 text-xs rounded font-mono">
                                PERP
                              </span>
                            )}
                          </div>
                        </td>
                        <td className="px-6 py-3 text-right text-gray-300">{formatNumber(pos.size)}</td>
                        <td className="px-6 py-3 text-right text-gray-300">${formatCurrency(pos.value)}</td>
                        <td className="px-6 py-3 text-right">
                          <span className={`inline-block px-2 py-1 rounded text-xs font-medium ${
                            pos.side === 'long' ? 'bg-green-500/10 text-green-300 border border-green-500/20' :
                            pos.side === 'short' ? 'bg-red-500/10 text-red-300 border border-red-500/20' :
                            'bg-gray-500/10 text-gray-300 border border-gray-500/20'
                          }`}>
                            {pos.side || 'flat'}
                          </span>
                        </td>
                        <td className="px-6 py-3 text-right text-gray-300">
                          ${formatNumber(pos.avg_price)}
                        </td>
                        <td className="px-6 py-3 text-right text-gray-400 text-sm">
                          {formatNumber((Math.abs(safeNumber(pos.value)) / Math.abs(safeNumber(position.value, 1))) * 100, 1)}%
                        </td>
                      </tr>
                    ))}
                  </React.Fragment>
                );
              })}
              {Object.keys(positionData.netPositions || {}).length === 0 && (
                <tr>
                  <td colSpan="8" className="px-6 py-8 text-center text-gray-400">
                    No positions found
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default RiskMonitoring; 