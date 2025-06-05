import React, { useState, useEffect } from 'react';
import { 
  CheckCircle, 
  XCircle, 
  AlertTriangle, 
  WifiOff, 
  Key,
  RefreshCw,
  Settings,
  TestTube,
  Eye,
  EyeOff,
  AlertCircle,
  Check,
  Trash2,
  User,
  Save,
  Users,
  Database
} from 'lucide-react';
import { api } from '../services/api';

const ExchangeStatus = ({ onRefresh }) => {
  const [exchanges, setExchanges] = useState([]);
  const [loading, setLoading] = useState(true);
  const [testing, setTesting] = useState({});
  const [showCredentials, setShowCredentials] = useState({});
  const [editingExchange, setEditingExchange] = useState(null);
  const [credentials, setCredentials] = useState({
    account_name: '',
    api_key: '',
    api_secret: '',
    wallet_address: '', // For Hyperliquid
    private_key: '', // For Hyperliquid
    passphrase: '', // For exchanges like Bitget
    testnet: false
  });
  const [messages, setMessages] = useState({});
  const [deleting, setDeleting] = useState({});
  const [confirmingDelete, setConfirmingDelete] = useState(null);
  const [credentialsError, setCredentialsError] = useState('');
  const [credentialsSuccess, setCredentialsSuccess] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [accounts, setAccounts] = useState({});
  const [showAccountManager, setShowAccountManager] = useState(false);
  const [bulkApplying, setBulkApplying] = useState({});
  const [bulkResults, setBulkResults] = useState({});

  useEffect(() => {
    fetchExchanges();
    loadAccounts();
    // Clear old storage format on first load
    clearLegacyStorage();
  }, []);

  const clearLegacyStorage = () => {
    // Clear old storage format and any saved accounts for fresh start
    localStorage.removeItem('trading_bot_exchange_accounts');
    localStorage.removeItem('trading_bot_accounts');
  };

  const loadAccounts = () => {
    const saved = localStorage.getItem('trading_bot_multi_exchange_accounts');
    if (saved) {
      try {
        setAccounts(JSON.parse(saved));
      } catch (error) {
        console.error('Failed to load accounts:', error);
      }
    }
  };

  const saveAccounts = (newAccounts) => {
    setAccounts(newAccounts);
    localStorage.setItem('trading_bot_multi_exchange_accounts', JSON.stringify(newAccounts));
  };

  const getAccountExchangeCredentials = (accountName, exchangeConnectionId) => {
    return accounts[accountName]?.exchanges?.[exchangeConnectionId] || null;
  };

  const saveAccountExchangeCredentials = (accountName, exchangeConnectionId, credData) => {
    const newAccounts = {
      ...accounts,
      [accountName]: {
        ...accounts[accountName],
        name: accountName,
        created: accounts[accountName]?.created || new Date().toISOString(),
        updated: new Date().toISOString(),
        exchanges: {
          ...accounts[accountName]?.exchanges,
          [exchangeConnectionId]: credData
        }
      }
    };
    saveAccounts(newAccounts);
  };

  const deleteAccount = (accountName) => {
    const newAccounts = { ...accounts };
    delete newAccounts[accountName];
    saveAccounts(newAccounts);
  };

  const loadAccountCredentialsForExchange = (accountName, exchangeConnectionId) => {
    const exchangeCreds = getAccountExchangeCredentials(accountName, exchangeConnectionId);
    if (exchangeCreds) {
      setCredentials({
        account_name: accountName,
        api_key: exchangeCreds.api_key || '',
        api_secret: exchangeCreds.api_secret || '',
        wallet_address: exchangeCreds.wallet_address || '',
        private_key: exchangeCreds.private_key || '',
        passphrase: exchangeCreds.passphrase || '',
        testnet: exchangeCreds.testnet || false
      });
    }
  };

  const isHyperliquid = (exchangeName) => {
    return exchangeName.toLowerCase() === 'hyperliquid';
  };

  const isBitget = (exchangeName) => {
    return exchangeName.toLowerCase() === 'bitget';
  };

  const fetchExchanges = async () => {
    try {
      setLoading(true);
      const response = await api.exchanges.getStatus();
      setExchanges(response.data.connections);
    } catch (error) {
      console.error('Failed to fetch exchanges:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleTestConnection = async (exchange) => {
    setTesting(prev => ({ ...prev, [exchange.connection_id]: true }));
    
    try {
      const response = await api.post(`/exchanges/${exchange.connection_id}/test`);
      await fetchExchanges(); // Refresh to get updated status
      console.log('Test result:', response.data);
    } catch (error) {
      console.error('Test failed:', error);
    } finally {
      setTesting(prev => ({ ...prev, [exchange.connection_id]: false }));
    }
  };

  const handleDeleteCredentials = async (exchange) => {
    setDeleting(prev => ({ ...prev, [exchange.connection_id]: true }));
    
    try {
      const response = await api.exchanges.deleteCredentials(exchange.connection_id);
      
      // Show success message
      setMessages(prev => ({
        ...prev,
        [exchange.connection_id]: {
          type: 'success',
          text: 'Credentials deleted successfully'
        }
      }));
      
      await fetchExchanges(); // Refresh to get updated status
      console.log('Delete result:', response.data);
      
      // Clear message after 3 seconds
      setTimeout(() => {
        setMessages(prev => {
          const newMessages = { ...prev };
          delete newMessages[exchange.connection_id];
          return newMessages;
        });
      }, 3000);
      
    } catch (error) {
      console.error('Delete failed:', error);
      setMessages(prev => ({
        ...prev,
        [exchange.connection_id]: {
          type: 'error',
          text: error.response?.data?.detail || 'Failed to delete credentials'
        }
      }));
      
      // Clear error message after 5 seconds
      setTimeout(() => {
        setMessages(prev => {
          const newMessages = { ...prev };
          delete newMessages[exchange.connection_id];
          return newMessages;
        });
      }, 5000);
    } finally {
      setDeleting(prev => ({ ...prev, [exchange.connection_id]: false }));
      setConfirmingDelete(null);
    }
  };

  const confirmDeleteCredentials = (exchange) => {
    setConfirmingDelete(exchange.connection_id);
  };

  const cancelDeleteCredentials = () => {
    setConfirmingDelete(null);
  };

  const testAllConnections = async () => {
    try {
      setTesting({ all: true });
      await api.exchanges.testAll();
      await fetchExchanges();
    } catch (error) {
      console.error('Failed to test all connections:', error);
    } finally {
      setTesting({ all: false });
    }
  };

  const handleCredentialsSubmit = async (e) => {
    e.preventDefault();
    if (!editingExchange) return;

    if (!credentials.account_name.trim()) {
      setCredentialsError('Please enter an account name');
      return;
    }

    setSubmitting(true);
    setCredentialsError('');
    setCredentialsSuccess('');

    try {
      // Prepare credentials based on exchange type
      const exchange = exchanges.find(ex => ex.connection_id === editingExchange);
      let credentialsData = {
        testnet: credentials.testnet
      };

      if (isHyperliquid(exchange.name)) {
        // Hyperliquid uses wallet_address and private_key
        credentialsData.wallet_address = credentials.wallet_address;
        credentialsData.private_key = credentials.private_key;
        // Also set api_key to wallet_address for compatibility
        credentialsData.api_key = credentials.wallet_address;
        credentialsData.api_secret = credentials.private_key;
      } else if (isBitget(exchange.name)) {
        // Bitget requires passphrase
        credentialsData.api_key = credentials.api_key;
        credentialsData.api_secret = credentials.api_secret;
        credentialsData.passphrase = credentials.passphrase;
      } else {
        // Standard exchanges
        credentialsData.api_key = credentials.api_key;
        credentialsData.api_secret = credentials.api_secret;
      }

      await api.exchanges.updateCredentials(editingExchange, credentialsData);
      
      // Save credentials to account system
      saveAccountExchangeCredentials(
        credentials.account_name, 
        exchange.connection_id,
        {
          ...credentialsData,
          wallet_address: credentials.wallet_address,
          private_key: credentials.private_key,
          passphrase: credentials.passphrase
        }
      );
      
      setCredentialsSuccess(`API credentials for account "${credentials.account_name}" updated and tested successfully!`);
      
      // Clear form and close modal after a delay
      setTimeout(() => {
        setEditingExchange(null);
        setCredentials({ 
          account_name: '',
          api_key: '', 
          api_secret: '', 
          wallet_address: '',
          private_key: '',
          passphrase: '',
          testnet: false 
        });
        setCredentialsSuccess('');
      }, 2000);
      
      await fetchExchanges();
    } catch (error) {
      console.error('Failed to update credentials:', error);
      
      // Extract error message from response
      let errorMessage = 'Failed to update credentials. Please try again.';
      
      if (error.response?.data?.detail) {
        errorMessage = error.response.data.detail;
      } else if (error.response?.data?.message) {
        errorMessage = error.response.data.message;
      } else if (error.message) {
        errorMessage = error.message;
      }
      
      // Handle specific error cases with detailed guidance
      if (errorMessage.includes('Invalid API-key, IP, or permissions for action') || errorMessage.includes('-2015')) {
        errorMessage = `Binance API Error (-2015): ${credentials.testnet ? 'Testnet' : 'Mainnet'} credentials invalid. Please check:
        • Are you using the correct API keys for ${credentials.testnet ? 'testnet' : 'mainnet'}?
        • Does your API key have "Spot & Margin Trading" permissions enabled?
        • Is your IP address whitelisted (if IP restrictions are enabled)?
        • Are the API key and secret correctly copied?`;
      } else if (errorMessage.includes('API-key format invalid') || errorMessage.includes('-2014')) {
        errorMessage = 'Binance API Error (-2014): Invalid API key format. Please check that your API key is correctly copied without extra spaces or characters.';
      } else if (errorMessage.includes('Signature for this request is not valid') || errorMessage.includes('-1022')) {
        errorMessage = 'Binance API Error (-1022): Invalid API secret. Please check that your secret key is correctly copied.';
      } else if (errorMessage.includes('invalid wallet') || errorMessage.includes('invalid address')) {
        errorMessage = 'Invalid wallet address. Please check that your Hyperliquid wallet address is correct.';
      } else if (errorMessage.includes('invalid private key')) {
        errorMessage = 'Invalid private key. Please check that your Hyperliquid private key is correct.';
      } else if (errorMessage.includes('API key is invalid') || errorMessage.includes('invalid')) {
        errorMessage = 'Invalid API credentials. Please verify your API key and secret are correct and have the required permissions.';
      } else if (errorMessage.includes('unauthorized') || errorMessage.includes('permission')) {
        errorMessage = 'API key does not have required permissions. Please enable trading permissions in your exchange account.';
      } else if (errorMessage.includes('network') || errorMessage.includes('timeout')) {
        errorMessage = 'Network error. Please check your connection and try again.';
      } else if (errorMessage.includes('IP')) {
        errorMessage = 'IP address not authorized. Please add your IP to the whitelist in your exchange API settings.';
      }
      
      setCredentialsError(errorMessage);
    } finally {
      setSubmitting(false);
    }
  };

  const openCredentialsModal = (exchange) => {
    setEditingExchange(exchange.connection_id);
    setCredentials({ 
      account_name: '',
      api_key: '', 
      api_secret: '', 
      wallet_address: '',
      private_key: '',
      passphrase: '',
      testnet: exchange.testnet 
    });
    setCredentialsError('');
    setCredentialsSuccess('');
  };

  const closeCredentialsModal = () => {
    setEditingExchange(null);
    setCredentialsError('');
    setCredentialsSuccess('');
  };

  const getStatusIcon = (status) => {
    switch (status) {
      case 'connected':
        return <CheckCircle className="h-5 w-5 text-green-500" />;
      case 'connecting':
        return <RefreshCw className="h-5 w-5 text-blue-500 animate-spin" />;
      case 'error':
        return <XCircle className="h-5 w-5 text-red-500" />;
      case 'invalid_credentials':
        return <Key className="h-5 w-5 text-yellow-500" />;
      case 'no_credentials':
        return <AlertTriangle className="h-5 w-5 text-orange-500" />;
      default:
        return <WifiOff className="h-5 w-5 text-gray-500" />;
    }
  };

  const getStatusBadge = (status) => {
    const baseClasses = "px-2 py-1 rounded-full text-xs font-medium";
    switch (status) {
      case 'connected':
        return `${baseClasses} bg-green-500/20 text-green-400`;
      case 'connecting':
        return `${baseClasses} bg-blue-500/20 text-blue-400`;
      case 'error':
        return `${baseClasses} bg-red-500/20 text-red-400`;
      case 'invalid_credentials':
        return `${baseClasses} bg-yellow-500/20 text-yellow-400`;
      case 'no_credentials':
        return `${baseClasses} bg-orange-500/20 text-orange-400`;
      default:
        return `${baseClasses} bg-gray-500/20 text-gray-400`;
    }
  };

  const getAccountsForExchange = (exchangeConnectionId) => {
    return Object.keys(accounts).filter(accountName => 
      accounts[accountName].exchanges?.[exchangeConnectionId]
    );
  };

  const applyAccountToAllExchanges = async (accountName) => {
    if (!accounts[accountName] || !accounts[accountName].exchanges) {
      console.error('Account not found or has no exchanges configured');
      return;
    }

    setBulkApplying(prev => ({ ...prev, [accountName]: true }));
    setBulkResults(prev => ({ ...prev, [accountName]: null }));

    try {
      const accountData = accounts[accountName];
      const credentials = {};

      // Prepare credentials for each exchange
      for (const [exchangeId, exchangeCreds] of Object.entries(accountData.exchanges)) {
        credentials[exchangeId] = {
          api_key: exchangeCreds.api_key || '',
          api_secret: exchangeCreds.api_secret || '',
          wallet_address: exchangeCreds.wallet_address || '',
          private_key: exchangeCreds.private_key || '',
          passphrase: exchangeCreds.passphrase || '',
          testnet: exchangeCreds.testnet || false
        };
      }

      // Apply credentials to all exchanges
      const response = await api.exchanges.applyBulkCredentials(accountName, credentials, true);
      
      setBulkResults(prev => ({ 
        ...prev, 
        [accountName]: {
          type: 'success',
          data: response.data,
          timestamp: new Date()
        }
      }));

      // Refresh exchange status after bulk application
      await fetchExchanges();

      // Auto-clear results after 10 seconds
      setTimeout(() => {
        setBulkResults(prev => ({ ...prev, [accountName]: null }));
      }, 10000);

    } catch (error) {
      console.error('Bulk credential application failed:', error);
      
      setBulkResults(prev => ({ 
        ...prev, 
        [accountName]: {
          type: 'error',
          message: error.userMessage || error.message || 'Failed to apply credentials to exchanges',
          timestamp: new Date()
        }
      }));

      // Auto-clear error after 10 seconds
      setTimeout(() => {
        setBulkResults(prev => ({ ...prev, [accountName]: null }));
      }, 10000);
    } finally {
      setBulkApplying(prev => ({ ...prev, [accountName]: false }));
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
          <h1 className="text-3xl font-bold text-white">Exchange Connections</h1>
          <p className="text-gray-400 mt-1">
            Manage API connections to cryptocurrency exchanges with multi-exchange accounts
          </p>
        </div>
        <div className="flex gap-3">
          <button
            onClick={() => setShowAccountManager(true)}
            className="flex items-center gap-2 bg-purple-600 hover:bg-purple-700 text-white px-4 py-2 rounded-lg transition-colors"
          >
            <Users className="h-4 w-4" />
            Manage Accounts ({Object.keys(accounts).length})
          </button>
          <button
            onClick={testAllConnections}
            disabled={testing.all}
            className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white px-4 py-2 rounded-lg transition-colors"
          >
            <TestTube className={`h-4 w-4 ${testing.all ? 'animate-pulse' : ''}`} />
            Test All
          </button>
          <button
            onClick={fetchExchanges}
            className="flex items-center gap-2 bg-gray-600 hover:bg-gray-700 text-white px-4 py-2 rounded-lg transition-colors"
          >
            <RefreshCw className="h-4 w-4" />
            Refresh
          </button>
        </div>
      </div>

      {/* Exchange Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {exchanges.map((exchange) => (
          <div key={exchange.connection_id} className="bg-gray-800 rounded-lg p-6">
            {/* Header */}
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-3">
                <div className="bg-gray-700 p-2 rounded-lg">
                  {getStatusIcon(exchange.status)}
                </div>
                <div>
                  <h3 className="font-semibold text-white capitalize">
                    {exchange.name}
                  </h3>
                  <p className="text-gray-400 text-sm">{exchange.exchange_type}</p>
                </div>
              </div>
              <span className={getStatusBadge(exchange.status)}>
                {exchange.status.replace('_', ' ')}
              </span>
            </div>

            {/* Status Details */}
            <div className="space-y-3 mb-4">
              <div className="flex justify-between text-sm">
                <span className="text-gray-400">API Keys:</span>
                <span className={exchange.has_credentials ? 'text-green-400' : 'text-red-400'}>
                  {exchange.has_credentials ? 'Configured' : 'Missing'}
                </span>
              </div>

              {/* Accounts with credentials for this exchange */}
              {getAccountsForExchange(exchange.connection_id).length > 0 && (
                <div className="text-sm">
                  <span className="text-gray-400">Saved Accounts:</span>
                  <div className="mt-1 space-y-1">
                    {getAccountsForExchange(exchange.connection_id).map(accountName => (
                      <div key={accountName} className="flex items-center justify-between text-xs bg-gray-700 rounded px-2 py-1">
                        <span className="text-white flex items-center gap-1">
                          <User className="h-3 w-3" />
                          {accountName}
                        </span>
                        <button
                          onClick={() => loadAccountCredentialsForExchange(accountName, exchange.connection_id)}
                          className="text-blue-400 hover:text-blue-300"
                          title="Load credentials for this exchange"
                        >
                          <Settings className="h-3 w-3" />
                        </button>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              
              {exchange.market_count > 0 && (
                <div className="flex justify-between text-sm">
                  <span className="text-gray-400">Markets:</span>
                  <span className="text-white">{exchange.market_count.toLocaleString()}</span>
                </div>
              )}

              {exchange.last_check && (
                <div className="flex justify-between text-sm">
                  <span className="text-gray-400">Last Check:</span>
                  <span className="text-white">
                    {new Date(exchange.last_check).toLocaleTimeString()}
                  </span>
                </div>
              )}

              {exchange.error_message && (
                <div className="bg-red-500/10 border border-red-500/20 rounded p-2">
                  <p className="text-red-400 text-xs">{exchange.error_message}</p>
                </div>
              )}
            </div>

            {/* Actions */}
            <div className="flex gap-2">
              <button
                onClick={() => handleTestConnection(exchange)}
                disabled={testing[exchange.connection_id]}
                className="flex-1 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white py-2 px-3 rounded text-sm transition-colors"
              >
                {testing[exchange.connection_id] ? (
                  <RefreshCw className="h-4 w-4 animate-spin mx-auto" />
                ) : (
                  'Test'
                )}
              </button>
              <button
                onClick={() => openCredentialsModal(exchange)}
                className="bg-gray-600 hover:bg-gray-700 text-white py-2 px-3 rounded text-sm transition-colors"
                title="Configure API credentials"
              >
                <Settings className="h-4 w-4" />
              </button>
              {exchange.has_credentials && (
                <button
                  onClick={() => confirmDeleteCredentials(exchange)}
                  disabled={deleting[exchange.connection_id]}
                  className="bg-red-600 hover:bg-red-700 disabled:opacity-50 text-white py-2 px-3 rounded text-sm transition-colors"
                  title="Delete credentials"
                >
                  {deleting[exchange.connection_id] ? (
                    <RefreshCw className="w-4 h-4 animate-spin" />
                  ) : (
                    <Trash2 className="w-4 h-4" />
                  )}
                </button>
              )}
            </div>

            {/* Message Display */}
            {messages[exchange.connection_id] && (
              <div className={`mt-2 p-2 rounded text-sm ${
                messages[exchange.connection_id].type === 'success' 
                  ? 'bg-green-500/10 text-green-400 border border-green-500/20' 
                  : 'bg-red-500/10 text-red-400 border border-red-500/20'
              }`}>
                <div className="flex items-center">
                  {messages[exchange.connection_id].type === 'success' ? (
                    <Check className="w-4 h-4 mr-2" />
                  ) : (
                    <AlertCircle className="w-4 h-4 mr-2" />
                  )}
                  {messages[exchange.connection_id].text}
                </div>
              </div>
            )}

            {/* Delete Confirmation Dialog */}
            {confirmingDelete === exchange.connection_id && (
              <div className="mt-2 p-3 bg-red-500/10 border border-red-500/20 rounded">
                <div className="flex items-center mb-2">
                  <AlertTriangle className="w-4 h-4 text-red-400 mr-2" />
                  <span className="text-sm font-medium text-red-400">
                    Delete Credentials
                  </span>
                </div>
                <p className="text-sm text-red-400 mb-3">
                  Are you sure you want to delete the API credentials for {exchange.name} ({exchange.exchange_type})? 
                  This action cannot be undone.
                </p>
                <div className="flex gap-2">
                  <button
                    onClick={() => handleDeleteCredentials(exchange)}
                    className="bg-red-600 hover:bg-red-700 text-white py-1 px-3 rounded text-sm transition-colors"
                  >
                    Delete
                  </button>
                  <button
                    onClick={cancelDeleteCredentials}
                    className="bg-gray-600 hover:bg-gray-700 text-white py-1 px-3 rounded text-sm transition-colors"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            )}
          </div>
        ))}
      </div>

      {/* Account Manager Modal */}
      {showAccountManager && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-gray-800 rounded-lg p-6 w-full max-w-2xl max-h-[90vh] overflow-y-auto">
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-xl font-semibold text-white">Account Manager</h2>
              <button
                onClick={() => setShowAccountManager(false)}
                className="text-gray-400 hover:text-white"
              >
                ✕
              </button>
            </div>

            <div className="space-y-4">
              {Object.keys(accounts).length === 0 ? (
                <div className="text-center py-8">
                  <Database className="h-16 w-16 text-gray-500 mx-auto mb-4" />
                  <h3 className="text-lg font-semibold text-white mb-2">No Accounts Created</h3>
                  <p className="text-gray-400">Create accounts by configuring exchange credentials</p>
                </div>
              ) : (
                Object.entries(accounts).map(([accountName, accountData]) => (
                  <div key={accountName} className="bg-gray-700 rounded-lg p-4">
                    <div className="flex justify-between items-start mb-3">
                      <div>
                        <h3 className="text-white font-semibold">{accountName}</h3>
                        <p className="text-gray-400 text-sm">
                          Created: {new Date(accountData.created).toLocaleDateString()}
                        </p>
                        <p className="text-gray-400 text-sm">
                          {Object.keys(accountData.exchanges || {}).length} exchanges configured
                        </p>
                      </div>
                      <div className="flex items-center gap-2">
                        <button
                          onClick={() => applyAccountToAllExchanges(accountName)}
                          disabled={bulkApplying[accountName] || Object.keys(accountData.exchanges || {}).length === 0}
                          className="flex items-center gap-1 bg-green-600 hover:bg-green-700 disabled:opacity-50 text-white px-3 py-1 rounded text-sm transition-colors"
                          title="Apply credentials to all exchanges"
                        >
                          {bulkApplying[accountName] ? (
                            <>
                              <RefreshCw className="h-3 w-3 animate-spin" />
                              Applying...
                            </>
                          ) : (
                            <>
                              <Save className="h-3 w-3" />
                              Apply to All
                            </>
                          )}
                        </button>
                        <button
                          onClick={() => {
                            if (window.confirm(`Are you sure you want to delete the account "${accountName}"?`)) {
                              deleteAccount(accountName);
                            }
                          }}
                          className="text-red-400 hover:text-red-300"
                        >
                          <Trash2 className="h-4 w-4" />
                        </button>
                      </div>
                    </div>

                    {/* Bulk Application Results */}
                    {bulkResults[accountName] && (
                      <div className={`mb-3 p-3 rounded-lg border ${
                        bulkResults[accountName].type === 'success' 
                          ? 'bg-green-500/10 border-green-500/20' 
                          : 'bg-red-500/10 border-red-500/20'
                      }`}>
                        <div className="flex items-center gap-2 mb-2">
                          {bulkResults[accountName].type === 'success' ? (
                            <CheckCircle className="h-4 w-4 text-green-400" />
                          ) : (
                            <XCircle className="h-4 w-4 text-red-400" />
                          )}
                          <span className={`text-sm font-medium ${
                            bulkResults[accountName].type === 'success' ? 'text-green-400' : 'text-red-400'
                          }`}>
                            {bulkResults[accountName].type === 'success' ? 'Bulk Application Results' : 'Bulk Application Failed'}
                          </span>
                        </div>
                        
                        {bulkResults[accountName].type === 'success' ? (
                          <div className="space-y-1">
                            <p className="text-green-400 text-sm">
                              {bulkResults[accountName].data.message}
                            </p>
                            <div className="grid grid-cols-2 gap-4 text-xs">
                              <div className="text-green-400">
                                ✅ Successful: {bulkResults[accountName].data.successful}
                              </div>
                              <div className="text-red-400">
                                ❌ Failed: {bulkResults[accountName].data.failed}
                              </div>
                            </div>
                            {/* Detailed results */}
                            {bulkResults[accountName].data.results.length > 0 && (
                              <details className="mt-2">
                                <summary className="text-xs text-gray-400 cursor-pointer hover:text-white">
                                  View detailed results
                                </summary>
                                <div className="mt-2 space-y-1 max-h-32 overflow-y-auto">
                                  {bulkResults[accountName].data.results.map((result, index) => (
                                    <div key={index} className="flex items-center justify-between bg-gray-600 rounded px-2 py-1">
                                      <span className="text-xs text-white">
                                        {result.exchange_name} ({result.exchange_type})
                                      </span>
                                      <div className="flex items-center gap-1">
                                        {result.success ? (
                                          <>
                                            <Check className="h-3 w-3 text-green-400" />
                                            {result.test_success === false && (
                                              <AlertTriangle className="h-3 w-3 text-yellow-400" title={`Test failed: ${result.test_error}`} />
                                            )}
                                          </>
                                        ) : (
                                          <XCircle className="h-3 w-3 text-red-400" title={result.error} />
                                        )}
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              </details>
                            )}
                          </div>
                        ) : (
                          <p className="text-red-400 text-sm">
                            {bulkResults[accountName].message}
                          </p>
                        )}
                      </div>
                    )}

                    {/* Exchange credentials for this account */}
                    <div className="space-y-2">
                      <h4 className="text-gray-300 text-sm font-medium">Configured Exchanges:</h4>
                      {Object.entries(accountData.exchanges || {}).map(([exchangeId, creds]) => {
                        const exchange = exchanges.find(ex => ex.connection_id === exchangeId);
                        return (
                          <div key={exchangeId} className="flex items-center justify-between bg-gray-600 rounded px-3 py-2">
                            <span className="text-white text-sm">
                              {exchange ? `${exchange.name} (${exchange.exchange_type})` : exchangeId}
                            </span>
                            <div className="flex items-center gap-2">
                              <span className={`text-xs px-2 py-1 rounded ${
                                creds.testnet ? 'bg-yellow-500/20 text-yellow-400' : 'bg-green-500/20 text-green-400'
                              }`}>
                                {creds.testnet ? 'Testnet' : 'Mainnet'}
                              </span>
                              {exchange && (
                                <button
                                  onClick={() => {
                                    loadAccountCredentialsForExchange(accountName, exchangeId);
                                    setEditingExchange(exchangeId);
                                    setShowAccountManager(false);
                                  }}
                                  className="text-blue-400 hover:text-blue-300"
                                  title="Edit credentials"
                                >
                                  <Settings className="h-3 w-3" />
                                </button>
                              )}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                ))
              )}
            </div>

            <div className="mt-4 pt-4 border-t border-gray-600">
              <button
                onClick={() => setShowAccountManager(false)}
                className="w-full bg-gray-600 hover:bg-gray-700 text-white py-2 rounded transition-colors"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Credentials Modal */}
      {editingExchange && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-gray-800 rounded-lg p-6 w-full max-w-md max-h-[90vh] overflow-y-auto">
            {(() => {
              const exchange = exchanges.find(ex => ex.connection_id === editingExchange);
              return (
                <>
                  <h2 className="text-xl font-semibold text-white mb-4">
                    Configure {exchange?.name} API Credentials
                  </h2>
                  
                  {/* Success Message */}
                  {credentialsSuccess && (
                    <div className="mb-4 p-3 bg-green-500/10 border border-green-500/20 rounded-lg flex items-center gap-2">
                      <Check className="h-4 w-4 text-green-400" />
                      <span className="text-green-400 text-sm">{credentialsSuccess}</span>
                    </div>
                  )}

                  {/* Error Message */}
                  {credentialsError && (
                    <div className="mb-4 p-3 bg-red-500/10 border border-red-500/20 rounded-lg flex items-start gap-2">
                      <AlertCircle className="h-4 w-4 text-red-400 mt-0.5 flex-shrink-0" />
                      <span className="text-red-400 text-sm">{credentialsError}</span>
                    </div>
                  )}
                  
                  <form onSubmit={handleCredentialsSubmit} className="space-y-4">
                    {/* Account Name */}
                    <div>
                      <label className="block text-sm font-medium text-gray-300 mb-2">
                        Account Name
                      </label>
                      <input
                        type="text"
                        value={credentials.account_name}
                        onChange={(e) => setCredentials(prev => ({ ...prev, account_name: e.target.value }))}
                        className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-white"
                        placeholder="e.g., Main Account, Trading Bot 1"
                        required
                      />
                      <p className="text-xs text-gray-400 mt-1">
                        This account can store credentials for multiple exchanges
                      </p>
                    </div>

                    {/* Load Account Dropdown */}
                    {Object.keys(accounts).length > 0 && (
                      <div>
                        <label className="block text-sm font-medium text-gray-300 mb-2">
                          Load Existing Account
                        </label>
                        <select
                          onChange={(e) => {
                            if (e.target.value) {
                              loadAccountCredentialsForExchange(e.target.value, exchange.connection_id);
                            }
                          }}
                          className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-white"
                        >
                          <option value="">Select an account...</option>
                          {Object.keys(accounts).map(accountName => (
                            <option key={accountName} value={accountName}>
                              {accountName} ({Object.keys(accounts[accountName].exchanges || {}).length} exchanges)
                            </option>
                          ))}
                        </select>
                      </div>
                    )}

                    {/* Exchange-specific fields */}
                    {isHyperliquid(exchange?.name) ? (
                      // Hyperliquid specific fields
                      <>
                        <div>
                          <label className="block text-sm font-medium text-gray-300 mb-2">
                            Wallet Address
                          </label>
                          <input
                            type="text"
                            value={credentials.wallet_address}
                            onChange={(e) => setCredentials(prev => ({ ...prev, wallet_address: e.target.value }))}
                            className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-white"
                            placeholder="0x..."
                            required
                          />
                          <p className="text-xs text-gray-400 mt-1">
                            Your Hyperliquid wallet address (starts with 0x)
                          </p>
                        </div>
                        
                        <div>
                          <label className="block text-sm font-medium text-gray-300 mb-2">
                            Private Key
                          </label>
                          <div className="relative">
                            <input
                              type={showCredentials[editingExchange] ? "text" : "password"}
                              value={credentials.private_key}
                              onChange={(e) => setCredentials(prev => ({ ...prev, private_key: e.target.value }))}
                              className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-white pr-10"
                              placeholder="Enter Private Key"
                              required
                            />
                            <button
                              type="button"
                              onClick={() => setShowCredentials(prev => ({ 
                                ...prev, 
                                [editingExchange]: !prev[editingExchange] 
                              }))}
                              className="absolute right-2 top-2 text-gray-400 hover:text-white"
                            >
                              {showCredentials[editingExchange] ? <EyeOff className="h-5 w-5" /> : <Eye className="h-5 w-5" />}
                            </button>
                          </div>
                          <p className="text-xs text-gray-400 mt-1">
                            ⚠️ Your wallet's private key (never share this!)
                          </p>
                        </div>
                      </>
                    ) : (
                      // Standard API Key/Secret fields
                      <>
                        <div>
                          <label className="block text-sm font-medium text-gray-300 mb-2">
                            API Key
                          </label>
                          <input
                            type="text"
                            value={credentials.api_key}
                            onChange={(e) => setCredentials(prev => ({ ...prev, api_key: e.target.value }))}
                            className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-white"
                            placeholder="Enter API Key"
                            required
                          />
                        </div>
                        
                        <div>
                          <label className="block text-sm font-medium text-gray-300 mb-2">
                            API Secret
                          </label>
                          <div className="relative">
                            <input
                              type={showCredentials[editingExchange] ? "text" : "password"}
                              value={credentials.api_secret}
                              onChange={(e) => setCredentials(prev => ({ ...prev, api_secret: e.target.value }))}
                              className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-white pr-10"
                              placeholder="Enter API Secret"
                              required
                            />
                            <button
                              type="button"
                              onClick={() => setShowCredentials(prev => ({ 
                                ...prev, 
                                [editingExchange]: !prev[editingExchange] 
                              }))}
                              className="absolute right-2 top-2 text-gray-400 hover:text-white"
                            >
                              {showCredentials[editingExchange] ? <EyeOff className="h-5 w-5" /> : <Eye className="h-5 w-5" />}
                            </button>
                          </div>
                        </div>

                        {/* Passphrase for Bitget */}
                        {isBitget(exchange?.name) && (
                          <div>
                            <label className="block text-sm font-medium text-gray-300 mb-2">
                              Passphrase
                            </label>
                            <input
                              type="text"
                              value={credentials.passphrase}
                              onChange={(e) => setCredentials(prev => ({ ...prev, passphrase: e.target.value }))}
                              className="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2 text-white"
                              placeholder="Enter Passphrase"
                              required
                            />
                            <p className="text-xs text-gray-400 mt-1">
                              Bitget requires a passphrase for API access
                            </p>
                          </div>
                        )}
                      </>
                    )}

                    <div>
                      <label className="block text-sm font-medium text-gray-300 mb-2">
                        Environment
                      </label>
                      <div className="flex items-center space-x-4">
                        <label className="flex items-center">
                          <input
                            type="radio"
                            name="environment"
                            checked={!credentials.testnet}
                            onChange={() => setCredentials(prev => ({ ...prev, testnet: false }))}
                            className="mr-2"
                          />
                          <span className="text-white">Mainnet (Live Trading)</span>
                        </label>
                        <label className="flex items-center">
                          <input
                            type="radio"
                            name="environment"
                            checked={credentials.testnet}
                            onChange={() => setCredentials(prev => ({ ...prev, testnet: true }))}
                            className="mr-2"
                          />
                          <span className="text-white">Testnet (Paper Trading)</span>
                        </label>
                      </div>
                      <p className="text-xs text-gray-400 mt-1">
                        {credentials.testnet 
                          ? "⚠️ Testnet mode: Create API keys from your exchange's testnet/sandbox environment"
                          : "🚨 Mainnet mode: Real trading with real money - ensure you trust this system"
                        }
                      </p>
                    </div>

                    <div className="flex gap-3 pt-4">
                      <button
                        type="submit"
                        disabled={submitting}
                        className="flex-1 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white py-2 rounded transition-colors flex items-center justify-center gap-2"
                      >
                        {submitting ? (
                          <>
                            <RefreshCw className="h-4 w-4 animate-spin" />
                            Testing...
                          </>
                        ) : (
                          <>
                            <Save className="h-4 w-4" />
                            Save & Test
                          </>
                        )}
                      </button>
                      <button
                        type="button"
                        onClick={closeCredentialsModal}
                        disabled={submitting}
                        className="flex-1 bg-gray-600 hover:bg-gray-700 disabled:opacity-50 text-white py-2 rounded transition-colors"
                      >
                        Cancel
                      </button>
                    </div>
                  </form>
                </>
              );
            })()}
          </div>
        </div>
      )}
    </div>
  );
};

export default ExchangeStatus; 