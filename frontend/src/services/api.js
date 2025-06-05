import axios from 'axios';

// Enhanced API client configuration
export const apiClient = axios.create({
  baseURL: process.env.REACT_APP_API_URL || 'http://localhost:8000',
  timeout: 30000, // Increased to 30 seconds
  headers: {
    'Content-Type': 'application/json',
  },
});

// Retry configuration
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // 1 second base delay

// Connection status tracker
let connectionStatus = {
  isOnline: true,
  lastError: null,
  retryCount: 0
};

// Retry logic with exponential backoff
const retryRequest = async (error, retryCount = 0) => {
  if (retryCount >= MAX_RETRIES) {
    throw error;
  }

  // Only retry on network errors or 5xx server errors
  const shouldRetry = 
    !error.response || // Network error
    error.code === 'ECONNABORTED' || // Timeout
    error.code === 'NETWORK_ERROR' ||
    (error.response && error.response.status >= 500); // Server error

  if (!shouldRetry) {
    throw error;
  }

  // Exponential backoff: 1s, 2s, 4s
  const delay = RETRY_DELAY * Math.pow(2, retryCount);
  
  console.log(`Retrying request in ${delay}ms (attempt ${retryCount + 1}/${MAX_RETRIES})...`);
  
  await new Promise(resolve => setTimeout(resolve, delay));
  
  // Retry the original request
  try {
    return await apiClient.request(error.config);
  } catch (retryError) {
    return retryRequest(retryError, retryCount + 1);
  }
};

// Request interceptor
apiClient.interceptors.request.use(
  (config) => {
    // Add timestamp to prevent caching issues
    config.params = {
      ...config.params,
      _t: Date.now()
    };
    
    // Reset retry count for new requests
    connectionStatus.retryCount = 0;
    
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Enhanced response interceptor with retry logic
apiClient.interceptors.response.use(
  (response) => {
    // Reset connection status on successful response
    connectionStatus.isOnline = true;
    connectionStatus.lastError = null;
    connectionStatus.retryCount = 0;
    
    return response;
  },
  async (error) => {
    // Update connection status
    connectionStatus.isOnline = false;
    connectionStatus.lastError = error;
    connectionStatus.retryCount++;

    // Log detailed error information
    console.error('API Error Details:', {
      message: error.message,
      code: error.code,
      status: error.response?.status,
      statusText: error.response?.statusText,
      url: error.config?.url,
      method: error.config?.method,
      retryCount: connectionStatus.retryCount
    });

    // Try to retry the request
    try {
      const result = await retryRequest(error);
      connectionStatus.isOnline = true;
      connectionStatus.lastError = null;
      return result;
    } catch (finalError) {
      // All retries failed
      console.error('All retry attempts failed:', finalError);
      
      // Provide user-friendly error messages
      if (finalError.code === 'ECONNABORTED') {
        finalError.userMessage = 'Request timed out. Please check your connection and try again.';
      } else if (finalError.code === 'NETWORK_ERROR' || !finalError.response) {
        finalError.userMessage = 'Network error. Please check your internet connection.';
      } else if (finalError.response?.status >= 500) {
        finalError.userMessage = 'Server error. Please try again in a few moments.';
      } else if (finalError.response?.status === 404) {
        finalError.userMessage = 'Requested resource not found.';
      } else if (finalError.response?.status === 401) {
        finalError.userMessage = 'Authentication required.';
      } else if (finalError.response?.status === 403) {
        finalError.userMessage = 'Access denied.';
      } else {
        finalError.userMessage = finalError.response?.data?.detail || 'An unexpected error occurred.';
      }
      
      return Promise.reject(finalError);
    }
  }
);

// Health check function
export const checkAPIHealth = async () => {
  try {
    const response = await apiClient.get('/api/system/health', { timeout: 5000 });
    connectionStatus.isOnline = true;
    connectionStatus.lastError = null;
    return { healthy: true, response: response.data };
  } catch (error) {
    connectionStatus.isOnline = false;
    connectionStatus.lastError = error;
    return { healthy: false, error: error.userMessage || error.message };
  }
};

// Get current connection status
export const getConnectionStatus = () => ({ ...connectionStatus });

// Enhanced API service functions with better error handling
const createAPIFunction = (requestFunction) => {
  return async (...args) => {
    try {
      return await requestFunction(...args);
    } catch (error) {
      // Re-throw with enhanced error info for UI handling
      throw {
        ...error,
        isConnectionError: !error.response || error.code === 'NETWORK_ERROR',
        userMessage: error.userMessage || 'An error occurred while communicating with the server.'
      };
    }
  };
};

// API service functions
export const api = {
  // System endpoints
  system: {
    getStatus: createAPIFunction(() => apiClient.get('/api/system/status')),
    getDashboard: createAPIFunction(() => apiClient.get('/api/system/dashboard')),
    getHealth: createAPIFunction(() => apiClient.get('/api/system/health')),
    getMetrics: createAPIFunction(() => apiClient.get('/api/system/metrics')),
  },

  // Bot endpoints
  bots: {
    getAll: createAPIFunction(() => apiClient.get('/api/bots/')),
    getRunning: createAPIFunction(() => apiClient.get('/api/bots/running')),
    getById: createAPIFunction((id) => apiClient.get(`/api/bots/${id}`)),
    create: createAPIFunction((botConfig) => apiClient.post('/api/bots/', botConfig)),
    start: createAPIFunction((id) => apiClient.post(`/api/bots/${id}/start`)),
    stop: createAPIFunction((id) => apiClient.post(`/api/bots/${id}/stop`)),
    delete: createAPIFunction((id) => apiClient.delete(`/api/bots/${id}`)),
    getStatus: createAPIFunction(() => apiClient.get('/api/bots/status/summary')),
  },

  // Exchange endpoints
  exchanges: {
    getAll: createAPIFunction(() => apiClient.get('/api/exchanges/')),
    getStatus: createAPIFunction(() => apiClient.get('/api/exchanges/status')),
    getConnected: createAPIFunction(() => apiClient.get('/api/exchanges/connected')),
    getSupported: createAPIFunction(() => apiClient.get('/api/exchanges/supported')),
    getById: createAPIFunction((id) => apiClient.get(`/api/exchanges/${id}`)),
    test: createAPIFunction((id) => apiClient.post(`/api/exchanges/${id}/test`)),
    testAll: createAPIFunction(() => apiClient.post('/api/exchanges/test-all')),
    updateCredentials: createAPIFunction((id, credentials) => 
      apiClient.post(`/api/exchanges/${id}/credentials`, credentials)),
    applyBulkCredentials: createAPIFunction((accountName, credentials, testConnections = true) => 
      apiClient.post('/api/exchanges/bulk-credentials', {
        account_name: accountName,
        credentials: credentials,
        test_connections: testConnections
      })),
    deleteCredentials: createAPIFunction((id) => 
      apiClient.delete(`/api/exchanges/${id}/credentials`)),
    getMarkets: createAPIFunction((id, limit = 100) => 
      apiClient.get(`/api/exchanges/${id}/markets?limit=${limit}`)),
    getBalance: createAPIFunction((id) => apiClient.get(`/api/exchanges/${id}/balance`)),
    getHealth: createAPIFunction((id) => apiClient.get(`/api/exchanges/${id}/health`)),
  },

  // Strategy endpoints
  strategies: {
    getAvailable: createAPIFunction(() => apiClient.get('/api/strategies/available-strategies')),
    
    // Passive quoting specific
    passiveQuoting: {
      getConfig: createAPIFunction(() => apiClient.get('/api/strategies/passive-quoting/config')),
      updateConfig: createAPIFunction((config) => apiClient.post('/api/strategies/passive-quoting/config', config)),
      getPresets: createAPIFunction(() => apiClient.get('/api/strategies/passive-quoting/presets')),
      savePreset: createAPIFunction((name, config) => apiClient.post(`/api/strategies/passive-quoting/presets/${name}`, config)),
      deletePreset: createAPIFunction((name) => apiClient.delete(`/api/strategies/passive-quoting/presets/${name}`)),
      createBot: createAPIFunction((request) => apiClient.post('/api/strategies/passive-quoting/create', request)),
      quickUpdate: createAPIFunction((request) => apiClient.post('/api/strategies/passive-quoting/quick-update', request)),
      getBotStatus: createAPIFunction((instanceId) => apiClient.get(`/api/strategies/passive-quoting/${instanceId}/status`)),
    },
    validateSymbol: createAPIFunction((symbol, exchanges = null) => 
      apiClient.post('/api/strategies/validate-symbol', { symbol, exchanges })),
    getAvailableStrategies: createAPIFunction(() => apiClient.get('/api/strategies/available-strategies')),
  },
  
  // Convenience methods for direct API calls
  get: createAPIFunction((url) => apiClient.get(url)),
  post: createAPIFunction((url, data) => apiClient.post(url, data)),
  put: createAPIFunction((url, data) => apiClient.put(url, data)),
  delete: createAPIFunction((url) => apiClient.delete(url)),

  // Health and connection utilities
  health: {
    check: checkAPIHealth,
    getStatus: getConnectionStatus,
  }
};

export default api;
