import React, { useState, useEffect, useCallback } from 'react';
import { AlertTriangle, CheckCircle, RefreshCw, Wifi, WifiOff } from 'lucide-react';
import { api, checkAPIHealth, getConnectionStatus } from '../services/api';

const ConnectionStatus = () => {
  const [status, setStatus] = useState({
    isOnline: true,
    isChecking: false,
    lastError: null,
    autoRetrying: false,
    retryCount: 0
  });

  const [showBanner, setShowBanner] = useState(false);
  const [retryTimeout, setRetryTimeout] = useState(null);

  // Check connection health
  const checkConnection = useCallback(async (isAutoRetry = false) => {
    setStatus(prev => ({ 
      ...prev, 
      isChecking: true,
      autoRetrying: isAutoRetry
    }));

    try {
      const healthCheck = await checkAPIHealth();
      const connStatus = getConnectionStatus();
      
      setStatus(prev => ({
        ...prev,
        isOnline: healthCheck.healthy && connStatus.isOnline,
        lastError: healthCheck.healthy ? null : healthCheck.error,
        isChecking: false,
        autoRetrying: false,
        retryCount: healthCheck.healthy ? 0 : prev.retryCount
      }));

      // Hide banner if connection is restored
      if (healthCheck.healthy) {
        setShowBanner(false);
        if (retryTimeout) {
          clearTimeout(retryTimeout);
          setRetryTimeout(null);
        }
      } else {
        setShowBanner(true);
        scheduleAutoRetry();
      }

    } catch (error) {
      setStatus(prev => ({
        ...prev,
        isOnline: false,
        lastError: error.userMessage || error.message || 'Connection check failed',
        isChecking: false,
        autoRetrying: false,
        retryCount: prev.retryCount + 1
      }));
      setShowBanner(true);
      scheduleAutoRetry();
    }
  }, [retryTimeout]);

  // Schedule automatic retry with exponential backoff
  const scheduleAutoRetry = useCallback(() => {
    if (retryTimeout) {
      clearTimeout(retryTimeout);
    }

    // Progressive retry intervals: 5s, 10s, 20s, 30s (max)
    const retryDelay = Math.min(5000 * Math.pow(2, status.retryCount), 30000);
    
    const timeout = setTimeout(() => {
      checkConnection(true);
    }, retryDelay);
    
    setRetryTimeout(timeout);
  }, [checkConnection, status.retryCount, retryTimeout]);

  // Manual retry
  const handleManualRetry = () => {
    if (retryTimeout) {
      clearTimeout(retryTimeout);
      setRetryTimeout(null);
    }
    checkConnection(false);
  };

  // Dismiss banner temporarily
  const dismissBanner = () => {
    setShowBanner(false);
    // Auto-show again after 30 seconds if still offline
    setTimeout(() => {
      if (!status.isOnline) {
        setShowBanner(true);
      }
    }, 30000);
  };

  // Monitor connection status periodically
  useEffect(() => {
    // Initial check
    checkConnection();

    // Set up periodic health checks (every 60 seconds)
    const healthInterval = setInterval(() => {
      const connStatus = getConnectionStatus();
      if (!connStatus.isOnline) {
        checkConnection(true);
      }
    }, 60000);

    // Listen for online/offline events
    const handleOnline = () => {
      console.log('Browser came online, checking API...');
      checkConnection(true);
    };

    const handleOffline = () => {
      console.log('Browser went offline');
      setStatus(prev => ({
        ...prev,
        isOnline: false,
        lastError: 'No internet connection'
      }));
      setShowBanner(true);
    };

    window.addEventListener('online', handleOnline);
    window.addEventListener('offline', handleOffline);

    return () => {
      clearInterval(healthInterval);
      if (retryTimeout) {
        clearTimeout(retryTimeout);
      }
      window.removeEventListener('online', handleOnline);
      window.removeEventListener('offline', handleOffline);
    };
  }, [checkConnection, retryTimeout]);

  // Don't render anything if connection is good and banner is hidden
  if (status.isOnline && !showBanner) {
    return null;
  }

  return (
    <>
      {/* Connection Banner */}
      {showBanner && (
        <div className={`fixed top-0 left-0 right-0 z-50 ${
          status.isOnline ? 'bg-green-600' : 'bg-red-600'
        } text-white px-4 py-3 shadow-lg`}>
          <div className="flex items-center justify-between max-w-7xl mx-auto">
            <div className="flex items-center gap-3">
              {status.isChecking || status.autoRetrying ? (
                <RefreshCw className="h-5 w-5 animate-spin" />
              ) : status.isOnline ? (
                <CheckCircle className="h-5 w-5" />
              ) : (
                <AlertTriangle className="h-5 w-5" />
              )}
              
              <div>
                <div className="font-medium">
                  {status.isChecking && !status.autoRetrying && 'Checking connection...'}
                  {status.autoRetrying && `Auto-retrying connection... (attempt ${status.retryCount})`}
                  {!status.isChecking && !status.autoRetrying && status.isOnline && 'Connection restored!'}
                  {!status.isChecking && !status.autoRetrying && !status.isOnline && 'Connection Error'}
                </div>
                
                {status.lastError && !status.isOnline && (
                  <div className="text-sm opacity-90">
                    {status.lastError}
                  </div>
                )}
              </div>
            </div>

            <div className="flex items-center gap-2">
              {!status.isOnline && !status.isChecking && (
                <button
                  onClick={handleManualRetry}
                  className="bg-white bg-opacity-20 hover:bg-opacity-30 px-3 py-1 rounded text-sm font-medium transition-colors"
                >
                  Retry Now
                </button>
              )}
              
              <button
                onClick={dismissBanner}
                className="bg-white bg-opacity-20 hover:bg-opacity-30 p-1 rounded transition-colors"
                title="Dismiss"
              >
                ✕
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Connection Status Indicator (bottom right) */}
      <div className="fixed bottom-4 right-4 z-40">
        <div className={`flex items-center gap-2 px-3 py-2 rounded-lg shadow-lg ${
          status.isOnline 
            ? 'bg-green-600 text-white' 
            : 'bg-red-600 text-white'
        }`}>
          {status.isChecking || status.autoRetrying ? (
            <RefreshCw className="h-4 w-4 animate-spin" />
          ) : status.isOnline ? (
            <Wifi className="h-4 w-4" />
          ) : (
            <WifiOff className="h-4 w-4" />
          )}
          
          <span className="text-sm font-medium">
            {status.isChecking && 'Checking...'}
            {status.autoRetrying && 'Retrying...'}
            {!status.isChecking && !status.autoRetrying && (status.isOnline ? 'Online' : 'Offline')}
          </span>
        </div>
      </div>
    </>
  );
};

export default ConnectionStatus; 