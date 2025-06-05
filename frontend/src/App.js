import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import './App.css';
import Dashboard from './components/Dashboard';
import ExchangeStatus from './components/ExchangeStatus';
import BotManager from './components/BotManager';
import RiskMonitoring from './components/RiskMonitoring';
import Navbar from './components/Navbar';
import ConnectionStatus from './components/ConnectionStatus';
import { api } from './services/api';

function App() {
  const [systemStatus, setSystemStatus] = useState(null);
  const [loading, setLoading] = useState(true);

  // Fetch system status on mount only (removed auto-refresh)
  useEffect(() => {
    fetchSystemStatus();
  }, []);

  const fetchSystemStatus = async () => {
    try {
      const response = await api.system.getDashboard();
      setSystemStatus(response.data);
    } catch (err) {
      console.error('Failed to fetch system status:', err);
      // Don't set error state - let ConnectionStatus handle it
      // Individual components will handle their own error states gracefully
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto mb-4"></div>
          <p className="text-gray-300">Loading Trading Bot System...</p>
        </div>
      </div>
    );
  }

  return (
    <Router>
      <div className="min-h-screen bg-gray-900 text-white">
        {/* Global Connection Status Monitor */}
        <ConnectionStatus />
        
        <Navbar systemStatus={systemStatus} />
        
        <main className="container mx-auto px-4 py-6">
          <Routes>
            <Route 
              path="/" 
              element={<Dashboard systemStatus={systemStatus} onRefresh={fetchSystemStatus} />} 
            />
            <Route 
              path="/exchanges" 
              element={<ExchangeStatus onRefresh={fetchSystemStatus} />} 
            />
            <Route 
              path="/bots" 
              element={<BotManager onRefresh={fetchSystemStatus} />} 
            />
            <Route 
              path="/risk" 
              element={<RiskMonitoring onRefresh={fetchSystemStatus} />} 
            />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </main>
      </div>
    </Router>
  );
}

export default App;
