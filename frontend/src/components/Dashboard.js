import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { 
  Activity, 
  TrendingUp, 
  Zap, 
  AlertCircle, 
  CheckCircle, 
  RefreshCw,
  Play,
  Pause,
  Settings
} from 'lucide-react';

const Dashboard = ({ systemStatus, onRefresh }) => {
  const [refreshing, setRefreshing] = useState(false);

  const handleRefresh = async () => {
    setRefreshing(true);
    await onRefresh();
    setRefreshing(false);
  };

  if (!systemStatus) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500"></div>
      </div>
    );
  }

  const overview = systemStatus?.overview || { bots: { running: 0, total: 0 }, exchanges: { connected: 0, total: 0 }, bots_status: {}, strategy_breakdown: {} };
  const running_bots = systemStatus?.running_bots || [];
  const connected_exchanges = systemStatus?.connected_exchanges || [];
  const last_updated = systemStatus?.last_updated || Date.now();

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-white">Trading Bot Dashboard</h1>
          <p className="text-gray-400 mt-1">
            Last updated: {new Date(last_updated).toLocaleString()}
          </p>
        </div>
        <button
          onClick={handleRefresh}
          disabled={refreshing}
          className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white px-4 py-2 rounded-lg transition-colors"
        >
          <RefreshCw className={`h-4 w-4 ${refreshing ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* Status Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {/* Bot Status */}
        <div className="bg-gray-800 rounded-lg p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-gray-400 text-sm">Running Bots</p>
              <p className="text-2xl font-bold text-white">
                {overview.bots.running}/{overview.bots.total}
              </p>
            </div>
            <div className="bg-green-500/20 p-3 rounded-lg">
              <Activity className="h-6 w-6 text-green-500" />
            </div>
          </div>
          <div className="mt-4">
            <Link
              to="/bots"
              className="text-blue-400 hover:text-blue-300 text-sm flex items-center gap-1"
            >
              Manage Bots →
            </Link>
          </div>
        </div>

        {/* Exchange Status */}
        <div className="bg-gray-800 rounded-lg p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-gray-400 text-sm">Connected Exchanges</p>
              <p className="text-2xl font-bold text-white">
                {overview.exchanges.connected}/{overview.exchanges.total}
              </p>
            </div>
            <div className="bg-blue-500/20 p-3 rounded-lg">
              <TrendingUp className="h-6 w-6 text-blue-500" />
            </div>
          </div>
          <div className="mt-4">
            <Link
              to="/exchanges"
              className="text-blue-400 hover:text-blue-300 text-sm flex items-center gap-1"
            >
              Manage Exchanges →
            </Link>
          </div>
        </div>

        {/* System Status */}
        <div className="bg-gray-800 rounded-lg p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-gray-400 text-sm">System Status</p>
              <p className="text-2xl font-bold text-green-400">Operational</p>
            </div>
            <div className="bg-green-500/20 p-3 rounded-lg">
              <CheckCircle className="h-6 w-6 text-green-500" />
            </div>
          </div>
          <div className="mt-4">
            <span className="text-green-400 text-sm">All systems online</span>
          </div>
        </div>

        {/* Performance */}
        <div className="bg-gray-800 rounded-lg p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-gray-400 text-sm">Active Strategies</p>
              <p className="text-2xl font-bold text-white">
                {Object.keys(overview.bots.strategy_breakdown || {}).length}
              </p>
            </div>
            <div className="bg-purple-500/20 p-3 rounded-lg">
              <Zap className="h-6 w-6 text-purple-500" />
            </div>
          </div>
          <div className="mt-4">
            <span className="text-gray-400 text-sm">Strategies running</span>
          </div>
        </div>
      </div>

      {/* Running Bots Section */}
      <div className="bg-gray-800 rounded-lg p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-xl font-semibold text-white flex items-center gap-2">
            <Play className="h-5 w-5 text-green-500" />
            Running Bot Instances
          </h2>
          <Link
            to="/bots"
            className="text-blue-400 hover:text-blue-300 text-sm"
          >
            View All
          </Link>
        </div>

        {running_bots && running_bots.length > 0 ? (
          <div className="space-y-3">
            {running_bots.map((bot) => (
              <div
                key={bot.instance_id}
                className="bg-gray-700 rounded-lg p-4 flex items-center justify-between"
              >
                <div className="flex items-center gap-4">
                  <div className="bg-green-500/20 p-2 rounded">
                    <Activity className="h-4 w-4 text-green-500" />
                  </div>
                  <div>
                    <h3 className="font-medium text-white">{bot.strategy}</h3>
                    <p className="text-gray-400 text-sm">
                      {bot.symbol} on {bot.exchanges.join(', ')}
                    </p>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  <span className="bg-green-500/20 text-green-400 px-2 py-1 rounded text-xs">
                    {bot.status}
                  </span>
                  <span className="text-gray-400 text-xs">
                    {new Date(bot.start_time).toLocaleTimeString()}
                  </span>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="text-center py-8">
            <Pause className="h-12 w-12 text-gray-500 mx-auto mb-4" />
            <p className="text-gray-400">No bots are currently running</p>
            <Link
              to="/bots"
              className="text-blue-400 hover:text-blue-300 text-sm mt-2 inline-block"
            >
              Start a bot →
            </Link>
          </div>
        )}
      </div>

      {/* Exchange Status Section */}
      <div className="bg-gray-800 rounded-lg p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-xl font-semibold text-white flex items-center gap-2">
            <TrendingUp className="h-5 w-5 text-blue-500" />
            Exchange Connections
          </h2>
          <Link
            to="/exchanges"
            className="text-blue-400 hover:text-blue-300 text-sm"
          >
            View All
          </Link>
        </div>

        {connected_exchanges && connected_exchanges.length > 0 ? (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {connected_exchanges.slice(0, 6).map((exchange) => (
              <div
                key={exchange.connection_id}
                className="bg-gray-700 rounded-lg p-4"
              >
                <div className="flex items-center justify-between mb-2">
                  <h3 className="font-medium text-white capitalize">
                    {exchange.name}
                  </h3>
                  <div className="flex items-center gap-1">
                    {exchange.status === 'connected' ? (
                      <CheckCircle className="h-4 w-4 text-green-500" />
                    ) : (
                      <AlertCircle className="h-4 w-4 text-red-500" />
                    )}
                  </div>
                </div>
                <p className="text-gray-400 text-xs mb-1">
                  Type: {exchange.exchange_type}
                </p>
                <p className="text-gray-400 text-xs">
                  Markets: {exchange.market_count.toLocaleString()}
                </p>
              </div>
            ))}
          </div>
        ) : (
          <div className="text-center py-8">
            <AlertCircle className="h-12 w-12 text-gray-500 mx-auto mb-4" />
            <p className="text-gray-400">No exchanges are connected</p>
            <Link
              to="/exchanges"
              className="text-blue-400 hover:text-blue-300 text-sm mt-2 inline-block"
            >
              Configure exchanges →
            </Link>
          </div>
        )}
      </div>

      {/* Quick Actions */}
      <div className="bg-gray-800 rounded-lg p-6">
        <h2 className="text-xl font-semibold text-white mb-4 flex items-center gap-2">
          <Settings className="h-5 w-5" />
          Quick Actions
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Link
            to="/bots"
            className="bg-green-600 hover:bg-green-700 text-white p-4 rounded-lg text-center transition-colors"
          >
            <Play className="h-6 w-6 mx-auto mb-2" />
            Start New Bot
          </Link>
          <Link
            to="/exchanges"
            className="bg-blue-600 hover:bg-blue-700 text-white p-4 rounded-lg text-center transition-colors"
          >
            <TrendingUp className="h-6 w-6 mx-auto mb-2" />
            Test Exchanges
          </Link>
          <button
            onClick={handleRefresh}
            className="bg-purple-600 hover:bg-purple-700 text-white p-4 rounded-lg text-center transition-colors"
          >
            <RefreshCw className="h-6 w-6 mx-auto mb-2" />
            Refresh Status
          </button>
        </div>
      </div>
    </div>
  );
};

export default Dashboard; 