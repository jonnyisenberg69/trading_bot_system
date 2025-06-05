import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import { Activity, TrendingUp, BarChart3, Settings, Shield } from 'lucide-react';

const Navbar = ({ systemStatus }) => {
  const location = useLocation();

  const navItems = [
    { path: '/', label: 'Dashboard', icon: BarChart3 },
    { path: '/bots', label: 'Bots', icon: Activity },
    { path: '/exchanges', label: 'Exchanges', icon: TrendingUp },
    { path: '/risk', label: 'Risk Monitoring', icon: Shield },
  ];

  const isActive = (path) => location.pathname === path;

  const getSystemStatusIndicator = () => {
    if (!systemStatus) {
      return <div className="w-2 h-2 bg-gray-500 rounded-full" />;
    }

    const hasRunningBots = systemStatus.overview?.bots?.running > 0;
    const hasConnectedExchanges = systemStatus.overview?.exchanges?.connected > 0;

    if (hasRunningBots && hasConnectedExchanges) {
      return <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse" />;
    } else if (hasConnectedExchanges) {
      return <div className="w-2 h-2 bg-yellow-500 rounded-full" />;
    } else {
      return <div className="w-2 h-2 bg-red-500 rounded-full" />;
    }
  };

  return (
    <nav className="bg-gray-800 border-b border-gray-700">
      <div className="container mx-auto px-4">
        <div className="flex items-center justify-between h-16">
          {/* Logo */}
          <Link to="/" className="flex items-center gap-3">
            <div className="bg-blue-600 p-2 rounded-lg">
              <Activity className="h-6 w-6 text-white" />
            </div>
            <div className="hidden md:block">
              <h1 className="text-xl font-bold text-white">Trading Bot</h1>
              <p className="text-xs text-gray-400">System v1.0</p>
            </div>
          </Link>

          {/* Navigation */}
          <div className="flex items-center gap-1">
            {navItems.map(({ path, label, icon: Icon }) => (
              <Link
                key={path}
                to={path}
                className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${
                  isActive(path)
                    ? 'bg-blue-600 text-white'
                    : 'text-gray-300 hover:text-white hover:bg-gray-700'
                }`}
              >
                <Icon className="h-4 w-4" />
                <span className="hidden sm:inline">{label}</span>
              </Link>
            ))}
          </div>

          {/* Status */}
          <div className="flex items-center gap-4">
            {/* System Status */}
            <div className="flex items-center gap-2">
              {getSystemStatusIndicator()}
              <span className="text-gray-400 text-sm hidden md:inline">
                {systemStatus ? (
                  `${systemStatus.overview?.bots?.running || 0} bots, ${systemStatus.overview?.exchanges?.connected || 0} exchanges`
                ) : (
                  'Loading...'
                )}
              </span>
            </div>

            {/* Settings */}
            <button className="text-gray-400 hover:text-white transition-colors">
              <Settings className="h-5 w-5" />
            </button>
          </div>
        </div>
      </div>
    </nav>
  );
};

export default Navbar; 