import React, { useState, useEffect, useCallback } from 'react';
import { X, Save } from 'lucide-react';
import AggressiveTwapConfig from './AggressiveTwapConfig';
import AggressiveTwapV2Config from './AggressiveTwapV2Config';
import PassiveQuotingConfig from './PassiveQuotingConfig';
import TargetedSellbotConfig from './TargetedSellbotConfig';
import TargetedBuybotConfig from './TargetedBuybotConfig';
import TopOfBookConfig from './TopOfBookConfig';
import VolumeWeightedTopOfBookConfig from './VolumeWeightedTopOfBookConfig';
import VolumeWeightedTopOfBookConfigDelta from './VolumeWeightedTopOfBookConfig_delta';
import StackedMarketMakingConfig from './StackedMarketMakingConfig';
import StackedMarketMakingConfigDelta from './StackedMarketMakingConfig_delta';
import MarketMakingConfig from './MarketMakingConfig';
import ExchangeSelector from './ExchangeSelector';

// Simple Error Boundary component
class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, errorInfo) {
    console.error('Error in config form:', error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="text-red-400 p-4 bg-red-900/20 rounded">
          <h3 className="font-semibold mb-2">Configuration Error</h3>
          <p className="text-sm">An error occurred while rendering the configuration form.</p>
          <p className="text-xs mt-2 text-gray-400">{this.state.error?.message}</p>
          <button 
            onClick={() => this.setState({ hasError: false, error: null })}
            className="mt-2 px-3 py-1 bg-red-600 hover:bg-red-700 text-white rounded text-sm"
          >
            Try Again
          </button>
        </div>
      );
    }

    return this.props.children;
  }
}

const BotConfigModal = ({ bot, onClose, onSave }) => {
  const [currentConfig, setCurrentConfig] = useState(null);
  const [currentExchanges, setCurrentExchanges] = useState([]);
  const [isDirty, setIsDirty] = useState(false);
  const [isSaving, setIsSaving] = useState(false);

  useEffect(() => {
    if (bot) {
      // Debug log to see what strategy value we're getting
      console.log('BotConfigModal received bot:', bot);
      console.log('Bot strategy:', bot.strategy);
      
      // Create deep copies to avoid direct mutation
      setCurrentConfig(JSON.parse(JSON.stringify(bot.config)));
      setCurrentExchanges([...bot.exchanges]);
      setIsDirty(false); // Reset dirty state when a new bot is loaded
    } else {
      console.log('BotConfigModal: bot prop is null/undefined');
    }
  }, [bot]);

  // Add effect to track when modal unmounts
  useEffect(() => {
    console.log('BotConfigModal mounted');
    return () => {
      console.log('BotConfigModal unmounting');
    };
  }, []);

  const handleStateChange = useCallback((newConfig, newExchanges) => {
    console.log('BotConfigModal handleStateChange called with:', { newConfig: !!newConfig, newExchanges: !!newExchanges });
    
    try {
      // Use the new values for comparison, fallback to current state
      const configToCompare = newConfig || currentConfig;
      const exchangesToCompare = newExchanges || currentExchanges;
      
      // Deep comparison to check if anything has actually changed
      const configChanged = JSON.stringify(configToCompare) !== JSON.stringify(bot.config);
      const exchangesChanged = JSON.stringify(exchangesToCompare) !== JSON.stringify(bot.exchanges);
      
      console.log('Config changed:', configChanged, 'Exchanges changed:', exchangesChanged);
      
      // Update state after comparison
      if (newConfig) {
        console.log('Updating currentConfig');
        setCurrentConfig(newConfig);
      }
      if (newExchanges) {
        console.log('Updating currentExchanges');
        setCurrentExchanges(newExchanges);
      }
      
      setIsDirty(configChanged || exchangesChanged);
    } catch (error) {
      console.error('Error in handleStateChange:', error);
    }
  }, [currentConfig, currentExchanges, bot.config, bot.exchanges]);

  const handleSave = async () => {
    console.log('BotConfigModal handleSave called - user clicked Save Changes button');
    setIsSaving(true);
    try {
      await onSave(bot.instance_id, currentConfig, currentExchanges);
      console.log('BotConfigModal save successful, closing modal');
      onClose(); // Close modal on successful save
    } catch (error) {
      console.error("Failed to save configuration:", error);
      alert(`Error saving configuration: ${error.message}`);
    } finally {
      setIsSaving(false);
    }
  };

  const renderConfigForm = () => {
    if (!currentConfig) return null;

    const commonProps = {
      config: currentConfig,
      onConfigChange: (newConfig) => handleStateChange(newConfig, null),
    };

    // Debug log the strategy we're switching on
    console.log('Rendering config form for strategy:', bot.strategy);

    switch (bot.strategy) {
      case 'aggressive_twap':
        return (
          <AggressiveTwapConfig
            {...commonProps}
            symbol={bot.symbol}
            exchanges={currentExchanges}
          />
        );
      case 'aggressive_twap_v2':
        return (
          <AggressiveTwapV2Config
            {...commonProps}
            symbol={bot.symbol}
            exchanges={currentExchanges}
          />
        );
      case 'passive_quoting':
        return <PassiveQuotingConfig {...commonProps} />;
      case 'targeted_sellbot':
        return <TargetedSellbotConfig {...commonProps} />;
      case 'targeted_buybot':
        return <TargetedBuybotConfig {...commonProps} />;
      case 'top_of_book':
      case 'TopOfBookStrategy':  // Backward compatibility
        return (
          <TopOfBookConfig
            {...commonProps}
            symbol={bot.symbol}
            exchanges={currentExchanges}
          />
        );
      case 'volume_weighted_top_of_book':
        return (
          <VolumeWeightedTopOfBookConfig
            config={currentConfig}
            onConfigChange={(newConfig) => handleStateChange(newConfig, null)}
            symbol={bot.symbol}
            exchanges={currentExchanges}
          />
        );
      case 'volume_weighted_top_of_book_delta':
        return (
          <VolumeWeightedTopOfBookConfigDelta
            config={currentConfig}
            onConfigChange={(newConfig) => handleStateChange(newConfig, null)}
            symbol={bot.symbol}
            exchanges={currentExchanges}
          />
        );
      case 'stacked_market_making':
        return (
          <StackedMarketMakingConfig
            config={currentConfig}
            onConfigChange={(newConfig) => handleStateChange(newConfig, null)}
            symbol={bot.symbol}
            exchanges={currentExchanges}
          />
        );
      case 'stacked_market_making_delta':
        return (
          <StackedMarketMakingConfigDelta
            config={currentConfig}
            onConfigChange={(newConfig) => handleStateChange(newConfig, null)}
            symbol={bot.symbol}
            exchanges={currentExchanges}
          />
        );
      case 'market_making':
        return (
          <MarketMakingConfig
            {...commonProps}
            symbol={bot.symbol}
            exchanges={currentExchanges}
          />
        );
      default:
        return (
          <div className="text-gray-400">
            Configuration editing for the '{bot.strategy}' strategy is not yet implemented.
            <pre className="bg-gray-900 p-4 rounded mt-4 text-sm text-gray-300">
              {JSON.stringify(currentConfig, null, 2)}
            </pre>
          </div>
        );
    }
  };

  if (!bot) {
    return null;
  }

  return (
    <div 
      className="fixed inset-0 bg-black bg-opacity-75 flex items-center justify-center z-50"
      onClick={(e) => {
        // Only close if clicking the overlay, not the modal content
        if (e.target === e.currentTarget) {
          console.log('BotConfigModal overlay clicked, closing modal');
          onClose();
        }
      }}
    >
      <div 
        className="bg-gray-800 rounded-lg p-6 w-full max-w-4xl max-h-[90vh] flex flex-col"
        onClick={(e) => {
          // Prevent modal from closing when clicking inside the modal
          e.stopPropagation();
        }}
      >
        <div className="flex justify-between items-center mb-4 flex-shrink-0">
          <h2 className="text-xl font-semibold text-white">
            Edit Config: <span className="text-blue-400">{bot.instance_id}</span>
          </h2>
          <button 
            onClick={() => {
              console.log('BotConfigModal close button (X) clicked');
              onClose();
            }} 
            className="text-gray-400 hover:text-white"
          >
            <X className="h-6 w-6" />
          </button>
        </div>
        
        <div className="overflow-y-auto pr-2 flex-grow space-y-6">
          <ExchangeSelector 
            selectedExchanges={currentExchanges}
            onSelectionChange={(newExchanges) => handleStateChange(null, newExchanges)}
          />
          <ErrorBoundary>
            {renderConfigForm()}
          </ErrorBoundary>
        </div>

        <div className="flex justify-end gap-4 mt-6 pt-4 border-t border-gray-700 flex-shrink-0">
          <button
            onClick={() => {
              console.log('BotConfigModal Cancel button clicked');
              onClose();
            }}
            className="bg-gray-600 hover:bg-gray-700 text-white px-4 py-2 rounded-lg transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            disabled={!isDirty || isSaving}
            className="flex items-center gap-2 bg-green-600 hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed text-white px-4 py-2 rounded-lg transition-colors"
          >
            <Save className="h-4 w-4" />
            {isSaving ? 'Saving...' : 'Save Changes'}
          </button>
        </div>
      </div>
    </div>
  );
};

export default BotConfigModal; 
