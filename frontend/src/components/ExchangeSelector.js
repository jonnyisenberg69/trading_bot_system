import React, { useState, useEffect } from 'react';
import { CheckCircle, AlertCircle } from 'lucide-react';
import { api } from '../services/api';

const ExchangeSelector = ({ selectedExchanges, onSelectionChange }) => {
  const [availableExchanges, setAvailableExchanges] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchExchanges = async () => {
      try {
        setLoading(true);
        const response = await api.exchanges.getConnected();
        setAvailableExchanges(response.data.connections || []);
      } catch (error) {
        console.error('Failed to fetch exchanges:', error);
      } finally {
        setLoading(false);
      }
    };
    fetchExchanges();
  }, []);

  const handleCheckboxChange = (exchangeId) => {
    const newSelection = [...selectedExchanges];
    if (newSelection.includes(exchangeId)) {
      onSelectionChange(newSelection.filter(id => id !== exchangeId));
    } else {
      onSelectionChange([...newSelection, exchangeId]);
    }
  };
  
  const selectAllConnected = () => {
    const allConnectedIds = availableExchanges
      .filter(ex => ex.status === 'connected')
      .map(ex => ex.connection_id);
    onSelectionChange(allConnectedIds);
  };

  const selectSpotOnly = () => {
    const spotIds = availableExchanges
      .filter(ex => ex.exchange_type === 'spot' && ex.status === 'connected')
      .map(ex => ex.connection_id);
    onSelectionChange(spotIds);
  };
  
  const selectPerpOnly = () => {
    const perpIds = availableExchanges
      .filter(ex => ex.exchange_type === 'perp' && ex.status === 'connected')
      .map(ex => ex.connection_id);
    onSelectionChange(perpIds);
  };

  if (loading) {
    return <p className="text-gray-400">Loading exchanges...</p>;
  }

  return (
    <div>
      <label className="block text-gray-400 text-sm mb-2">
        Select Exchanges ({selectedExchanges.length} selected)
      </label>
      <div className="space-y-2 max-h-64 overflow-y-auto p-2 bg-gray-900 rounded">
        {availableExchanges.length > 0 ? availableExchanges.map(exchange => {
          const displayName = `${exchange.name.charAt(0).toUpperCase() + exchange.name.slice(1)} ${exchange.exchange_type.toUpperCase()}`;
          return (
            <label key={exchange.connection_id} className="flex items-center">
              <input
                type="checkbox"
                checked={selectedExchanges.includes(exchange.connection_id)}
                onChange={() => handleCheckboxChange(exchange.connection_id)}
                className="mr-2"
              />
              <span className="text-white font-medium">{displayName}</span>
              <span className="text-gray-400 text-xs ml-2">({exchange.connection_id})</span>
              {exchange.status === 'connected' ? (
                <CheckCircle className="h-4 w-4 text-green-500 ml-2" />
              ) : (
                <AlertCircle className="h-4 w-4 text-red-500 ml-2" />
              )}
            </label>
          );
        }) : (
          <p className="text-red-400 text-sm">
            No connected exchanges available. Please configure exchanges first.
          </p>
        )}
      </div>
      {availableExchanges.length > 0 && (
        <div className="mt-3 space-y-2">
          <div className="flex gap-2 flex-wrap">
            <button type="button" onClick={selectAllConnected} className="text-xs bg-blue-600 hover:bg-blue-700 text-white px-2 py-1 rounded">
              Select All Connected
            </button>
            <button type="button" onClick={selectSpotOnly} className="text-xs bg-green-600 hover:bg-green-700 text-white px-2 py-1 rounded">
              Spot Only
            </button>
            <button type="button" onClick={selectPerpOnly} className="text-xs bg-purple-600 hover:bg-purple-700 text-white px-2 py-1 rounded">
              Perp Only
            </button>
            <button type="button" onClick={() => onSelectionChange([])} className="text-xs bg-gray-600 hover:bg-gray-700 text-white px-2 py-1 rounded">
              Clear All
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

export default ExchangeSelector; 
