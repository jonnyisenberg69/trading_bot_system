import React from 'react';
import { X } from 'lucide-react';

const ConfigDisplayModal = ({ config, onClose }) => {
  if (!config) {
    return null;
  }

  const renderValue = (value) => {
    if (typeof value === 'boolean') {
      return value ? 'true' : 'false';
    }
    if (typeof value === 'object' && value !== null) {
      return (
        <pre className="bg-gray-900 p-2 rounded text-sm text-gray-300">
          {JSON.stringify(value, null, 2)}
        </pre>
      );
    }
    return String(value);
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-75 flex items-center justify-center z-50">
      <div className="bg-gray-800 rounded-lg p-6 w-full max-w-2xl max-h-[80vh] overflow-y-auto">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-xl font-semibold text-white">Bot Configuration</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-white">
            <X className="h-6 w-6" />
          </button>
        </div>
        <div className="space-y-2">
          {Object.entries(config).map(([key, value]) => (
            <div key={key} className="grid grid-cols-3 gap-4 items-start">
              <span className="text-gray-400 font-medium break-words col-span-1">{key}</span>
              <div className="text-white break-words col-span-2">{renderValue(value)}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default ConfigDisplayModal; 
