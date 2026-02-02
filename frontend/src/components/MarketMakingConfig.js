import React, { useEffect, useMemo } from 'react';

const MarketMakingConfig = ({ config, onConfigChange, symbol, exchanges }) => {
	// Ensure required fields exist
	const safeConfig = useMemo(() => ({
		base_coin: config?.base_coin || (symbol?.includes('/') ? symbol.split('/')[0].toUpperCase() : ''),
		quantity_currency: config?.quantity_currency || 'base',
		reference_exchange: config?.reference_exchange || '',
		lines: Array.isArray(config?.lines) && config.lines.length > 0 ? config.lines : [
			{ timeout: 300, drift: 50, quantity: 0.01, quantity_randomization_factor: 10, spread: 25, sides: 'both' }
		],
		max_position: config?.max_position ?? '',
		hedge_spread: config?.hedge_spread ?? '',
	}), [config, symbol]);

	// Default reference exchange to first selected
	useEffect(() => {
		if (!safeConfig.reference_exchange && Array.isArray(exchanges) && exchanges.length > 0) {
			onConfigChange({ ...safeConfig, reference_exchange: exchanges[0] });
		}
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [exchanges]);

	const updateField = (field, value) => {
		onConfigChange({ ...safeConfig, [field]: value });
	};

	const updateLine = (index, field, value) => {
		const newLines = safeConfig.lines.map((line, i) => (i === index ? { ...line, [field]: value } : line));
		onConfigChange({ ...safeConfig, lines: newLines });
	};

	const addLine = () => {
		onConfigChange({
			...safeConfig,
			lines: [
				...safeConfig.lines,
				{ timeout: 300, drift: 50, quantity: 0.01, quantity_randomization_factor: 10, spread: 25, sides: 'both' },
			],
		});
	};

	const removeLine = (index) => {
		if (safeConfig.lines.length <= 1) return;
		onConfigChange({
			...safeConfig,
			lines: safeConfig.lines.filter((_, i) => i !== index),
		});
	};

	return (
		<div className="space-y-4">
			{/* Reference exchange */}
			<div className="grid grid-cols-1 md:grid-cols-3 gap-4">
				<div>
					<label className="block text-gray-400 text-sm mb-2">Reference Exchange</label>
					<select
						value={safeConfig.reference_exchange}
						onChange={(e) => updateField('reference_exchange', e.target.value)}
						className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
						required
					>
						{(exchanges || []).map((ex) => (
							<option key={ex} value={ex}>{ex}</option>
						))}
					</select>
				</div>

				{/* Quantity currency */}
				<div>
					<label className="block text-gray-400 text-sm mb-2">Quantity Currency</label>
					<select
						value={safeConfig.quantity_currency}
						onChange={(e) => updateField('quantity_currency', e.target.value)}
						className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
					>
						<option value="base">Base</option>
						<option value="quote">Quote</option>
					</select>
				</div>

				{/* Optional limits */}
				<div>
					<label className="block text-gray-400 text-sm mb-2">Max Position (base units, optional)</label>
					<input
						type="number"
						step="0.00000001"
						value={safeConfig.max_position}
						onChange={(e) => updateField('max_position', e.target.value)}
						placeholder="e.g., 0.05"
						className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
					/>
					<p className="text-xs text-gray-500 mt-1">Quotes that would exceed this net position are skipped.</p>
				</div>
			</div>

			{/* Hedge spread */}
			<div className="grid grid-cols-1 md:grid-cols-3 gap-4">
				<div>
					<label className="block text-gray-400 text-sm mb-2">Hedge Spread (bps, optional)</label>
					<input
						type="number"
						step="0.01"
						value={safeConfig.hedge_spread}
						onChange={(e) => updateField('hedge_spread', e.target.value)}
						placeholder="Defaults to first line spread or 10"
						className="w-full bg-gray-700 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
					/>
				</div>
			</div>

			{/* Quote lines */}
			<div className="bg-gray-700 rounded p-3">
				<div className="flex items-center justify-between mb-2">
					<h4 className="text-white font-medium">Quote Lines</h4>
					<button
						type="button"
						onClick={addLine}
						className="bg-blue-600 hover:bg-blue-700 text-white px-3 py-1 rounded"
					>
						Add Line
					</button>
				</div>
				<div className="space-y-3">
					{safeConfig.lines.map((line, idx) => (
						<div key={idx} className="grid grid-cols-1 md:grid-cols-7 gap-3 items-end">
							<div>
								<label className="block text-gray-400 text-sm mb-1">Timeout (s)</label>
								<input
									type="number"
									min="1"
									value={line.timeout}
									onChange={(e) => updateLine(idx, 'timeout', Number(e.target.value))}
									className="w-full bg-gray-800 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
								/>
							</div>
							<div>
								<label className="block text-gray-400 text-sm mb-1">Drift (bps)</label>
								<input
									type="number"
									step="0.01"
									value={line.drift}
									onChange={(e) => updateLine(idx, 'drift', Number(e.target.value))}
									className="w-full bg-gray-800 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
								/>
							</div>
							<div>
								<label className="block text-gray-400 text-sm mb-1">Quantity</label>
								<input
									type="number"
									step="0.00000001"
									value={line.quantity}
									onChange={(e) => updateLine(idx, 'quantity', Number(e.target.value))}
									className="w-full bg-gray-800 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
								/>
							</div>
							<div>
								<label className="block text-gray-400 text-sm mb-1">Random %</label>
								<input
									type="number"
									step="1"
									value={line.quantity_randomization_factor || 0}
									onChange={(e) => updateLine(idx, 'quantity_randomization_factor', Number(e.target.value))}
									className="w-full bg-gray-800 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
								/>
							</div>
							<div>
								<label className="block text-gray-400 text-sm mb-1">Spread (bps)</label>
								<input
									type="number"
									step="0.01"
									value={line.spread}
									onChange={(e) => updateLine(idx, 'spread', Number(e.target.value))}
									className="w-full bg-gray-800 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
								/>
							</div>
							<div>
								<label className="block text-gray-400 text-sm mb-1">Sides</label>
								<select
									value={line.sides}
									onChange={(e) => updateLine(idx, 'sides', e.target.value)}
									className="w-full bg-gray-800 text-white px-3 py-2 rounded border border-gray-600 focus:border-blue-500 focus:outline-none"
								>
									<option value="both">Both</option>
									<option value="bid">Bid</option>
									<option value="offer">Offer</option>
								</select>
							</div>
							<div className="flex items-end">
								<button
									type="button"
									onClick={() => removeLine(idx)}
									className="w-full bg-red-600 hover:bg-red-700 text-white px-3 py-2 rounded"
									disabled={safeConfig.lines.length <= 1}
								>
									Remove
								</button>
							</div>
						</div>
					))}
				</div>
			</div>
		</div>
	);
};

export default MarketMakingConfig;
