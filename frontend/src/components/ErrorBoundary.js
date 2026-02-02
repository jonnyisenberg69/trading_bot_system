import React from 'react';

class ErrorBoundary extends React.Component {
	constructor(props) {
		super(props);
		this.state = { hasError: false, error: null };
	}

	static getDerivedStateFromError(error) {
		return { hasError: true, error };
	}

	componentDidCatch(error, errorInfo) {
		console.error('ErrorBoundary caught an error:', error, errorInfo);
	}

	render() {
		if (this.state.hasError) {
			return (
				<div style={{ padding: '16px', background: '#1f2937', color: 'white', minHeight: '100vh' }}>
					<h1 style={{ fontSize: '20px', marginBottom: '8px' }}>An error occurred in the UI</h1>
					<pre style={{ whiteSpace: 'pre-wrap' }}>{String(this.state.error)}</pre>
					<p style={{ opacity: 0.8 }}>Check the browser console for details.</p>
				</div>
			);
		}

		return this.props.children; 
	}
}

export default ErrorBoundary;
