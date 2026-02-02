// Frontend configuration

// Prefer explicit env var when provided
const envUrl = process.env.REACT_APP_API_URL;

// Default to same-origin API when hosted (supports proxies)
const origin = window.location.origin;

export const API_URL = envUrl || origin;

// If you need WS in this app, derive it here (kept for parity)
const wsScheme = API_URL.startsWith('https') ? 'wss' : 'ws';
export const WS_URL = `${wsScheme}://${new URL(API_URL).host}`;

export const config = {
	API_URL,
	WS_URL,
}; 
