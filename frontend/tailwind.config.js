/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        gray: {
          900: '#0f1419',
          800: '#1a202c',
          700: '#2d3748',
          600: '#4a5568',
          500: '#718096',
          400: '#a0aec0',
          300: '#cbd5e0',
        }
      },
      animation: {
        'pulse': 'pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'spin': 'spin 1s linear infinite',
      }
    },
  },
  plugins: [],
} 