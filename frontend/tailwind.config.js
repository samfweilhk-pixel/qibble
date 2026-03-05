/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        bg: {
          primary: '#0a0a0f',
          card: '#111118',
          hover: '#1a1a24',
        },
        border: {
          DEFAULT: '#1e1e2e',
          bright: '#2a2a3e',
        },
        accent: {
          cyan: '#00d4ff',
          purple: '#7c3aed',
          green: '#00ff88',
          red: '#ff3366',
          orange: '#ff8c00',
          yellow: '#ffd700',
        },
      },
      fontFamily: {
        mono: ['"JetBrains Mono"', '"Fira Code"', 'monospace'],
      },
    },
  },
  plugins: [],
}
