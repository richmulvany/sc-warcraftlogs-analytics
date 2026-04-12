/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      fontFamily: {
        sans: ['IBM Plex Sans', 'system-ui', 'sans-serif'],
        mono: ['IBM Plex Mono', 'monospace'],
      },
      colors: {
        brand: { 50:'#f0f4ff', 100:'#dce6ff', 200:'#b9ccff', 400:'#6b96f5', 600:'#3060d0', 800:'#1a3a82', 900:'#0e2254' },
        surface: { 0:'#0a0d14', 1:'#10151f', 2:'#161c2a', 3:'#1e2638', 4:'#263045' },
        accent: { gold:'#e8a020', teal:'#22c9a0', coral:'#f06050' }
      },
      boxShadow: {
        card: '0 1px 3px rgba(0,0,0,0.4), 0 0 0 1px rgba(255,255,255,0.04)',
        glow: '0 0 20px rgba(107,150,245,0.15)',
      }
    },
  },
  plugins: [],
}
