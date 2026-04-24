/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
        mono: ['IBM Plex Mono', 'JetBrains Mono', 'monospace'],
      },
      colors: {
        // ── Catppuccin Mocha ──────────────────────────────────
        ctp: {
          crust:     '#11111b',
          mantle:    '#181825',
          base:      '#1e1e2e',
          surface0:  '#313244',
          surface1:  '#45475a',
          surface2:  '#585b70',
          overlay0:  '#6c7086',
          overlay1:  '#7f849c',
          overlay2:  '#9399b2',
          subtext0:  '#a6adc8',
          subtext1:  '#bac2de',
          text:      '#cdd6f4',
          lavender:  '#b4befe',
          blue:      '#89b4fa',
          sapphire:  '#74c7ec',
          sky:       '#89dceb',
          teal:      '#94e2d5',
          green:     '#a6e3a1',
          yellow:    '#f9e2af',
          peach:     '#fab387',
          maroon:    '#eba0ac',
          red:       '#f38ba8',
          mauve:     '#cba6f7',
          pink:      '#f5c2e7',
          flamingo:  '#f2cdcd',
          rosewater: '#f5e0dc',
        },
        // ── WoW class colours (Blizzard spec) ─────────────────
        wow: {
          'death-knight': '#C41E3A',
          'demon-hunter': '#A330C9',
          'druid':        '#FF7C0A',
          'evoker':       '#33937F',
          'hunter':       '#AAD372',
          'mage':         '#3FC7EB',
          'monk':         '#00FF98',
          'paladin':      '#F48CBA',
          'priest':       '#E8E8E8',
          'rogue':        '#FFF468',
          'shaman':       '#2459FF',
          'warlock':      '#8788EE',
          'warrior':      '#C69B3A',
          'unknown':      '#6c7086',
        },
      },
      borderRadius: {
        '2xl': '1rem',
        '3xl': '1.5rem',
      },
      boxShadow: {
        // Soft dark shadows with a hint of colour
        card:       '0 2px 12px rgba(0,0,0,0.35), 0 1px 0 rgba(255,255,255,0.04)',
        'card-hover':'0 8px 32px rgba(0,0,0,0.5), 0 1px 0 rgba(255,255,255,0.06)',
        'mauve-glow':'0 0 0 1px rgba(203,166,247,0.25), 0 0 16px rgba(203,166,247,0.08)',
        'blue-glow': '0 0 0 1px rgba(137,180,250,0.25), 0 0 16px rgba(137,180,250,0.08)',
      },
      animation: {
        'fade-in':    'fadeIn 0.2s ease-out',
        'slide-up':   'slideUp 0.25s ease-out',
        'pulse-slow': 'pulse 3s cubic-bezier(0.4,0,0.6,1) infinite',
      },
      keyframes: {
        fadeIn:  { from: { opacity: '0' }, to: { opacity: '1' } },
        slideUp: { from: { opacity: '0', transform: 'translateY(6px)' }, to: { opacity: '1', transform: 'translateY(0)' } },
      },
    },
  },
  plugins: [],
}
