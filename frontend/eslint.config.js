import tseslint from 'typescript-eslint'
import reactHooks from 'eslint-plugin-react-hooks'
import jsxA11y from 'eslint-plugin-jsx-a11y'

export default tseslint.config(
  { ignores: ['dist', 'node_modules'] },
  ...tseslint.configs.recommended,
  {
    files: ['src/**/*.{ts,tsx}'],
    plugins: {
      'react-hooks': reactHooks,
      'jsx-a11y': jsxA11y,
    },
    rules: {
      // Warn on explicit any — will be tightened to error once chart tooltips are typed (Phase 5)
      '@typescript-eslint/no-explicit-any': 'warn',

      // React hooks
      'react-hooks/rules-of-hooks': 'error',
      'react-hooks/exhaustive-deps': 'warn',

      // Accessibility
      'jsx-a11y/anchor-is-valid': 'warn',
      'jsx-a11y/click-events-have-key-events': 'warn',
      'jsx-a11y/no-static-element-interactions': 'warn',

      // Null-coercion guard — Number(x) || 0 silently turns null into 0.
      // Use toFiniteNumber(x) ?? fallback from utils/format.ts instead.
      // Set to 'warn' in Phase 1; promoted to 'error' in Phase 2 after all violations are fixed.
      'no-restricted-syntax': [
        'warn',
        {
          selector: "LogicalExpression[operator='||'][left.callee.name='Number']",
          message:
            "Number(x) || 0 silently coerces null to 0. Use toFiniteNumber(x) ?? fallback from utils/format.ts.",
        },
      ],
    },
  },
)
