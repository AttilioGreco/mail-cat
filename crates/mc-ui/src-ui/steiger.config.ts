import { defineConfig } from 'steiger';
import fsd from '@feature-sliced/steiger-plugin';

export default defineConfig([
  ...fsd.configs.recommended,
  {
    // mail-cat is a single-window desktop app with exactly one page (pages/main),
    // so every top-level widget is by definition referenced from just one place.
    // The heuristic doesn't apply here — it's not a sign of over-slicing.
    rules: {
      'fsd/insignificant-slice': 'off',
    },
  },
]);
