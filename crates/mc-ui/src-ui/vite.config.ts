import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
    // CodeMirror requires singleton @codemirror/state & /view: transitive deps
    // pull older nested copies, so instanceof checks break ("Unrecognized
    // extension value in extension set"). Force one copy each.
    dedupe: ['@codemirror/state', '@codemirror/view'],
  },
  server: {
    port: 5173,
    strictPort: true,
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    sourcemap: true,
  },
  clearScreen: false,
});
