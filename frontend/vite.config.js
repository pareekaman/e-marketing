import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Builds into public/react/, which Express already serves; there is no build
// step on Vercel, so the built files are committed. The entry keeps a fixed
// name (app.html loads it); pages split into hashed chunks loaded on demand.
export default defineConfig({
  plugins: [react()],
  base: '/react/',
  build: {
    outDir: '../public/react',
    emptyOutDir: true,
    rollupOptions: {
      input: 'src/main.jsx',
      output: {
        entryFileNames: 'emk-react.js',
        chunkFileNames: 'chunks/[name]-[hash].js',
        assetFileNames: 'assets/[name]-[hash][extname]',
        // React in its own hashed chunk. Pages import it from there, never from
        // the entry, so the ?v= on the entry cannot load a second React.
        manualChunks: id => (/node_modules[\\/](react|react-dom|scheduler)[\\/]/.test(id) ? 'vendor' : undefined),
      },
    },
  },
});
