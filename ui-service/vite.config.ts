import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// The dev-server origin the proxy forwards `/api` and `/ws` to. Overridable so
// the server can run on a non-default port when 8090 is already taken on the host.
const serverPort = process.env.UI_SERVER_PORT || '8090';
const httpTarget = `http://localhost:${serverPort}`;
const wsTarget = `ws://localhost:${serverPort}`;

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: 'dist',
  },
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: httpTarget,
        changeOrigin: true,
      },
      '/ws': {
        target: wsTarget,
        ws: true,
      },
    },
  },
});
