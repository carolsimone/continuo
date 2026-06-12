import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// The dev-server origin the proxy forwards `/api` and `/ws` to. Uses the same
// `PORT` variable the backend listens on, so a single override keeps the backend
// and the proxy target in sync when 8090 is already taken on the host.
const serverPort = process.env.PORT || '8090';
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
