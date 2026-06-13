import http from 'http';
import path from 'path';
import express from 'express';
import { createGrpcClient } from './grpc-client';
import { createGrpcGraphClient } from './grpc-graph-client';
import { createApp } from './app';
import { attachChatWebSocket } from './ws/chat';
import { loadAuthConfig } from './auth/config';
import { buildAuth } from './auth';

const PORT = parseInt(process.env.PORT || '8090', 10);
const STATE_GRPC_ADDR = process.env.STATE_GRPC_ADDR || 'localhost:50051';
const ORCHESTRATOR_GRPC_ADDR = process.env.ORCHESTRATOR_GRPC_ADDR || 'localhost:50052';
const CONFIG_FILE = process.env.CONFIG_FILE;
const RELEASE_CONTROLLER_URL = process.env.RELEASE_CONTROLLER_URL || 'http://release-controller:8088';
const CHAT_BRIDGE_ENABLED = process.env.CHAT_BRIDGE_ENABLED === 'true';

async function main() {
  // Fail fast: missing/invalid auth configuration must never boot an open UI.
  const authConfig = loadAuthConfig(process.env);
  const auth = await buildAuth(authConfig);

  const client = createGrpcClient(STATE_GRPC_ADDR);
  const graphClient = createGrpcGraphClient(ORCHESTRATOR_GRPC_ADDR);
  const app = createApp(client, graphClient, auth.app, CONFIG_FILE, RELEASE_CONTROLLER_URL, CHAT_BRIDGE_ENABLED);

  if (process.env.NODE_ENV === 'production') {
    const staticDir = path.join(__dirname, '../dist');
    app.use(express.static(staticDir));
    app.get('*', (_req, res) => {
      res.sendFile(path.join(staticDir, 'index.html'));
    });
  }

  const server = http.createServer(app);
  if (CHAT_BRIDGE_ENABLED) {
    attachChatWebSocket(server, { authenticate: auth.authenticateWs, allowedOrigin: auth.publicOrigin });
    console.log('Chat bridge enabled at /ws/chat (operator-only)');
  }
  server.listen(PORT, () => {
    console.log(`Continuo UI running on http://localhost:${PORT} (auth mode: ${authConfig.mode})`);
  });
}

main().catch((err) => {
  console.error('ui-service failed to start:', err);
  process.exit(1);
});
