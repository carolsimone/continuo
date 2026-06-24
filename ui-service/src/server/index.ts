import http from 'http';
import path from 'path';
import express from 'express';
import { createGrpcClient } from './grpc-client';
import { createGrpcGraphClient } from './grpc-graph-client';
import { createAgentClient } from './agent-client';
import { createRemediationClient } from './remediation-client';
import { createGithubAppPullRequestCreator } from './github/pull-request-creator';
import { createApp } from './app';
import { attachChatWebSocket } from './ws/chat';
import { loadAuthConfig } from './auth/config';
import { buildAuth } from './auth';

const PORT = parseInt(process.env.PORT || '8090', 10);
const STATE_GRPC_ADDR = process.env.STATE_GRPC_ADDR || 'localhost:50051';
const ORCHESTRATOR_GRPC_ADDR = process.env.ORCHESTRATOR_GRPC_ADDR || 'localhost:50052';
const AGENT_RUNNER_GRPC_ADDR = process.env.AGENT_RUNNER_GRPC_ADDR || 'localhost:50053';
const REMEDIATION_GRPC_ADDR = process.env.REMEDIATION_GRPC_ADDR || 'localhost:50054';
const CONFIG_FILE = process.env.CONFIG_FILE;
const RELEASE_CONTROLLER_URL = process.env.RELEASE_CONTROLLER_URL || 'http://release-controller:8088';
const CHAT_BRIDGE_ENABLED = process.env.CHAT_BRIDGE_ENABLED === 'true';
const GITHUB_APP_ID = process.env.GITHUB_APP_ID || '';
// Replace literal \n sequences with real newlines so a single-line env value
// (e.g. from docker-compose) is accepted as valid PEM. This is a no-op for
// real multiline keys from prod secrets, which contain no backslash-n.
const GITHUB_APP_PRIVATE_KEY = (process.env.GITHUB_APP_PRIVATE_KEY ?? '').replace(/\\n/g, '\n');
const GITHUB_APP_INSTALLATION_ID = process.env.GITHUB_APP_INSTALLATION_ID || '';
const GITHUB_API_BASE_URL = process.env.GITHUB_API_BASE_URL || undefined;

async function main() {
  // Fail fast: missing/invalid auth configuration must never boot an open UI.
  const authConfig = loadAuthConfig(process.env);
  const auth = await buildAuth(authConfig);

  const client = createGrpcClient(STATE_GRPC_ADDR);
  const graphClient = createGrpcGraphClient(ORCHESTRATOR_GRPC_ADDR);
  const remediationClient = createRemediationClient(REMEDIATION_GRPC_ADDR);

  // Optional GitHub App integration for PR creation — absent when env vars are empty.
  const prCreator =
    GITHUB_APP_ID && GITHUB_APP_PRIVATE_KEY && GITHUB_APP_INSTALLATION_ID
      ? createGithubAppPullRequestCreator({
          appId: GITHUB_APP_ID,
          privateKey: GITHUB_APP_PRIVATE_KEY,
          installationId: GITHUB_APP_INSTALLATION_ID,
          baseUrl: GITHUB_API_BASE_URL,
        })
      : undefined;

  const app = createApp(client, graphClient, auth.app, CONFIG_FILE, RELEASE_CONTROLLER_URL, CHAT_BRIDGE_ENABLED, remediationClient, prCreator);

  if (process.env.NODE_ENV === 'production') {
    const staticDir = path.join(__dirname, '../dist');
    app.use(express.static(staticDir));
    app.get('*', (_req, res) => {
      res.sendFile(path.join(staticDir, 'index.html'));
    });
  }

  const server = http.createServer(app);
  if (CHAT_BRIDGE_ENABLED) {
    attachChatWebSocket(server, {
      agentClient: createAgentClient(AGENT_RUNNER_GRPC_ADDR),
      authenticate: auth.authenticateWs,
      allowedOrigin: auth.publicOrigin,
    });
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
