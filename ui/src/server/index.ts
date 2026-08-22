import http from 'http';
import path from 'path';
import express from 'express';
import { rateLimit } from 'express-rate-limit';
import { createGrpcClient } from './grpc-client';
import { createGrpcGraphClient } from './grpc-graph-client';
import { createAgentClient } from './agent-client';
import { createRemediationClient } from './remediation-client';
import { resolveGithubAppPullRequestCreator } from './github/pull-request-creator';
import { normalizePemPrivateKey } from './github/private-key';
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
// Reconstructs a well-formed PEM regardless of how its line breaks arrived
// (real newlines, \n escapes, spaces, CRLF, or a mix) so a value mangled in
// transit — e.g. a quoted YAML scalar in a Helm values file — is still
// accepted. See normalizePemPrivateKey for the encodings this repairs.
const GITHUB_APP_PRIVATE_KEY = normalizePemPrivateKey(process.env.GITHUB_APP_PRIVATE_KEY ?? '');
const GITHUB_APP_INSTALLATION_ID = process.env.GITHUB_APP_INSTALLATION_ID || '';
const GITHUB_API_BASE_URL = process.env.GITHUB_API_BASE_URL || undefined;

async function main() {
  // Fail fast: missing/invalid auth configuration must never boot an open UI.
  const authConfig = loadAuthConfig(process.env);
  const auth = await buildAuth(authConfig);

  const client = createGrpcClient(STATE_GRPC_ADDR);
  const graphClient = createGrpcGraphClient(ORCHESTRATOR_GRPC_ADDR);
  const remediationClient = createRemediationClient(REMEDIATION_GRPC_ADDR);

  // Optional GitHub App integration for PR creation — disabled (not fatal)
  // when env vars are absent, since GitHub App credentials are not configured
  // in every deployment. A malformed key is a distinct case, logged loudly by
  // resolveGithubAppPullRequestCreator rather than passing silently for the
  // same "absent" reason, because the operator dashboard as a whole must stay
  // up even though this one integration cannot.
  const prCreator = resolveGithubAppPullRequestCreator({
    appId: GITHUB_APP_ID,
    privateKey: GITHUB_APP_PRIVATE_KEY,
    installationId: GITHUB_APP_INSTALLATION_ID,
    baseUrl: GITHUB_API_BASE_URL,
  });

  const app = createApp(client, graphClient, auth.app, CONFIG_FILE, RELEASE_CONTROLLER_URL, CHAT_BRIDGE_ENABLED, remediationClient, prCreator);

  if (process.env.NODE_ENV === 'production') {
    const staticDir = path.join(__dirname, '../dist');
    app.use(express.static(staticDir));
    // The SPA fallback reads index.html from disk per request; bound it per
    // client IP (navigation loads it once — the limit only bites automated
    // hammering). xForwardedForHeader validation is off because the service
    // runs behind an ingress that always sets the header while Express has
    // no trust proxy.
    const spaFallbackLimiter = rateLimit({
      windowMs: 60 * 1000,
      limit: 300,
      standardHeaders: true,
      legacyHeaders: false,
      validate: { xForwardedForHeader: false },
    });
    app.get('*', spaFallbackLimiter, (_req, res) => {
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
  // Log only name/message/stack, never the raw error object: startup errors
  // can carry the config that produced them (OIDC client secret, session
  // keys) in extra properties, which must not reach the logs.
  const detail = err instanceof Error ? (err.stack ?? `${err.name}: ${err.message}`) : 'unknown error';
  console.error('ui failed to start:', detail);
  process.exit(1);
});
