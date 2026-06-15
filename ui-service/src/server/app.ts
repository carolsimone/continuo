import express from 'express';
import { GrpcClient } from './grpc-client';
import { GrpcGraphClient } from './grpc-graph-client';
import { createSchedulersRouter } from './routes/schedulers';
import { createNodesRouter } from './routes/nodes';
import { createSchedulesRouter, createRunsRouter } from './routes/schedules';
import { createExecutionsRouter } from './routes/executions';
import { createTaskExecutionRouter } from './routes/task-execution';
import { createConfigRouter } from './routes/config';
import { createFeaturesRouter } from './routes/features';
import { createTopologyRouter } from './routes/topology';
import { createReleasesRouter } from './routes/releases';
import { createReleaseClient } from './release-client';
import { getLogObject } from './s3';
import type { AppAuth } from './auth/types';

export function createApp(
  client: GrpcClient,
  graphClient: GrpcGraphClient,
  auth: AppAuth,
  configFilePath = '/app/config/cancel-config.json',
  releaseControllerUrl = 'http://release-controller:8088',
  chatBridgeEnabled = false,
) {
  const app = express();
  app.use(express.json());

  // Public: Kubernetes probes (moved off GET /, which serves the SPA shell).
  app.get('/healthz', (_req, res) => res.json({ ok: true }));

  // Identity first (dev fixed user or Redis session), then the login machinery,
  // then the guarded API. Static SPA assets stay public — the shell holds no
  // data and renders the sign-in page when /auth/me is 401.
  app.use(...(auth.authn as [express.RequestHandler, ...express.RequestHandler[]]));
  app.use('/auth', auth.router);
  app.use('/api', ...auth.apiGuards);

  app.use('/api/schedulers', createSchedulersRouter(client));
  app.use('/api/nodes', createNodesRouter(client));
  app.use('/api/schedules', createSchedulesRouter(client, graphClient));
  app.use('/api/runs', createRunsRouter(graphClient));
  app.use('/api/schedulers', createExecutionsRouter(client));
  app.use('/api/task-execution', createTaskExecutionRouter());
  app.use('/api/topology', createTopologyRouter(graphClient));
  app.use('/api/config', createConfigRouter(configFilePath));
  app.use('/api/features', createFeaturesRouter(chatBridgeEnabled));
  app.use('/api/releases', createReleasesRouter(createReleaseClient(releaseControllerUrl), getLogObject));

  app.use(auth.errorHandler);
  return app;
}
