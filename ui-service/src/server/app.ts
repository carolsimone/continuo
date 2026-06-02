import express from 'express';
import { GrpcClient } from './grpc-client';
import { GrpcGraphClient } from './grpc-graph-client';
import { createSchedulersRouter } from './routes/schedulers';
import { createNodesRouter } from './routes/nodes';
import { createSchedulesRouter, createRunsRouter } from './routes/schedules';
import { createExecutionsRouter } from './routes/executions';
import { createTaskExecutionRouter } from './routes/task-execution';
import { createConfigRouter } from './routes/config';
import { createTopologyRouter } from './routes/topology';
import { createReleasesRouter } from './routes/releases';
import { createReleaseClient } from './release-client';
import { getLogObject } from './s3';

export function createApp(
  client: GrpcClient,
  graphClient: GrpcGraphClient,
  configFilePath = '/app/config/cancel-config.json',
  releaseControllerUrl = 'http://release-controller:8088',
) {
  const app = express();
  app.use(express.json());
  app.use('/api/schedulers', createSchedulersRouter(client));
  app.use('/api/nodes', createNodesRouter(client));
  app.use('/api/schedules', createSchedulesRouter(client, graphClient));
  app.use('/api/runs', createRunsRouter(graphClient));
  app.use('/api/schedulers', createExecutionsRouter(client));
  app.use('/api/task-execution', createTaskExecutionRouter());
  app.use('/api/topology', createTopologyRouter(graphClient));
  app.use('/api/config', createConfigRouter(configFilePath));
  app.use('/api/releases', createReleasesRouter(createReleaseClient(releaseControllerUrl), getLogObject));
  return app;
}
