import express from 'express';
import Redis from 'ioredis';
import { GrpcClient } from './grpc-client';
import { GrpcGraphClient } from './grpc-graph-client';
import { createSchedulersRouter } from './routes/schedulers';
import { createNodesRouter } from './routes/nodes';
import { createSchedulesRouter, createRunsRouter } from './routes/schedules';
import { createExecutionsRouter } from './routes/executions';
import { createTaskExecutionRouter } from './routes/task-execution';
import { createGraphRouter, createDashboardGraphRouter } from './routes/graph';
import { createConfigRouter } from './routes/config';
import { createTopologyRouter } from './routes/topology';

export function createApp(
  client: GrpcClient,
  graphClient: GrpcGraphClient,
  redisClient: Redis | null,
  configFilePath = '/app/config/cancel-config.json',
) {
  const app = express();
  app.use(express.json());
  app.use('/api/schedulers', createSchedulersRouter(client));
  app.use('/api/nodes', createNodesRouter(client));
  app.use('/api/schedules', createSchedulesRouter(client, graphClient));
  app.use('/api/runs', createRunsRouter(graphClient));
  app.use('/api/schedulers', createExecutionsRouter(client));
  app.use('/api/task-execution', createTaskExecutionRouter());
  app.use('/api/graph', createGraphRouter(redisClient));
  app.use('/api/dashboard', createDashboardGraphRouter(redisClient));
  app.use('/api/topology', createTopologyRouter(graphClient));
  app.use('/api/config', createConfigRouter(configFilePath));
  return app;
}
