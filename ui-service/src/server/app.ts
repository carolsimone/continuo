import express from 'express';
import Redis from 'ioredis';
import { GrpcClient } from './grpc-client';
import { GrpcGraphClient } from './grpc-graph-client';
import { createSchedulersRouter } from './routes/schedulers';
import { createSchedulesRouter, createRunsRouter } from './routes/schedules';
import { createExecutionsRouter } from './routes/executions';
import { createTaskExecutionRouter } from './routes/task-execution';
import { createGraphRouter } from './routes/graph';

export function createApp(client: GrpcClient, graphClient: GrpcGraphClient, redisClient: Redis | null) {
  const app = express();
  app.use(express.json());
  app.use('/api/schedulers', createSchedulersRouter(client));
  app.use('/api/schedules', createSchedulesRouter(client, graphClient));
  app.use('/api/runs', createRunsRouter(graphClient));
  app.use('/api/schedulers', createExecutionsRouter(client));
  app.use('/api/task-execution', createTaskExecutionRouter());
  app.use('/api/graph', createGraphRouter(redisClient));
  return app;
}
