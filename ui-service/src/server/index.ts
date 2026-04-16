import path from 'path';
import express from 'express';
import { createGrpcClient } from './grpc-client';
import { createGrpcGraphClient } from './grpc-graph-client';
import { createRedisClient } from './redis-client';
import { createApp } from './app';

const PORT = parseInt(process.env.PORT || '8090', 10);
const STATE_GRPC_ADDR = process.env.STATE_GRPC_ADDR || 'localhost:50051';
const ORCHESTRATOR_GRPC_ADDR = process.env.ORCHESTRATOR_GRPC_ADDR || 'localhost:50052';

const client = createGrpcClient(STATE_GRPC_ADDR);
const graphClient = createGrpcGraphClient(ORCHESTRATOR_GRPC_ADDR);
const redisClient = createRedisClient();
const app = createApp(client, graphClient, redisClient);

if (process.env.NODE_ENV === 'production') {
  // dist/ is one level up from dist-server/ at runtime
  const staticDir = path.join(__dirname, '../dist');
  app.use(express.static(staticDir));
  app.get('*', (_req, res) => {
    res.sendFile(path.join(staticDir, 'index.html'));
  });
}

app.listen(PORT, () => {
  console.log(`Continuo UI running on http://localhost:${PORT}`);
});
