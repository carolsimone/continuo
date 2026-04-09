import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';

// process.cwd() is always the project root (ui-service/ in dev, /app in Docker).
// Using __dirname would break prod: dist-server/ is 1 level deep, src/server/ is 2 levels deep.
const PROTO_PATH = path.join(process.cwd(), 'proto/state.proto');
const PROTO_DIR = path.join(process.cwd(), 'proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
  includeDirs: [PROTO_DIR],
});

const proto = grpc.loadPackageDefinition(packageDefinition) as any;

export interface GrpcClient {
  listAllSchedules: (request: any, callback: (err: any, res: any) => void) => void;
  listTasks: (request: any, callback: (err: any, res: any) => void) => void;
  getScheduler: (request: any, callback: (err: any, res: any) => void) => void;
  listTaskExecutions: (request: any, callback: (err: any, res: any) => void) => void;
}

export function createGrpcClient(address: string): GrpcClient {
  const client = new proto.state.v1.StateService(
    address,
    grpc.credentials.createInsecure()
  );
  return client as GrpcClient;
}
