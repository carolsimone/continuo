import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';

const PROTO_PATH = path.join(process.cwd(), 'proto/graph/v1/graph.proto');
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

export interface GrpcGraphClient {
  getScheduleGraph: (request: any, callback: (err: any, res: any) => void) => void;
  listRuns: (request: any, callback: (err: any, res: any) => void) => void;
  getRunGraph: (request: any, callback: (err: any, res: any) => void) => void;
}

export function createGrpcGraphClient(address: string): GrpcGraphClient {
  const client = new proto.graph.v1.GraphService(
    address,
    grpc.credentials.createInsecure()
  );
  return client as GrpcGraphClient;
}
