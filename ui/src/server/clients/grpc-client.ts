import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';
import type { Request } from 'express';

// Canonical gRPC metadata header carrying the initiating user's stable id into
// the backend services. It MUST match pkg/identity.MetadataKey on the Go side
// (the state server interceptor reads this exact, lowercase key); the two are
// the single propagation contract. gRPC lowercases metadata keys, so this is
// already lowercase.
export const USER_ID_METADATA_KEY = 'x-continuo-user-id';

// Sentinel recorded when no authenticated user is present. Mirrors
// pkg/identity.SystemUserID.
const SYSTEM_USER_ID = 'system';

// userMetadata builds the outgoing gRPC metadata for a mutating call, stamping
// the authenticated user's id under USER_ID_METADATA_KEY. An unauthenticated
// request (no req.user) falls back to the system sentinel so the backend
// records provenance rather than failing.
export function userMetadata(req: Request): grpc.Metadata {
  const md = new grpc.Metadata();
  md.set(USER_ID_METADATA_KEY, req.user?.userId || SYSTEM_USER_ID);
  return md;
}

// process.cwd() is always the project root (ui/ in dev, /app in Docker).
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
  triggerRerun: (request: any, metadata: grpc.Metadata, callback: (err: any) => void) => void;
  triggerSchedule: (request: any, metadata: grpc.Metadata, callback: (err: any, res: any) => void) => void;
  cancelSchedule: (request: any, metadata: grpc.Metadata, callback: (err: any, res: any) => void) => void;
  triggerSingleNodeRun: (request: any, metadata: grpc.Metadata, callback: (err: any, res: any) => void) => void;
  triggerRebase: (request: any, metadata: grpc.Metadata, callback: (err: any, res: any) => void) => void;
  listNodeRuns: (request: any, callback: (err: any, res: any) => void) => void;
  listNodes: (request: any, callback: (err: any, res: any) => void) => void;
  listNodeNames: (request: any, callback: (err: any, res: any) => void) => void;
  listNodeServices: (request: any, callback: (err: any, res: any) => void) => void;
}

export function createGrpcClient(address: string): GrpcClient {
  const client = new proto.state.v1.StateService(
    address,
    grpc.credentials.createInsecure()
  );
  return client as GrpcClient;
}
