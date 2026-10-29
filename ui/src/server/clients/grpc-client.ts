import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';
import type { Request } from 'express';

// Canonical gRPC metadata header carrying the initiating user's stable id into
// the backend services. It MUST match pkg/identity.MetadataKey on the Go side
// (the run-controller server interceptor reads this exact, lowercase key); the two are
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
const PROTO_PATH = path.join(process.cwd(), 'proto/run.proto');
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

type Callback = (err: any, res: any) => void;

// GrpcClient is the RunService client of run-controller: the one public API for runs,
// their tasks and attempts, nodes, schedules and the live release's topology.
// A call that changes state takes the caller's metadata (userMetadata) before
// its callback; a read does not.
export interface GrpcClient {
  startRun: (request: any, metadata: grpc.Metadata, callback: Callback) => void;
  triggerRerun: (request: any, metadata: grpc.Metadata, callback: Callback) => void;
  triggerRebase: (request: any, metadata: grpc.Metadata, callback: Callback) => void;
  triggerSingleNodeRun: (request: any, metadata: grpc.Metadata, callback: Callback) => void;
  cancelRun: (request: any, metadata: grpc.Metadata, callback: Callback) => void;
  upsertSchedule: (request: any, metadata: grpc.Metadata, callback: Callback) => void;
  deleteSchedule: (request: any, metadata: grpc.Metadata, callback: Callback) => void;
  previewSchedule: (request: any, callback: Callback) => void;
  previewRebase: (request: any, callback: Callback) => void;
  getRun: (request: any, callback: Callback) => void;
  listRuns: (request: any, callback: Callback) => void;
  listTasks: (request: any, callback: Callback) => void;
  listRunAttempts: (request: any, callback: Callback) => void;
  getRunGraph: (request: any, callback: Callback) => void;
  listNodeTasks: (request: any, callback: Callback) => void;
  listNodes: (request: any, callback: Callback) => void;
  listNodeNames: (request: any, callback: Callback) => void;
  listNodeServices: (request: any, callback: Callback) => void;
  listSchedules: (request: any, callback: Callback) => void;
  getScheduleGraph: (request: any, callback: Callback) => void;
  listScheduleTopologies: (request: any, callback: Callback) => void;
}

export function createGrpcClient(address: string): GrpcClient {
  const client = new proto.run.v1.RunService(
    address,
    grpc.credentials.createInsecure()
  );
  return client as GrpcClient;
}
