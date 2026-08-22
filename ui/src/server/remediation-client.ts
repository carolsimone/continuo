import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';

const PROTO_PATH = path.join(process.cwd(), 'proto/remediation/v1/remediation.proto');
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

export interface RemediationClient {
  listProposals: (request: any) => Promise<any>;
  getProposal: (request: any) => Promise<any>;
  beginPullRequest: (request: any) => Promise<any>;
  recordPullRequest: (request: any) => Promise<any>;
  failPullRequest: (request: any) => Promise<any>;
}

export function createRemediationClient(address: string): RemediationClient {
  const stub = new proto.remediation.v1.RemediationProposals(
    address,
    grpc.credentials.createInsecure()
  );

  return {
    listProposals: (request: any) =>
      new Promise((resolve, reject) =>
        stub.listProposals(request, (err: any, res: any) => (err ? reject(err) : resolve(res)))
      ),
    getProposal: (request: any) =>
      new Promise((resolve, reject) =>
        stub.getProposal(request, (err: any, res: any) => (err ? reject(err) : resolve(res)))
      ),
    beginPullRequest: (request: any) =>
      new Promise((resolve, reject) =>
        stub.beginPullRequest(request, (err: any, res: any) => (err ? reject(err) : resolve(res)))
      ),
    recordPullRequest: (request: any) =>
      new Promise((resolve, reject) =>
        stub.recordPullRequest(request, (err: any, res: any) => (err ? reject(err) : resolve(res)))
      ),
    failPullRequest: (request: any) =>
      new Promise((resolve, reject) =>
        stub.failPullRequest(request, (err: any, res: any) => (err ? reject(err) : resolve(res)))
      ),
  };
}
