import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';

const PROTO_PATH = path.join(process.cwd(), 'proto/agentchat/v1/agentchat.proto');
const PROTO_DIR = path.join(process.cwd(), 'proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: false,
  oneofs: true,
  includeDirs: [PROTO_DIR],
});

const proto = grpc.loadPackageDefinition(packageDefinition) as any;

export interface AgentChatStream {
  write(event: object): void;
  end(): void;
  cancel(): void;
  on(event: 'data', cb: (ev: any) => void): void;
  on(event: 'error', cb: (err: Error) => void): void;
  on(event: 'end', cb: () => void): void;
}

// createAgentClient returns a factory function that opens a new bidirectional
// gRPC stream to agent-runner each time it is called. One stream is opened per
// WebSocket connection.
export function createAgentClient(address: string): () => AgentChatStream {
  const client = new proto.agentchat.v1.AgentChat(address, grpc.credentials.createInsecure());
  return () => client.Chat() as AgentChatStream;
}
