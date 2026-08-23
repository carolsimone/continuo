import { WebSocketServer, WebSocket } from 'ws';
import type { IncomingMessage, Server } from 'http';
import { status as grpcStatus } from '@grpc/grpc-js';
import type { AgentChatStream } from '../agent-client';
import type { ClientMessage } from './chat-protocol';
import { toClientEvent, fromServerEvent } from './chat-protocol';
import { audit } from '../auth/audit';
import type { AuthUser } from '../auth/types';

export interface ChatWsOptions {
  // Factory that opens a new bidirectional gRPC stream to agent-chat.
  // Called once per WebSocket connection upgrade. The stream is cancelled when
  // the socket closes.
  agentClient: () => AgentChatStream;
  // Resolves the incoming HTTP upgrade request to a user; returning null rejects
  // the upgrade. Defaults to deny-all so an unwired chat socket can never be
  // opened. The chat endpoint is operator-only: it spends LLM tokens and can
  // request run triggers.
  authenticate?: (req: IncomingMessage) => Promise<AuthUser | null>;
  // When set, browser upgrade requests whose Origin header does not exactly match
  // this value are rejected with 403 before authentication is attempted. Mirrors
  // the csrfOriginCheck policy used for /api routes to prevent cross-site
  // WebSocket hijacking (CSWSH). Requests without an Origin header (non-browser
  // clients, server-side tests) are always allowed regardless of this setting.
  // Leave undefined (dev mode) to skip the check entirely.
  allowedOrigin?: string;
}

function sendJSON(ws: WebSocket, msg: object): void {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

export function attachChatWebSocket(server: Server, opts: ChatWsOptions): WebSocketServer {
  const authenticate = opts.authenticate ?? (async () => null);
  const wss = new WebSocketServer({ noServer: true });

  server.on('upgrade', (req, socket, head) => {
    const { pathname } = new URL(req.url ?? '/', 'http://localhost');
    if (pathname !== '/ws/chat') {
      socket.destroy();
      return;
    }
    const origin = req.headers.origin;
    if (opts.allowedOrigin && origin && origin !== opts.allowedOrigin) {
      audit('ws_denied', { path: '/ws/chat', origin, outcome: 'cross_origin' });
      socket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n');
      socket.destroy();
      return;
    }
    authenticate(req)
      .then((user) => {
        if (!user || user.role !== 'operator') {
          audit('ws_denied', {
            path: '/ws/chat',
            user_id: user?.userId,
            outcome: user ? 'forbidden' : 'unauthenticated',
          });
          socket.write('HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n');
          socket.destroy();
          return;
        }
        // Pass the authenticated user as the third connection argument so
        // connection handlers can inspect role and identity without re-reading
        // session state.
        wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req, user));
      })
      .catch((err) => {
        console.error('ws auth error:', err);
        socket.write('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n');
        socket.destroy();
      });
  });

  wss.on('connection', (ws: WebSocket, req: IncomingMessage, user: AuthUser) => {
    const url = new URL(req.url ?? '/', 'http://localhost');
    const threadId = url.searchParams.get('threadId') ?? '';

    let stream: AgentChatStream;
    try {
      stream = opts.agentClient();
    } catch (err: any) {
      sendJSON(ws, { type: 'error', code: 'agent_unavailable', message: err?.message ?? 'agent unavailable' });
      ws.close();
      return;
    }

    // Tracks whether the gRPC stream is still usable. Set to false on stream
    // error or socket close so late message handlers never attempt a write to a
    // destroyed stream (which would throw ERR_STREAM_DESTROYED).
    let streamAlive = true;

    stream.write({ open: { user_id: user.userId, thread_id: threadId } });

    stream.on('data', (ev) => {
      const msg = fromServerEvent(ev);
      if (msg) sendJSON(ws, msg);
    });

    stream.on('error', (err: any) => {
      // A CANCELLED status means we triggered stream.cancel() ourselves (e.g.
      // on socket close). That is a clean teardown — do not surface it as an
      // error to the client.
      const isCancelled = err?.code === grpcStatus.CANCELLED;
      if (!isCancelled) {
        // Map the gRPC status to a precise client error. agent-chat returns
        // NOT_FOUND when a resumed thread_id does not exist or is not owned by
        // the user; surface that distinctly as thread_not_found so the client
        // can drop its stale thread and start fresh instead of looping on
        // "agent unavailable". Everything else stays agent_unavailable.
        const code = err?.code === grpcStatus.NOT_FOUND ? 'thread_not_found' : 'agent_unavailable';
        sendJSON(ws, { type: 'error', code, message: err?.message ?? 'agent unavailable' });
      }
      streamAlive = false;
      ws.close();
    });

    stream.on('end', () => {
      if (ws.readyState === WebSocket.OPEN) ws.close();
    });

    ws.on('message', (data: Buffer | ArrayBuffer | Buffer[]) => {
      let raw: unknown;
      try {
        raw = JSON.parse(data.toString());
      } catch {
        return;
      }
      // Valid JSON can still decode to null, a number, or an array; reject anything
      // that is not a plain object before dereferencing so a malformed frame cannot
      // throw an uncaught exception and tear down the server.
      if (typeof raw !== 'object' || raw === null) return;
      const parsed = raw as ClientMessage;
      const ev = toClientEvent(parsed);
      if (!ev) return;
      if (!streamAlive) {
        sendJSON(ws, { type: 'error', code: 'agent_unavailable', message: 'stream closed' });
        ws.close();
        return;
      }
      try {
        stream.write(ev);
      } catch (writeErr: any) {
        sendJSON(ws, { type: 'error', code: 'agent_unavailable', message: writeErr?.message ?? 'write failed' });
        ws.close();
      }
    });

    // Absorb WebSocket-level errors (e.g. ECONNRESET) so they never become
    // unhandled exceptions. Tear down the gRPC stream when the socket errors.
    ws.on('error', () => {
      try { stream.cancel(); } catch {}
    });

    ws.on('close', () => {
      streamAlive = false;
      try { stream.cancel(); } catch {}
    });
  });

  return wss;
}
