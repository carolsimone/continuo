import { WebSocketServer, WebSocket } from 'ws';
import type { IncomingMessage, Server } from 'http';
import type { AgentChatStream } from '../agent-client';
import type { ClientMessage } from './chat-protocol';
import { toClientEvent, fromServerEvent } from './chat-protocol';
import { audit } from '../auth/audit';
import type { AuthUser } from '../auth/types';

export interface ChatWsOptions {
  // Factory that opens a new bidirectional gRPC stream to agent-runner.
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

    stream.write({ open: { user_id: user.userId, thread_id: threadId } });

    stream.on('data', (ev) => {
      const msg = fromServerEvent(ev);
      if (msg) sendJSON(ws, msg);
    });

    stream.on('error', (err) => {
      sendJSON(ws, { type: 'error', code: 'agent_unavailable', message: err.message });
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
      if (ev) stream.write(ev);
    });

    ws.on('close', () => stream.cancel());
  });

  return wss;
}
