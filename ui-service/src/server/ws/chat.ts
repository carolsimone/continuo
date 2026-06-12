import { WebSocketServer, WebSocket } from 'ws';
import type { IncomingMessage, Server } from 'http';
import { ClaudeProcess } from './claude-process';
import type { ClientMessage, ServerMessage } from './chat-protocol';
import { audit } from '../auth/audit';
import type { AuthUser } from '../auth/types';

export interface ChatWsOptions {
  createProcess?: (sessionId?: string) => ClaudeProcess;
  // Resolves the incoming HTTP upgrade request to a user; returning null rejects
  // the upgrade. Defaults to deny-all so an unwired chat socket can never be
  // opened. The chat endpoint is operator-only: it spends LLM tokens and can
  // request run triggers.
  authenticate?: (req: IncomingMessage) => Promise<AuthUser | null>;
}

export function attachChatWebSocket(server: Server, opts: ChatWsOptions = {}): WebSocketServer {
  const createProcess = opts.createProcess ?? ((sessionId?: string) => new ClaudeProcess({ sessionId }));
  const authenticate = opts.authenticate ?? (async () => null);
  const wss = new WebSocketServer({ noServer: true });

  server.on('upgrade', (req, socket, head) => {
    const { pathname } = new URL(req.url ?? '/', 'http://localhost');
    if (pathname !== '/ws/chat') {
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
        // The authenticated user is passed as the third connection argument.
        // Today's handler ignores it; the agent-runner relay will use it on the
        // gRPC stream.
        wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req, user));
      })
      .catch((err) => {
        console.error('ws auth error:', err);
        socket.write('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n');
        socket.destroy();
      });
  });

  wss.on('connection', (ws, req) => {
    const url = new URL(req.url ?? '/', 'http://localhost');
    let sessionId = url.searchParams.get('sessionId') ?? undefined;
    let alive = false;

    let proc = wire(sessionId);

    function wire(sid?: string): ClaudeProcess {
      const p = createProcess(sid);
      alive = true;
      p.on('message', (msg: ServerMessage) => {
        if (msg.type === 'session') sessionId = msg.sessionId;
        if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
      });
      p.on('exit', () => {
        alive = false;
      });
      return p;
    }

    // A persistent claude process may exit between turns (clean end or crash).
    // Respawn lazily on the next user turn, resuming the captured session so the
    // conversation continues instead of writing to a dead stdin and hanging.
    function ensureAlive(): void {
      if (!alive) {
        proc.removeAllListeners();
        proc = wire(sessionId);
      }
    }

    ws.on('message', (data) => {
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
      if (parsed.type === 'user_message' && typeof parsed.text === 'string') {
        ensureAlive();
        proc.send(parsed.text);
      } else if (parsed.type === 'new_chat') {
        proc.removeAllListeners();
        proc.kill();
        sessionId = undefined;
        proc = wire(undefined);
      }
    });

    ws.on('close', () => {
      proc.removeAllListeners();
      proc.kill();
    });
  });

  return wss;
}
