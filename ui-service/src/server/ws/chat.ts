import { WebSocketServer, WebSocket } from 'ws';
import type { Server } from 'http';
import { ClaudeProcess } from './claude-process';
import type { ClientMessage, ServerMessage } from './chat-protocol';

export interface ChatWsOptions {
  createProcess?: (sessionId?: string) => ClaudeProcess;
}

export function attachChatWebSocket(server: Server, opts: ChatWsOptions = {}): WebSocketServer {
  const createProcess = opts.createProcess ?? ((sessionId?: string) => new ClaudeProcess({ sessionId }));
  const wss = new WebSocketServer({ server, path: '/ws/chat' });

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
      let parsed: ClientMessage;
      try {
        parsed = JSON.parse(data.toString());
      } catch {
        return;
      }
      if (parsed.type === 'user_message') {
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
