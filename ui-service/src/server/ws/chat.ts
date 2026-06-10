import { WebSocketServer } from 'ws';
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
    const initialSession = url.searchParams.get('sessionId') ?? undefined;

    let proc = wire(initialSession);

    function wire(sessionId?: string): ClaudeProcess {
      const p = createProcess(sessionId);
      p.on('message', (msg: ServerMessage) => {
        if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
      });
      return p;
    }

    ws.on('message', (data) => {
      let parsed: ClientMessage;
      try {
        parsed = JSON.parse(data.toString());
      } catch {
        return;
      }
      if (parsed.type === 'user_message') {
        proc.send(parsed.text);
      } else if (parsed.type === 'new_chat') {
        proc.kill();
        proc = wire(undefined);
      }
    });

    ws.on('close', () => proc.kill());
  });

  return wss;
}
