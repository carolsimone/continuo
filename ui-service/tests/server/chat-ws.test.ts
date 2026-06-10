import { describe, it, expect, vi, afterEach } from 'vitest';
import { createServer, Server } from 'http';
import type { AddressInfo } from 'net';
import { EventEmitter } from 'events';
import WebSocket from 'ws';
import { attachChatWebSocket } from '../../src/server/ws/chat';

class FakeProcess extends EventEmitter {
  send = vi.fn();
  kill = vi.fn();
}

function listen(server: Server): Promise<number> {
  return new Promise((resolve) => server.listen(0, () => resolve((server.address() as AddressInfo).port)));
}

const tick = () => new Promise((r) => setTimeout(r, 20));

let server: Server | undefined;
afterEach(() => server?.close());

describe('attachChatWebSocket', () => {
  it('relays user_message to the process and process messages to the client', async () => {
    const fake = new FakeProcess();
    server = createServer();
    attachChatWebSocket(server, { createProcess: () => fake as any });
    const port = await listen(server);

    const client = new WebSocket(`ws://localhost:${port}/ws/chat`);
    await new Promise((r) => client.on('open', r));
    const received: any[] = [];
    client.on('message', (d) => received.push(JSON.parse(d.toString())));

    client.send(JSON.stringify({ type: 'user_message', text: 'how is daily?' }));
    await tick();
    expect(fake.send).toHaveBeenCalledWith('how is daily?');

    fake.emit('message', { type: 'final', text: 'all green' });
    await tick();
    expect(received).toContainEqual({ type: 'final', text: 'all green' });

    client.close();
  });

  it('respawns the process on new_chat', async () => {
    const procs: FakeProcess[] = [];
    server = createServer();
    attachChatWebSocket(server, {
      createProcess: () => {
        const p = new FakeProcess();
        procs.push(p);
        return p as any;
      },
    });
    const port = await listen(server);
    const client = new WebSocket(`ws://localhost:${port}/ws/chat`);
    await new Promise((r) => client.on('open', r));

    client.send(JSON.stringify({ type: 'new_chat' }));
    await tick();
    expect(procs.length).toBe(2);
    expect(procs[0].kill).toHaveBeenCalled();
    client.close();
  });
});
