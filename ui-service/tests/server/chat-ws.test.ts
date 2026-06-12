import { describe, it, expect, vi, afterEach } from 'vitest';
import { createServer, Server } from 'http';
import type { AddressInfo } from 'net';
import { EventEmitter } from 'events';
import WebSocket from 'ws';
import { attachChatWebSocket } from '../../src/server/ws/chat';
import type { AuthUser } from '../../src/server/auth/types';

class FakeProcess extends EventEmitter {
  send = vi.fn();
  kill = vi.fn();
}

function listen(server: Server): Promise<number> {
  return new Promise((resolve) => server.listen(0, () => resolve((server.address() as AddressInfo).port)));
}

const tick = () => new Promise((r) => setTimeout(r, 20));

const operator: AuthUser = { userId: 'i|o', email: 'o@c.com', name: 'O', role: 'operator' };
const viewer: AuthUser = { userId: 'i|v', email: 'v@c.com', name: 'V', role: 'viewer' };
const asOperator = async () => operator;

let server: Server | undefined;
afterEach(() => server?.close());

describe('attachChatWebSocket', () => {
  it('relays user_message to the process and process messages to the client', async () => {
    const fake = new FakeProcess();
    server = createServer();
    attachChatWebSocket(server, { createProcess: () => fake as any, authenticate: asOperator });
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
      authenticate: asOperator,
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

  it('kills the process when the socket closes', async () => {
    const fake = new FakeProcess();
    server = createServer();
    attachChatWebSocket(server, { createProcess: () => fake as any, authenticate: asOperator });
    const port = await listen(server);
    const client = new WebSocket(`ws://localhost:${port}/ws/chat`);
    await new Promise((r) => client.on('open', r));
    client.close();
    await tick();
    expect(fake.kill).toHaveBeenCalled();
  });

  it('ignores malformed JSON and keeps serving later messages', async () => {
    const fake = new FakeProcess();
    server = createServer();
    attachChatWebSocket(server, { createProcess: () => fake as any, authenticate: asOperator });
    const port = await listen(server);
    const client = new WebSocket(`ws://localhost:${port}/ws/chat`);
    await new Promise((r) => client.on('open', r));
    client.send('not json');
    client.send(JSON.stringify({ type: 'user_message', text: 'hi' }));
    await tick();
    expect(fake.send).toHaveBeenCalledWith('hi');
    client.close();
  });

  it('ignores non-object JSON frames without crashing and keeps serving', async () => {
    const fake = new FakeProcess();
    server = createServer();
    attachChatWebSocket(server, { createProcess: () => fake as any, authenticate: asOperator });
    const port = await listen(server);
    const client = new WebSocket(`ws://localhost:${port}/ws/chat`);
    await new Promise((r) => client.on('open', r));
    client.send(JSON.stringify(null));
    client.send(JSON.stringify(42));
    client.send(JSON.stringify(['user_message']));
    client.send(JSON.stringify({ type: 'user_message', text: 'hi' }));
    await tick();
    expect(fake.send).toHaveBeenCalledTimes(1);
    expect(fake.send).toHaveBeenCalledWith('hi');
    client.close();
  });

  it('ignores a user_message whose text is not a string', async () => {
    const fake = new FakeProcess();
    server = createServer();
    attachChatWebSocket(server, { createProcess: () => fake as any, authenticate: asOperator });
    const port = await listen(server);
    const client = new WebSocket(`ws://localhost:${port}/ws/chat`);
    await new Promise((r) => client.on('open', r));
    client.send(JSON.stringify({ type: 'user_message' }));
    client.send(JSON.stringify({ type: 'user_message', text: 99 }));
    await tick();
    expect(fake.send).not.toHaveBeenCalled();
    client.close();
  });

  it('passes the sessionId query param to the process factory', async () => {
    const seen: (string | undefined)[] = [];
    server = createServer();
    attachChatWebSocket(server, {
      createProcess: (sid) => { seen.push(sid); return new FakeProcess() as any; },
      authenticate: asOperator,
    });
    const port = await listen(server);
    const client = new WebSocket(`ws://localhost:${port}/ws/chat?sessionId=abc`);
    await new Promise((r) => client.on('open', r));
    await tick();
    expect(seen).toContain('abc');
    client.close();
  });

  it('respawns resuming the session after the process exits, on the next message', async () => {
    const procs: FakeProcess[] = [];
    const seen: (string | undefined)[] = [];
    server = createServer();
    attachChatWebSocket(server, {
      createProcess: (sid) => { seen.push(sid); const p = new FakeProcess(); procs.push(p); return p as any; },
      authenticate: asOperator,
    });
    const port = await listen(server);
    const client = new WebSocket(`ws://localhost:${port}/ws/chat`);
    await new Promise((r) => client.on('open', r));

    procs[0].emit('message', { type: 'session', sessionId: 'sess-1' });
    procs[0].emit('exit', 0);
    await tick();

    client.send(JSON.stringify({ type: 'user_message', text: 'again' }));
    await tick();
    expect(procs.length).toBe(2);
    expect(seen[1]).toBe('sess-1');
    expect(procs[1].send).toHaveBeenCalledWith('again');
    client.close();
  });

  it('relays a crash error and respawns resuming the session on the next message', async () => {
    const procs: FakeProcess[] = [];
    const seen: (string | undefined)[] = [];
    server = createServer();
    attachChatWebSocket(server, {
      createProcess: (sid) => { seen.push(sid); const p = new FakeProcess(); procs.push(p); return p as any; },
      authenticate: asOperator,
    });
    const port = await listen(server);
    const client = new WebSocket(`ws://localhost:${port}/ws/chat`);
    await new Promise((r) => client.on('open', r));
    const received: any[] = [];
    client.on('message', (d) => received.push(JSON.parse(d.toString())));

    procs[0].emit('message', { type: 'session', sessionId: 'sess-9' });
    procs[0].emit('message', { type: 'error', code: 'agent_failed', message: 'kaboom' });
    procs[0].emit('exit', 1);
    await tick();
    expect(received).toContainEqual({ type: 'error', code: 'agent_failed', message: 'kaboom' });

    client.send(JSON.stringify({ type: 'user_message', text: 'retry' }));
    await tick();
    expect(procs.length).toBe(2);
    expect(seen[1]).toBe('sess-9');
    expect(procs[1].send).toHaveBeenCalledWith('retry');
    client.close();
  });

  it('relays agent_unavailable to the client', async () => {
    const fake = new FakeProcess();
    server = createServer();
    attachChatWebSocket(server, { createProcess: () => fake as any, authenticate: asOperator });
    const port = await listen(server);
    const client = new WebSocket(`ws://localhost:${port}/ws/chat`);
    await new Promise((r) => client.on('open', r));
    const received: any[] = [];
    client.on('message', (d) => received.push(JSON.parse(d.toString())));
    fake.emit('message', { type: 'error', code: 'agent_unavailable', message: 'spawn claude ENOENT' });
    await tick();
    expect(received).toContainEqual({ type: 'error', code: 'agent_unavailable', message: 'spawn claude ENOENT' });
    client.close();
  });

  it('rejects the upgrade when no authenticate callback is configured (default deny)', async () => {
    server = createServer();
    attachChatWebSocket(server, { createProcess: () => new FakeProcess() as any });
    const port = await listen(server);
    const client = new WebSocket(`ws://localhost:${port}/ws/chat`);
    const err = await new Promise<Error>((resolve) => client.on('error', resolve));
    expect(err.message).toContain('401');
  });

  it('rejects the upgrade for a viewer (operator-only)', async () => {
    server = createServer();
    attachChatWebSocket(server, {
      createProcess: () => new FakeProcess() as any,
      authenticate: async () => viewer,
    });
    const port = await listen(server);
    const client = new WebSocket(`ws://localhost:${port}/ws/chat`);
    const err = await new Promise<Error>((resolve) => client.on('error', resolve));
    expect(err.message).toContain('401');
  });

  it('accepts the upgrade for an operator', async () => {
    server = createServer();
    attachChatWebSocket(server, {
      createProcess: () => new FakeProcess() as any,
      authenticate: asOperator,
    });
    const port = await listen(server);
    const client = new WebSocket(`ws://localhost:${port}/ws/chat`);
    await new Promise((resolve) => client.on('open', resolve));
    client.close();
  });
});
