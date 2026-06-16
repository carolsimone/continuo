import { describe, it, expect, vi, afterEach } from 'vitest';
import { createServer, Server } from 'http';
import type { AddressInfo } from 'net';
import { EventEmitter } from 'events';
import WebSocket from 'ws';
import { status as grpcStatus } from '@grpc/grpc-js';
import { attachChatWebSocket } from '../../src/server/ws/chat';
import type { AuthUser } from '../../src/server/auth/types';

class FakeStream extends EventEmitter {
  written: object[] = [];
  cancelled = false;
  write(ev: object) { this.written.push(ev); }
  end() {}
  cancel() { this.cancelled = true; }
}

const operator: AuthUser = { userId: 'i|o', email: 'o@c.com', name: 'O', role: 'operator' };
const viewer: AuthUser = { userId: 'i|v', email: 'v@c.com', name: 'V', role: 'viewer' };
const asOperator = async () => operator;

function listen(server: Server): Promise<number> {
  return new Promise((r) => server.listen(0, () => r((server.address() as AddressInfo).port)));
}
function connected(ws: WebSocket): Promise<void> {
  return new Promise((res) => ws.on('open', () => res()));
}
function nextMessage(ws: WebSocket): Promise<any> {
  return new Promise((res) => ws.once('message', (d) => res(JSON.parse(d.toString()))));
}

let server: Server | undefined;
afterEach(() => server?.close());

describe('chat auth upgrade', () => {
  it('rejects the upgrade when no authenticate callback is configured (default deny)', async () => {
    server = createServer();
    attachChatWebSocket(server, { agentClient: () => new FakeStream() as any });
    const port = await listen(server);
    const client = new WebSocket(`ws://localhost:${port}/ws/chat`);
    const err = await new Promise<Error>((resolve) => client.on('error', resolve));
    expect(err.message).toContain('401');
  });

  it('rejects the upgrade for a viewer (operator-only)', async () => {
    server = createServer();
    attachChatWebSocket(server, { agentClient: () => new FakeStream() as any, authenticate: async () => viewer });
    const port = await listen(server);
    const client = new WebSocket(`ws://localhost:${port}/ws/chat`);
    const err = await new Promise<Error>((resolve) => client.on('error', resolve));
    expect(err.message).toContain('401');
  });

  it('accepts the upgrade for an operator', async () => {
    server = createServer();
    attachChatWebSocket(server, { agentClient: () => new FakeStream() as any, authenticate: asOperator });
    const port = await listen(server);
    const client = new WebSocket(`ws://localhost:${port}/ws/chat`);
    await new Promise((resolve) => client.on('open', resolve));
    client.close();
  });

  it('rejects the upgrade when the browser Origin does not match allowedOrigin', async () => {
    server = createServer();
    attachChatWebSocket(server, { agentClient: () => new FakeStream() as any, authenticate: asOperator, allowedOrigin: 'https://app.example.com' });
    const port = await listen(server);
    const client = new WebSocket(`ws://localhost:${port}/ws/chat`, { origin: 'https://evil.example.com' });
    const err = await new Promise<Error>((resolve) => client.on('error', resolve));
    expect(err.message).toContain('403');
  });

  it('accepts the upgrade when the browser Origin matches allowedOrigin', async () => {
    server = createServer();
    attachChatWebSocket(server, { agentClient: () => new FakeStream() as any, authenticate: asOperator, allowedOrigin: 'https://app.example.com' });
    const port = await listen(server);
    const client = new WebSocket(`ws://localhost:${port}/ws/chat`, { origin: 'https://app.example.com' });
    await new Promise((resolve) => client.on('open', resolve));
    client.close();
  });

  it('accepts the upgrade with no Origin header (non-browser client)', async () => {
    server = createServer();
    attachChatWebSocket(server, { agentClient: () => new FakeStream() as any, authenticate: asOperator, allowedOrigin: 'https://app.example.com' });
    const port = await listen(server);
    const client = new WebSocket(`ws://localhost:${port}/ws/chat`);
    await new Promise((resolve) => client.on('open', resolve));
    client.close();
  });
});

describe('chat gRPC relay (operator session)', () => {
  function setup() {
    const stream = new FakeStream();
    server = createServer();
    attachChatWebSocket(server, { agentClient: () => stream as any, authenticate: asOperator });
    return listen(server).then((port) => ({ stream, url: `ws://127.0.0.1:${port}/ws/chat` }));
  }

  it('opens the stream with the authenticated userId and the threadId query param', async () => {
    const { stream, url } = await setup();
    const ws = new WebSocket(`${url}?threadId=t-1`);
    await connected(ws);
    expect(stream.written[0]).toEqual({ open: { user_id: 'i|o', thread_id: 't-1' } });
    ws.close();
  });

  it('relays client frames to ClientEvents and ServerEvents to frames', async () => {
    const { stream, url } = await setup();
    const ws = new WebSocket(url);
    await connected(ws);

    ws.send(JSON.stringify({ type: 'user_message', text: 'hi' }));
    ws.send(JSON.stringify({ type: 'confirm_response', actionId: 'a1', approved: true }));
    ws.send(JSON.stringify({ type: 'interrupt' }));
    await vi.waitFor(() => expect(stream.written.length).toBe(4));

    expect(stream.written[1]).toEqual({ user_message: { text: 'hi' } });
    expect(stream.written[2]).toEqual({ confirm_response: { action_id: 'a1', approved: true } });
    expect(stream.written[3]).toEqual({ interrupt: {} });

    const got = nextMessage(ws);
    stream.emit('data', { event: 'text', text: { text: 'All ' } });
    expect(await got).toEqual({ type: 'text', text: 'All ' });

    const got2 = nextMessage(ws);
    stream.emit('data', { event: 'confirm_request', confirm_request: { action_id: 'a1', summary: 'Run `x`?' } });
    expect(await got2).toEqual({ type: 'confirm_request', actionId: 'a1', summary: 'Run `x`?' });

    ws.close();
  });

  it('cancels the gRPC stream when the socket closes, and reports stream errors in-panel', async () => {
    const { stream, url } = await setup();
    const ws = new WebSocket(url);
    await connected(ws);
    const got = nextMessage(ws);
    stream.emit('error', new Error('connect ECONNREFUSED'));
    const err = await got;
    expect(err.type).toBe('error');
    expect(err.code).toBe('agent_unavailable');
    ws.close();
    await vi.waitFor(() => expect(stream.cancelled).toBe(true));
  });

  it('maps a gRPC NOT_FOUND stream error to thread_not_found (deleted/foreign thread)', async () => {
    const { stream, url } = await setup();
    const ws = new WebSocket(`${url}?threadId=missing`);
    await connected(ws);
    const got = nextMessage(ws);
    const notFound: any = new Error('open session: agent-runner: not found');
    notFound.code = grpcStatus.NOT_FOUND;
    stream.emit('error', notFound);
    const err = await got;
    expect(err.type).toBe('error');
    expect(err.code).toBe('thread_not_found');
    ws.close();
  });

  it('does not crash when a message is sent after the stream errors', async () => {
    const { stream, url } = await setup();
    const ws = new WebSocket(url);
    await connected(ws);

    // Collect all messages received by the client.
    const received: any[] = [];
    ws.on('message', (d) => received.push(JSON.parse(d.toString())));

    // Emit a non-CANCELLED stream error — this should send an error frame and
    // close the WebSocket.
    stream.emit('error', new Error('boom'));

    // Wait for the error frame to arrive.
    await vi.waitFor(() => expect(received.length).toBeGreaterThan(0));
    expect(received[0]).toMatchObject({ type: 'error', code: 'agent_unavailable' });

    // Wait for the socket to be closed by the server.
    await vi.waitFor(() => expect(ws.readyState).toBe(WebSocket.CLOSED));

    // Attempt to send a message AFTER the stream has errored. This must not
    // throw ERR_STREAM_DESTROYED or produce an unhandled rejection — it is a
    // no-op because the WebSocket is already closed.
    ws.send(JSON.stringify({ type: 'user_message', text: 'after' }));

    // Allow the event loop to drain so any synchronous throws or unhandled
    // promise rejections would surface before the assertion.
    await new Promise((r) => setImmediate(r));

    // The stream write method must not have been called after the error (only
    // the initial open write was present before the error).
    const writeCountAtError = stream.written.length;
    expect(writeCountAtError).toBe(1); // only the open frame
  });
});
