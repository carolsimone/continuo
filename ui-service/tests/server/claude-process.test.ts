import { describe, it, expect, vi } from 'vitest';
import { EventEmitter } from 'events';
import { PassThrough } from 'stream';
import { ClaudeProcess } from '../../src/server/ws/claude-process';

function makeFakeChild() {
  const child: any = new EventEmitter();
  child.stdout = new PassThrough();
  child.stderr = new PassThrough();
  child.stdin = { write: vi.fn() };
  child.kill = vi.fn();
  return child;
}

const tick = () => new Promise((r) => setTimeout(r, 10));

describe('ClaudeProcess', () => {
  it('spawns claude with streaming + guardrail flags and no resume by default', () => {
    const child = makeFakeChild();
    const spawnFn = vi.fn().mockReturnValue(child);
    new ClaudeProcess({ spawnFn: spawnFn as any });
    const [bin, args] = spawnFn.mock.calls[0];
    expect(bin).toBe('claude');
    expect(args).toEqual(expect.arrayContaining(['-p', '--input-format', 'stream-json', '--output-format', 'stream-json']));
    expect(args).toContain('--allowedTools');
    const allow = args[args.indexOf('--allowedTools') + 1];
    expect(allow).toContain('Bash(continuo schedule status:*)');
    expect(allow).not.toContain('trigger');
    expect(allow).not.toBe('Bash(continuo:*)');
    expect(args).not.toContain('--resume');
  });

  it('adds --resume when a sessionId is supplied', () => {
    const child = makeFakeChild();
    const spawnFn = vi.fn().mockReturnValue(child);
    new ClaudeProcess({ sessionId: 'abc123', spawnFn: spawnFn as any });
    const [, args] = spawnFn.mock.calls[0];
    expect(args).toEqual(expect.arrayContaining(['--resume', 'abc123']));
  });

  it('emits classified messages from stdout lines', async () => {
    const child = makeFakeChild();
    const proc = new ClaudeProcess({ spawnFn: (() => child) as any });
    const messages: any[] = [];
    proc.on('message', (m) => messages.push(m));
    child.stdout.write(JSON.stringify({ type: 'system', subtype: 'init', session_id: 'abc' }) + '\n');
    child.stdout.write(JSON.stringify({ type: 'result', result: 'done', is_error: false }) + '\n');
    await tick();
    expect(messages).toEqual([
      { type: 'session', sessionId: 'abc' },
      { type: 'final', text: 'done' },
    ]);
  });

  it('writes a stream-json user turn to stdin on send', () => {
    const child = makeFakeChild();
    const proc = new ClaudeProcess({ spawnFn: (() => child) as any });
    proc.send('hello');
    expect(child.stdin.write).toHaveBeenCalledOnce();
    expect(JSON.parse(child.stdin.write.mock.calls[0][0])).toMatchObject({ type: 'user', message: { role: 'user' } });
  });

  it('emits an error message with stderr on non-zero exit', async () => {
    const child = makeFakeChild();
    const proc = new ClaudeProcess({ spawnFn: (() => child) as any });
    const messages: any[] = [];
    proc.on('message', (m) => messages.push(m));
    child.stderr.write('kaboom');
    await tick();
    child.emit('exit', 1);
    await tick();
    expect(messages).toContainEqual({ type: 'error', code: 'agent_failed', message: 'kaboom' });
  });

  it('emits an unavailable error when spawn errors', async () => {
    const child = makeFakeChild();
    const proc = new ClaudeProcess({ spawnFn: (() => child) as any });
    const messages: any[] = [];
    proc.on('message', (m) => messages.push(m));
    child.emit('error', new Error('ENOENT claude'));
    await tick();
    expect(messages).toContainEqual({ type: 'error', code: 'agent_unavailable', message: 'ENOENT claude' });
  });

  it('emits exit with code 0 and no error message on clean exit', async () => {
    const child = makeFakeChild();
    const proc = new ClaudeProcess({ spawnFn: (() => child) as any });
    const messages: any[] = [];
    let exitCode: number | null | undefined;
    proc.on('message', (m) => messages.push(m));
    proc.on('exit', (code) => { exitCode = code; });
    child.emit('exit', 0);
    await tick();
    expect(exitCode).toBe(0);
    expect(messages).toEqual([]);
  });

  it('maps STATE_GRPC_ADDR / ORCHESTRATOR_GRPC_ADDR to the CLI env vars', () => {
    const prevState = process.env.STATE_GRPC_ADDR;
    const prevOrch = process.env.ORCHESTRATOR_GRPC_ADDR;
    process.env.STATE_GRPC_ADDR = 'state:50051';
    process.env.ORCHESTRATOR_GRPC_ADDR = 'orchestrator:50052';
    try {
      const child = makeFakeChild();
      const spawnFn = vi.fn().mockReturnValue(child);
      new ClaudeProcess({ spawnFn: spawnFn as any });
      const opts = spawnFn.mock.calls[0][2];
      expect(opts.env.CONTINUO_STATE_ADDR).toBe('state:50051');
      expect(opts.env.CONTINUO_ORCHESTRATOR_ADDR).toBe('orchestrator:50052');
    } finally {
      if (prevState === undefined) delete process.env.STATE_GRPC_ADDR; else process.env.STATE_GRPC_ADDR = prevState;
      if (prevOrch === undefined) delete process.env.ORCHESTRATOR_GRPC_ADDR; else process.env.ORCHESTRATOR_GRPC_ADDR = prevOrch;
    }
  });

  it('emits an exit event after a spawn error', async () => {
    const child = makeFakeChild();
    const proc = new ClaudeProcess({ spawnFn: (() => child) as any });
    const events: string[] = [];
    proc.on('message', () => events.push('message'));
    proc.on('exit', () => events.push('exit'));
    child.emit('error', new Error('ENOENT claude'));
    await tick();
    expect(events).toEqual(['message', 'exit']);
  });
});
