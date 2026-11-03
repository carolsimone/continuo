import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { EventEmitter } from 'events';
import { mkdtempSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import path from 'path';
import { refreshRedisCredentials, watchRedisCredentials } from '../../src/server/auth/redis-credentials';

describe('refreshRedisCredentials', () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(path.join(tmpdir(), 'redis-credentials-'));
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it('refreshes the password from a rewritten file', () => {
    const file = path.join(dir, 'REDIS_PASSWORD');
    writeFileSync(file, 'first\n');
    const env = { REDIS_USERNAME: 'ui', REDIS_PASSWORD_FILE: file };
    const client: { options: { username?: string; password?: string } } = { options: {} };

    refreshRedisCredentials(client, env);
    expect(client.options).toEqual({ username: 'ui', password: 'first' });

    writeFileSync(file, 'second\n');
    refreshRedisCredentials(client, env);
    expect(client.options.password).toBe('second');
  });

  it('leaves the username and password unset when neither is configured', () => {
    const client = { options: { username: 'old', password: 'old' } };
    refreshRedisCredentials(client, {});
    expect(client.options.username).toBeUndefined();
    expect(client.options.password).toBeUndefined();
  });
});

describe('watchRedisCredentials', () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(path.join(tmpdir(), 'redis-credentials-'));
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  class FakeRedis extends EventEmitter {
    options: { username?: string; password?: string } = { password: 'first' };
  }

  it('picks up a rewritten password file on reconnecting', () => {
    const file = path.join(dir, 'REDIS_PASSWORD');
    writeFileSync(file, 'first\n');
    const redis = new FakeRedis();
    const log = { error: vi.fn() };
    watchRedisCredentials(redis, { REDIS_USERNAME: 'ui', REDIS_PASSWORD_FILE: file }, log);

    writeFileSync(file, 'second\n');
    redis.emit('reconnecting');

    expect(redis.options).toEqual({ username: 'ui', password: 'second' });
    expect(log.error).not.toHaveBeenCalled();
  });

  it('logs an unreadable file and keeps the previous password', () => {
    const file = path.join(dir, 'REDIS_PASSWORD');
    const redis = new FakeRedis();
    const log = { error: vi.fn() };
    watchRedisCredentials(redis, { REDIS_PASSWORD_FILE: file }, log);

    expect(() => redis.emit('reconnecting')).not.toThrow();

    expect(redis.options.password).toBe('first');
    expect(log.error).toHaveBeenCalledTimes(1);
    expect(log.error.mock.calls[0][0]).toMatch(/^redis: keeping the previous credentials: REDIS_PASSWORD_FILE: /);
  });
});
