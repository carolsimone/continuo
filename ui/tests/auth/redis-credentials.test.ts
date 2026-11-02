import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtempSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import path from 'path';
import { refreshRedisCredentials } from '../../src/server/auth/redis-credentials';

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
