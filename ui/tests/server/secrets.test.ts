import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtempSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import path from 'path';
import { readSecret } from '../../src/server/secrets';

describe('readSecret', () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(path.join(tmpdir(), 'secrets-'));
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  function fileWith(content: string): string {
    const p = path.join(dir, 'secret');
    writeFileSync(p, content);
    return p;
  }

  it('returns the environment variable', () => {
    expect(readSecret({ REDIS_PASSWORD: 'pw' }, 'REDIS_PASSWORD')).toBe('pw');
  });

  it('is undefined when neither form is set', () => {
    expect(readSecret({}, 'REDIS_PASSWORD')).toBeUndefined();
    expect(readSecret({ REDIS_PASSWORD: '', REDIS_PASSWORD_FILE: '' }, 'REDIS_PASSWORD')).toBeUndefined();
  });

  it('reads a secret from a file and trims one newline', () => {
    const cases: Array<[string, string]> = [
      ['pw\n', 'pw'],
      ['pw\r\n', 'pw'],
      ['pw\n\n', 'pw\n'],
      ["p w'@:\\", "p w'@:\\"],
    ];
    for (const [content, want] of cases) {
      const env = { REDIS_PASSWORD_FILE: fileWith(content) };
      expect(readSecret(env, 'REDIS_PASSWORD')).toBe(want);
    }
  });

  it('throws when both forms are set', () => {
    const env = { REDIS_PASSWORD: 'pw', REDIS_PASSWORD_FILE: fileWith('pw') };
    expect(() => readSecret(env, 'REDIS_PASSWORD')).toThrow(
      'REDIS_PASSWORD: set REDIS_PASSWORD or REDIS_PASSWORD_FILE, not both',
    );
  });

  it('reads the file again on every call', () => {
    const p = fileWith('a');
    const env = { REDIS_PASSWORD_FILE: p };
    expect(readSecret(env, 'REDIS_PASSWORD')).toBe('a');
    writeFileSync(p, 'b');
    expect(readSecret(env, 'REDIS_PASSWORD')).toBe('b');
  });

  it('throws when the file is missing', () => {
    const missing = path.join(dir, 'absent');
    expect(() => readSecret({ REDIS_PASSWORD_FILE: missing }, 'REDIS_PASSWORD')).toThrow(
      new RegExp(`^REDIS_PASSWORD_FILE: .*${missing}`),
    );
  });

  it('throws when the file is empty', () => {
    const p = fileWith('');
    expect(() => readSecret({ REDIS_PASSWORD_FILE: p }, 'REDIS_PASSWORD')).toThrow(
      `REDIS_PASSWORD_FILE: ${p} is empty`,
    );
  });
});
