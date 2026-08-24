import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { assertS3Config } from '../../src/server/s3';

// assertS3Config must fail closed: a UI booted without explicit S3
// credentials refuses to start instead of falling back to placeholder keys
// that break on the first log fetch.
describe('assertS3Config', () => {
  const saved: Record<string, string | undefined> = {};
  const KEYS = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'] as const;

  beforeEach(() => {
    for (const k of KEYS) saved[k] = process.env[k];
  });

  afterEach(() => {
    for (const k of KEYS) {
      if (saved[k] === undefined) delete process.env[k];
      else process.env[k] = saved[k];
    }
  });

  it('passes when both credential variables are set', () => {
    process.env.AWS_ACCESS_KEY_ID = 'minioadmin';
    process.env.AWS_SECRET_ACCESS_KEY = 'minioadmin';
    expect(() => assertS3Config()).not.toThrow();
  });

  it('throws when AWS_ACCESS_KEY_ID is missing', () => {
    delete process.env.AWS_ACCESS_KEY_ID;
    process.env.AWS_SECRET_ACCESS_KEY = 'minioadmin';
    expect(() => assertS3Config()).toThrow(/AWS_ACCESS_KEY_ID is required/);
  });

  it('throws when AWS_SECRET_ACCESS_KEY is blank', () => {
    process.env.AWS_ACCESS_KEY_ID = 'minioadmin';
    process.env.AWS_SECRET_ACCESS_KEY = '   ';
    expect(() => assertS3Config()).toThrow(/AWS_SECRET_ACCESS_KEY is required/);
  });
});
