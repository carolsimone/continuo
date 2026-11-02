import { describe, it, expect } from 'vitest';
import { readdirSync, readFileSync } from 'fs';
import path from 'path';

// The closed list of credential variables. Each is read only through
// readSecret, so the <NAME>_FILE form works everywhere the variable does.
// pkg/deploycheck pins it to the Go services' list.
const CREDENTIAL_VARS = [
  'POSTGRES_PASSWORD',
  'REDIS_PASSWORD',
  'NEO4J_PASSWORD',
  'AWS_ACCESS_KEY_ID',
  'AWS_SECRET_ACCESS_KEY',
  'LLM_API_KEY',
  'GITHUB_TOKEN',
  'GITHUB_APP_PRIVATE_KEY',
  'AUTH_OIDC_CLIENT_SECRET',
];

const SERVER_DIR = path.resolve(__dirname, '../../src/server');

function tsFiles(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) return tsFiles(p);
    return e.name.endsWith('.ts') ? [p] : [];
  });
}

describe('credential reads', () => {
  it('reads credentials only through readSecret', () => {
    const names = CREDENTIAL_VARS.join('|');
    const direct = new RegExp(`\\benv\\s*(?:\\.\\s*(?:${names})\\b|\\[\\s*['"\`](?:${names})['"\`]\\s*\\])`);
    const files = tsFiles(SERVER_DIR).filter((f) => path.basename(f) !== 'secrets.ts');
    expect(files.length).toBeGreaterThan(0);
    const offenders = files
      .flatMap((f) =>
        readFileSync(f, 'utf8')
          .split('\n')
          .map((line, i) => ({ f, line, n: i + 1 }))
          .filter(({ line }) => direct.test(line))
          .map(({ f: file, line, n }) => `${path.relative(SERVER_DIR, file)}:${n}: ${line.trim()}`),
      );
    expect(offenders).toEqual([]);
  });
});
