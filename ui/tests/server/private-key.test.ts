import { describe, it, expect } from 'vitest';
import crypto from 'crypto';
import { normalizePemPrivateKey, privateKeyCanSign } from '../../src/server/github/private-key';

// Generates a real RSA key pair per encoding-shape test so the assertion is
// "the normalized output actually signs", not just "the string looks PEM-shaped".
function generateKey(type: 'pkcs1' | 'pkcs8'): string {
  const { privateKey } = crypto.generateKeyPairSync('rsa', {
    modulusLength: 2048,
    privateKeyEncoding: {
      type,
      format: 'pem',
    },
    publicKeyEncoding: {
      type: 'spki',
      format: 'pem',
    },
  });
  return privateKey;
}

// Folds every newline in a PEM into a single space, mirroring what a quoted
// (non-block) YAML scalar does to a multiline value.
function spaceFold(pem: string): string {
  return pem.replace(/\n/g, ' ').trim();
}

function backslashNEscape(pem: string): string {
  return pem.trim().replace(/\n/g, '\\n');
}

function toCRLF(pem: string): string {
  return pem.replace(/\n/g, '\r\n');
}

describe('normalizePemPrivateKey', () => {
  describe('PKCS#1 (RSA PRIVATE KEY)', () => {
    const original = generateKey('pkcs1');

    it('passes an already-correct PEM through unchanged in meaning (idempotent)', () => {
      const normalized = normalizePemPrivateKey(original);
      expect(privateKeyCanSign(normalized)).toBe(true);
      expect(normalizePemPrivateKey(normalized)).toBe(normalized);
      expect(normalized.includes('RSA PRIVATE KEY')).toBe(true);
    });

    it('repairs a \\n-escaped key', () => {
      const normalized = normalizePemPrivateKey(backslashNEscape(original));
      expect(privateKeyCanSign(normalized)).toBe(true);
      expect(normalized).toContain('-----BEGIN RSA PRIVATE KEY-----\n');
      expect(normalized).toContain('-----END RSA PRIVATE KEY-----\n');
    });

    it('repairs a space-folded key (the YAML quoted-scalar incident shape)', () => {
      const folded = spaceFold(original);
      expect(folded.includes('\n')).toBe(false);
      const normalized = normalizePemPrivateKey(folded);
      expect(privateKeyCanSign(normalized)).toBe(true);
    });

    it('repairs a CRLF key', () => {
      const normalized = normalizePemPrivateKey(toCRLF(original));
      expect(privateKeyCanSign(normalized)).toBe(true);
    });

    it('repairs a mixed encoding (some real newlines, some \\n escapes, stray spaces)', () => {
      const lines = original.trim().split('\n');
      // Alternate separators between consecutive lines so the input carries
      // a real mix of encodings rather than one uniform substitution.
      let rebuilt = '';
      lines.forEach((line, i) => {
        rebuilt += line;
        if (i < lines.length - 1) {
          rebuilt += i % 3 === 0 ? '\\n' : i % 3 === 1 ? '\n' : ' \n ';
        }
      });
      const normalized = normalizePemPrivateKey(rebuilt);
      expect(privateKeyCanSign(normalized)).toBe(true);
    });

    it('preserves the RSA PRIVATE KEY label exactly (does not rewrite to PKCS#8)', () => {
      const normalized = normalizePemPrivateKey(spaceFold(original));
      expect(normalized).toMatch(/^-----BEGIN RSA PRIVATE KEY-----\n/);
      expect(normalized.trimEnd()).toMatch(/-----END RSA PRIVATE KEY-----$/);
      // Read the label back out of the envelope rather than asserting against a
      // literal PEM header, which a secret scanner reads as a committed key.
      expect(normalized.match(/-----BEGIN ([A-Z0-9 ]+)-----/)?.[1]).toBe('RSA PRIVATE KEY');
    });
  });

  describe('PKCS#8 (PRIVATE KEY)', () => {
    const original = generateKey('pkcs8');

    it('passes an already-correct PEM through unchanged in meaning (idempotent)', () => {
      const normalized = normalizePemPrivateKey(original);
      expect(privateKeyCanSign(normalized)).toBe(true);
      expect(normalizePemPrivateKey(normalized)).toBe(normalized);
    });

    it('repairs a space-folded key without relabeling it as PKCS#1', () => {
      const normalized = normalizePemPrivateKey(spaceFold(original));
      expect(privateKeyCanSign(normalized)).toBe(true);
      // Read the label back out of the envelope rather than asserting against a
      // literal PEM header, which a secret scanner reads as a committed key.
      expect(normalized.match(/-----BEGIN ([A-Z0-9 ]+)-----/)?.[1]).toBe('PRIVATE KEY');
      expect(normalized).not.toContain('RSA PRIVATE KEY');
    });

    it('repairs a \\n-escaped key', () => {
      const normalized = normalizePemPrivateKey(backslashNEscape(original));
      expect(privateKeyCanSign(normalized)).toBe(true);
    });
  });

  describe('garbage input', () => {
    it('returns input with no recognisable header/footer unchanged', () => {
      const garbage = 'not a key, just some text';
      expect(normalizePemPrivateKey(garbage)).toBe(garbage);
    });

    it('returns an empty string unchanged', () => {
      expect(normalizePemPrivateKey('')).toBe('');
    });

    it('returns a header/footer with empty body unchanged rather than fabricating a key', () => {
      const empty = '-----BEGIN RSA PRIVATE KEY-----\n-----END RSA PRIVATE KEY-----\n';
      expect(normalizePemPrivateKey(empty)).toBe(empty);
    });
  });
});

describe('privateKeyCanSign', () => {
  it('returns true for a valid, correctly formatted key', () => {
    expect(privateKeyCanSign(generateKey('pkcs1'))).toBe(true);
  });

  it('returns false for a space-folded key that was never normalized', () => {
    // This is the exact production failure: newlines folded to spaces. The
    // key material is intact but createSign cannot parse it in this form.
    expect(privateKeyCanSign(spaceFold(generateKey('pkcs1')))).toBe(false);
  });

  it('returns false for garbage', () => {
    expect(privateKeyCanSign('not a key')).toBe(false);
  });

  it('returns false for an empty string', () => {
    expect(privateKeyCanSign('')).toBe(false);
  });
});
