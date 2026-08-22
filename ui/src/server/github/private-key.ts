import crypto from 'crypto';

// Matches a PEM envelope: a BEGIN header, the base64 body, and a matching END
// footer carrying the same label (e.g. "RSA PRIVATE KEY" or "PRIVATE KEY").
// `[\s\S]*?` (rather than `.*?`) spans real newlines, since the body between
// header and footer may or may not already contain line breaks.
const PEM_ENVELOPE_RE = /-----BEGIN ([A-Z0-9 ]+)-----([\s\S]*?)-----END \1-----/;

const BASE64_LINE_LENGTH = 64;

/**
 * Reconstructs a well-formed PEM from a private key whose line breaks may have
 * arrived in any encoding: real newlines, literal `\n` escapes (two chars,
 * backslash + n), spaces, CRLF, or a mix of these. A YAML values file that
 * quotes a multiline scalar instead of using a literal block (`|`) is a common
 * source of the space-folded variant — the key material survives intact, but
 * every line break is collapsed into a single space, and nothing about the
 * YAML parse complains.
 *
 * The key material itself is never touched: only whitespace standing in for
 * line breaks is removed from the base64 body, which is then re-wrapped at
 * the standard 64 characters per line. The BEGIN/END label is preserved
 * verbatim so PKCS#1 ("RSA PRIVATE KEY") and PKCS#8 ("PRIVATE KEY") headers
 * are never rewritten into each other.
 *
 * A value with no recognisable BEGIN/END envelope is returned unchanged —
 * this function repairs line-break encoding, it does not guess at malformed
 * or truncated input. Startup validation (see `privateKeyCanSign`) is
 * responsible for rejecting whatever comes out the other end.
 */
export function normalizePemPrivateKey(raw: string): string {
  // Literal `\n` escapes (backslash + n, as opposed to a real newline byte)
  // are folded to real newlines first so the envelope regex and the
  // whitespace-stripping pass below both see them as line breaks rather than
  // as literal characters embedded in the base64 body.
  const withRealNewlines = raw.replace(/\\n/g, '\n');

  const match = PEM_ENVELOPE_RE.exec(withRealNewlines);
  if (!match) {
    return raw;
  }

  const label = match[1];
  const body = match[2].replace(/\s+/g, '');
  if (body === '') {
    return raw;
  }

  const lines: string[] = [];
  for (let i = 0; i < body.length; i += BASE64_LINE_LENGTH) {
    lines.push(body.slice(i, i + BASE64_LINE_LENGTH));
  }

  return `-----BEGIN ${label}-----\n${lines.join('\n')}\n-----END ${label}-----\n`;
}

/**
 * Verifies a private key can actually sign, without any network access.
 * A key that fails to parse or produce a signature here will fail identically
 * — and much less diagnosably — the first time GitHub App auth needs it.
 */
export function privateKeyCanSign(pemKey: string): boolean {
  try {
    const signer = crypto.createSign('RSA-SHA256');
    signer.update('continuo-github-app-private-key-startup-check');
    signer.end();
    signer.sign(pemKey);
    return true;
  } catch {
    return false;
  }
}
