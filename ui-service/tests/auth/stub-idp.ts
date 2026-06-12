import { createServer, type Server } from 'http';
import type { AddressInfo } from 'net';
import { SignJWT, exportJWK, generateKeyPair } from 'jose';

export interface StubIdp {
  issuer: string;
  // Claims to embed in the next issued ID token (sub, email, groups, nonce…).
  setNextClaims(claims: Record<string, unknown>): void;
  lastTokenRequest(): URLSearchParams | undefined;
  close(): void;
}

export async function startStubIdp(): Promise<StubIdp> {
  const { publicKey, privateKey } = await generateKeyPair('RS256');
  const jwk = await exportJWK(publicKey);
  jwk.kid = 'stub-key';
  jwk.alg = 'RS256';
  jwk.use = 'sig';

  let nextClaims: Record<string, unknown> = {};
  let lastToken: URLSearchParams | undefined;
  let issuer = '';

  const server: Server = createServer(async (req, res) => {
    const url = new URL(req.url ?? '/', issuer);
    if (url.pathname === '/.well-known/openid-configuration') {
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({
        issuer,
        authorization_endpoint: `${issuer}/authorize`,
        token_endpoint: `${issuer}/token`,
        jwks_uri: `${issuer}/jwks`,
        end_session_endpoint: `${issuer}/logout`,
        response_types_supported: ['code'],
        grant_types_supported: ['authorization_code'],
        subject_types_supported: ['public'],
        id_token_signing_alg_values_supported: ['RS256'],
        token_endpoint_auth_methods_supported: ['client_secret_post', 'client_secret_basic'],
        code_challenge_methods_supported: ['S256'],
      }));
      return;
    }
    if (url.pathname === '/jwks') {
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ keys: [jwk] }));
      return;
    }
    if (url.pathname === '/token' && req.method === 'POST') {
      let body = '';
      for await (const chunk of req) body += chunk;
      lastToken = new URLSearchParams(body);
      let clientId = lastToken.get('client_id');
      if (!clientId && req.headers.authorization?.startsWith('Basic ')) {
        clientId = decodeURIComponent(
          Buffer.from(req.headers.authorization.slice(6), 'base64').toString().split(':')[0],
        );
      }
      const idToken = await new SignJWT({ ...nextClaims })
        .setProtectedHeader({ alg: 'RS256', kid: 'stub-key' })
        .setIssuer(issuer)
        .setAudience(clientId ?? 'continuo-ui')
        .setIssuedAt()
        .setExpirationTime('5m')
        .sign(privateKey);
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ access_token: 'stub-access-token', token_type: 'bearer', expires_in: 300, id_token: idToken }));
      return;
    }
    res.statusCode = 404;
    res.end();
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  issuer = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  return {
    issuer,
    setNextClaims: (c) => { nextClaims = c; },
    lastTokenRequest: () => lastToken,
    close: () => server.close(),
  };
}
