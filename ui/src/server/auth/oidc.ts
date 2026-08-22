import * as oidc from 'openid-client';
import type { OidcAuthConfig } from './config';

export interface PendingAuth {
  state: string;
  nonce: string;
  codeVerifier: string;
  returnTo: string;
}

export interface IdentityClaims {
  userId: string; // "<issuer-host>|<sub>" — stable across IdPs
  email: string;
  name: string;
  claims: Record<string, unknown>;
}

export interface OidcFlow {
  buildLoginRedirect(returnTo: string): Promise<{ url: string; pending: PendingAuth }>;
  handleCallback(currentUrl: URL, pending: PendingAuth): Promise<IdentityClaims>;
  endSessionUrl(postLogoutRedirect: string): string | null;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// Issuer discovery with bounded retries; on exhaustion the caller exits and the
// platform restart policy takes over. http:// issuers (Dex in e2e, the unit-test
// stub) need openid-client's explicit insecure opt-in — warned loudly.
export async function discoverOidc(
  cfg: OidcAuthConfig,
  opts: { maxAttempts?: number; backoffMs?: number } = {},
): Promise<OidcFlow> {
  const { maxAttempts = 5, backoffMs = 2000 } = opts;
  const issuer = new URL(cfg.issuerUrl);
  const insecure = issuer.protocol === 'http:';
  if (insecure) {
    console.warn(`AUTH: issuer ${cfg.issuerUrl} is plain http — acceptable only for local development and e2e`);
  }

  let config: oidc.Configuration | undefined;
  let lastErr: unknown;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      config = await oidc.discovery(
        issuer,
        cfg.clientId,
        cfg.clientSecret,
        undefined,
        insecure ? { execute: [oidc.allowInsecureRequests] } : undefined,
      );
      break;
    } catch (err) {
      lastErr = err;
      console.error(`AUTH: OIDC discovery attempt ${attempt}/${maxAttempts} failed:`, err);
      if (attempt < maxAttempts) await sleep(backoffMs * attempt);
    }
  }
  if (!config) {
    throw new Error(`OIDC discovery failed after ${maxAttempts} attempts: ${String(lastErr)}`);
  }
  const conf = config;
  const redirectUri = `${cfg.publicUrl}/auth/callback`;

  return {
    async buildLoginRedirect(returnTo: string) {
      const codeVerifier = oidc.randomPKCECodeVerifier();
      const codeChallenge = await oidc.calculatePKCECodeChallenge(codeVerifier);
      const state = oidc.randomState();
      const nonce = oidc.randomNonce();
      const url = oidc.buildAuthorizationUrl(conf, {
        redirect_uri: redirectUri,
        scope: cfg.scopes,
        state,
        nonce,
        code_challenge: codeChallenge,
        code_challenge_method: 'S256',
      });
      return { url: url.href, pending: { state, nonce, codeVerifier, returnTo } };
    },

    async handleCallback(currentUrl: URL, pending: PendingAuth) {
      const tokens = await oidc.authorizationCodeGrant(conf, currentUrl, {
        pkceCodeVerifier: pending.codeVerifier,
        expectedState: pending.state,
        expectedNonce: pending.nonce,
      });
      const claims = tokens.claims();
      if (!claims) throw new Error('IdP returned no ID token claims');
      const email = typeof claims.email === 'string' ? claims.email : '';
      const name = typeof claims.name === 'string' ? claims.name : email || String(claims.sub);
      return {
        userId: `${issuer.host}|${String(claims.sub)}`,
        email,
        name,
        claims: claims as Record<string, unknown>,
      };
    },

    endSessionUrl(postLogoutRedirect: string) {
      if (!conf.serverMetadata().end_session_endpoint) return null;
      return oidc.buildEndSessionUrl(conf, { post_logout_redirect_uri: postLogoutRedirect }).href;
    },
  };
}
