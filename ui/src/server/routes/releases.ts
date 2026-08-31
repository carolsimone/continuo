import { Router } from 'express';
import { rateLimit } from 'express-rate-limit';
import { ReleaseClient } from '../release-client';
import type { CommitAuthorResolver, ReleaseAuthor } from '../github/commit-author';

// The subset of a release list row enrichAuthors reads and writes. The list
// itself crosses the release-controller→ui boundary untyped; this pins the two
// provenance fields and the author it attaches.
type EnrichableRelease = { repo?: string; commit_sha?: string; author?: ReleaseAuthor };

// normalizeKey strips a leading s3://<bucket>/ so getLogObject receives a key.
function normalizeKey(raw: string): string {
  const m = raw.match(/^s3:\/\/[^/]+\/(.+)$/);
  return m ? m[1] : raw;
}

// enrichAuthors resolves the commit author for every release in place. Each
// distinct (repo, commit_sha) is looked up once; a lookup that fails leaves that
// release without an author rather than failing the whole list, so a GitHub
// outage degrades to a missing column, not a broken page.
async function enrichAuthors(releases: EnrichableRelease[], resolver: CommitAuthorResolver): Promise<void> {
  const distinct = new Map<string, { repo: string; sha: string }>();
  for (const r of releases) {
    if (r && r.repo && r.commit_sha) distinct.set(`${r.repo}@${r.commit_sha}`, { repo: r.repo, sha: r.commit_sha });
  }
  const resolved = new Map<string, ReleaseAuthor>();
  await Promise.all(
    [...distinct].map(async ([key, { repo, sha }]) => {
      try {
        const author = await resolver.resolve(repo, sha);
        if (author) resolved.set(key, author);
      } catch {
        // Leave this commit unresolved; other releases still get their author.
      }
    }),
  );
  for (const r of releases) {
    if (!r || !r.repo || !r.commit_sha) continue;
    const author = resolved.get(`${r.repo}@${r.commit_sha}`);
    if (author) r.author = author;
  }
}

export function createReleasesRouter(
  client: ReleaseClient,
  getLog: (key: string) => Promise<string>,
  authorResolver?: CommitAuthorResolver,
) {
  const router = Router();

  // Bound the list route only: it is the one that fans out to GitHub (one
  // getCommit per distinct commit), so an authenticated client must not be able
  // to hammer it. The other release routes are left unlimited — current-prod is
  // polled every 5s and would otherwise share this budget. The key is the
  // authenticated user (this router is mounted behind the /api auth guard), not
  // the client IP: behind an ingress with no `trust proxy` the IP is the
  // ingress socket, which would bucket every operator together.
  const listLimiter = rateLimit({
    windowMs: 60 * 1000,
    limit: 120,
    standardHeaders: true,
    legacyHeaders: false,
    keyGenerator: (req) => req.user?.userId ?? 'anon',
  });

  // GET /api/releases/log?key=  — registered before /:id so "log" is not treated as an id.
  router.get('/log', async (req, res) => {
    const key = (req.query.key as string) || '';
    if (!key) return res.status(400).json({ error: 'key query param is required' });
    try {
      const content = await getLog(normalizeKey(key));
      res.setHeader('Content-Type', 'text/plain');
      res.send(content);
    } catch {
      res.status(502).json({ error: 'Failed to fetch log from storage' });
    }
  });

  // GET /api/releases/current-prod
  router.get('/current-prod', async (_req, res) => {
    try {
      res.json(await client.getCurrentProd());
    } catch (err: any) {
      res.status(err.status || 502).json({ error: 'release-controller request failed' });
    }
  });

  // GET /api/releases?status=&limit=&cursor=
  router.get('/', listLimiter, async (req, res) => {
    const query: Record<string, string> = {};
    for (const k of ['status', 'limit', 'cursor']) {
      const v = req.query[k];
      if (typeof v === 'string' && v !== '') query[k] = v;
    }
    try {
      const data = await client.listReleases(query);
      if (authorResolver && Array.isArray(data?.releases)) {
        await enrichAuthors(data.releases, authorResolver);
      }
      res.json(data);
    } catch (err: any) {
      res.status(err.status || 502).json({ error: 'release-controller request failed' });
    }
  });

  // POST /api/releases/:id/retry-remediation — start another remediation round.
  // release-controller's status and body are passed through: 202 with the new
  // round, or 409 with a reason the page turns into "look at the proposal instead".
  router.post('/:id/retry-remediation', async (req, res) => {
    try {
      const { status, body } = await client.retryRemediation(req.params.id);
      res.status(status).json(body);
    } catch {
      res.status(502).json({ error: 'release-controller request failed' });
    }
  });

  // GET /api/releases/:id
  router.get('/:id', async (req, res) => {
    try {
      res.json(await client.getRelease(req.params.id));
    } catch (err: any) {
      res.status(err.status || 502).json({ error: 'release-controller request failed' });
    }
  });

  return router;
}
