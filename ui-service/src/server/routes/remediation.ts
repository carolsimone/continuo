import { Router } from 'express';
import * as grpc from '@grpc/grpc-js';
import type { RemediationClient } from '../remediation-client';
import type { PullRequestCreator } from '../github/pull-request-creator';

// normalizeKey strips a leading s3://<bucket>/ so getObject receives a plain key.
function normalizeKey(raw: string): string {
  const m = raw.match(/^s3:\/\/[^/]+\/(.+)$/);
  return m ? m[1] : raw;
}

// extractPrUrl extracts the first https://... URL from a gRPC error message,
// which the remediation service embeds when a PR already exists.
function extractPrUrl(message: string): string | undefined {
  const m = message.match(/https?:\/\/\S+/);
  return m ? m[0] : undefined;
}

export function createRemediationRouter(
  remediation: RemediationClient,
  prCreator: PullRequestCreator | undefined,
  getObject: (key: string) => Promise<string>,
): Router {
  const router = Router();

  // GET /api/remediation/proposals?status=&pr_state=&limit=
  router.get('/proposals', async (req, res) => {
    const params: Record<string, string> = {};
    for (const k of ['status', 'pr_state', 'limit']) {
      const v = req.query[k];
      if (typeof v === 'string' && v !== '') params[k] = v;
    }
    try {
      const result = await remediation.listProposals(params);
      res.json({ proposals: result.proposals ?? [] });
    } catch (err: any) {
      res.status(502).json({ error: 'remediation service request failed' });
    }
  });

  // GET /api/remediation/proposals/:id
  router.get('/proposals/:id', async (req, res) => {
    try {
      const proposal = await remediation.getProposal({ id: req.params.id });
      res.json(proposal);
    } catch (err: any) {
      if (err?.code === grpc.status.NOT_FOUND) {
        return res.status(404).json({ error: 'proposal not found' });
      }
      res.status(502).json({ error: 'remediation service request failed' });
    }
  });

  // POST /api/remediation/proposals/:id/pull-request
  // Operator gating is enforced automatically by the app-level requireApiAuth
  // middleware applied to all /api routes — no role check is needed here.
  router.post('/proposals/:id/pull-request', async (req, res) => {
    const id = req.params.id;

    // 503 when GitHub App credentials are not configured.
    if (!prCreator) {
      return res.status(503).json({ error: 'PR creation not configured' });
    }

    // Claim the proposal — guard against concurrent or duplicate PR creation.
    let claim: any;
    try {
      claim = await remediation.beginPullRequest({ id });
    } catch (err: any) {
      if (err?.code === grpc.status.NOT_FOUND) {
        return res.status(404).json({ error: 'proposal not found' });
      }
      if (err?.code === grpc.status.FAILED_PRECONDITION) {
        // The service embeds the existing PR URL in the error message.
        const message: string = err.details ?? err.message ?? '';
        const pr_url = extractPrUrl(message);
        return res.status(409).json({ error: message, pr_url });
      }
      return res.status(502).json({ error: 'remediation service request failed' });
    }

    // Fetch the proposed SQL content from S3.
    const content = await getObject(normalizeKey(claim.proposed_sql_uri));

    // Build the PR title and body.
    const nodeId = claim.node_id ?? id;
    const releaseId = claim.release_id ?? '';
    const title = `[remediation] fix ${nodeId} (release ${releaseId})`;

    let diffBlock = '';
    if (claim.diff_uri) {
      try {
        const diff = await getObject(normalizeKey(claim.diff_uri));
        diffBlock = `\n\n### Proposed diff\n\`\`\`diff\n${diff}\n\`\`\``;
      } catch {
        // Diff is best-effort — omit on failure.
      }
    }

    const body = [
      `## Automated remediation proposal`,
      ``,
      `**Node:** \`${nodeId}\``,
      `**Release:** \`${releaseId}\``,
      claim.error_signature ? `**Error signature:** ${claim.error_signature}` : '',
      claim.model ? `**Model:** ${claim.model}` : '',
      claim.confidence !== undefined ? `**Confidence:** ${claim.confidence}` : '',
      ``,
      claim.rationale ? `### Rationale\n${claim.rationale}` : '',
      diffBlock,
      ``,
      `---`,
      `*Proposed by the automated remediation agent — review before merge.*`,
      ``,
      `[View in Continuo UI](/?tab=remediation)`,
    ]
      .filter((line) => line !== null && line !== undefined)
      .join('\n')
      .trim();

    const commitMessage = title;

    // Open the pull request via the GitHub App.
    let pr: { url: string; number: number };
    try {
      pr = await prCreator.create({
        repo: claim.repo,
        baseBranch: 'main',
        headBranch: claim.branch,
        filePath: claim.file_path,
        content,
        commitMessage,
        title,
        body,
      });
    } catch {
      // Record the failure so the proposal transitions back to a retryable state.
      await remediation.failPullRequest({ id });
      return res.status(502).json({ error: 'failed to open pull request' });
    }

    // Record the opened PR against the proposal.
    await remediation.recordPullRequest({
      id,
      pr_url: pr.url,
      pr_number: pr.number,
      opened_by: (req as any).user?.userId ?? '',
    });

    res.json({ pr_url: pr.url, pr_number: pr.number });
  });

  return router;
}
