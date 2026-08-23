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

// safeFailPullRequest releases a stuck 'opening' claim back to 'failed' so it
// can be retried immediately. This is a best-effort courtesy, not a
// requirement for correctness: the reconciler's opening sweep recovers any
// claim left in 'opening' on its own, independent of whether this call ever
// runs. claimedAt is empty when the caller never received one — a
// agent-remediation instance still on a pre-claimed_at protocol during a
// rolling upgrade sends an empty proto3 default — and the RPC rejects an
// empty claimed_at outright, so that case is skipped rather than attempted.
// Any other failure is logged and swallowed rather than propagated: this
// function runs from inside a catch block with no try of its own around it,
// so a rejection here would otherwise surface as an unhandled promise
// rejection and, under this service's default Node settings, crash the
// process.
async function safeFailPullRequest(
  remediation: RemediationClient,
  id: string,
  claimedAt: string | undefined,
): Promise<void> {
  if (!claimedAt) {
    console.warn(
      '[remediation] skipping failPullRequest for proposal %s: no claimed_at available (version skew) — the opening sweep will recover this claim',
      id,
    );
    return;
  }
  try {
    await remediation.failPullRequest({ id, claimed_at: claimedAt });
  } catch (err: any) {
    console.error(
      '[remediation] failPullRequest failed for proposal %s (status=%s): %s — the opening sweep will recover this claim',
      id,
      err?.code ?? 'unknown',
      err?.message ?? String(err),
    );
  }
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
      console.error(
        '[remediation] listProposals failed (status=%s): %s',
        err?.code ?? 'unknown',
        err?.message ?? String(err),
      );
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
      console.error(
        '[remediation] getProposal failed for proposal %s (status=%s): %s',
        req.params.id,
        err?.code ?? 'unknown',
        err?.message ?? String(err),
      );
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
      console.error(
        '[remediation] beginPullRequest failed for proposal %s (status=%s): %s',
        id,
        err?.code ?? 'unknown',
        err?.message ?? String(err),
      );
      return res.status(502).json({ error: 'remediation service request failed' });
    }

    // claimed_at is the pr_claimed_at value BeginPullRequest's CAS persisted
    // for this claim. Every failure path below must echo it back on
    // failPullRequest so the repository only resets this exact claim — never
    // a fresher one taken by someone else (a re-claim, or the reconciler's
    // opening sweep) since this request began.
    const claimedAt = claim.claimed_at;

    // Every file the proposal changes, in the order the agent produced them.
    // The list is empty when the claim came from a agent-remediation instance
    // that predates it — the same rolling-upgrade skew safeFailPullRequest
    // tolerates on claimed_at — so fall back to the claim's single-file
    // fields, which describe exactly the one file such a peer proposes. This
    // mirrors the synthesis the repository itself applies to a row stored
    // before the edits list existed.
    let edits: Array<{ path: string; content_uri: string; diff_uri: string }> = claim.edits ?? [];
    // The Go read path (editsOrLegacy in agent-remediation's proposal
    // repository) already guarantees every PRClaim carries a non-empty edits
    // list, synthesizing one from the single-file fields when the row has
    // none. So this branch is only reachable when talking to a
    // agent-remediation build that predates the edits field entirely — not a
    // routinely-exercised path against the current fleet.
    if (edits.length === 0 && claim.file_path && claim.proposed_sql_uri) {
      edits = [
        {
          path: claim.file_path,
          content_uri: claim.proposed_sql_uri,
          diff_uri: claim.diff_uri ?? '',
        },
      ];
    }
    // With neither a list nor a single-file description there is nothing to
    // commit at all, and a pull request built from it would change no files.
    // Release the claim and fail loudly rather than open one.
    if (edits.length === 0) {
      console.error(
        '[remediation] proposal %s was claimed with no file edits and no single file to fall back to — refusing to open an empty pull request',
        id,
      );
      await safeFailPullRequest(remediation, id, claimedAt);
      return res.status(502).json({ error: 'proposal carries no file edits' });
    }

    // Fetch the proposed content of every edited file from S3.
    let files: Array<{ path: string; content: string }>;
    try {
      files = await Promise.all(
        edits.map(async (edit) => ({
          path: edit.path,
          content: await getObject(normalizeKey(edit.content_uri)),
        })),
      );
    } catch (err) {
      console.error(
        '[remediation] failed to fetch proposed file content for proposal %s: %s',
        id,
        err instanceof Error ? err.message : String(err),
      );
      await safeFailPullRequest(remediation, id, claimedAt);
      return res.status(502).json({ error: 'failed to fetch proposed file content from S3' });
    }

    // Build the PR title and body.
    const nodeId = claim.node_id ?? id;
    const releaseId = claim.release_id ?? '';
    const title = `[remediation] fix ${nodeId} (release ${releaseId})`;

    // A single inline preview keeps the body readable on a multi-file proposal;
    // the rest of the diffs are one click away in the pull request itself.
    let diffBlock = '';
    if (edits[0].diff_uri) {
      try {
        const diff = await getObject(normalizeKey(edits[0].diff_uri));
        diffBlock = `\n\n### Proposed diff\n\`\`\`diff\n${diff}\n\`\`\``;
      } catch (err) {
        // Diff is best-effort — omit on failure, but still log why so a
        // missing diff in a PR body is diagnosable without a repro.
        console.warn(
          '[remediation] failed to fetch proposed diff for proposal %s (continuing without it): %s',
          id,
          err instanceof Error ? err.message : String(err),
        );
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
      `### Files changed`,
      ...files.map((file) => `- \`${file.path}\``),
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
        // Branch from the commit the proposal was generated against, so the diff
        // is exactly the proposed change and GitHub flags a conflict if the file
        // drifted on main since — rather than silently reverting that drift.
        baseSha: claim.commit_sha,
        headBranch: claim.branch,
        files,
        commitMessage,
        title,
        body,
      });
    } catch (err) {
      // Log the proposal id plus the error's status/message (Octokit errors
      // carry both) so a GitHub-side rejection is diagnosable without a
      // repro. Never log the full error object here: an Octokit
      // RequestError can carry the outgoing request, including the
      // Authorization header used to authenticate as the GitHub App.
      const e = err as { status?: number; message?: string };
      console.error(
        '[remediation] pull request creation failed for proposal %s (status=%s): %s',
        id,
        e?.status ?? 'unknown',
        e?.message ?? String(err),
      );
      // Record the failure so the proposal transitions back to a retryable state.
      await safeFailPullRequest(remediation, id, claimedAt);
      return res.status(502).json({ error: 'failed to open pull request' });
    }

    // Record the opened PR against the proposal. This is best-effort bookkeeping:
    // the PR already exists on GitHub at this point, so a recording failure must
    // not prevent the client from receiving the PR link. Log loudly on failure so
    // the stuck row is diagnosable; the reconciler's opening sweep recovers it
    // automatically on its next pass, so no manual intervention is required.
    try {
      await remediation.recordPullRequest({
        id,
        pr_url: pr.url,
        pr_number: pr.number,
        opened_by: (req as any).user?.userId ?? '',
      });
    } catch (err) {
      console.error(
        '[remediation] recordPullRequest failed for proposal %s (PR %s); proposal may remain in pr_state=opening — the reconciler opening sweep recovers it automatically (see REMEDIATION_PR_OPENING_GRACE_PERIOD):',
        id,
        pr.url,
        err,
      );
    }

    res.json({ pr_url: pr.url, pr_number: pr.number });
  });

  return router;
}
