import { Router } from 'express';
import * as grpc from '@grpc/grpc-js';
import type { RemediationClient } from '../clients/remediation-client';
import {
  FileChangedOnBaseBranchError,
  type CreatedPullRequest,
  type PullRequestCreator,
  type RefusedReviewers,
} from '../github/pull-request-creator';
import { classifyOwner, splitOwners } from '../github/reviewers';
import { buildPullRequestBody, type OwnerReview, type PullRequestOwner } from './pr-body';

// normalizeKey strips a leading s3://<bucket>/ so getObject receives a plain key.
function normalizeKey(raw: string): string {
  const m = raw.match(/^s3:\/\/[^/]+\/(.+)$/);
  return m ? m[1] : raw;
}

// extractPrUrl extracts the first https://... URL from a gRPC error message,
// which agent-remediation embeds when a PR already exists.
function extractPrUrl(message: string): string | undefined {
  const m = message.match(/https?:\/\/\S+/);
  return m ? m[0] : undefined;
}

// extractPrNumber reads the PR number off the end of a GitHub pull-request
// URL (".../pull/42"), the only place the already-open error carries it.
function extractPrNumber(url: string | undefined): number {
  if (!url) return 0;
  const m = url.match(/\/pull\/(\d+)/);
  return m ? Number(m[1]) : 0;
}

// changedOnBaseBranchText tells the operator why no pull request was opened
// for a fix whose files changed on the base branch since the release it was
// made for, and what to do: "a", "a and b", "a, b and c" changed.
function changedOnBaseBranchText(err: FileChangedOnBaseBranchError): string {
  const { paths, baseBranch } = err;
  const named = paths.length === 1 ? paths[0] : `${paths.slice(0, -1).join(', ')} and ${paths[paths.length - 1]}`;
  const change = paths.length === 1 ? 'that change' : 'those changes';
  return (
    `${named} changed on ${baseBranch} since the release this fix was made for, so the pull request would overwrite ${change}. ` +
    `Check ${baseBranch}: the failure may already be fixed; if it still fails, the next release of the project is remediated again.`
  );
}

// safeFailPullRequest releases a stuck 'opening' claim for one project back
// to 'failed' so it can be retried immediately. This is a best-effort
// courtesy, not a requirement for correctness: the reconciler's opening
// sweep recovers any claim left in 'opening' on its own, independent of
// whether this call ever runs. claimedAt is empty when the caller never
// received one — a agent-remediation instance still on a pre-claimed_at
// protocol during a rolling upgrade sends an empty proto3 default — and the
// RPC rejects an empty claimed_at outright, so that case is skipped rather
// than attempted. Any other failure is logged and swallowed rather than
// propagated: this function runs from inside a catch block with no try of
// its own around it, so a rejection here would otherwise surface as an
// unhandled promise rejection and, under this component's default Node
// settings, crash the process.
async function safeFailPullRequest(
  remediation: RemediationClient,
  id: string,
  claimedAt: string | undefined,
  project: string,
): Promise<void> {
  if (!claimedAt) {
    console.warn(
      '[remediation] skipping failPullRequest for fix %s project %s: no claimed_at available (version skew) — the opening sweep will recover this claim',
      id,
      project,
    );
    return;
  }
  try {
    await remediation.failPullRequest({ id, claimed_at: claimedAt, project });
  } catch (err: any) {
    console.error(
      '[remediation] failPullRequest failed for fix %s project %s (status=%s): %s — the opening sweep will recover this claim',
      id,
      project,
      err?.code ?? 'unknown',
      err?.message ?? String(err),
    );
  }
}

// A pull request that exists after this request completes — either just
// opened, or already open from an earlier attempt (FAILED_PRECONDITION).
interface PullRequestResult {
  project: string;
  pr_url: string;
  pr_number: number;
}

// One owning-project group that failed to produce a pull request.
interface PullRequestError {
  project: string;
  error: string;
}

// One owning-project group whose pull request is open but whose owners could
// not be asked to review it.
interface PullRequestWarning {
  project: string;
  warning: string;
}

// An approver as BeginPullRequest sends it: the owner of one fixed node, empty
// when the node has none.
interface Approver {
  node_id: string;
  owner: string;
}

// ownersOf lists each approver's owner against its node for the body of a pull
// request in a repository that repoOwner owns, with what became of its review:
// requested when the owner is a GitHub user or a team of repoOwner (refused
// when it is one of those GitHub refused, by case-blind login or slug), and
// only listed otherwise.
function ownersOf(approvers: Approver[], repoOwner: string, refused: RefusedReviewers): PullRequestOwner[] {
  const refusedUsers = new Set(refused.users.map((u) => u.toLowerCase()));
  const refusedTeams = new Set(refused.teams.map((t) => t.toLowerCase()));
  return approvers.map((a) => {
    const c = classifyOwner(a.owner, repoOwner);
    let review: OwnerReview = 'listed';
    if (c?.kind === 'user') review = refusedUsers.has(c.name.toLowerCase()) ? 'refused' : 'requested';
    if (c?.kind === 'team') review = refusedTeams.has(c.name.toLowerCase()) ? 'refused' : 'requested';
    return { nodeId: a.node_id, owner: a.owner, review };
  });
}

export function createRemediationRouter(
  remediation: RemediationClient,
  prCreator: PullRequestCreator | undefined,
  getObject: (key: string) => Promise<string>,
  uiUrl?: string,
): Router {
  const router = Router();

  // GET /api/remediation/fixes?status=&pr_state=&limit=&project=
  // `project` narrows to fixes whose project set includes it (forwarded to
  // ListFixes, which matches when `project` is among a fix's projects).
  router.get('/fixes', async (req, res) => {
    const params: Record<string, string> = {};
    for (const k of ['status', 'pr_state', 'limit', 'project']) {
      const v = req.query[k];
      if (typeof v === 'string' && v !== '') params[k] = v;
    }
    try {
      const result = await remediation.listFixes(params);
      res.json({ fixes: result.fixes ?? [] });
    } catch (err: any) {
      console.error(
        '[remediation] listFixes failed (status=%s): %s',
        err?.code ?? 'unknown',
        err?.message ?? String(err),
      );
      res.status(502).json({ error: 'agent-remediation request failed' });
    }
  });

  // GET /api/remediation/fixes/:id
  router.get('/fixes/:id', async (req, res) => {
    try {
      const fix = await remediation.getFix({ id: req.params.id });
      res.json(fix);
    } catch (err: any) {
      if (err?.code === grpc.status.NOT_FOUND) {
        return res.status(404).json({ error: 'fix not found' });
      }
      console.error(
        '[remediation] getFix failed for fix %s (status=%s): %s',
        req.params.id,
        err?.code ?? 'unknown',
        err?.message ?? String(err),
      );
      res.status(502).json({ error: 'agent-remediation request failed' });
    }
  });

  // openPullRequestForProject drives BeginPullRequest -> S3 fetch -> GitHub
  // create -> RecordPullRequest for one owning-project group of the
  // fix. It mirrors the single-shot flow this route used before the
  // per-project split, scoped to one claim, and never throws: every failure
  // mode resolves to a PullRequestError so the caller can keep looping over
  // the remaining projects instead of losing an already-created PR.
  async function openPullRequestForProject(
    id: string,
    project: string,
    openedBy: string,
    creator: PullRequestCreator,
  ): Promise<
    { ok: true; value: PullRequestResult; warnings: PullRequestWarning[] } | { ok: false; error: PullRequestError }
  > {
    // Claim this project's group — guard against concurrent or duplicate PR creation.
    let claim: any;
    try {
      claim = await remediation.beginPullRequest({ id, project });
    } catch (err: any) {
      if (err?.code === grpc.status.FAILED_PRECONDITION) {
        // agent-remediation maps three distinct causes to FAILED_PRECONDITION:
        // this project already has a pull request (the message embeds its
        // URL), the fix has no real source to change, or the claim raced
        // past 'proposed' (a release check reached a verdict, or the
        // attempt budget was exhausted) since pr_projects was read. Only the
        // first is a success — skip creating a duplicate and report the
        // existing PR instead of losing it from the response. The other two
        // carry no URL and are genuine per-project failures, not an
        // already-open PR; reporting them as a success would fabricate an
        // empty-url entry and silently mask the real failure from the
        // 200/207/502 accounting.
        const message: string = err.details ?? err.message ?? '';
        const pr_url = extractPrUrl(message);
        if (pr_url) {
          return { ok: true, value: { project, pr_url, pr_number: extractPrNumber(pr_url) }, warnings: [] };
        }
        console.error(
          '[remediation] beginPullRequest refused for fix %s project %s: %s',
          id,
          project,
          message,
        );
        return { ok: false, error: { project, error: message } };
      }
      console.error(
        '[remediation] beginPullRequest failed for fix %s project %s (status=%s): %s',
        id,
        project,
        err?.code ?? 'unknown',
        err?.message ?? String(err),
      );
      return { ok: false, error: { project, error: 'agent-remediation request failed' } };
    }

    // claimed_at is the pr_claimed_at value BeginPullRequest's CAS persisted
    // for this claim. Every failure path below must echo it back on
    // failPullRequest so the repository only resets this exact claim — never
    // a fresher one taken by someone else (a re-claim, or the reconciler's
    // opening sweep) since this request began.
    const claimedAt = claim.claimed_at;

    // Every file this project's claim changes, in the order the agent
    // produced them. The list is empty when the claim came from an
    // agent-remediation instance that predates it — the same rolling-upgrade
    // skew safeFailPullRequest tolerates on claimed_at — so fall back to the
    // claim's single-file fields, which describe exactly the one file such a
    // peer proposes. This mirrors the synthesis the repository itself
    // applies to a row stored before the edits list existed.
    // An edit with renamed_from moves its file: the pull request deletes that
    // path and writes path. The loader keeps proto3 defaults, so an edit in
    // place carries renamed_from ''.
    let edits: Array<{
      path: string;
      content_uri: string;
      diff_uri: string;
      target_node_id?: string;
      renamed_from?: string;
    }> = claim.edits ?? [];
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
        '[remediation] fix %s project %s was claimed with no file edits and no single file to fall back to — refusing to open an empty pull request',
        id,
        project,
      );
      await safeFailPullRequest(remediation, id, claimedAt, project);
      return { ok: false, error: { project, error: 'fix carries no file edits' } };
    }

    // Fetch the proposed content of every edited file from S3.
    let files: Array<{ path: string; content: string; target_node_id?: string; renamed_from?: string }>;
    try {
      files = await Promise.all(
        edits.map(async (edit) => ({
          path: edit.path,
          target_node_id: edit.target_node_id,
          ...(edit.renamed_from ? { renamed_from: edit.renamed_from } : {}),
          content: await getObject(normalizeKey(edit.content_uri)),
        })),
      );
    } catch (err) {
      console.error(
        '[remediation] failed to fetch proposed file content for fix %s project %s: %s',
        id,
        project,
        err instanceof Error ? err.message : String(err),
      );
      await safeFailPullRequest(remediation, id, claimedAt, project);
      return { ok: false, error: { project, error: 'failed to fetch proposed file content from S3' } };
    }

    // Build the PR title and body. A batched fix resolves several
    // failing nodes under one attempt, so the title names how many rather
    // than picking one; a single-node fix (or a legacy row with no
    // resolved_node_ids) still names that one node exactly as before. A
    // fix split across several owning projects suffixes each project's
    // title with its name so the PR list isn't several identical titles;
    // the legacy whole-fix group (project === '') carries no suffix,
    // keeping today's exact title unchanged.
    const nodeIds: string[] = (claim.resolved_node_ids && claim.resolved_node_ids.length > 0)
      ? claim.resolved_node_ids
      : [claim.node_id ?? id];
    const releaseId = claim.release_id ?? '';
    let title = nodeIds.length === 1
      ? `[remediation] fix ${nodeIds[0]} (release ${releaseId})`
      : `[remediation] fix ${nodeIds.length} nodes (release ${releaseId})`;
    if (project) title += ` (${project})`;

    // A single inline preview keeps the body readable on a multi-file fix;
    // the rest of the diffs are one click away in the pull request itself.
    let diff: string | undefined;
    if (edits[0].diff_uri) {
      try {
        diff = await getObject(normalizeKey(edits[0].diff_uri));
      } catch (err) {
        // Diff is best-effort — omit on failure, but still log why so a
        // missing diff in a PR body is diagnosable without a repro.
        console.warn(
          '[remediation] failed to fetch proposed diff for fix %s project %s (continuing without it): %s',
          id,
          project,
          err instanceof Error ? err.message : String(err),
        );
      }
    }

    // The owners of the fixed nodes are asked to review the pull request. A
    // claim of a fix that records no owners (a row with no remediation id)
    // has no approvers, so it asks nobody and its body names no owners.
    // A team is asked only when its org owns the repository (claim.repo is
    // "owner/name"); another org's team is listed.
    const approvers: Approver[] = claim.approvers ?? [];
    const repoOwner = (claim.repo ?? '').split('/')[0] ?? '';
    const { users: reviewers, teams: teamReviewers } = splitOwners(approvers.map((a) => a.owner), repoOwner);

    // The body names each owner as asked, so it is built again, with the
    // owners GitHub refused marked, if GitHub turns any of them down.
    const bodyFor = (refused: RefusedReviewers) =>
      buildPullRequestBody({
        nodeIds,
        releaseId,
        errorSignature: claim.error_signature,
        model: claim.model,
        confidence: claim.confidence,
        files,
        rationale: claim.rationale,
        diff,
        owners: ownersOf(approvers, repoOwner, refused),
        uiUrl,
      });
    const body = bodyFor({ users: [], teams: [] });

    const commitMessage = title;

    // Open the pull request via the GitHub App.
    let pr: CreatedPullRequest;
    try {
      pr = await creator.create({
        repo: claim.repo,
        baseBranch: 'main',
        // The commit the fix was generated against. The branch is cut
        // from main's head, and the creator refuses (FileChangedOnBaseBranchError)
        // when a file the fix edits is no longer the same on main, rather
        // than overwrite main's newer version of it.
        baseSha: claim.commit_sha,
        headBranch: claim.branch,
        files,
        commitMessage,
        title,
        body,
        reviewers,
        teamReviewers,
        bodyWithRefused: bodyFor,
      });
    } catch (err) {
      // A file the fix edits changed on main since the release: nothing was
      // written to the repository. The claim is released like any other
      // failure, and the operator is told which files changed and what to do.
      if (err instanceof FileChangedOnBaseBranchError) {
        console.warn(
          '[remediation] pull request not opened for fix %s project %s: %s',
          id,
          project,
          err.message,
        );
        await safeFailPullRequest(remediation, id, claimedAt, project);
        return { ok: false, error: { project, error: changedOnBaseBranchText(err) } };
      }
      // Log the fix id plus the error's status/message (Octokit errors
      // carry both) so a GitHub-side rejection is diagnosable without a
      // repro. Never log the full error object here: an Octokit
      // RequestError can carry the outgoing request, including the
      // Authorization header used to authenticate as the GitHub App.
      const e = err as { status?: number; message?: string };
      console.error(
        '[remediation] pull request creation failed for fix %s project %s (status=%s): %s',
        id,
        project,
        e?.status ?? 'unknown',
        e?.message ?? String(err),
      );
      // Record the failure so this project's group transitions back to a retryable state.
      await safeFailPullRequest(remediation, id, claimedAt, project);
      return { ok: false, error: { project, error: 'failed to open pull request' } };
    }

    // Record the opened PR against this project's group. This is best-effort
    // bookkeeping: the PR already exists on GitHub at this point, so a
    // recording failure must not prevent the client from receiving the PR
    // link. Log loudly on failure so the stuck row is diagnosable; the
    // reconciler's opening sweep recovers it automatically on its next pass,
    // so no manual intervention is required.
    try {
      await remediation.recordPullRequest({
        id,
        pr_url: pr.url,
        pr_number: pr.number,
        opened_by: openedBy,
        project,
      });
    } catch (err) {
      console.error(
        '[remediation] recordPullRequest failed for fix %s project %s (PR %s); fix may remain in pr_state=opening — the reconciler opening sweep recovers it automatically (see REMEDIATION_PR_OPENING_GRACE_PERIOD):',
        id,
        project,
        pr.url,
        err,
      );
    }

    // The pull request is open and recorded even when GitHub refused to ask
    // some owners; each refusal is logged and reported beside the pull request.
    const warnings: PullRequestWarning[] = [];
    for (const warning of pr.warnings ?? []) {
      console.warn('[remediation] reviewer request refused for fix %s project %s: %s', id, project, warning);
      warnings.push({ project, warning });
    }
    return { ok: true, value: { project, pr_url: pr.url, pr_number: pr.number }, warnings };
  }

  // POST /api/remediation/fixes/:id/pull-request
  // Operator gating is enforced automatically by the app-level requireApiAuth
  // middleware applied to all /api routes — no role check is needed here.
  //
  // A fix opens one pull request per owning project — pr_projects names
  // the sorted groups ([""] for a legacy, unsplit fix). Every group is
  // attempted; a group that already has a pull request is skipped and listed
  // as-is, and a group that fails is reported in `errors` without losing the
  // groups that already succeeded. The response is 200 when every group
  // succeeded, 207 when some but not all did, and 502 only when none did. A
  // group whose pull request opened but whose owners GitHub would not take as
  // reviewers is a success, listed in `warnings` with the reason.
  router.post('/fixes/:id/pull-request', async (req, res) => {
    const id = req.params.id;

    // 503 when GitHub App credentials are not configured.
    if (!prCreator) {
      return res.status(503).json({ error: 'PR creation not configured' });
    }
    const creator = prCreator;

    // Fetch the fix first to learn which owning-project groups its pull
    // requests split into.
    let fix: any;
    try {
      fix = await remediation.getFix({ id });
    } catch (err: any) {
      if (err?.code === grpc.status.NOT_FOUND) {
        return res.status(404).json({ error: 'fix not found' });
      }
      console.error(
        '[remediation] getFix failed for fix %s (status=%s): %s',
        id,
        err?.code ?? 'unknown',
        err?.message ?? String(err),
      );
      return res.status(502).json({ error: 'agent-remediation request failed' });
    }

    // pr_projects is always non-empty on the wire — [""] for a legacy,
    // unsplit fix — but tolerate an absent/empty field defensively
    // against an older peer that predates it.
    const projects: string[] = [...(fix.pr_projects && fix.pr_projects.length > 0 ? fix.pr_projects : [''])].sort();
    const openedBy = (req as any).user?.userId ?? '';

    const pull_requests: PullRequestResult[] = [];
    const errors: PullRequestError[] = [];
    const warnings: PullRequestWarning[] = [];
    for (const project of projects) {
      const result = await openPullRequestForProject(id, project, openedBy, creator);
      if (result.ok) {
        pull_requests.push(result.value);
        warnings.push(...result.warnings);
      } else {
        errors.push(result.error);
      }
    }

    if (pull_requests.length === 0) {
      return res.status(502).json({ pull_requests, errors, warnings });
    }
    if (errors.length > 0) {
      return res.status(207).json({ pull_requests, errors, warnings });
    }
    return res.status(200).json({ pull_requests, errors, warnings });
  });

  return router;
}
