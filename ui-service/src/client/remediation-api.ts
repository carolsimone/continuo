import { ProposalDTO } from './types';

export interface CreatePullRequestResult {
  pr_url: string;
  pr_number: number;
}

export interface CreatePullRequestError {
  status: number;
  pr_url?: string;
  message: string;
}

export function fetchProposals(status?: string): Promise<ProposalDTO[]> {
  const qs = new URLSearchParams();
  if (status) qs.set('status', status);
  const url = status
    ? `/api/remediation/proposals?${qs.toString()}`
    : '/api/remediation/proposals';
  return fetch(url)
    .then(r => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
    .then((data: { proposals: ProposalDTO[] }) => data.proposals);
}

export function fetchProposal(id: string): Promise<ProposalDTO> {
  return fetch(`/api/remediation/proposals/${encodeURIComponent(id)}`)
    .then(r => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))));
}

export function createPullRequest(id: string): Promise<CreatePullRequestResult> {
  return fetch(`/api/remediation/proposals/${encodeURIComponent(id)}/pull-request`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
  }).then(async r => {
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      const err: CreatePullRequestError = {
        status: r.status,
        message: body.error || body.message || `HTTP ${r.status}`,
        ...(body.pr_url ? { pr_url: body.pr_url } : {}),
      };
      throw err;
    }
    return r.json() as Promise<CreatePullRequestResult>;
  });
}
