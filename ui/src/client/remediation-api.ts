import { ProposalDTO, ServicesResponse } from './types';

// One pull request the server opened (or already had open) for one owning
// service, as returned by POST .../pull-request.
export interface CreatePullRequestResultEntry {
  service: string;
  pr_url: string;
  pr_number: number;
}

// One owning-service group the server failed to open a pull request for.
export interface CreatePullRequestErrorEntry {
  service: string;
  error: string;
}

// CreatePullRequestResponse is the body of a 200 (every service succeeded)
// or 207 (some, but not all, succeeded) response.
export interface CreatePullRequestResponse {
  pull_requests: CreatePullRequestResultEntry[];
  errors: CreatePullRequestErrorEntry[];
}

export interface CreatePullRequestError {
  status: number;
  pr_url?: string;
  message: string;
  // errors carries the per-service failures when the server's response body
  // was itself the {pull_requests, errors} shape (the 502-nothing-succeeded
  // case) rather than a single {error} message.
  errors?: CreatePullRequestErrorEntry[];
}

export function fetchProposals(filter: { status?: string; pr_state?: string; service?: string } = {}): Promise<ProposalDTO[]> {
  const qs = new URLSearchParams();
  if (filter.status) qs.set('status', filter.status);
  if (filter.pr_state) qs.set('pr_state', filter.pr_state);
  if (filter.service) qs.set('service', filter.service);
  const query = qs.toString();
  const url = query
    ? `/api/remediation/proposals?${query}`
    : '/api/remediation/proposals';
  return fetch(url)
    .then(r => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
    .then((data: { proposals: ProposalDTO[] }) => data.proposals);
}

export function fetchProposal(id: string): Promise<ProposalDTO> {
  return fetch(`/api/remediation/proposals/${encodeURIComponent(id)}`)
    .then(r => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))));
}

// fetchNodeServices returns the distinct active service names for the
// Remediation tab's Service filter.
export function fetchNodeServices(): Promise<string[]> {
  return fetch('/api/nodes/services')
    .then(r => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
    .then((data: ServicesResponse) => data.services ?? []);
}

// createPullRequest opens one pull request per owning service the proposal
// still needs one for. r.ok covers both 200 (every service succeeded) and
// 207 (a partial success) — both resolve with the full {pull_requests,
// errors} body so the caller can show whichever services still failed. Only
// a response with no successes at all (503 unconfigured, 404 not found, or
// the 502 issued when every service failed) rejects.
export function createPullRequest(id: string): Promise<CreatePullRequestResponse> {
  return fetch(`/api/remediation/proposals/${encodeURIComponent(id)}/pull-request`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
  }).then(async r => {
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      const combinedFromErrors = Array.isArray(body.errors) && body.errors.length > 0
        ? body.errors.map((e: CreatePullRequestErrorEntry) => `${e.service ? `${e.service}: ` : ''}${e.error}`).join('; ')
        : undefined;
      const err: CreatePullRequestError = {
        status: r.status,
        message: body.error || combinedFromErrors || body.message || `HTTP ${r.status}`,
        ...(body.pr_url ? { pr_url: body.pr_url } : {}),
        ...(Array.isArray(body.errors) ? { errors: body.errors } : {}),
      };
      throw err;
    }
    return r.json() as Promise<CreatePullRequestResponse>;
  });
}
